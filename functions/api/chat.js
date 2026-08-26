export async function onRequest({ request, env }) {
  if (request.method !== 'POST') {
    return new Response('Method Not Allowed', { status: 405 });
  }

  const body = await request.json();
  const { content, attachments, AGENT_ID } = body;
  const apiKey = env.YINGDAO_API_KEY;

  if (!apiKey) {
    return new Response(JSON.stringify({
      error: 'API key not configured',
      envKeys: Object.keys(env),
      envYingdao: env.YINGDAO_API_KEY,
      typeofEnv: typeof env
    }), {
      status: 500,
      headers: { 'Content-Type': 'application/json' }
    });
  }

  const agentId = AGENT_ID || '09d08458-9b9c-41c7-ba5d-2daeb70e148a';

  // Step 1: create conversation
  const convRes = await fetch(`https://power-api.yingdao.com/oapi/agent/v1/agents/${agentId}/conversations`, {
    method: 'POST',
    headers: { 'Authorization': `Bearer ${apiKey}`, 'Content-Type': 'application/json' },
    body: '{}'
  });

  if (!convRes.ok) {
    return new Response(await convRes.text(), { status: convRes.status, headers: { 'Content-Type': 'text/plain' } });
  }

  const convData = await convRes.json();
  const convId = convData.data?.conversationUuid;
  if (!convId) {
    return new Response(JSON.stringify({ error: 'No conversationUuid returned', raw: convData }), {
      status: 500,
      headers: { 'Content-Type': 'application/json' }
    });
  }

  // ensureConv mode — return JSON
  if (!content && (!attachments || attachments.length === 0)) {
    return new Response(JSON.stringify({ data: { conversationUuid: convId } }), {
      status: 200,
      headers: { 'Content-Type': 'application/json' }
    });
  }

  // Step 2: stream execute
  const streamRes = await fetch(`https://power-api.yingdao.com/oapi/agent/v1/conversations/${convId}/execute/stream`, {
    method: 'POST',
    headers: {
      'Authorization': `Bearer ${apiKey}`,
      'Content-Type': 'application/json',
      'Accept': 'text/event-stream'
    },
    body: JSON.stringify({ content: content || '', attachments: attachments || [] })
  });

  if (!streamRes.ok) {
    return new Response(await streamRes.text(), { status: streamRes.status, headers: { 'Content-Type': 'text/plain' } });
  }

  // Transform 影刀 SSE → 前端期望的格式
  const { readable, writable } = new TransformStream();
  const writer = writable.getWriter();
  const encoder = new TextEncoder();

  const write = (event, data) => {
    writer.write(encoder.encode(`event:${event}\ndata:${JSON.stringify(data)}\n\n`));
  };

  // Fire-and-forget transform pump
  (async () => {
    const reader = streamRes.body.getReader();
    const decoder = new TextDecoder();
    let buf = '';

    try {
      while (true) {
        const { done, value } = await reader.read();
        if (done) break;

        buf += decoder.decode(value, { stream: true });
        const lines = buf.split('\n');
        buf = lines.pop() || '';

        for (const line of lines) {
          const trimmed = line.trim();
          if (!trimmed || trimmed.startsWith(':')) continue;

          if (trimmed.startsWith('event:xybot-run-lifecycle')) continue;
          if (trimmed.startsWith('event:xybot-message') || trimmed.startsWith('event:message')) continue;

          if (trimmed.startsWith('data:')) {
            const rawData = trimmed.slice(5);
            let parsed;
            try { parsed = JSON.parse(rawData); } catch { continue; }

            const innerStr = parsed.data;
            let inner = null;
            if (typeof innerStr === 'string') {
              try { inner = JSON.parse(innerStr); } catch { inner = null; }
            } else if (innerStr) {
              inner = innerStr;
            }

            if (!inner) {
              if (parsed.phase === 'COMPLETED' || parsed.phase === 'FINISHED') {
                write('run.terminal', { phase: parsed.phase });
              }
              continue;
            }

            const type = inner.type || parsed.type;

            if (type === 'message.updated') {
              const info = inner.properties?.info || {};
              const parts = inner.properties?.parts || [];
              write('message.updated', {
                properties: {
                  info: { finish: info.finish, role: info.role },
                  parts: parts.map(p => ({ id: p.id, type: p.type, text: p.text }))
                }
              });
              for (const part of parts) {
                write('message.part.updated', {
                  properties: { part: { id: part.id, type: part.type, text: part.text } }
                });
                if (part.type === 'text' && part.text) {
                  write('message.part.delta', {
                    properties: { partID: part.id, field: 'text', delta: part.text }
                  });
                }
              }
            } else if (type === 'message.part.updated' || type === 'message.part.delta') {
              write(type, inner);
            } else {
              write('run.terminal', { type });
            }
          }
        }
      }
    } catch (e) {
      console.error('SSE transform error:', e);
    } finally {
      writer.close();
    }
  })();

  return new Response(readable, {
    status: 200,
    headers: { 'Content-Type': 'text/event-stream', 'Cache-Control': 'no-cache' }
  });
}
