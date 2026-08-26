export async function onRequest({ request, env }) {
  if (request.method !== 'POST') {
    return new Response('Method Not Allowed', { status: 405 });
  }

  // 1. Read API key from Authorization header (前端通过 Authorization: Bearer <key> 传入).
  const authHeader = request.headers.get('Authorization') || request.headers.get('authorization');
  let apiKey = null;
  if (authHeader && authHeader.toLowerCase().startsWith('bearer ')) {
    apiKey = authHeader.slice(7).trim();
  }
  // Fallback: env (for local dev / wrangler secret).
  if (!apiKey && env && env.YINGDAO_API_KEY) {
    apiKey = env.YINGDAO_API_KEY;
  }
  if (!apiKey) {
    return new Response(JSON.stringify({ error: 'API key required' }), {
      status: 401,
      headers: { 'Content-Type': 'application/json' }
    });
  }

  // 2. Parse body — 影刀原始 {content, attachments}，外加可选 conversationId/AGENT_ID.
  let body = {};
  try { body = await request.json(); } catch {}
  const { content, attachments, conversationId, AGENT_ID } = body;
  const agentId = AGENT_ID || '09d08458-9b9c-41c7-ba5d-2daeb70e148a';

  const ydHeaders = {
    'Authorization': `Bearer ${apiKey}`,
    'Content-Type': 'application/json'
  };

  // 3. ensureConv 模式：空 content + 无 attachments → 仅创建会话并返回 UUID JSON.
  const isEnsureConv =
    !content &&
    (!attachments || attachments.length === 0) &&
    !conversationId;

  if (isEnsureConv) {
    const convRes = await fetch(
      `https://power-api.yingdao.com/oapi/agent/v1/agents/${agentId}/conversations`,
      { method: 'POST', headers: ydHeaders, body: '{}' }
    );
    if (!convRes.ok) {
      return new Response(
        JSON.stringify({ success: false, msg: await convRes.text() }),
        { status: convRes.status, headers: { 'Content-Type': 'application/json' } }
      );
    }
    const convData = await convRes.json();
    const convId = convData.data && convData.data.conversationUuid;
    if (!convId) {
      return new Response(
        JSON.stringify({ success: false, msg: 'No conversationUuid', raw: convData }),
        { status: 500, headers: { 'Content-Type': 'application/json' } }
      );
    }
    return new Response(
      JSON.stringify({ success: true, data: { conversationUuid: convId } }),
      { status: 200, headers: { 'Content-Type': 'application/json' } }
    );
  }

  // 4. stream 模式：需要 conversationId；没有就先创建.
  let convId = conversationId;
  if (!convId) {
    const convRes = await fetch(
      `https://power-api.yingdao.com/oapi/agent/v1/agents/${agentId}/conversations`,
      { method: 'POST', headers: ydHeaders, body: '{}' }
    );
    if (!convRes.ok) {
      return new Response(
        JSON.stringify({ success: false, msg: await convRes.text() }),
        { status: convRes.status, headers: { 'Content-Type': 'application/json' } }
      );
    }
    const convData = await convRes.json();
    convId = convData.data && convData.data.conversationUuid;
    if (!convId) {
      return new Response(
        JSON.stringify({ success: false, msg: 'No conversationUuid' }),
        { status: 500, headers: { 'Content-Type': 'application/json' } }
      );
    }
  }

  // 5. 调用影刀 stream 端点；body 用影刀原始字段，不带 conversationId.
  const streamRes = await fetch(
    `https://power-api.yingdao.com/oapi/agent/v1/conversations/${convId}/execute/stream`,
    {
      method: 'POST',
      headers: {
        'Authorization': `Bearer ${apiKey}`,
        'Content-Type': 'application/json',
        'Accept': 'text/event-stream'
      },
      body: JSON.stringify({ content: content || '', attachments: attachments || [] })
    }
  );

  if (!streamRes.ok) {
    return new Response(
      JSON.stringify({ success: false, msg: await streamRes.text() }),
      { status: streamRes.status, headers: { 'Content-Type': 'application/json' } }
    );
  }

  // 6. Pass-through 流：直接转发影刀 SSE，只改 event 名字.
  const reader = streamRes.body.getReader();
  const decoder = new TextDecoder();
  const encoder = new TextEncoder();
  let curEvent = '';
  let curData = '';
  let curId = '';
  let buf = '';

  const flushBlock = () => {
    const ev = curEvent;
    const dt = curData;
    const id = curId;
    curEvent = '';
    curData = '';
    curId = '';
    if (!ev && !dt) return;

    // 丢弃 lifecycle 事件
    if (ev === 'xybot-run-lifecycle') return;

    // 转换 event 名字
    const outEv = ev === 'xybot-message' ? 'message.part.updated' : (ev || 'message');
    return { event: outEv, data: dt, id };
  };

  const stream = new ReadableStream({
    async pull(controller) {
      const { done, value } = await reader.read();
      if (done) {
        const block = flushBlock();
        if (block && block.data) {
          controller.enqueue(encoder.encode(`id:${block.id || ''}\nevent:${block.event}\ndata:${block.data}\n\n`));
        }
        controller.close();
        return;
      }

      buf += decoder.decode(value, { stream: true });
      const lines = buf.split('\n');
      buf = lines.pop() || '';

      for (const line of lines) {
        const trimmed = line.replace(/\r$/, '');
        if (trimmed === '') {
          const block = flushBlock();
          if (block && block.data) {
            controller.enqueue(encoder.encode(`id:${block.id || ''}\nevent:${block.event}\ndata:${block.data}\n\n`));
          }
        } else if (trimmed.startsWith(':')) {
          // SSE comment
        } else if (trimmed.startsWith('id:')) {
          curId = trimmed.slice(3).trim();
        } else if (trimmed.startsWith('event:')) {
          curEvent = trimmed.slice(6).trim();
        } else if (trimmed.startsWith('data:')) {
          const v = trimmed.slice(5);
          const val = v.startsWith(' ') ? v.slice(1) : v;
          curData = curData ? curData + '\n' + val : val;
        }
      }
    },
    cancel(reason) {
      console.error('Stream cancelled:', reason);
      reader.cancel(reason).catch(() => {});
    }
  });

  return new Response(stream, {
    status: 200,
    headers: {
      'Content-Type': 'text/event-stream',
      'X-Accel-Buffering': 'no',
      'Cache-Control': 'no-cache'
    }
  });
}
