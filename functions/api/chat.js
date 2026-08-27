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

  // 6. 转换 SSE：xybot-run-lifecycle 丢弃；xybot-message → message.part.updated；
  //    其余事件原样转发.
  const { readable, writable } = new TransformStream();
  const writer = writable.getWriter();
  const encoder = new TextEncoder();

  const write = (event, data) => {
    writer.write(encoder.encode(`event:${event}\ndata:${JSON.stringify(data)}\n\n`));
  };

  (async () => {
    const reader = streamRes.body.getReader();
    const decoder = new TextDecoder();
    let buf = '';
    let curEvent = '';
    let curData = '';

    const flushBlock = () => {
      const ev = curEvent;
      const dt = curData;
      curEvent = '';
      curData = '';
      if (!ev && !dt) return;

      if (ev === 'xybot-run-lifecycle') return;

      // 解析 data 字段获取实际事件类型和内容
      let actualType = '';
      let actualData = {};
      try {
        const parsed = JSON.parse(dt);
        // parsed.data 是影刀内部 JSON 字符串
        const inner = typeof parsed.data === 'string' ? JSON.parse(parsed.data) : parsed.data;
        actualType = inner && inner.type || '';
        // properties 包含实际数据
        actualData = (inner && inner.properties) || {};
        // text 可能藏在 properties.parts[].text 或 properties.info.content
        if (!actualData.text) {
          const parts = actualData.parts;
          if (Array.isArray(parts)) {
            actualData.text = parts.map(p => p.text || '').join('');
          } else if (actualData.info && actualData.info.content) {
            actualData.text = actualData.info.content;
          }
        }
      } catch (e) {
        // 解析失败，原样转发
        actualType = ev || 'message';
        try { actualData = JSON.parse(dt); } catch { actualData = dt; }
      }

      // 丢弃的生命周期事件
      if (actualType === 'xybot-run-lifecycle' || actualType === 'server.connected' || actualType === 'session.status') return;

      // message.updated 类型 → 前端的 message.part.updated
      if (actualType === 'message.updated') {
        const text = actualData.text || '';
        if (text) {
          write('message.part.updated', {
            properties: { part: { type: 'text', text } }
          });
        }
        return;
      }

      // 其它事件原样转发，保留原始 data.type
      write(actualType || ev || 'message', actualData);
    };

    try {
      while (true) {
        const { done, value } = await reader.read();
        if (done) break;

        buf += decoder.decode(value, { stream: true });
        const lines = buf.split('\n');
        buf = lines.pop() || '';

        for (const line of lines) {
          const trimmed = line.replace(/\r$/, '');
          if (trimmed === '') {
            flushBlock();
          } else if (trimmed.startsWith(':')) {
            // SSE 注释，跳过.
          } else if (trimmed.startsWith('event:')) {
            curEvent = trimmed.slice(6).trim();
          } else if (trimmed.startsWith('data:')) {
            const v = trimmed.slice(5);
            const val = v.startsWith(' ') ? v.slice(1) : v;
            curData = curData ? curData + '\n' + val : val;
          }
        }
      }
      if (curEvent || curData) flushBlock();
    } catch (e) {
      console.error('SSE transform error:', e);
    } finally {
      try { await writer.close(); } catch {}
    }
  })();

  return new Response(readable, {
    status: 200,
    headers: {
      'Content-Type': 'text/event-stream',
      'Cache-Control': 'no-cache',
      'Connection': 'keep-alive'
    }
  });
}
