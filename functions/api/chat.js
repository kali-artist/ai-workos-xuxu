export async function onRequest({ request, env }) {
  if (request.method !== 'POST') {
    return new Response('Method Not Allowed', { status: 405 });
  }

  const apiKey = env.YINGDAO_API_KEY;
  if (!apiKey) {
    return new Response(JSON.stringify({ error: 'API key not configured' }), {
      status: 500,
      headers: { 'Content-Type': 'application/json' }
    });
  }

  const body = await request.json().catch(() => ({}));
  const { content, attachments, conversationId } = body;
  const AGENT_ID = '09d08458-9b9c-41c7-ba5d-2daeb70e148a';
  const RELAY_HOST = '39.97.248.219';
  const RELAY_PORT = 8789;

  const ydHeaders = {
    'Authorization': `Bearer ${apiKey}`,
    'Content-Type': 'application/json'
  };

  // 有 conversationId 就直接用，没有就创建新的
  let convId = conversationId;
  if (!convId) {
    // 创建 conversation
    const cr = await fetch(
      `https://power-api.yingdao.com/oapi/agent/v1/agents/${AGENT_ID}/conversations`,
      { method: 'POST', headers: ydHeaders, body: '{}' }
    );
    if (!cr.ok) return new Response(await cr.text(), { status: cr.status });
    const convData = await cr.json();
    convId = convData.data?.conversationUuid;
    if (!convId) {
      return new Response(JSON.stringify({ success: false, msg: 'No conversationUuid', raw: convData }), {
        status: 500,
        headers: { 'Content-Type': 'application/json' }
      });
    }
    // 空内容 = ensureConv，返回 JSON
    if (!content && (!attachments || attachments.length === 0)) {
      return new Response(JSON.stringify({ success: true, data: { conversationUuid: convId } }), {
        status: 200,
        headers: { 'Content-Type': 'application/json' }
      });
    }
  }

  // 通过 HTTP relay 代理影刀 SSE 流
  // Node.js 中转服务监听 8789，接收 /api/chat 并转发
  const postData = JSON.stringify({ content: content || '', attachments: attachments || [], conversationId: convId });

  const streamRes = await fetch(`http://${RELAY_HOST}:${RELAY_PORT}/api/chat`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: postData
  });

  if (!streamRes.ok) {
    return new Response(await streamRes.text(), {
      status: streamRes.status,
      headers: { 'Content-Type': 'application/json' }
    });
  }

  // 直接透传 SSE，不过滤任何内容
  return new Response(streamRes.body, {
    status: 200,
    headers: {
      'Content-Type': 'text/event-stream',
      'Cache-Control': 'no-cache'
    }
  });
}
