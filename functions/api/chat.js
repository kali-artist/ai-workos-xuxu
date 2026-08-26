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

  const ydHeaders = {
    'Authorization': `Bearer ${apiKey}`,
    'Content-Type': 'application/json'
  };

  // 有 conversationId 就直接用，没有就创建新的
  let convId = conversationId;
  if (!convId) {
    const convRes = await fetch(
      `https://power-api.yingdao.com/oapi/agent/v1/agents/${AGENT_ID}/conversations`,
      { method: 'POST', headers: ydHeaders, body: '{}' }
    );
    if (!convRes.ok) {
      return new Response(await convRes.text(), {
        status: convRes.status,
        headers: { 'Content-Type': 'application/json' }
      });
    }
    const convData = await convRes.json();
    convId = convData.data?.conversationUuid;
    if (!convId) {
      return new Response(JSON.stringify({ success: false, msg: 'No conversationUuid', raw: convData }), {
        status: 500,
        headers: { 'Content-Type': 'application/json' }
      });
    }
    // 没有 conversationId 说明是 ensureConv 请求，返回 JSON
    if (!content && (!attachments || attachments.length === 0)) {
      return new Response(JSON.stringify({ success: true, data: { conversationUuid: convId } }), {
        status: 200,
        headers: { 'Content-Type': 'application/json' }
      });
    }
  }

  // stream 请求
  const streamRes = await fetch(
    `https://power-api.yingdao.com/oapi/agent/v1/conversations/${convId}/execute/stream`,
    {
      method: 'POST',
      headers: { ...ydHeaders, 'Accept': 'text/event-stream' },
      body: JSON.stringify({ content: content || '', attachments: attachments || [] })
    }
  );

  if (!streamRes.ok) {
    return new Response(await streamRes.text(), {
      status: streamRes.status,
      headers: { 'Content-Type': 'application/json' }
    });
  }

  // 直接透传影刀的 SSE，不做任何转换
  return new Response(streamRes.body, {
    status: 200,
    headers: {
      'Content-Type': 'text/event-stream',
      'Cache-Control': 'no-cache'
    }
  });
}
