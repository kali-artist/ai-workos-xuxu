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

  let body = {};
  try { body = await request.json(); } catch {}
  const { content, attachments, conversationId, AGENT_ID } = body;
  const agentId = AGENT_ID || '09d08458-9b9c-41c7-ba5d-2daeb70e148a';

  const ydHeaders = {
    'Authorization': `Bearer ${apiKey}`,
    'Content-Type': 'application/json'
  };

  // ensureConv 模式：空 content + 无 attachments + 无 conversationId → 仅创建会话返回 UUID
  const isEnsureConv =
    !content &&
    (!attachments || attachments.length === 0) &&
    !conversationId;

  const createConversation = async () => {
    const convRes = await fetch(
      `https://power-api.yingdao.com/oapi/agent/v1/agents/${agentId}/conversations`,
      { method: 'POST', headers: ydHeaders, body: '{}' }
    );
    if (!convRes.ok) {
      throw { status: convRes.status, msg: await convRes.text() };
    }
    const convData = await convRes.json();
    const convId = convData.data && convData.data.conversationUuid;
    if (!convId) {
      throw { status: 500, msg: 'No conversationUuid: ' + JSON.stringify(convData) };
    }
    return convId;
  };

  const jsonErr = (status, msg) =>
    new Response(JSON.stringify({ success: false, msg }), {
      status,
      headers: { 'Content-Type': 'application/json' }
    });

  try {
    if (isEnsureConv) {
      const convId = await createConversation();
      return new Response(
        JSON.stringify({ success: true, data: { conversationUuid: convId } }),
        { status: 200, headers: { 'Content-Type': 'application/json' } }
      );
    }

    // stream 模式：需要 conversationId；没有就先创建
    let convId = conversationId;
    if (!convId) {
      convId = await createConversation();
    }

    // 调用影刀 stream 端点
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
      return jsonErr(streamRes.status, await streamRes.text());
    }

    // 直接透传影刀 SSE，不做任何转换（前端按原始格式解析）
    return new Response(streamRes.body, {
      status: 200,
      headers: {
        'Content-Type': 'text/event-stream',
        'Cache-Control': 'no-cache',
        'Connection': 'keep-alive'
      }
    });
  } catch (e) {
    return jsonErr(e.status || 500, e.msg || String(e));
  }
}
