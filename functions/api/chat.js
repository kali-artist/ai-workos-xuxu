export async function onRequest({ request, env }) {
  if (request.method !== 'POST') {
    return new Response('Method Not Allowed', { status: 405 });
  }

  const body = await request.json();
  const { content, attachments } = body;

  const apiKey = env.YINGDAO_API_KEY;
  if (!apiKey) {
    return new Response(JSON.stringify({ error: 'API key not configured' }), {
      status: 500,
      headers: { 'Content-Type': 'application/json' }
    });
  }

  const AGENT_ID = '09d08458-9b9c-41c7-ba5d-2daeb70e148a';

  // Step 1: create conversation
  const convRes = await fetch(`https://power-api.yingdao.com/oapi/agent/v1/agents/${AGENT_ID}/conversations`, {
    method: 'POST',
    headers: {
      'Authorization': `Bearer ${apiKey}`,
      'Content-Type': 'application/json'
    },
    body: '{}'
  });

  if (!convRes.ok) {
    const text = await convRes.text();
    return new Response(text, { status: convRes.status, headers: { 'Content-Type': 'text/plain' } });
  }

  const convData = await convRes.json();
  const convId = convData.data?.conversationUuid;
  if (!convId) {
    return new Response(JSON.stringify({ error: 'No conversationUuid returned', raw: convData }), {
      status: 500,
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
    const text = await streamRes.text();
    return new Response(text, { status: streamRes.status, headers: { 'Content-Type': 'text/plain' } });
  }

  return new Response(streamRes.body, {
    status: streamRes.status,
    headers: {
      'Content-Type': 'text/event-stream',
      'Cache-Control': 'no-cache',
      'X-Conversation-Id': convId
    }
  });
}
