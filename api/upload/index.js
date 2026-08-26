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

  // Forward the multipart form data as-is
  const streamRes = await fetch('https://power-api.yingdao.com/oapi/power/v1/file/upload', {
    method: 'POST',
    headers: {
      'Authorization': `Bearer ${apiKey}`
    },
    body: request.body
  });

  const contentType = streamRes.headers.get('Content-Type') || 'application/json';
  const text = await streamRes.text();
  return new Response(text, {
    status: streamRes.status,
    headers: { 'Content-Type': contentType }
  });
}
