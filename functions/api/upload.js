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

  // 转发原 Content-Type（multipart/form-data 的 boundary 必须保留，否则影刀无法解析）
  const ct = request.headers.get('Content-Type') || request.headers.get('content-type');

  const streamRes = await fetch('https://power-api.yingdao.com/oapi/power/v1/file/upload', {
    method: 'POST',
    headers: {
      'Authorization': `Bearer ${apiKey}`,
      ...(ct ? { 'Content-Type': ct } : {})
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
