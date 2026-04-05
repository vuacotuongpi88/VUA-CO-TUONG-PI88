export async function POST(request) {
  try {
    const { paymentId, txid } = await request.json();

    if (!paymentId || !txid) {
      return Response.json(
        { ok: false, error: "Thiếu paymentId hoặc txid" },
        { status: 400 }
      );
    }

    const res = await fetch(`https://api.minepi.com/v2/payments/${paymentId}/complete`, {
      method: "POST",
      headers: {
        "Authorization": `Key ${process.env.PI_API_KEY}`,
        "Content-Type": "application/json"
      },
      body: JSON.stringify({ txid })
    });

    const data = await res.json().catch(() => ({}));
    return Response.json({ ok: res.ok, data }, { status: res.status });
  } catch (err) {
    return Response.json({ ok: false, error: err.message }, { status: 500 });
  }
}