export async function POST(request) {
  try {
    const { paymentId } = await request.json();

    if (!paymentId) {
      return Response.json({ ok: false, error: "Thiếu paymentId" }, { status: 400 });
    }

    const res = await fetch(`https://api.minepi.com/v2/payments/${paymentId}/approve`, {
      method: "POST",
      headers: {
        "Authorization": `Key ${process.env.PI_API_KEY}`,
        "Content-Type": "application/json"
      }
    });

    const data = await res.json().catch(() => ({}));
    return Response.json({ ok: res.ok, data }, { status: res.status });
  } catch (err) {
    return Response.json({ ok: false, error: err.message }, { status: 500 });
  }
}
console.log("APPROVE HIT");
console.log("HAS_KEY:", !!process.env.PI_API_KEY);
console.log("KEY_PREFIX:", (process.env.PI_API_KEY || "").slice(0, 6));
