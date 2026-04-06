export async function POST(request) {
  try {
    console.log("APPROVE HIT");

    const { paymentId } = await request.json();

    console.log("APPROVE paymentId:", paymentId);
    console.log("HAS_KEY:", !!process.env.PI_API_KEY);
    console.log("KEY_PREFIX:", (process.env.PI_API_KEY || "").slice(0, 6));

    if (!process.env.PI_API_KEY) {
      return Response.json(
        { ok: false, error: "Thiếu PI_API_KEY trên Vercel" },
        { status: 500 }
      );
    }

    if (!paymentId) {
      return Response.json(
        { ok: false, error: "Thiếu paymentId" },
        { status: 400 }
      );
    }

    const res = await fetch(`https://api.minepi.com/v2/payments/${paymentId}/approve`, {
      method: "POST",
      headers: {
        "Authorization": `Key ${process.env.PI_API_KEY}`,
        "Content-Type": "application/json"
      }
    });

    const raw = await res.text();
    let data = {};
    try {
      data = raw ? JSON.parse(raw) : {};
    } catch {
      data = { raw };
    }

    console.log("APPROVE STATUS:", res.status);
    console.log("APPROVE DATA:", data);

    return Response.json(
      { ok: res.ok, status: res.status, data },
      { status: res.status }
    );
  } catch (err) {
    console.error("APPROVE ERROR:", err);
    return Response.json(
      { ok: false, error: err?.message || "approve error" },
      { status: 500 }
    );
  }
}
