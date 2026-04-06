export async function POST(request) {
  try {
    console.log("COMPLETE HIT");

    const { paymentId, txid } = await request.json();

    console.log("COMPLETE paymentId:", paymentId);
    console.log("COMPLETE txid:", txid);
    console.log("HAS_KEY:", !!process.env.PI_API_KEY);
    console.log("KEY_PREFIX:", (process.env.PI_API_KEY || "").slice(0, 6));

    if (!process.env.PI_API_KEY) {
      return Response.json(
        { ok: false, error: "Thiếu PI_API_KEY trên Vercel" },
        { status: 500 }
      );
    }

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

    const raw = await res.text();
    let data = {};
    try {
      data = raw ? JSON.parse(raw) : {};
    } catch {
      data = { raw };
    }

    console.log("COMPLETE STATUS:", res.status);
    console.log("COMPLETE DATA:", data);

    return Response.json(
      { ok: res.ok, status: res.status, data },
      { status: res.status }
    );
  } catch (err) {
    console.error("COMPLETE ERROR:", err);
    return Response.json(
      { ok: false, error: err?.message || "complete error" },
      { status: 500 }
    );
  }
}
