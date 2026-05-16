module.exports = async function handler(req, res) {
  if (req.method !== "POST") {
    return res.status(405).json({ ok: false, error: "Method not allowed" });
  }

  try {
    const body =
      typeof req.body === "string"
        ? JSON.parse(req.body || "{}")
        : (req.body || {});

    const paymentId = String(body.paymentId || "").trim();

    const PI_API_BASE = String(
      process.env.PI_API_BASE_URL || "https://api.minepi.com"
    ).trim();

    // 🔥 ÉP CHẾT CỨNG 100% LẤY API KEY TESTNET TRÊN VERCEL NÀY
    const PI_API_KEY = String(process.env.PI_API_KEY_TESTNET || "").trim();

    console.log("APPROVE HIT", {
      paymentId,
      network: "testnet", 
      hasKey: !!PI_API_KEY,
      keyPrefix: PI_API_KEY.slice(0, 6)
    });

    if (!PI_API_KEY) {
      return res.status(500).json({
        ok: false,
        error: "Chưa cài PI_API_KEY_TESTNET trên Vercel!"
      });
    }

    if (!paymentId) {
      return res.status(400).json({
        ok: false,
        error: "Thiếu paymentId"
      });
    }

    const piRes = await fetch(
      `${PI_API_BASE}/v2/payments/${encodeURIComponent(paymentId)}/approve`,
      {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "Authorization": `Key ${PI_API_KEY}`,
          "Pi-Api-Key": PI_API_KEY
        }
      }
    );

    const raw = await piRes.text();
    let data = {};
    try {
      data = raw ? JSON.parse(raw) : {};
    } catch (_) {
      data = { raw };
    }

    console.log("APPROVE STATUS:", piRes.status);
    console.log("APPROVE DATA:", data);

    return res.status(piRes.status).json({
      ok: piRes.ok,
      status: piRes.status,
      data
    });
  } catch (err) {
    console.error("APPROVE ERROR:", err);
    return res.status(500).json({
      ok: false,
      error: err?.message || "approve error"
    });
  }
};