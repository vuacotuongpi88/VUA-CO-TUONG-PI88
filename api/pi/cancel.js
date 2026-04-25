module.exports = async function handler(req, res) {
  res.setHeader("Cache-Control", "no-store");

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
    ).trim().replace(/\/+$/, "");

    const PI_API_KEY = String(
      process.env.PI_API_KEY ||
      process.env.PI_SERVER_API_KEY ||
      process.env.PI_APIKEY ||
      ""
    ).trim();

    console.log("PI CANCEL HIT", {
      paymentId,
      hasKey: !!PI_API_KEY,
      keyPrefix: PI_API_KEY ? PI_API_KEY.slice(0, 6) : ""
    });

    if (!PI_API_KEY) {
      return res.status(500).json({
        ok: false,
        error: "Thiếu PI_API_KEY / PI_SERVER_API_KEY trên Vercel."
      });
    }

    if (!paymentId) {
      return res.status(400).json({
        ok: false,
        error: "Thiếu paymentId."
      });
    }

    const piRes = await fetch(
      `${PI_API_BASE}/v2/payments/${encodeURIComponent(paymentId)}/cancel`,
      {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          Authorization: `Key ${PI_API_KEY}`,
          "Pi-Api-Key": PI_API_KEY
        },
        body: JSON.stringify({
          reason: "manual_clear_stuck_payment"
        })
      }
    );

    const raw = await piRes.text();

    let data = {};
    try {
      data = raw ? JSON.parse(raw) : {};
    } catch (_) {
      data = { raw };
    }

    console.log("PI CANCEL STATUS:", piRes.status);
    console.log("PI CANCEL DATA:", data);

    return res.status(piRes.ok ? 200 : piRes.status || 500).json({
      ok: piRes.ok,
      status: piRes.status,
      paymentId,
      data,
      error: piRes.ok
        ? ""
        : (
            data?.error ||
            data?.message ||
            data?.error_message ||
            "Cancel payment thất bại."
          )
    });
  } catch (err) {
    console.error("PI CANCEL ERROR:", err);
    return res.status(500).json({
      ok: false,
      error: err?.message || "cancel error"
    });
  }
};
