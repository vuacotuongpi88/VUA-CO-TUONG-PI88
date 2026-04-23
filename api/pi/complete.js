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
    const txid = String(body.txid || "").trim();

    const PI_API_BASE = String(
      process.env.PI_API_BASE_URL || "https://api.minepi.com"
    ).trim();

    const PI_API_KEY = String(
      process.env.PI_API_KEY ||
      process.env.PI_SERVER_API_KEY ||
      process.env.PI_APIKEY ||
      ""
    ).trim();

    console.log("COMPLETE HIT", {
      paymentId,
      txid,
      hasKey: !!PI_API_KEY,
      keyPrefix: PI_API_KEY.slice(0, 6)
    });

    if (!PI_API_KEY) {
      return res.status(500).json({
        ok: false,
        error: "Thiếu PI_API_KEY / PI_SERVER_API_KEY trên Vercel."
      });
    }

    if (!paymentId || !txid) {
      return res.status(400).json({
        ok: false,
        error: "Thiếu paymentId hoặc txid"
      });
    }

    const piRes = await fetch(
      `${PI_API_BASE}/v2/payments/${encodeURIComponent(paymentId)}/complete`,
      {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "Authorization": `Key ${PI_API_KEY}`,
          "Pi-Api-Key": PI_API_KEY
        },
        body: JSON.stringify({ txid })
      }
    );

    const raw = await piRes.text();
    let data = {};
    try {
      data = raw ? JSON.parse(raw) : {};
    } catch (_) {
      data = { raw };
    }

    const verifyErr = String(
      data?.verification_error ||
      data?.error ||
      data?.message ||
      ""
    ).trim();

    console.log("COMPLETE STATUS:", piRes.status);
    console.log("COMPLETE DATA:", data);

    const treatAsOk =
      piRes.ok || verifyErr === "payment_already_linked_with_a_tx";

    return res.status(treatAsOk ? 200 : piRes.status).json({
      ok: treatAsOk,
      status: piRes.status,
      data,
      note: !piRes.ok && verifyErr === "payment_already_linked_with_a_tx"
        ? "Pi báo payment đã linked với tx cũ, tạm coi là đã complete."
        : ""
    });
  } catch (err) {
    console.error("COMPLETE ERROR:", err);
    return res.status(500).json({
      ok: false,
      error: err?.message || "complete error"
    });
  }
};