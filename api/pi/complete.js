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
    const txid = String(body.txid || "").trim();

    const PI_API_BASE = String(
      process.env.PI_API_BASE_URL || "https://api.minepi.com"
    ).trim().replace(/\/+$/, "");

    const PI_API_KEY = String(
      process.env.PI_API_KEY ||
      process.env.PI_SERVER_API_KEY ||
      process.env.PI_APIKEY ||
      ""
    ).trim();

    console.log("PI COMPLETE HIT", {
      paymentId,
      txid,
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

    if (!txid) {
      return res.status(409).json({
        ok: false,
        needCancel: true,
        error: "Payment Pi đang treo nhưng chưa có txid. Cần hủy/cancel payment này, không thể complete."
      });
    }

    const piRes = await fetch(
      `${PI_API_BASE}/v2/payments/${encodeURIComponent(paymentId)}/complete`,
      {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          Authorization: `Key ${PI_API_KEY}`,
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

    console.log("PI COMPLETE STATUS:", piRes.status);
    console.log("PI COMPLETE DATA:", data);

    const alreadyLinked = verifyErr === "payment_already_linked_with_a_tx";

if (piRes.ok) {
  return res.status(200).json({
    ok: true,
    completed: true,
    alreadyLinked: false,
    status: piRes.status,
    paymentId,
    txid,
    data
  });
}

if (alreadyLinked) {
  return res.status(409).json({
    ok: false,
    needCancel: true,
    alreadyLinked: true,
    status: piRes.status,
    paymentId,
    txid,
    error:
      "Pi báo payment đã linked với tx cũ nhưng Pi Browser vẫn còn pending. Cần cancel payment treo này.",
    data
  });
}

    return res.status(piRes.status || 500).json({
      ok: false,
      status: piRes.status,
      paymentId,
      txid,
      error:
        verifyErr ||
        data?.error ||
        data?.message ||
        "Pi complete thất bại.",
      data
    });
  } catch (err) {
    console.error("PI COMPLETE ERROR:", err);
    return res.status(500).json({
      ok: false,
      error: err?.message || "complete error"
    });
  }
};
