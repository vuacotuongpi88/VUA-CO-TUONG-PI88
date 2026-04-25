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

    if (!PI_API_KEY) {
      return res.status(500).json({
        ok: false,
        error: "Thiếu PI_API_KEY / PI_SERVER_API_KEY trên Vercel."
      });
    }

    if (!paymentId) {
      return res.status(400).json({
        ok: false,
        error: "Thiếu paymentId"
      });
    }

    if (!txid) {
      return res.status(400).json({
        ok: false,
        error: "Thiếu txid"
      });
    }

    const headers = {
      "Content-Type": "application/json",
      "Authorization": `Key ${PI_API_KEY}`,
      "Pi-Api-Key": PI_API_KEY
    };

    const out = {
      ok: false,
      paymentId,
      txid,
      approve: null,
      complete: null,
      note: ""
    };

    // 1) thử approve trước
    try {
      const approveRes = await fetch(
        `${PI_API_BASE}/v2/payments/${encodeURIComponent(paymentId)}/approve`,
        {
          method: "POST",
          headers
        }
      );

      const approveRaw = await approveRes.text();
      let approveData = {};
      try {
        approveData = approveRaw ? JSON.parse(approveRaw) : {};
      } catch (_) {
        approveData = { raw: approveRaw };
      }

      const approveMsg = String(
        approveData?.error ||
        approveData?.message ||
        approveData?.status ||
        ""
      ).trim();

      const approveTreatAsOk =
        approveRes.ok ||
        /already approved|approved/i.test(approveMsg);

      out.approve = {
        ok: approveTreatAsOk,
        status: approveRes.status,
        data: approveData
      };
    } catch (e) {
      out.approve = {
        ok: false,
        status: 0,
        data: { error: e?.message || String(e) }
      };
    }

    // 2) complete
    const completeRes = await fetch(
      `${PI_API_BASE}/v2/payments/${encodeURIComponent(paymentId)}/complete`,
      {
        method: "POST",
        headers,
        body: JSON.stringify({ txid })
      }
    );

    const completeRaw = await completeRes.text();
    let completeData = {};
    try {
      completeData = completeRaw ? JSON.parse(completeRaw) : {};
    } catch (_) {
      completeData = { raw: completeRaw };
    }

    const verifyErr = String(
      completeData?.verification_error ||
      completeData?.error ||
      completeData?.message ||
      ""
    ).trim();

    const completeTreatAsOk =
      completeRes.ok ||
      verifyErr === "payment_already_linked_with_a_tx";

    out.complete = {
      ok: completeTreatAsOk,
      status: completeRes.status,
      data: completeData
    };

    out.ok = !!completeTreatAsOk;
    out.note = completeTreatAsOk
      ? (
          verifyErr === "payment_already_linked_with_a_tx"
            ? "Pi báo payment đã linked với tx cũ. Route này coi như đã gỡ xong phía server."
            : "Approve/complete đã chạy xong phía server."
        )
      : (
          verifyErr ||
          completeData?.error ||
          "Complete chưa xong."
        );

    return res.status(out.ok ? 200 : 409).json(out);
  } catch (err) {
    return res.status(500).json({
      ok: false,
      error: err?.message || "cleanup pending error"
    });
  }
};
