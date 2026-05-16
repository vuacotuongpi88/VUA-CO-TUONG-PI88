module.exports = async function handler(req, res) {
  if (req.method !== "POST") {
    return res.status(405).json({ ok: false, error: "Method not allowed" });
  }

  try {
    const body = typeof req.body === "string" ? JSON.parse(req.body || "{}") : (req.body || {});
    const { action, paymentId, txid } = body;

    const PI_API_BASE = String(process.env.PI_API_BASE_URL || "https://api.minepi.com").trim();
    // 🔥 Ép chết cứng lấy Key Testnet (Đổi thành PI_API_KEY khi qua Mainnet)
    const PI_API_KEY = String(process.env.PI_API_KEY_TESTNET || "").trim();

    if (!PI_API_KEY) {
      return res.status(500).json({ ok: false, error: "Chưa cài PI_API_KEY_TESTNET trên Vercel!" });
    }
    if (!paymentId) {
      return res.status(400).json({ ok: false, error: "Thiếu paymentId" });
    }

    // ==========================================
    // LUỒNG 1: APPROVE (Duyệt nạp)
    // ==========================================
    if (action === "approve") {
      const piRes = await fetch(`${PI_API_BASE}/v2/payments/${encodeURIComponent(paymentId)}/approve`, {
        method: "POST",
        headers: { "Content-Type": "application/json", "Authorization": `Key ${PI_API_KEY}` }
      });

      const raw = await piRes.text();
      let data = {}; try { data = JSON.parse(raw); } catch (_) { data = { raw }; }

      return res.status(piRes.status).json({ ok: piRes.ok, status: piRes.status, data });
    }

    // ==========================================
    // LUỒNG 2: COMPLETE (Hoàn tất nạp)
    // ==========================================
    if (action === "complete") {
      if (!txid) return res.status(400).json({ ok: false, error: "Thiếu txid" });

      const piRes = await fetch(`${PI_API_BASE}/v2/payments/${encodeURIComponent(paymentId)}/complete`, {
        method: "POST",
        headers: { "Content-Type": "application/json", "Authorization": `Key ${PI_API_KEY}` },
        body: JSON.stringify({ txid })
      });

      const raw = await piRes.text();
      let data = {}; try { data = JSON.parse(raw); } catch (_) { data = { raw }; }

      const verifyErr = String(data?.verification_error || data?.error || data?.message || "").trim();
      const treatAsOk = piRes.ok || verifyErr === "payment_already_linked_with_a_tx";

      return res.status(treatAsOk ? 200 : piRes.status).json({
        ok: treatAsOk, status: piRes.status, data,
        note: !piRes.ok && verifyErr === "payment_already_linked_with_a_tx" ? "Đã complete từ trước." : ""
      });
    }

    return res.status(400).json({ ok: false, error: "Hành động không hợp lệ" });

  } catch (err) {
    console.error("Lỗi API Nạp Pi:", err);
    return res.status(500).json({ ok: false, error: err?.message || "Server error" });
  }
};