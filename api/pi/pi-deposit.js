const admin = require('firebase-admin');

// 1. KHỞI TẠO FIREBASE ADMIN
if (!admin.apps.length) {
  try {
    admin.initializeApp({
      credential: admin.credential.cert({
        projectId: process.env.FIREBASE_PROJECT_ID,
        clientEmail: process.env.FIREBASE_CLIENT_EMAIL,
        privateKey: (process.env.FIREBASE_PRIVATE_KEY || "").replace(/\\n/g, '\n'),
      }),
      databaseURL: "https://co-tuong-bd072-default-rtdb.asia-southeast1.firebasedatabase.app"
    });
    console.log("Firebase Admin inited successfully!");
  } catch (e) {
    console.warn("Lỗi khởi tạo Admin:", e);
  }
}

const db = admin.apps.length ? admin.database() : null;

module.exports = async function handler(req, res) {
  if (req.method !== "POST") {
    return res.status(405).json({ ok: false, error: "Method not allowed" });
  }

  try {
    const body = typeof req.body === "string" ? JSON.parse(req.body || "{}") : (req.body || {});
    const { action, paymentId, txid, walletKey, amount, network } = body;

    const PI_API_BASE = String(process.env.PI_API_BASE_URL || "https://api.minepi.com").trim();
    
    // 🔥 Lấy API KEY tự động theo biến network
    let PI_API_KEY = network === "testnet" 
        ? String(process.env.PI_API_KEY_TESTNET || "").trim()
        : String(process.env.PI_API_KEY || "").trim();

    if (!PI_API_KEY) {
      return res.status(500).json({ ok: false, error: `Chưa cài API KEY cho mạng ${network} trên Vercel!` });
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
        headers: { 
            "Content-Type": "application/json", 
            "Authorization": `Key ${PI_API_KEY}`
        },
        body: JSON.stringify({}) // Bùa: Pi API nhiều khi bắt buộc phải có body cho lệnh POST
      });

      const raw = await piRes.text();
      let data = {}; try { data = JSON.parse(raw); } catch (_) { data = { raw }; }

      if (!piRes.ok) console.error("PI APPROVE FAIL:", data);
      return res.status(piRes.status).json({ ok: piRes.ok, status: piRes.status, data });
    }

    // ==========================================
    // LUỒNG 2: COMPLETE VÀ CỘNG TIỀN VÀO VÍ
    // ==========================================
    if (action === "complete") {
      if (!txid) return res.status(400).json({ ok: false, error: "Thiếu txid" });

      const piRes = await fetch(`${PI_API_BASE}/v2/payments/${encodeURIComponent(paymentId)}/complete`, {
        method: "POST",
        headers: { 
            "Content-Type": "application/json", 
            "Authorization": `Key ${PI_API_KEY}`
        },
        body: JSON.stringify({ txid })
      });

      const raw = await piRes.text();
      let data = {}; try { data = JSON.parse(raw); } catch (_) { data = { raw }; }

      const verifyErr = String(data?.verification_error || data?.error || data?.message || "").trim();
      const treatAsOk = piRes.ok || verifyErr === "payment_already_linked_with_a_tx";

      if (!treatAsOk) {
        return res.status(piRes.status).json({
          ok: false, status: piRes.status, data, error: "Pi Server từ chối Complete"
        });
      }

      // 🔥 BỌC THÉP CHỐNG HACK VÔ HẠN TIỀN (INFINITE MONEY GLITCH) 🔥
      if (walletKey && amount > 0 && db) {
         // Tạo 1 quyển sổ riêng ghi chép các đơn đã xử lý
         const paymentRef = db.ref("processed_payments/" + paymentId);
         
         // Dùng transaction để chốt sổ: 1 mã paymentId CHỈ ĐƯỢC CỘNG TIỀN ĐÚNG 1 LẦN DUY NHẤT
         const txResult = await paymentRef.transaction((currentData) => {
             if (currentData) {
                 return; // Nếu đã có chữ trong sổ -> Đã cộng tiền rồi -> Hủy lệnh, đéo làm gì cả (trả về undefined)
             }
             // Nếu sổ trắng -> Đóng mộc xác nhận đã xử lý
             return {
                 processedAt: Date.now(),
                 txid: txid,
                 amount: Number(amount),
                 walletKey: walletKey
             };
         });

         if (txResult.committed) {
             // CHỈ KHI ĐÓNG MỘC THÀNH CÔNG MỚI ĐƯỢC BƠM TIỀN VÀO VÍ
             const userRef = db.ref("wallets/" + walletKey);
             await userRef.transaction((current) => {
                 if (!current) return current;
                 const currentPi = Number(current.balance || current.piBalance || 0);
                 current.balance = currentPi + Number(amount);
                 current.piBalance = currentPi + Number(amount);
                 current.updatedAt = Date.now();
                 return current;
             });

             await db.ref("walletTransactions").push({
                 type: "deposit_pi", walletKey, amount: Number(amount), paymentId, txid, createdAt: Date.now(), status: "done"
             });
             console.log(`✅ [CHỐNG HACK] Đã bơm ${amount} Pi cho đơn ${paymentId}`);
         } else {
             // Đơn này đã được cộng tiền từ trước, đéo cộng nữa nhưng vẫn báo OK về cho Pi SDK để nó tắt cái popup đi
             console.log(`🚨 [CHỐNG HACK] Bắt quả tang spam payment ${paymentId}, từ chối cộng tiền!`);
         }
      }

      return res.status(200).json({ ok: true, data });
    }

    return res.status(400).json({ ok: false, error: "Hành động không hợp lệ" });

  } catch (err) {
    console.error("Lỗi API Nạp Pi:", err);
    return res.status(500).json({ ok: false, error: err?.message || "Server error" });
  }
};