const { getDatabase } = require('firebase-admin/database');
const adminBundle = require('./_firebaseAdmin.js');

const PMC_PER_PI = 500;
const ADMIN_WALLET_KEY = "pi_admin_master";
const MIN_ADMIN_FEE_PMC_WITHDRAW = 500;

function safeKey(value = "") {
  return String(value || "").replace(/[.#$\[\]\/]/g, "_");
}

function readPiBalance(obj = {}) {
  return Number(obj.balance != null ? obj.balance : (obj.piBalance != null ? obj.piBalance : 0)) || 0;
}

module.exports = async function handler(req, res) {
  res.setHeader("Content-Type", "application/json; charset=utf-8");

  if (req.method !== "GET" && req.method !== "POST") {
    return res.status(405).json({ ok: false, error: "Method not allowed" });
  }

  let db;
  try {
    const adminApp = adminBundle.app || adminBundle;
    db = getDatabase(adminApp);
  } catch (e) {
    return res.status(500).json({ ok: false, error: "Lỗi kết nối Firebase Admin: " + e.message });
  }

  // --- XỬ LÝ GET REQUEST (Lấy số dư Admin nếu cần) ---
  if (req.method === "GET") {
    try {
      const currentAdminWalletKey = safeKey(req.headers["x-wallet-key"] || ADMIN_WALLET_KEY);
      const [systemSnap, adminSnap] = await Promise.all([
        db.ref("wallets/" + ADMIN_WALLET_KEY).once("value"),
        db.ref("wallets/" + currentAdminWalletKey).once("value")
      ]);
      return res.status(200).json({
        ok: true,
        pmcBalance: Math.max(0, Number(systemSnap.val()?.pmcBalance || 0)),
        piBalance: readPiBalance(adminSnap.val() || {})
      });
    } catch (err) {
      return res.status(500).json({ ok: false, error: "Lỗi đọc số dư admin" });
    }
  }

  // --- XỬ LÝ POST REQUEST (CÁC CHỨC NĂNG ĐỔI TIỀN) ---
  try {
    const body = typeof req.body === "string" ? JSON.parse(req.body || "{}") : (req.body || {});
    const action = body.action; // Biến này để phân luồng

    const walletKeyRaw = String(req.headers["x-wallet-key"] || body.walletKey || "").trim().toLowerCase();
    const safeWalletKey = safeKey(walletKeyRaw);

    if (!safeWalletKey) {
        return res.status(401).json({ ok: false, error: "Thiếu định danh ví." });
    }

    // ==========================================
    // LUỒNG 1: NGƯỜI CHƠI ĐỔI PMC SANG PI
    // ==========================================
    if (action === "user_exchange") {
        const safePmc = Math.max(0, Math.floor(Number(body.pmcAmount || 0) || 0));
        if (safePmc <= 0) return res.status(400).json({ ok: false, error: "Số PMC muốn đổi không hợp lệ." });

        const walletRef = db.ref("wallets/" + safeWalletKey);
        let exchangeResult = null;

        const txResult = await walletRef.transaction(current => {
            if (!current) return current;
            const currentPi = readPiBalance(current);
            const currentPmc = Math.floor(Number(current.pmcBalance ?? 0) || 0);

            if (currentPmc < safePmc) return; // Không đủ tiền thì bỏ qua

            const piAmount = safePmc / PMC_PER_PI;
            exchangeResult = { piAmount, newPmcBalance: currentPmc - safePmc, newPiBalance: currentPi + piAmount };

            current.balance = exchangeResult.newPiBalance;
            current.piBalance = exchangeResult.newPiBalance;
            current.pmcBalance = exchangeResult.newPmcBalance;
            current.updatedAt = Date.now();
            return current;
        });

        if (!exchangeResult || !txResult?.committed) {
            return res.status(400).json({ ok: false, error: "PMC không đủ hoặc giao dịch bị kẹt." });
        }

        await db.ref("walletTransactions").push({
            type: "pmc_to_pi", walletKey: safeWalletKey, pmcAmount: safePmc, piAmount: exchangeResult.piAmount, rate: PMC_PER_PI, createdAt: Date.now(), status: "done"
        });

        return res.status(200).json({ 
            ok: true, pmcAmount: safePmc, piAmount: exchangeResult.piAmount, 
            newPmcBalance: exchangeResult.newPmcBalance, newPiBalance: exchangeResult.newPiBalance 
        });
    }

    // ==========================================
    // LUỒNG 2: ADMIN RÚT QUỸ PMC SANG PI VÍ CÁ NHÂN
    // ==========================================
    if (action === "admin_convert") {
        const allowedPiKeys = String(process.env.ADMIN_ALLOWED_PI_KEYS || "").split(",").map(s => s.trim().toLowerCase()).filter(Boolean);
        const secretFromEnv = String(process.env.ADMIN_FEE_SECRET || "").trim();

        if (!allowedPiKeys.length || !secretFromEnv) return res.status(500).json({ ok: false, error: "Server chưa cấu hình ENV cho Admin." });
        if (!allowedPiKeys.includes(walletKeyRaw)) return res.status(403).json({ ok: false, error: "Ví Pi này không có quyền thao tác ví phí hệ thống." });

        const { pmcAmount, adminSecret } = body;
        const safePmc = Math.max(0, Math.floor(Number(pmcAmount || 0) || 0));
        
        if (String(adminSecret || "").trim() !== secretFromEnv) return res.status(403).json({ ok: false, error: "Mã bí mật admin không đúng." });
        if (safePmc < MIN_ADMIN_FEE_PMC_WITHDRAW) return res.status(400).json({ ok: false, error: `Mức rút tối thiểu là ${MIN_ADMIN_FEE_PMC_WITHDRAW} PMC.` });

        const treasuryRef = db.ref("wallets/" + ADMIN_WALLET_KEY);
        const targetRef = db.ref("wallets/" + safeWalletKey);

        // Trừ Quỹ Admin
        let treasuryResult = null;
        const treasuryTx = await treasuryRef.transaction(current => {
            if (!current) return current;
            const currentPmc = Number(current.pmcBalance ?? 0) || 0;
            if (currentPmc < safePmc) return;

            const piAmount = safePmc / PMC_PER_PI;
            treasuryResult = { piAmount, oldTreasuryPmcBalance: currentPmc, newTreasuryPmcBalance: currentPmc - safePmc };

            current.pmcBalance = treasuryResult.newTreasuryPmcBalance;
            current.updatedAt = Date.now();
            return current;
        });

        if (!treasuryResult || !treasuryTx?.committed) return res.status(400).json({ ok: false, error: "PMC quỹ hệ thống không đủ." });

        // Cộng Pi Ví Cá Nhân Admin
        let playerResult = null;
        const playerTx = await targetRef.transaction(current => {
            if (!current) return current;
            const currentPi = readPiBalance(current);
            playerResult = { oldPlayerPiBalance: currentPi, newPlayerPiBalance: currentPi + treasuryResult.piAmount };

            current.balance = playerResult.newPlayerPiBalance;
            current.piBalance = playerResult.newPlayerPiBalance;
            current.updatedAt = Date.now();
            return current;
        });

        // Hủy thao tác nếu cộng Pi lỗi
        if (!playerResult || !playerTx?.committed) {
            await treasuryRef.transaction(cur => {
                if(!cur) return cur;
                cur.pmcBalance = (Number(cur.pmcBalance) || 0) + safePmc;
                return cur;
            });
            return res.status(500).json({ ok: false, error: "Cộng Pi thất bại, đã hoàn lại tiền quỹ." });
        }

        await db.ref("walletTransactions").push({
            type: "admin_fee_pmc_to_player_pi", treasuryWalletKey: ADMIN_WALLET_KEY, targetWalletKey: safeWalletKey, pmcAmount: safePmc, piAmount: treasuryResult.piAmount, rate: PMC_PER_PI, createdAt: Date.now(), status: "done", byWalletKey: walletKeyRaw
        });

        return res.status(200).json({
            ok: true, pmcAmount: safePmc, piAmount: treasuryResult.piAmount, 
            newTreasuryPmcBalance: treasuryResult.newTreasuryPmcBalance, newPlayerPiBalance: playerResult.newPlayerPiBalance
        });
    }
    // ==========================================
    // LUỒNG 3: NGƯỜI CHƠI ĐỔI PI SANG PMC
    // ==========================================
    if (action === "pi_to_pmc") {
        const safePi = Number(body.piAmount || 0);
        if (safePi <= 0) return res.status(400).json({ ok: false, error: "Số Pi muốn đổi không hợp lệ." });

        const pmcAdd = Math.floor(safePi * PMC_PER_PI);
        const walletRef = db.ref("wallets/" + safeWalletKey);
        let exchangeResult = null;

        const txResult = await walletRef.transaction(current => {
            // Nếu chưa có ví thì khởi tạo ảo để tránh lỗi null
            if (!current) current = { balance: 0, piBalance: 0, pmcBalance: 0 };
            
            const currentPi = readPiBalance(current);
            const currentPmc = Math.floor(Number(current.pmcBalance ?? 0) || 0);

            if (currentPi < safePi) return; // Không đủ Pi thì hủy lệnh không đổi

            exchangeResult = { newPiBalance: currentPi - safePi, newPmcBalance: currentPmc + pmcAdd };

            current.balance = exchangeResult.newPiBalance;
            current.piBalance = exchangeResult.newPiBalance;
            current.pmcBalance = exchangeResult.newPmcBalance;
            current.updatedAt = Date.now();
            return current;
        });

        if (!exchangeResult || !txResult?.committed) {
            return res.status(400).json({ ok: false, error: "Pi không đủ hoặc giao dịch bị kẹt." });
        }

        await db.ref("walletTransactions").push({
            type: "pi_to_pmc", walletKey: safeWalletKey, piAmount: safePi, pmcAmount: pmcAdd, rate: PMC_PER_PI, createdAt: Date.now(), status: "done"
        });

        return res.status(200).json({ 
            ok: true, piAmount: safePi, pmcAmount: pmcAdd, 
            newPiBalance: exchangeResult.newPiBalance, newPmcBalance: exchangeResult.newPmcBalance 
        });
    }
    return res.status(400).json({ ok: false, error: "Hành động không hợp lệ." });

  } catch (err) {
    console.error("Finance API crash:", err);
    return res.status(500).json({ ok: false, error: err?.message || "Server error" });
  }
};