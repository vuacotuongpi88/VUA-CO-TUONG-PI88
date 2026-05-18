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
function cleanText(value = "") {
  return String(value || "").trim();
}

function normalizePiAddress(value = "") {
  return String(value || "").trim().toUpperCase();
}

function isValidPiAddress(address = "") {
  return /^G[A-Z2-7]{55}$/.test(String(address || "").trim().toUpperCase());
}
// ===== HISTORY / LEDGER HELPERS - GOM VÀO FINANCE ĐỂ NÉ GIỚI HẠN API VERCEL =====
function histNum(value, fallback = 0) {
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

function histRound(value) {
  const n = Number(value || 0);
  if (!Number.isFinite(n)) return 0;
  return Math.round(n * 1000000) / 1000000;
}

function histSearchText(value = "") {
  return String(value || "")
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .trim();
}

function histTs(row = {}) {
  return (
    histNum(row.createdAt) ||
    histNum(row.paidAt) ||
    histNum(row.endedAt) ||
    histNum(row.boughtAt) ||
    histNum(row.at) ||
    histNum(row.updatedAt) ||
    0
  );
}

function histPickName(walletKey, wallet = {}) {
  return (
    wallet.name ||
    wallet.displayName ||
    wallet.username ||
    wallet.usernameNorm ||
    wallet.phone ||
    String(walletKey || "").replace(/^pi_/, "") ||
    "Người chơi"
  );
}

async function histReadRecent(db, path, limit = 120) {
  const snap = await db.ref(path).limitToLast(limit).once("value");
  const arr = [];

  snap.forEach(child => {
    arr.push({
      _key: child.key,
      ...(child.val() || {})
    });
  });

  return arr;
}

async function histLoadWalletMap(db, keys) {
  const unique = Array.from(new Set(keys.map(safeKey).filter(Boolean)));
  const map = {};

  await Promise.all(
    unique.map(async key => {
      try {
        const snap = await db.ref("wallets/" + key).once("value");
        map[key] = snap.val() || {};
      } catch (_) {
        map[key] = {};
      }
    })
  );

  return map;
}

function histNormalizePlayerMatch(row = {}, key = "") {
  const resultRaw = String(row.result || row.lastResult || "").toLowerCase();
  const result =
    resultRaw === "win" ? "win" :
    (resultRaw === "lose" || resultRaw === "loss" ? "lose" : resultRaw || "done");

  const stake = histRound(row.stakePmc || row.stakePMC || row.stake || 0);
  const oppKey = safeKey(row.opponentWalletKey || row.rivalWalletKey || row.opponentKey || "");
  const oppName = row.opponentName || row.rivalName || row.enemyName || "Đối thủ";
  const mode = row.mode || row.gameMode || "co-tuong";

  let title = row.title || "";
  if (!title) {
    if (result === "win") title = `Bạn thắng ${oppName} - kèo ${stake} PMC`;
    else if (result === "lose") title = `Bạn thua ${oppName} - kèo ${stake} PMC`;
    else title = `Ván với ${oppName} - kèo ${stake} PMC`;
  }

  return {
    id: key,
    roomId: row.roomId || "",
    mode,
    result,
    title,
    opponentWalletKey: oppKey,
    opponentName: oppName,
    opponentPhoto: row.opponentPhoto || row.rivalPhoto || "images/do_tuong.png",
    stakePmc: stake,
    netPmc: histRound(row.netPmc || row.netPMC || row.deltaPmc || 0),
    createdAt: histTs(row),
    searchText: histSearchText(`${title} ${oppName} ${oppKey} ${mode} ${stake}`)
  };
}

function histAdminItemFromLedger(row = {}, walletMap = {}) {
  const wk = safeKey(row.walletKey || row.buyerWalletKey || row.winnerWalletKey || "");
  const wallet = walletMap[wk] || {};
  const name = row.playerName || row.buyerName || row.winnerName || histPickName(wk, wallet);

  return {
    id: "ledger_" + row._key,
    type: row.type || "admin_ledger",
    title: row.title || "Sao kê hệ thống",
    detail: row.detail || "",
    amountPmc: histRound(row.amountPmc || row.adminAmountPmc || row.adminMasterSharePmc || 0),
    missionPoolPmc: histRound(row.missionPoolPmc || row.missionPoolSharePmc || 0),
    stakePmc: histRound(row.stakePmc || row.stakePMC || 0),
    feePmc: histRound(row.feePmc || 0),
    walletKey: wk,
    playerName: name,
    itemId: row.itemId || "",
    itemName: row.itemName || "",
    roomId: row.roomId || "",
    createdAt: histTs(row),
    searchText: histSearchText(`${row.title || ""} ${row.detail || ""} ${name} ${wk} ${row.itemName || ""} ${row.roomId || ""}`)
  };
}
function histAdminItemFromCosmeticLog(row = {}, walletMap = {}) {
  const wk = safeKey(row.walletKey || row.buyerWalletKey || row.userWalletKey || "");
  const wallet = walletMap[wk] || {};
  const name = row.playerName || row.buyerName || histPickName(wk, wallet);

  const itemName = row.itemName || row.itemId || "Skin";
  const amount = histRound(row.pricePmc || row.amountPmc || row.amountPMC || row.price || 0);

  return {
    id: "cosmetic_" + row._key,
    type: "buy_skin",
    title: `${name} mua skin ${itemName}`,
    detail: `Ví hệ thống nhận +${amount} PMC`,
    amountPmc: amount,
    missionPoolPmc: 0,
    walletKey: wk,
    playerName: name,
    itemId: row.itemId || "",
    itemName,
    roomId: "",
    createdAt: histTs(row),
    searchText: histSearchText(`${name} ${wk} ${itemName} mua skin cosmetic`)
  };
}
function histAdminItemFromWalletTx(row = {}, walletMap = {}) {
  const type = String(row.type || "");
  const wk = safeKey(row.walletKey || row.buyerWalletKey || row.winnerWalletKey || "");
  const wallet = walletMap[wk] || {};
  const name = histPickName(wk, wallet);

  if (type === "buy_exp_package") {
    const amount = histRound(row.feePMC || row.pricePmc || row.amountPmc || 0);

    return {
      id: "wtx_" + row._key,
      type: "buy_exp",
      title: `${name} mua gói EXP ${row.pkgId || ""}`,
      detail: `Ví hệ thống nhận +${amount} PMC · +${histNum(row.expGained)} EXP · +${histNum(row.ticketsGained)} vé`,
      amountPmc: amount,
      missionPoolPmc: 0,
      walletKey: wk,
      playerName: name,
      itemId: row.pkgId || "",
      itemName: row.pkgName || row.pkgId || "Gói EXP",
      roomId: "",
      createdAt: histTs(row),
      searchText: histSearchText(`${name} ${wk} ${row.pkgId || ""} mua exp goi exp`)
    };
  }

  if (type === "cosmetic_shop_buy") {
    const amount = histRound(Math.abs(histNum(row.amountPMC || row.pricePmc || row.amountPmc || 0)));
    const itemName = row.itemName || row.itemId || "Skin";

    return {
      id: "wtx_" + row._key,
      type: "buy_skin",
      title: `${name} mua skin ${itemName}`,
      detail: `Ví hệ thống nhận +${amount} PMC`,
      amountPmc: amount,
      missionPoolPmc: 0,
      walletKey: wk,
      playerName: name,
      itemId: row.itemId || "",
      itemName,
      roomId: "",
      createdAt: histTs(row),
      searchText: histSearchText(`${name} ${wk} ${itemName} mua skin`)
    };
  }

  if (type === "match_winner_settle") {
    const amount = histRound(row.adminMasterSharePmc || row.amountPmc || 0);
    const mission = histRound(row.missionPoolSharePmc || 0);

    return {
      id: "wtx_" + row._key,
      type: "match_fee",
      title: `Ván cờ đã chia tiền`,
      detail: `Kèo ${histRound(row.stakePMC || row.stakePmc)} PMC · phí ${histRound(row.feePmc)} PMC · ví hệ thống +${amount} PMC · quỹ nhiệm vụ +${mission} PMC`,
      amountPmc: amount,
      missionPoolPmc: mission,
      stakePmc: histRound(row.stakePMC || row.stakePmc || 0),
      feePmc: histRound(row.feePmc || 0),
      walletKey: wk,
      playerName: name,
      roomId: row.roomId || "",
      createdAt: histTs(row),
      searchText: histSearchText(`${name} ${wk} ${row.roomId || ""} van co phi bot thang thua`)
    };
  }

  return null;
}

function histAdminItemFromMatchFee(row = {}, walletMap = {}) {
  const wk = safeKey(row.winnerWalletKey || row.walletKey || "");
  const wallet = walletMap[wk] || {};
  const name = row.winnerName || histPickName(wk, wallet);

  const amount = histRound(row.adminMasterSharePmc || row.amountPmc || 0);
  const mission = histRound(row.missionPoolSharePmc || row.missionPoolPmc || 0);

  return {
    id: "matchfee_" + row._key,
    type: "match_fee",
    title: `Phí ván cờ: ${name} thắng`,
    detail: `Kèo ${histRound(row.stakePMC || row.stakePmc)} PMC · phí ${histRound(row.feePmc)} PMC · ví hệ thống +${amount} PMC · quỹ +${mission} PMC`,
    amountPmc: amount,
    missionPoolPmc: mission,
    stakePmc: histRound(row.stakePMC || row.stakePmc || 0),
    feePmc: histRound(row.feePmc || 0),
    walletKey: wk,
    playerName: name,
    roomId: row.roomId || "",
    createdAt: histTs(row),
    searchText: histSearchText(`${name} ${wk} ${row.roomId || ""} phi van co bot thang thua`)
  };
}

function histAdminItemFromMmo(row = {}, walletMap = {}) {
  const wk = safeKey(row.walletKey || row.buyerWalletKey || row.userWalletKey || "");
  const wallet = walletMap[wk] || {};
  const name = row.name || row.kyc?.name || histPickName(wk, wallet);
  const itemName = row.itemName || row.productName || row.itemId || row.productId || "Tài khoản MMO";
  const amount = histRound(row.totalCost || row.totalPmc || row.amountPmc || row.price || 0);

  return {
    id: "mmo_" + row._key,
    type: "buy_mmo",
    title: `${name} mua ${itemName}`,
    detail: `Số lượng ${row.qty || row.quantity || 1} · ví hệ thống +${amount} PMC`,
    amountPmc: amount,
    missionPoolPmc: 0,
    walletKey: wk,
    playerName: name,
    itemId: row.itemId || row.productId || "",
    itemName,
    roomId: "",
    createdAt: histTs(row),
    searchText: histSearchText(`${name} ${wk} ${itemName} mmo gmail acc tai khoan`)
  };
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
// LỊCH SỬ ĐẤU NGƯỜI CHƠI + SAO KÊ HỆ THỐNG
// GOM VÀO FINANCE.JS ĐỂ KHÔNG TẠO API FILE MỚI
// ==========================================
if (action === "player_history") {
    const limit = Math.max(10, Math.min(100, Math.floor(histNum(body.limit, 50))));
    const q = histSearchText(body.q || "");

    const snap = await db
        .ref(`wallets/${safeWalletKey}/matchHistoryV2`)
        .limitToLast(limit)
        .once("value");

    const rows = [];

    snap.forEach(child => {
        const item = histNormalizePlayerMatch(child.val() || {}, child.key);
        if (!q || item.searchText.includes(q)) rows.push(item);
    });

    rows.sort((a, b) => b.createdAt - a.createdAt);

    return res.status(200).json({
        ok: true,
        action,
        walletKey: safeWalletKey,
        items: rows
    });
}

if (action === "admin_ledger") {
    const isAdmin406 =
        safeWalletKey === "pi_0962903406" ||
        safeWalletKey === "0962903406" ||
        safeWalletKey === ADMIN_WALLET_KEY;

    if (!isAdmin406) {
        return res.status(403).json({
            ok: false,
            error: "Chỉ admin 406 được xem sao kê hệ thống."
        });
    }

    const limit = Math.max(20, Math.min(250, Math.floor(histNum(body.limit, 120))));
    const q = histSearchText(body.q || "");

    const [adminLedger, walletTx, matchFees, mmoLogs, cosmeticLogs] = await Promise.all([
    histReadRecent(db, "adminLedgerV1", limit),
    histReadRecent(db, "walletTransactions", limit),
    histReadRecent(db, "matchFeeTransactions", limit),
    histReadRecent(db, "mmo_kyc_logs", limit),
    histReadRecent(db, "cosmeticShopLogsV1", limit)
]);

    const walletKeys = [];

    for (const r of adminLedger) {
        if (r.walletKey) walletKeys.push(r.walletKey);
        if (r.buyerWalletKey) walletKeys.push(r.buyerWalletKey);
        if (r.winnerWalletKey) walletKeys.push(r.winnerWalletKey);
        if (r.loserWalletKey) walletKeys.push(r.loserWalletKey);
    }

    for (const r of walletTx) {
        if (r.walletKey) walletKeys.push(r.walletKey);
        if (r.buyerWalletKey) walletKeys.push(r.buyerWalletKey);
        if (r.winnerWalletKey) walletKeys.push(r.winnerWalletKey);
    }

    for (const r of matchFees) {
        if (r.winnerWalletKey) walletKeys.push(r.winnerWalletKey);
        if (r.loserWalletKey) walletKeys.push(r.loserWalletKey);
    }

    for (const r of cosmeticLogs) {
    if (r.walletKey) walletKeys.push(r.walletKey);
    if (r.buyerWalletKey) walletKeys.push(r.buyerWalletKey);
    if (r.userWalletKey) walletKeys.push(r.userWalletKey);
}

    const walletMap = await histLoadWalletMap(db, walletKeys);

    const items = [];

    for (const r of adminLedger) {
        items.push(histAdminItemFromLedger(r, walletMap));
    }

    for (const r of walletTx) {
        const item = histAdminItemFromWalletTx(r, walletMap);
        if (item) items.push(item);
    }

    for (const r of matchFees) {
        const item = histAdminItemFromMatchFee(r, walletMap);
        if (item) items.push(item);
    }

    for (const r of mmoLogs) {
        const item = histAdminItemFromMmo(r, walletMap);
        if (item) items.push(item);
    }
for (const r of cosmeticLogs) {
    const item = histAdminItemFromCosmeticLog(r, walletMap);
    if (item) items.push(item);
}
    const dedup = new Map();

    for (const item of items) {
        if (!item) continue;
        if (!item.createdAt) item.createdAt = Date.now();

        if (!q || item.searchText.includes(q)) {
            dedup.set(item.id, item);
        }
    }

    const finalItems = Array.from(dedup.values())
        .sort((a, b) => b.createdAt - a.createdAt)
        .slice(0, limit);

    return res.status(200).json({
        ok: true,
        action,
        walletKey: safeWalletKey,
        items: finalItems
    });
}
    // ==========================================
    // LUỒNG 0A: ĐỌC TRẠNG THÁI LIÊN KẾT VÍ PI
    // Không tạo thêm API route để né giới hạn 12 function của Vercel Hobby
    // ==========================================
    if (action === "pi_link_status") {
        const walletSnap = await db.ref("wallets/" + safeWalletKey).once("value");
        const wallet = walletSnap.val() || {};

        return res.status(200).json({
            ok: true,
            walletKey: safeWalletKey,
            piVerified: wallet.piVerified === true,
            piUid: cleanText(wallet.piUid || ""),
            piUsername: cleanText(wallet.piUsername || ""),
            piWalletAddress: cleanText(
                wallet.piWalletAddress ||
                wallet.linkedWalletAddress ||
                wallet.piBrowserWalletAddress ||
                wallet.withdrawWalletAddress ||
                wallet.withdrawAddress ||
                ""
            )
        });
    }

    // ==========================================
    // LUỒNG 0B: LƯU ĐỊA CHỈ VÍ PI RÚT TIỀN
    // Ghi bằng Firebase Admin trong finance.js, không ghi client để tránh PERMISSION_DENIED
    // ==========================================
    if (action === "pi_link_wallet") {
        const piUid = cleanText(body.piUid || body.uid || "");
        const piUsername = cleanText(body.piUsername || body.username || "");
        const piWalletAddress = normalizePiAddress(
            body.piWalletAddress ||
            body.walletAddress ||
            body.linkedWalletAddress ||
            body.withdrawWalletAddress ||
            ""
        );

        if (!piUid) {
            return res.status(400).json({ ok: false, error: "Thiếu Pi UID." });
        }

        if (!piUsername) {
            return res.status(400).json({ ok: false, error: "Thiếu username Pi." });
        }

        if (!piWalletAddress) {
            return res.status(400).json({ ok: false, error: "Thiếu địa chỉ ví Pi nhận tiền." });
        }

        if (!isValidPiAddress(piWalletAddress)) {
            return res.status(400).json({
                ok: false,
                error: "Địa chỉ ví Pi không hợp lệ. Địa chỉ phải bắt đầu bằng G và dài 56 ký tự."
            });
        }

        const walletRef = db.ref("wallets/" + safeWalletKey);
        const existsSnap = await walletRef.once("value");

        if (!existsSnap.exists()) {
            return res.status(404).json({
                ok: false,
                error: "Không tìm thấy ví game để liên kết Pi."
            });
        }

        const now = Date.now();
const currentWallet = existsSnap.val() || {};

const finalWallet = {
    walletKey: safeWalletKey,
    piUid,
    piUsername,
    piVerified: true,
    verifiedAt: currentWallet.verifiedAt || now,
    piLinkSource: "api_finance_pi_link",

    piWalletAddress,
    linkedWalletAddress: piWalletAddress,
    piBrowserWalletAddress: piWalletAddress,
    withdrawWalletAddress: piWalletAddress,
    withdrawAddress: piWalletAddress,

    linkedWallet: {
        ...(currentWallet.linkedWallet || {}),
        address: piWalletAddress,
        linkedAt: now,
        source: "api_finance_pi_link"
    },

    piLink: {
        ...(currentWallet.piLink || {}),
        piUid,
        piUsername,
        walletAddress: piWalletAddress,
        linkedAt: now,
        source: "api_finance_pi_link"
    },

    updatedAt: now
};

await walletRef.update(finalWallet);

        await db.ref("piWalletLinkLogs").push({
            walletKey: safeWalletKey,
            piUid,
            piUsername,
            piWalletAddress,
            source: "api_finance_pi_link",
            createdAt: now,
            status: "done"
        }).catch(() => {});

        return res.status(200).json({
            ok: true,
            walletKey: safeWalletKey,
            piVerified: true,
            piUid,
            piUsername,
            piWalletAddress
        });
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
            // Lần 1: Firebase luôn truyền current = null. Phải trả về dummy để ép nó lên Server check số thật!
            if (current === null) {
                return { _dummy: true };
            }
            
            // Lần 2: Nó đã lấy được số thật từ Server. Nếu ví trắng trơn thật sự thì hủy
            if (current._dummy) return;

            const currentPi = readPiBalance(current);
            const currentPmc = Math.floor(Number(current.pmcBalance ?? 0) || 0);

            if (currentPi < safePi) return; // Không đủ Pi thật sự thì mới hủy lệnh

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