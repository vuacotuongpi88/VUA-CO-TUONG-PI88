const { getDatabase } = require("firebase-admin/database");

let adminBundle;
try {
  adminBundle = require("./_firebaseAdmin.js");
} catch (_) {
  adminBundle = require("./firebaseAdmin.js");
}

const ADMIN_WALLET_KEYS = new Set([
  "pi_0962903406",
  "0962903406",
  "pi_admin_master"
]);

function safeKey(value = "") {
  return String(value || "").trim().replace(/[.#$\[\]\/]/g, "_");
}

function nowMs() {
  return Date.now();
}

function toNumber(value, fallback = 0) {
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

function roundPmc(value) {
  const n = Number(value || 0);
  if (!Number.isFinite(n)) return 0;
  return Math.round(n * 1000000) / 1000000;
}

function cleanText(value = "") {
  return String(value || "")
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .trim();
}

function pickName(walletKey, wallet = {}) {
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

function getTs(row = {}) {
  return (
    toNumber(row.createdAt) ||
    toNumber(row.paidAt) ||
    toNumber(row.boughtAt) ||
    toNumber(row.endedAt) ||
    toNumber(row.at) ||
    toNumber(row.updatedAt) ||
    0
  );
}

async function readRecent(db, path, limit = 120) {
  const snap = await db.ref(path).limitToLast(limit).once("value");
  const rows = [];
  snap.forEach(child => {
    rows.push({
      _key: child.key,
      ...(child.val() || {})
    });
  });
  return rows;
}

async function loadWalletMap(db, keys) {
  const unique = Array.from(new Set(keys.map(safeKey).filter(Boolean)));
  const out = {};

  await Promise.all(
    unique.map(async key => {
      try {
        const snap = await db.ref("wallets/" + key).once("value");
        out[key] = snap.val() || {};
      } catch (_) {
        out[key] = {};
      }
    })
  );

  return out;
}

function buildAdminEntryFromWalletTx(row, walletMap = {}) {
  const type = String(row.type || "");
  const ts = getTs(row);

  if (type === "cosmetic_shop_buy") {
    const wk = safeKey(row.walletKey);
    const wallet = walletMap[wk] || {};
    const name = pickName(wk, wallet);
    const itemName = row.itemName || row.itemId || "Skin";

    return {
      id: "wallet_" + row._key,
      type: "buy_skin",
      title: `${name} mua skin ${itemName}`,
      detail: `Ví hệ thống nhận ${roundPmc(Math.abs(toNumber(row.amountPMC || row.pricePmc || 0))).toLocaleString("vi-VN")} PMC`,
      amountPmc: roundPmc(Math.abs(toNumber(row.amountPMC || row.pricePmc || 0))),
      missionPoolPmc: 0,
      walletKey: wk,
      playerName: name,
      itemId: row.itemId || "",
      itemName,
      roomId: "",
      createdAt: ts,
      searchText: cleanText(`${name} ${wk} ${itemName} mua skin cosmetic_shop_buy`)
    };
  }

  if (type === "buy_exp_package") {
    const wk = safeKey(row.walletKey);
    const wallet = walletMap[wk] || {};
    const name = pickName(wk, wallet);

    return {
      id: "wallet_" + row._key,
      type: "buy_exp",
      title: `${name} mua gói EXP ${row.pkgId || ""}`,
      detail: `Ví hệ thống nhận ${roundPmc(toNumber(row.feePMC || row.pricePmc || 0)).toLocaleString("vi-VN")} PMC · +${toNumber(row.expGained)} EXP · +${toNumber(row.ticketsGained)} vé`,
      amountPmc: roundPmc(toNumber(row.feePMC || row.pricePmc || 0)),
      missionPoolPmc: 0,
      walletKey: wk,
      playerName: name,
      itemId: row.pkgId || "",
      itemName: row.pkgName || row.pkgId || "Gói EXP",
      roomId: "",
      createdAt: ts,
      searchText: cleanText(`${name} ${wk} ${row.pkgId || ""} exp mua goi exp`)
    };
  }

  if (type === "match_winner_settle") {
    const winnerKey = safeKey(row.winnerWalletKey);
    const winnerWallet = walletMap[winnerKey] || {};
    const winnerName = pickName(winnerKey, winnerWallet);

    return {
      id: "wallet_" + row._key,
      type: "match_fee",
      title: `Ván cờ đã chia tiền: ${winnerName} thắng`,
      detail: `Kèo ${roundPmc(row.stakePMC)} PMC · phí ${roundPmc(row.feePmc)} · ví hệ thống +${roundPmc(row.adminMasterSharePmc)} · quỹ nhiệm vụ +${roundPmc(row.missionPoolSharePmc)}`,
      amountPmc: roundPmc(row.adminMasterSharePmc),
      missionPoolPmc: roundPmc(row.missionPoolSharePmc),
      stakePmc: roundPmc(row.stakePMC),
      feePmc: roundPmc(row.feePmc),
      walletKey: winnerKey,
      playerName: winnerName,
      roomId: row.roomId || "",
      createdAt: ts,
      searchText: cleanText(`${winnerName} ${winnerKey} ${row.roomId || ""} match fee thắng thua bot co tuong co up`)
    };
  }

  return null;
}

function buildAdminEntryFromMatchFee(row, walletMap = {}) {
  const ts = getTs(row);
  const winnerKey = safeKey(row.winnerWalletKey);
  const winnerWallet = walletMap[winnerKey] || {};
  const winnerName = pickName(winnerKey, winnerWallet);

  return {
    id: "matchfee_" + row._key,
    type: "match_fee",
    title: `Phí ván cờ: ${winnerName} thắng`,
    detail: `Kèo ${roundPmc(row.stakePMC)} PMC · tổng pot ${roundPmc(row.grossPot)} · phí ${roundPmc(row.feePmc)} · ví hệ thống +${roundPmc(row.adminMasterSharePmc)} · quỹ +${roundPmc(row.missionPoolSharePmc)}`,
    amountPmc: roundPmc(row.adminMasterSharePmc),
    missionPoolPmc: roundPmc(row.missionPoolSharePmc),
    stakePmc: roundPmc(row.stakePMC),
    feePmc: roundPmc(row.feePmc),
    walletKey: winnerKey,
    playerName: winnerName,
    roomId: row.roomId || "",
    createdAt: ts,
    searchText: cleanText(`${winnerName} ${winnerKey} ${row.roomId || ""} phi van co match fee bot thang thua`)
  };
}

function buildAdminEntryFromSkinLog(row, walletMap = {}) {
  const wk = safeKey(row.walletKey);
  const wallet = walletMap[wk] || {};
  const name = pickName(wk, wallet);
  const itemName = row.itemName || row.itemId || "Skin";

  return {
    id: "skin_" + row._key,
    type: "buy_skin",
    title: `${name} mua skin ${itemName}`,
    detail: `Ví hệ thống nhận ${roundPmc(row.pricePmc).toLocaleString("vi-VN")} PMC`,
    amountPmc: roundPmc(row.pricePmc),
    missionPoolPmc: 0,
    walletKey: wk,
    playerName: name,
    itemId: row.itemId || "",
    itemName,
    roomId: "",
    createdAt: getTs(row),
    searchText: cleanText(`${name} ${wk} ${itemName} skin mua`)
  };
}

function buildAdminEntryFromMmo(row, walletMap = {}) {
  const wk = safeKey(row.walletKey || row.buyerWalletKey || row.userWalletKey);
  const wallet = walletMap[wk] || {};
  const name = row.kyc?.name || row.name || pickName(wk, wallet);
  const itemName = row.itemName || row.productName || row.itemId || row.productId || "Tài khoản MMO";
  const total = roundPmc(row.totalCost || row.totalPmc || row.amountPmc || row.price || 0);

  return {
    id: "mmo_" + row._key,
    type: "buy_mmo",
    title: `${name} mua ${itemName}`,
    detail: `Số lượng ${row.qty || row.quantity || 1} · ví hệ thống +${total.toLocaleString("vi-VN")} PMC`,
    amountPmc: total,
    missionPoolPmc: 0,
    walletKey: wk,
    playerName: name,
    itemId: row.itemId || row.productId || "",
    itemName,
    roomId: "",
    createdAt: getTs(row),
    searchText: cleanText(`${name} ${wk} ${itemName} mmo gmail acc tai khoan ${row.phone || ""} ${row.email || ""}`)
  };
}

function normalizePlayerHistory(row, key) {
  const ts = getTs(row);
  const result = String(row.result || row.lastResult || "").toLowerCase();
  const stake = roundPmc(row.stakePmc || row.stakePMC || row.stake || 0);
  const oppKey = safeKey(row.opponentWalletKey || row.rivalWalletKey || row.loserWalletKey || row.winnerWalletKey || "");
  const oppName = row.opponentName || row.rivalName || row.winnerName || row.loserName || "Đối thủ";
  const mode = row.mode || row.gameMode || "co-tuong";
  const isWin = result === "win";
  const isLose = result === "lose" || result === "loss";

  let title = row.title || "";
  if (!title) {
    if (isWin) title = `Bạn thắng ${oppName} - kèo ${stake} PMC`;
    else if (isLose) title = `Bạn thua ${oppName} - kèo ${stake} PMC`;
    else title = `Ván với ${oppName} - kèo ${stake} PMC`;
  }

  return {
    id: key,
    roomId: row.roomId || "",
    roundId: row.roundId || key,
    mode,
    result: isWin ? "win" : (isLose ? "lose" : result || "done"),
    title,
    opponentWalletKey: oppKey,
    opponentName: oppName,
    opponentPhoto: row.opponentPhoto || row.rivalPhoto || "images/do_tuong.png",
    stakePmc: stake,
    netPmc: roundPmc(row.netPmc || row.netPMC || row.deltaPmc || 0),
    createdAt: ts,
    searchText: cleanText(`${title} ${oppName} ${oppKey} ${mode} ${stake}`)
  };
}

module.exports = async function handler(req, res) {
  res.setHeader("Content-Type", "application/json; charset=utf-8");

  if (req.method !== "POST") {
    return res.status(405).json({ ok: false, error: "Method not allowed" });
  }

  try {
    const adminApp = adminBundle.app || adminBundle;
    const db = getDatabase(adminApp);

    const body = typeof req.body === "string" ? JSON.parse(req.body || "{}") : (req.body || {});
    const action = String(body.action || "").trim();

    const walletKey = safeKey(
      req.headers["x-wallet-key"] ||
      body.walletKey ||
      ""
    );

    if (!walletKey) {
      return res.status(400).json({ ok: false, error: "Thiếu walletKey." });
    }

    if (action === "player_history") {
      const limit = Math.max(10, Math.min(100, Math.floor(toNumber(body.limit, 50))));
      const q = cleanText(body.q || "");

      const snap = await db
        .ref(`wallets/${walletKey}/matchHistoryV2`)
        .limitToLast(limit)
        .once("value");

      const rows = [];
      snap.forEach(child => {
        const row = normalizePlayerHistory(child.val() || {}, child.key);
        if (!q || row.searchText.includes(q)) rows.push(row);
      });

      rows.sort((a, b) => b.createdAt - a.createdAt);

      return res.status(200).json({
        ok: true,
        action,
        walletKey,
        items: rows
      });
    }

    if (action === "admin_ledger") {
      if (!ADMIN_WALLET_KEYS.has(walletKey)) {
        return res.status(403).json({
          ok: false,
          error: "Chỉ admin 406 được xem sao kê hệ thống."
        });
      }

      const limit = Math.max(20, Math.min(300, Math.floor(toNumber(body.limit, 120))));
      const q = cleanText(body.q || "");

      const [walletTx, matchFees, skinLogs, mmoLogs, directLedger] = await Promise.all([
        readRecent(db, "walletTransactions", limit),
        readRecent(db, "matchFeeTransactions", limit),
        readRecent(db, "cosmeticShopLogsV1", limit),
        readRecent(db, "mmo_kyc_logs", limit),
        readRecent(db, "adminLedgerV1", limit)
      ]);

      const walletKeys = [];

      for (const r of walletTx) {
        if (r.walletKey) walletKeys.push(r.walletKey);
        if (r.winnerWalletKey) walletKeys.push(r.winnerWalletKey);
      }
      for (const r of matchFees) {
        if (r.winnerWalletKey) walletKeys.push(r.winnerWalletKey);
      }
      for (const r of skinLogs) {
        if (r.walletKey) walletKeys.push(r.walletKey);
      }
      for (const r of mmoLogs) {
        if (r.walletKey || r.buyerWalletKey || r.userWalletKey) {
          walletKeys.push(r.walletKey || r.buyerWalletKey || r.userWalletKey);
        }
      }
      for (const r of directLedger) {
        if (r.walletKey) walletKeys.push(r.walletKey);
        if (r.buyerWalletKey) walletKeys.push(r.buyerWalletKey);
      }

      const walletMap = await loadWalletMap(db, walletKeys);

      const items = [];

      for (const r of directLedger) {
        const wk = safeKey(r.walletKey || r.buyerWalletKey || "");
        const wallet = walletMap[wk] || {};
        const name = r.playerName || r.buyerName || pickName(wk, wallet);
        const item = {
          id: "ledger_" + r._key,
          type: r.type || "admin_ledger",
          title: r.title || "Sao kê hệ thống",
          detail: r.detail || "",
          amountPmc: roundPmc(r.amountPmc || r.adminAmountPmc || 0),
          missionPoolPmc: roundPmc(r.missionPoolPmc || 0),
          stakePmc: roundPmc(r.stakePmc || r.stakePMC || 0),
          feePmc: roundPmc(r.feePmc || 0),
          walletKey: wk,
          playerName: name,
          itemId: r.itemId || "",
          itemName: r.itemName || "",
          roomId: r.roomId || "",
          createdAt: getTs(r),
          searchText: cleanText(`${r.title || ""} ${r.detail || ""} ${name} ${wk} ${r.itemName || ""} ${r.roomId || ""}`)
        };
        items.push(item);
      }

      for (const r of walletTx) {
        const item = buildAdminEntryFromWalletTx(r, walletMap);
        if (item) items.push(item);
      }

      for (const r of matchFees) {
        const item = buildAdminEntryFromMatchFee(r, walletMap);
        if (item) items.push(item);
      }

      for (const r of skinLogs) {
        const item = buildAdminEntryFromSkinLog(r, walletMap);
        if (item) items.push(item);
      }

      for (const r of mmoLogs) {
        const item = buildAdminEntryFromMmo(r, walletMap);
        if (item) items.push(item);
      }

      const dedup = new Map();
      for (const item of items) {
        if (!item.createdAt) item.createdAt = nowMs();
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
        walletKey,
        items: finalItems
      });
    }

    return res.status(400).json({
      ok: false,
      error: "Action không hợp lệ."
    });
  } catch (err) {
    console.error("HISTORY_V1_FAIL:", err);
    return res.status(500).json({
      ok: false,
      error: err?.message || "Lỗi tải lịch sử."
    });
  }
};