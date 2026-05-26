const { getDatabase } = require("firebase-admin/database");
const { getAuth } = require("firebase-admin/auth");

let adminBundle;
try {
  adminBundle = require("../_firebaseAdmin.js");
} catch (_) {
  adminBundle = require("./_firebaseAdmin.js");
}

const adminApp = adminBundle.app || adminBundle;

function safeKey(value = "") {
  return String(value || "").trim().replace(/[.#$\[\]\/]/g, "_");
}

function normalizePmc(value) {
  const n = Number(value || 0);
  if (!Number.isFinite(n)) return 0;
  return Math.max(0, Math.round(n * 1000000) / 1000000);
}

function runTx(ref, updater) {
  return new Promise((resolve, reject) => {
    ref.transaction(
      updater,
      (error, committed, snapshot) => {
        if (error) return reject(error);
        resolve({ committed, snapshot });
      },
      false
    );
  });
}

module.exports = async function handler(req, res) {
  res.setHeader("Content-Type", "application/json; charset=utf-8");

  if (req.method !== "POST") {
    return res.status(405).json({ ok: false, error: "Method not allowed" });
  }

  try {
    const body =
      typeof req.body === "string"
        ? JSON.parse(req.body || "{}")
        : (req.body || {});

    const token = String(req.headers.authorization || "")
      .replace(/^Bearer\s+/i, "")
      .trim();

    if (!token) {
      return res.status(401).json({ ok: false, error: "Thiếu Firebase token" });
    }

    const decoded = await getAuth(adminApp).verifyIdToken(token);
    const uid = decoded.uid;

   const roomId = safeKey(body.roomId);
const side = String(body.side || "").trim();

// walletKey là ví sẽ bị trừ tiền, thường là ví Google master
const walletKey = safeKey(body.walletKey);

// rawWalletKey là ví thật đang đăng nhập hiện tại, ví dụ Pi Browser là pi_098...
const rawWalletKey = safeKey(
  req.headers["x-raw-wallet-key"] ||
  body.rawWalletKey ||
  body.piWalletKey ||
  ""
);

const useTicket = !!body.useTicket;

    if (!roomId || !["do", "den"].includes(side)) {
      return res.status(400).json({ ok: false, error: "Thiếu roomId hoặc side" });
    }

    if (!walletKey) {
      return res.status(400).json({ ok: false, error: "Thiếu walletKey" });
    }

    const db = getDatabase(adminApp);

    const roomRef = db.ref(`matches/${roomId}`);
    const roomSnap = await roomRef.once("value");
    const room = roomSnap.val();

    if (!room) {
      return res.status(404).json({ ok: false, error: "Không tìm thấy phòng" });
    }

    const player = room?.players?.[side] || {};
    const playerUid = String(player.uid || "");

    if (playerUid && playerUid !== uid) {
      return res.status(403).json({
        ok: false,
        error: "Mày không phải người chơi bên này"
      });
    }

    const walletRef = db.ref(`wallets/${walletKey}`);
const rawWalletRef = rawWalletKey ? db.ref(`wallets/${rawWalletKey}`) : walletRef;

const [walletSnap, rawWalletSnap] = await Promise.all([
  walletRef.once("value"),
  rawWalletRef.once("value")
]);

const wallet = walletSnap.val() || {};
const rawWallet = rawWalletSnap.val() || {};

function sameKey(a, b) {
  const aa = safeKey(a);
  const bb = safeKey(b);
  return !!aa && !!bb && aa === bb;
}

function canUseChargeWallet(baseWallet = wallet) {
  const baseUid = String(baseWallet.uid || "");
  const rawUid = String(rawWallet.uid || "");

  // Trường hợp bình thường: ví thuộc đúng tài khoản đang đăng nhập
  if (baseUid === uid) return true;

  // Trường hợp Pi Browser đang đăng nhập ví Pi, nhưng được phép xài ví Google master đã liên kết
  if (rawWalletKey && rawUid === uid) {
    return (
      sameKey(rawWallet.masterWalletKey, walletKey) ||
      sameKey(rawWallet.linkedMasterWalletKey, walletKey) ||
      sameKey(rawWallet.linkedGoogleWalletKey, walletKey) ||
      sameKey(baseWallet.linkedPiWalletKey, rawWalletKey) ||
      sameKey(baseWallet.piWalletKey, rawWalletKey) ||
      sameKey(baseWallet.linkedPiWalletKey, rawWallet.walletKey)
    );
  }

  return false;
}

if (!canUseChargeWallet(wallet)) {
  return res.status(403).json({
    ok: false,
    error: "Ví này không thuộc tài khoản đang đăng nhập"
  });
}

    // FIX QUAN TRỌNG:
    // Không dùng Math.floor ở đây nữa.
    // Nếu stake/PMC có số lẻ thì giữ lại, tránh 4.80 bị ép thành 4.
    const stake = normalizePmc(room.stakePMC || room.stakePmc || 0);

    await roomRef.child(`players/${side}`).update({
      walletKey,
      uid,
      name: String(body.name || player.name || wallet.name || "Người chơi"),
      photo: String(body.photo || player.photo || wallet.photo || "images/do_tuong.png"),
      updatedAt: Date.now()
    });

    if (!stake) {
      await roomRef.child(`ready/${side}`).set(true);
      return res.status(200).json({
        ok: true,
        stage: "no_stake",
        paid: normalizePmc(wallet.pmcBalance || 0)
      });
    }

    const lockedSnap = await roomRef.child(`stakeLocked/${side}`).once("value");
    if (lockedSnap.exists() && lockedSnap.val()) {
      await roomRef.child(`ready/${side}`).set(true);
      return res.status(200).json({
        ok: true,
        stage: "already_locked",
        paid: normalizePmc(wallet.pmcBalance || 0)
      });
    }

    if (useTicket && stake <= 10000) {
      let ticketLeft = null;
      let paidPmc = null;
      let beforeTickets = null;

      console.log("CHARGE_TICKET_DEBUG_BEFORE", {
        roomId,
        side,
        uid,
        walletKey,
        stake,
        walletUid: wallet.uid,
        walletName: wallet.name,
        walletFreeTickets: wallet.freeTickets,
        walletPmcBalance: wallet.pmcBalance
      });

      // Transaction nguyên ví, không transaction riêng child freeTickets.
      const ticketTx = await runTx(walletRef, (current) => {
        const base =
          current && typeof current === "object"
            ? current
            : wallet && typeof wallet === "object"
              ? { ...wallet }
              : null;

        if (!base) return;

if (!canUseChargeWallet(base)) {
  console.log("CHARGE_TICKET_UID_MISMATCH_TX", {
    walletKey,
    rawWalletKey,
    uid,
    walletUid: base.uid,
    rawWalletUid: rawWallet.uid
  });
  return;
}
        beforeTickets = Math.max(0, Math.floor(Number(base.freeTickets || 0) || 0));

        console.log("CHARGE_TICKET_TX_WALLET", {
          walletKey,
          beforeTickets,
          stake,
          rawFreeTickets: base.freeTickets
        });

        if (beforeTickets <= 0) return;

        ticketLeft = beforeTickets - 1;
        paidPmc = normalizePmc(base.pmcBalance || 0);

        return {
          ...base,
          freeTickets: ticketLeft,
          pmcBalance: paidPmc,
          updatedAt: Date.now()
        };
      });

      if (!ticketTx.committed || ticketLeft == null) {
        const liveSnap = await walletRef.once("value");
        const liveWallet = liveSnap.val() || {};

        console.log("CHARGE_TICKET_NOT_ENOUGH_DEBUG", {
          roomId,
          side,
          uid,
          walletKey,
          stake,
          beforeTickets,
          liveFreeTickets: liveWallet.freeTickets,
          livePmcBalance: liveWallet.pmcBalance,
          liveUid: liveWallet.uid,
          liveName: liveWallet.name
        });

        return res.status(400).json({
          ok: false,
          error: "Không đủ lượt miễn phí",
          debug: {
            roomId,
            side,
            walletKey,
            stake,
            beforeTickets,
            liveFreeTickets: liveWallet.freeTickets,
            livePmcBalance: liveWallet.pmcBalance,
            liveUid: liveWallet.uid,
            liveName: liveWallet.name
          }
        });
      }

      await roomRef.child(`players/${side}`).update({
        walletKey,
        uid,
        freeTickets: ticketLeft,
        pmcBalance: paidPmc,
        updatedAt: Date.now()
      });

      await roomRef.child(`stakeLocked/${side}`).set({
        done: true,
        walletKey,
        stake,
        isTicketUsed: true,
        paidPmc: 0,
        uid,
        at: Date.now()
      });

      await roomRef.child(`ready/${side}`).set(true);

      return res.status(200).json({
        ok: true,
        stage: "ticket_used",
        paid: paidPmc,
        usedTicket: true,
        freeTickets: ticketLeft
      });
    }

    let nextPmc = null;
    let beforePmc = null;

    console.log("CHARGE_STAKE_DEBUG_BEFORE", {
      roomId,
      side,
      uid,
      walletKey,
      playerUid,
      stake,
      walletUid: wallet.uid,
      walletName: wallet.name,
      walletPmcBalance: wallet.pmcBalance,
      walletBalance: wallet.balance,
      walletFreeTickets: wallet.freeTickets
    });

    // FIX QUAN TRỌNG:
    // Không Math.floor pmcBalance nữa.
    // Ví dụ 4.80 - 1 = 3.80, không bị thành 3.
    const pmcTx = await runTx(walletRef, (current) => {
      const base =
        current && typeof current === "object"
          ? current
          : wallet && typeof wallet === "object"
            ? { ...wallet }
            : null;

      if (!base) return;

if (!canUseChargeWallet(base)) {
  console.log("CHARGE_STAKE_UID_MISMATCH_TX", {
    walletKey,
    rawWalletKey,
    uid,
    walletUid: base.uid,
    rawWalletUid: rawWallet.uid
  });
  return;
}

      beforePmc = normalizePmc(base.pmcBalance || 0);

      console.log("CHARGE_STAKE_TX_WALLET", {
        walletKey,
        beforePmc,
        stake,
        rawPmcBalance: base.pmcBalance
      });

      if (beforePmc < stake) return;

      nextPmc = normalizePmc(beforePmc - stake);

      return {
        ...base,
        pmcBalance: nextPmc,
        updatedAt: Date.now()
      };
    });

    if (!pmcTx.committed || nextPmc == null) {
      const liveSnap = await walletRef.once("value");
      const liveWallet = liveSnap.val() || {};

      console.log("CHARGE_STAKE_NOT_ENOUGH_DEBUG", {
        roomId,
        side,
        uid,
        walletKey,
        stake,
        beforePmc,
        livePmcBalance: liveWallet.pmcBalance,
        liveBalance: liveWallet.balance,
        liveUid: liveWallet.uid,
        liveName: liveWallet.name
      });

      return res.status(400).json({
        ok: false,
        error: `Số dư PMC không đủ để trừ ${stake} PMC`,
        debug: {
          roomId,
          side,
          walletKey,
          stake,
          beforePmc,
          livePmcBalance: liveWallet.pmcBalance,
          liveBalance: liveWallet.balance,
          liveUid: liveWallet.uid,
          liveName: liveWallet.name
        }
      });
    }

    await walletRef.child("updatedAt").set(Date.now());

    await roomRef.child(`players/${side}`).update({
      walletKey,
      uid,
      pmcBalance: nextPmc,
      updatedAt: Date.now()
    });

    await roomRef.child(`stakeLocked/${side}`).set({
      done: true,
      walletKey,
      stake,
      uid,
      at: Date.now()
    });

    await roomRef.child(`ready/${side}`).set(true);

    return res.status(200).json({
      ok: true,
      stage: "paid",
      paid: nextPmc
    });
  } catch (err) {
    console.error("CHARGE_STAKE_ERROR", err);
    return res.status(500).json({
      ok: false,
      error: err?.message || "charge stake error"
    });
  }
};