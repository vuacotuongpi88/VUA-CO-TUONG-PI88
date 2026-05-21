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

function isGoogleWalletKey(walletKey) {
  return String(walletKey || "").startsWith("google_");
}

function isPiLinkedWallet(wallet = {}) {
  return wallet.piVerified === true && !!wallet.piUid;
}

function noTicketPayload(isGoogleOnly) {
  if (isGoogleOnly) {
    return {
      ok: false,
      code: "NO_FREE_TICKETS_NEED_PI_LINK",
      error: "Bạn đã hết vé chơi thử. Hãy mở Pi Browser để liên kết Pi hoặc nạp Pi để chơi tiếp."
    };
  }

  return {
    ok: false,
    code: "NO_FREE_TICKETS",
    error: "Không đủ lượt miễn phí"
  };
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
    const walletKey = safeKey(body.walletKey);
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
    const walletSnap = await walletRef.once("value");
    const wallet = walletSnap.val() || {};

    if (String(wallet.uid || "") !== uid) {
      return res.status(403).json({
        ok: false,
        error: "Ví này không thuộc tài khoản đang đăng nhập"
      });
    }

    const stake = normalizePmc(room.stakePMC || room.stakePmc || 0);
    const googleOnly = isGoogleWalletKey(walletKey) && !isPiLinkedWallet(wallet);

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
    const lockedVal = lockedSnap.val();

    if (lockedVal && lockedVal.done) {
      await roomRef.child(`ready/${side}`).set(true);

      return res.status(200).json({
        ok: true,
        stage: "already_locked",
        paid: normalizePmc(wallet.pmcBalance || 0),
        usedTicket: lockedVal.isTicketUsed === true,
        freeTickets: Math.max(0, Math.floor(Number(wallet.freeTickets || 0))),
        freeTicketLocked: Math.max(0, Math.floor(Number(wallet.freeTicketLocked || 0)))
      });
    }

    // GOOGLE CHƯA LIÊN KẾT PI:
    // Không được trừ PMC thật. Chỉ được dùng vé chơi thử.
    const shouldUseTicket = googleOnly || useTicket;

    if (shouldUseTicket) {
      let ticketLeft = null;
      let ticketLocked = null;
      let paidPmc = null;
      let beforeTickets = null;
      let beforeLocked = null;

      console.log("CHARGE_TICKET_DEBUG_BEFORE", {
        roomId,
        side,
        uid,
        walletKey,
        stake,
        googleOnly,
        walletUid: wallet.uid,
        walletName: wallet.name,
        walletFreeTickets: wallet.freeTickets,
        walletFreeTicketLocked: wallet.freeTicketLocked,
        walletPmcBalance: wallet.pmcBalance
      });

      const ticketTx = await runTx(walletRef, (current) => {
        const base =
          current && typeof current === "object"
            ? current
            : wallet && typeof wallet === "object"
              ? { ...wallet }
              : null;

        if (!base) return;

        if (String(base.uid || "") !== uid) {
          console.log("CHARGE_TICKET_UID_MISMATCH_TX", {
            walletKey,
            uid,
            walletUid: base.uid
          });
          return;
        }

        beforeTickets = Math.max(0, Math.floor(Number(base.freeTickets || 0) || 0));
        beforeLocked = Math.max(0, Math.floor(Number(base.freeTicketLocked || 0) || 0));

        if (beforeTickets <= 0) return;

        ticketLeft = beforeTickets - 1;
        ticketLocked = beforeLocked + 1;
        paidPmc = normalizePmc(base.pmcBalance || 0);

        return {
          ...base,
          freeTickets: ticketLeft,
          freeTicketLocked: ticketLocked,
          pmcBalance: paidPmc,
          lastTicketLockedAt: Date.now(),
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
          googleOnly,
          beforeTickets,
          beforeLocked,
          liveFreeTickets: liveWallet.freeTickets,
          liveFreeTicketLocked: liveWallet.freeTicketLocked,
          livePmcBalance: liveWallet.pmcBalance,
          liveUid: liveWallet.uid,
          liveName: liveWallet.name
        });

        return res.status(409).json({
          ...noTicketPayload(googleOnly),
          debug: {
            roomId,
            side,
            walletKey,
            stake,
            beforeTickets,
            beforeLocked,
            liveFreeTickets: liveWallet.freeTickets,
            liveFreeTicketLocked: liveWallet.freeTicketLocked,
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
        freeTicketLocked: ticketLocked,
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

      await roomRef.child(`ticketStakes/${side}`).set({
        type: googleOnly ? "google_free_ticket" : "free_ticket",
        walletKey,
        uid,
        stake,
        locked: 1,
        status: "locked",
        lockedAt: Date.now()
      });

      await roomRef.child(`ready/${side}`).set(true);

      return res.status(200).json({
        ok: true,
        stage: "ticket_locked",
        paid: paidPmc,
        usedTicket: true,
        googleTrial: googleOnly,
        freeTickets: ticketLeft,
        freeTicketLocked: ticketLocked
      });
    }

    // Google chưa liên kết Pi mà không còn vé thì tuyệt đối không cho rơi xuống trừ PMC.
    if (googleOnly) {
      return res.status(409).json(noTicketPayload(true));
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

    const pmcTx = await runTx(walletRef, (current) => {
      const base =
        current && typeof current === "object"
          ? current
          : wallet && typeof wallet === "object"
            ? { ...wallet }
            : null;

      if (!base) return;

      if (String(base.uid || "") !== uid) {
        console.log("CHARGE_STAKE_UID_MISMATCH_TX", {
          walletKey,
          uid,
          walletUid: base.uid
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
        code: "NOT_ENOUGH_PMC",
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