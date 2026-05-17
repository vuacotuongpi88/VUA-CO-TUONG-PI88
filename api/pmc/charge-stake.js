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

    const stake = Math.max(0, Math.floor(Number(room.stakePMC || 0) || 0));

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
        paid: Math.max(0, Number(wallet.pmcBalance || 0) || 0)
      });
    }

    const lockedSnap = await roomRef.child(`stakeLocked/${side}`).once("value");
    if (lockedSnap.exists() && lockedSnap.val()) {
      await roomRef.child(`ready/${side}`).set(true);
      return res.status(200).json({
        ok: true,
        stage: "already_locked",
        paid: Math.max(0, Number(wallet.pmcBalance || 0) || 0)
      });
    }

    if (useTicket && stake <= 10000) {
      const ticketTx = await runTx(walletRef.child("freeTickets"), (current) => {
        const n = Math.max(0, Math.floor(Number(current || 0) || 0));
        if (n <= 0) return;
        return n - 1;
      });

      if (!ticketTx.committed) {
        return res.status(400).json({
          ok: false,
          error: "Không đủ lượt miễn phí"
        });
      }

      await roomRef.child(`stakeLocked/${side}`).set({
        done: true,
        walletKey,
        stake,
        isTicketUsed: true,
        uid,
        at: Date.now()
      });

      await roomRef.child(`ready/${side}`).set(true);

      return res.status(200).json({
        ok: true,
        stage: "ticket_used",
        paid: Math.max(0, Number(wallet.pmcBalance || 0) || 0),
        usedTicket: true
      });
    }

    let nextPmc = null;

    const pmcTx = await runTx(walletRef.child("pmcBalance"), (current) => {
      const currentPmc = Math.max(0, Math.floor(Number(current || 0) || 0));
      if (currentPmc < stake) return;
      nextPmc = currentPmc - stake;
      return nextPmc;
    });

    if (!pmcTx.committed || nextPmc == null) {
      return res.status(400).json({
        ok: false,
        error: `Số dư PMC không đủ để trừ ${stake} PMC`
      });
    }

    await walletRef.child("updatedAt").set(Date.now());

    await roomRef.child(`players/${side}`).update({
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