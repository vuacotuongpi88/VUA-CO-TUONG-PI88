const admin = require("../_firebaseAdmin");

function safeWalletKey(walletKey) {
  return String(walletKey || "").replace(/[.#$\[\]\/]/g, "_");
}

async function adjustPmcWalletByKey(db, walletKey, delta, profile = {}) {
  const ref = db.ref("wallets/" + safeWalletKey(walletKey));

  const result = await ref.transaction((current) => {
    const safeCurrent = current && typeof current === "object" ? current : {};

    const currentPi = Number(safeCurrent.balance ?? 0) || 0;
    const currentPmc = Math.floor(Number(safeCurrent.pmcBalance ?? 0) || 0);
    const nextPmc = currentPmc + Math.floor(Number(delta || 0));

    if (nextPmc < 0) return;

    return {
      ...safeCurrent,
      balance: currentPi,
      pmcBalance: nextPmc,
      updatedAt: Date.now(),
      name: profile.name || safeCurrent.name || "Người chơi",
      photo: profile.photo || safeCurrent.photo || "images/do_tuong.png",
    };
  });

  if (!result.committed) return null;
  return result.snapshot?.val() || null;
}

module.exports = async function handler(req, res) {
  if (req.method !== "POST") {
    return res.status(405).json({ ok: false, error: "method_not_allowed" });
  }

  try {
    const body =
      typeof req.body === "string" ? JSON.parse(req.body || "{}") : (req.body || {});
    const roomId = String(body.roomId || "").trim();

    if (!roomId) {
      return res.status(400).json({ ok: false, error: "missing_roomId" });
    }

    const db = admin.database();
    const roomRef = db.ref(`matches/${roomId}`);
    const settlementRef = db.ref(`matches/${roomId}/settlement`);

    const roomSnap = await roomRef.once("value");
    const room = roomSnap.val() || {};

    const winner = room.winner;
    const stake = Math.max(0, Math.floor(Number(room.stakePMC || 0) || 0));
    const doPlayer = room.players?.do || {};
    const denPlayer = room.players?.den || {};

    if (!winner) {
      return res.status(400).json({ ok: false, error: "winner_missing" });
    }

    if (!stake) {
      return res.status(400).json({ ok: false, error: "stake_zero" });
    }

    const lock = await settlementRef.transaction((current) => {
      if (current && (current.done || current.processing)) return;
      return {
        ...(current || {}),
        processing: true,
        done: false,
        paid: false,
        winner,
        stakePMC: stake,
        at: Date.now(),
        by: "api-server",
      };
    });

    if (!lock.committed) {
      return res.status(200).json({
        ok: true,
        skipped: true,
        reason: "already_processed",
      });
    }

    try {
      if (winner === "hoa") {
        if (!doPlayer.walletKey || !denPlayer.walletKey) {
          throw new Error("draw_missing_wallet");
        }

        const paidDo = await adjustPmcWalletByKey(db, doPlayer.walletKey, stake, {
          name: doPlayer.name,
          photo: doPlayer.photo,
        });

        const paidDen = await adjustPmcWalletByKey(db, denPlayer.walletKey, stake, {
          name: denPlayer.name,
          photo: denPlayer.photo,
        });

        if (!paidDo || !paidDen) {
          throw new Error("draw_refund_failed");
        }
      } else {
        const winnerPlayer = winner === "do" ? doPlayer : denPlayer;

        if (!winnerPlayer.walletKey) {
          throw new Error("winner_wallet_missing");
        }

        const paid = await adjustPmcWalletByKey(
          db,
          winnerPlayer.walletKey,
          stake * 2,
          {
            name: winnerPlayer.name,
            photo: winnerPlayer.photo,
          }
        );

        if (!paid) {
          throw new Error("winner_payout_failed");
        }
      }

      await settlementRef.update({
        processing: false,
        done: true,
        paid: true,
        paidAt: Date.now(),
        winner,
        stakePMC: stake,
        by: "api-server",
      });

      return res.status(200).json({ ok: true, paid: true, roomId, winner, stake });
    } catch (err) {
      await settlementRef.update({
        processing: false,
        done: false,
        paid: false,
        error: String(err?.message || err || "settle_failed"),
        errorAt: Date.now(),
        winner,
        stakePMC: stake,
        by: "api-server",
      });

      return res.status(500).json({
        ok: false,
        error: String(err?.message || err || "settle_failed"),
      });
    }
  } catch (err) {
    return res.status(500).json({
      ok: false,
      error: String(err?.message || err || "server_error"),
    });
  }
};