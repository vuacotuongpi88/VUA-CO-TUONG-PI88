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
    return res.status(405).json({
      ok: false,
      error: "method_not_allowed"
    });
  }

  try {
    const body =
      typeof req.body === "string"
        ? JSON.parse(req.body || "{}")
        : (req.body || {});

    const roomId = String(body.roomId || "").trim();

    if (!roomId) {
      return res.status(400).json({
        ok: false,
        error: "missing_roomId"
      });
    }

    const db = admin.database();
    const roomRef = db.ref(`matches/${roomId}`);
    const settlementRef = db.ref(`matches/${roomId}/settlement`);

    const roomSnap = await roomRef.once("value");
    const room = roomSnap.val() || {};

    const winnerRaw = String(room.winner || "").trim().toLowerCase();
    const stake = Math.max(0, Math.floor(Number(room.stakePMC || 0) || 0));

    const doPlayer = room.players?.do || {};
    const denPlayer = room.players?.den || {};

    const doWalletKey = String(doPlayer.walletKey || doPlayer.uid || "").trim();
    const denWalletKey = String(denPlayer.walletKey || denPlayer.uid || "").trim();

    if (!winnerRaw) {
      return res.status(400).json({
        ok: false,
        error: "missing_winner"
      });
    }

    if (!stake) {
      return res.status(400).json({
        ok: false,
        error: "invalid_stakePMC"
      });
    }

    if (!doWalletKey || !denWalletKey) {
      return res.status(400).json({
        ok: false,
        error: "missing_player_walletKey"
      });
    }

    // khóa settle để không chia tiền 2 lần
    const lockResult = await settlementRef.transaction(current => {
      if (current?.done || current?.locking) return;
      return {
        locking: true,
        done: false,
        at: Date.now()
      };
    });

    if (!lockResult.committed) {
      return res.status(200).json({
        ok: true,
        alreadySettled: true
      });
    }

    // HÒA => HOÀN ĐỦ, KHÔNG ĂN PHÍ
    if (winnerRaw === "hoa" || winnerRaw === "draw") {
      const doAfter = await adjustPmcWalletByKey(db, doWalletKey, stake, {
        name: doPlayer.name || doPlayer.usernameNorm || doPlayer.username || "Người chơi đỏ",
        photo: doPlayer.photo || "images/do_tuong.png"
      });

      const denAfter = await adjustPmcWalletByKey(db, denWalletKey, stake, {
        name: denPlayer.name || denPlayer.usernameNorm || denPlayer.username || "Người chơi đen",
        photo: denPlayer.photo || "images/do_tuong.png"
      });

      await settlementRef.set({
        done: true,
        type: "draw_refund",
        refundedEach: stake,
        feePmc: 0,
        at: Date.now()
      });

      await db.ref("walletTransactions").push({
        type: "match_draw_refund",
        roomId,
        refundedEach: stake,
        doWalletKey: safeWalletKey(doWalletKey),
        denWalletKey: safeWalletKey(denWalletKey),
        createdAt: Date.now(),
        status: "done"
      });

      return res.status(200).json({
        ok: true,
        type: "draw_refund",
        refundedEach: stake,
        doPmcBalance: doAfter?.pmcBalance ?? null,
        denPmcBalance: denAfter?.pmcBalance ?? null
      });
    }

    let winnerWalletKey = "";
    let winnerProfile = {};

    if (winnerRaw === "do" || winnerRaw === "red") {
      winnerWalletKey = doWalletKey;
      winnerProfile = {
        name: doPlayer.name || doPlayer.usernameNorm || doPlayer.username || "Người chơi đỏ",
        photo: doPlayer.photo || "images/do_tuong.png"
      };
    } else if (winnerRaw === "den" || winnerRaw === "black") {
      winnerWalletKey = denWalletKey;
      winnerProfile = {
        name: denPlayer.name || denPlayer.usernameNorm || denPlayer.username || "Người chơi đen",
        photo: denPlayer.photo || "images/do_tuong.png"
      };
    } else {
      await settlementRef.remove();
      return res.status(400).json({
        ok: false,
        error: "invalid_winner_value"
      });
    }

    // THẮNG => ĂN PHÍ 2% TỪ TỔNG POT
    const grossPot = stake * 2;
    const feePmc = Math.floor(grossPot * 0.02);
    const winnerReceivePmc = grossPot - feePmc;

    const winnerAfter = await adjustPmcWalletByKey(
      db,
      winnerWalletKey,
      winnerReceivePmc,
      winnerProfile
    );

    const adminAfter = await adjustPmcWalletByKey(
      db,
      "pi_admin_master",
      feePmc,
      {
        name: "Ví phí hệ thống",
        photo: "images/do_tuong.png"
      }
    );

    await settlementRef.set({
      done: true,
      type: "winner_settle",
      grossPot,
      feePmc,
      winnerReceivePmc,
      winnerWalletKey: safeWalletKey(winnerWalletKey),
      adminWalletKey: "pi_admin_master",
      at: Date.now()
    });

    await db.ref("matchFeeTransactions").push({
      roomId,
      type: "match_fee_pmc",
      grossPot,
      feeRate: 0.02,
      feePmc,
      winnerReceivePmc,
      winnerWalletKey: safeWalletKey(winnerWalletKey),
      adminWalletKey: "pi_admin_master",
      createdAt: Date.now(),
      status: "done"
    });

    await db.ref("walletTransactions").push({
      type: "match_winner_settle",
      roomId,
      grossPot,
      feePmc,
      winnerReceivePmc,
      winnerWalletKey: safeWalletKey(winnerWalletKey),
      adminWalletKey: "pi_admin_master",
      createdAt: Date.now(),
      status: "done"
    });

    return res.status(200).json({
      ok: true,
      type: "winner_settle",
      grossPot,
      feePmc,
      winnerReceivePmc,
      winnerWalletKey: safeWalletKey(winnerWalletKey),
      adminWalletKey: "pi_admin_master",
      winnerPmcBalance: winnerAfter?.pmcBalance ?? null,
      adminPmcBalance: adminAfter?.pmcBalance ?? null
    });
  } catch (err) {
    console.error("pmc settle error =", err);
    return res.status(500).json({
      ok: false,
      error: err?.message || "server_error"
    });
  }
};