const { getDatabase } = require("firebase-admin/database");
const adminBundle = require("../_firebaseAdmin.js");

const ADMIN_TREASURY_WALLET_KEY = String(
  process.env.ADMIN_TREASURY_WALLET_KEY || "pi_admin_master"
).trim();

const MATCH_SYSTEM_FEE_RATE = Number(
  process.env.MATCH_SYSTEM_FEE_RATE || 0.15
);

function safeWalletKey(value) {
  return String(value || "").replace(/[.#$\[\]\/]/g, "_");
}

function readPmc(obj = {}) {
  return Math.max(
    0,
    Math.floor(Number(obj?.pmcBalance ?? obj?.pmc ?? 0) || 0)
  );
}

function readPi(obj = {}) {
  return Number(obj?.balance ?? obj?.piBalance ?? 0) || 0;
}

function nowMs() {
  return Date.now();
}

function clamp(n, min, max) {
  return Math.max(min, Math.min(max, n));
}

async function adjustPmcWalletByKey(db, walletKey, delta, profile = {}, preRead = null) {
  const safeKey = safeWalletKey(walletKey);
  const ref = db.ref("wallets/" + safeKey);

  let afterBalance = 0;

  const result = await ref.transaction(current => {
    const base =
      current && typeof current === "object"
        ? current
        : (preRead && typeof preRead === "object" ? preRead : {});

    const currentPi = readPi(base);
    const currentPmc = readPmc(base);
    const nextPmc = currentPmc + Math.floor(Number(delta || 0));

    if (nextPmc < 0) return;

    afterBalance = nextPmc;

    return {
      ...base,
      balance: currentPi,
      piBalance: currentPi,
      pmcBalance: nextPmc,
      updatedAt: nowMs(),
      name: profile.name || base.name || "Người chơi",
      photo: profile.photo || base.photo || "images/do_tuong.png"
    };
  });

  if (!result.committed) return null;

  const val = result.snapshot?.val() || {};
  return {
    ...val,
    pmcBalance: afterBalance
  };
}

async function lockSettlement(settlementRef) {
  return await settlementRef.transaction(current => {
    if (current?.done || current?.locking) return;

    return {
      locking: true,
      done: false,
      route: "settle-safe-v2",
      lockedAt: nowMs()
    };
  });
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

    const feeRate = clamp(
      Number(body.feeRate ?? MATCH_SYSTEM_FEE_RATE) || MATCH_SYSTEM_FEE_RATE,
      0.02,
      0.30
    );

    const adminApp = adminBundle.app || adminBundle;
    const db = getDatabase(adminApp);

    const roomRef = db.ref(`matches/${roomId}`);
    const settlementRef = db.ref(`matches/${roomId}/settlement`);

    const roomSnap = await roomRef.once("value");
    const room = roomSnap.val() && typeof roomSnap.val() === "object"
      ? roomSnap.val()
      : {};

    const winnerRaw = String(room.winner || "").trim().toLowerCase();
    const stake = Math.max(0, Math.floor(Number(room.stakePMC || 0) || 0));

    const doPlayer = room.players?.do || {};
    const denPlayer = room.players?.den || {};

    const doWalletKey = String(
      doPlayer.walletKey || doPlayer.wallet || doPlayer.walletDbKey || doPlayer.uid || ""
    ).trim();

    const denWalletKey = String(
      denPlayer.walletKey || denPlayer.wallet || denPlayer.walletDbKey || denPlayer.uid || ""
    ).trim();

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

    const lockResult = await lockSettlement(settlementRef);

    if (!lockResult.committed) {
      const settlementSnap = await settlementRef.once("value");
      return res.status(200).json({
        ok: true,
        alreadySettled: true,
        settlement: settlementSnap.val() || null
      });
    }

    if (winnerRaw === "hoa" || winnerRaw === "draw") {
      const doAfter = await adjustPmcWalletByKey(db, doWalletKey, stake, {
        name: doPlayer.name || doPlayer.usernameNorm || doPlayer.username || "Người chơi đỏ",
        photo: doPlayer.photo || "images/do_tuong.png"
      });

      const denAfter = await adjustPmcWalletByKey(db, denWalletKey, stake, {
        name: denPlayer.name || denPlayer.usernameNorm || denPlayer.username || "Người chơi đen",
        photo: denPlayer.photo || "images/do_tuong.png"
      });

      if (!doAfter || !denAfter) {
        await settlementRef.remove().catch(() => {});
        return res.status(409).json({
          ok: false,
          error: "refund_failed"
        });
      }

      const settlementPayload = {
        done: true,
        route: "settle-safe-v2",
        type: "draw_refund",
        refundedEach: stake,
        feeRate: 0,
        feePmc: 0,
        doWalletKey: safeWalletKey(doWalletKey),
        denWalletKey: safeWalletKey(denWalletKey),
        at: nowMs()
      };

      await settlementRef.set(settlementPayload);

      await db.ref("walletTransactions").push({
        type: "match_draw_refund_safe_v2",
        roomId,
        refundedEach: stake,
        doWalletKey: safeWalletKey(doWalletKey),
        denWalletKey: safeWalletKey(denWalletKey),
        createdAt: nowMs(),
        status: "done"
      });

      return res.status(200).json({
        ok: true,
        ...settlementPayload,
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
      await settlementRef.remove().catch(() => {});
      return res.status(400).json({
        ok: false,
        error: "invalid_winner_value"
      });
    }

    const grossPot = stake * 2;
    const feePmc = Math.max(0, Math.floor(grossPot * feeRate));
    const winnerReceivePmc = grossPot - feePmc;
    const adminWalletKey = safeWalletKey(ADMIN_TREASURY_WALLET_KEY);

    const winnerRef = db.ref("wallets/" + safeWalletKey(winnerWalletKey));
    const adminRef = db.ref("wallets/" + adminWalletKey);

    const [winnerPreSnap, adminPreSnap] = await Promise.all([
      winnerRef.once("value"),
      adminRef.once("value")
    ]);

    const winnerAfter = await adjustPmcWalletByKey(
      db,
      winnerWalletKey,
      winnerReceivePmc,
      winnerProfile,
      winnerPreSnap.val()
    );

    if (!winnerAfter) {
      await settlementRef.remove().catch(() => {});
      return res.status(409).json({
        ok: false,
        error: "winner_credit_failed"
      });
    }

    let adminAfter = null;

    if (feePmc > 0) {
      adminAfter = await adjustPmcWalletByKey(
        db,
        adminWalletKey,
        feePmc,
        {
          name: "Ví phí hệ thống",
          photo: "images/do_tuong.png"
        },
        adminPreSnap.val()
      );

      if (!adminAfter) {
        await adjustPmcWalletByKey(
          db,
          winnerWalletKey,
          -winnerReceivePmc,
          winnerProfile,
          winnerPreSnap.val()
        ).catch(() => {});

        await settlementRef.remove().catch(() => {});

        return res.status(500).json({
          ok: false,
          error: "admin_fee_credit_failed_rollback_attempted"
        });
      }
    }

    const settlementPayload = {
      done: true,
      route: "settle-safe-v2",
      type: "winner_settle",
      grossPot,
      stakePMC: stake,
      feeRate,
      feePmc,
      winnerReceivePmc,
      winner: winnerRaw,
      winnerWalletKey: safeWalletKey(winnerWalletKey),
      adminWalletKey,
      at: nowMs()
    };

    await settlementRef.set(settlementPayload);

    await db.ref("matchFeeTransactions").push({
      roomId,
      type: "match_fee_pmc_safe_v2",
      grossPot,
      stakePMC: stake,
      feeRate,
      feePmc,
      winnerReceivePmc,
      winnerWalletKey: safeWalletKey(winnerWalletKey),
      adminWalletKey,
      createdAt: nowMs(),
      status: "done"
    });

    await db.ref("walletTransactions").push({
      type: "match_winner_settle_safe_v2",
      roomId,
      grossPot,
      stakePMC: stake,
      feeRate,
      feePmc,
      winnerReceivePmc,
      winnerWalletKey: safeWalletKey(winnerWalletKey),
      adminWalletKey,
      createdAt: nowMs(),
      status: "done"
    });

    return res.status(200).json({
      ok: true,
      ...settlementPayload,
      winnerPmcBalance: winnerAfter?.pmcBalance ?? null,
      adminPmcBalance: adminAfter?.pmcBalance ?? null
    });
  } catch (err) {
    console.error("pmc settle-safe-v2 error =", err);

    return res.status(500).json({
      ok: false,
      error: err?.message || "server_error"
    });
  }
};
