const { getDatabase } = require("firebase-admin/database");

let adminBundle;
try {
  // settle.js đang nằm trong api/pmc nên ưu tiên lấy _firebaseAdmin cùng thư mục
  adminBundle = require("./_firebaseAdmin.js");
} catch (e1) {
  // fallback nếu sau này mày dời _firebaseAdmin ra api/
  adminBundle = require("../_firebaseAdmin.js");
}

const LEVEL_MAX = 160;

// LUẬT CHIA PHÍ VÁN PMC
// Ví dụ stake 50 PMC mỗi bên => grossPot = 100
// feePmc = 20% tiền lời/stake = 50 * 0.20 = 10
// winnerReceivePmc = 100 - 10 = 90
// adminMasterSharePmc = 10 * 80% = 8
// missionPoolSharePmc = 10 * 20% = 2
const MATCH_FEE_RATE_ON_STAKE = 0.20;
const MATCH_FEE_ADMIN_SHARE_RATE = 0.80;
const MATCH_FEE_MISSION_SHARE_RATE = 0.20;
const ADMIN_WALLET_KEY = "pi_admin_master";
const PMC_DECIMALS = 6;

function safeWalletKey(walletKey) {
  return String(walletKey || "").replace(/[.#$\[\]\/]/g, "_");
}

function normalizePmc(value) {
  const n = Number(value || 0) || 0;
  const pow = Math.pow(10, PMC_DECIMALS);
  return Math.round(n * pow) / pow;
}

function getLevelXpNeedFromLevel(level) {
  const lv = Math.max(1, Math.min(LEVEL_MAX - 1, Math.floor(Number(level || 1))));

  /*
    Lv.1 -> Lv.2 cần 100 EXP.
    Càng lên cao càng khó theo cấp số nhân.
  */
  return Math.floor(100 * Math.pow(1.075, lv - 1));
}

function getLevelFrameByLevel(level) {
  if (level >= 150) return "king";
  if (level >= 120) return "master";
  if (level >= 90) return "diamond";
  if (level >= 60) return "platinum";
  if (level >= 40) return "gold";
  if (level >= 25) return "silver";
  if (level >= 10) return "bronze";
  return "beginner";
}

function getLevelTitleByLevel(level) {
  if (level >= 160) return "Kỳ Thánh";
  if (level >= 150) return "Vua Cờ";
  if (level >= 140) return "Thiên Vương";
  if (level >= 130) return "Tông Sư";
  if (level >= 120) return "Đại Cao Thủ";
  if (level >= 100) return "Tinh Nhuệ";
  if (level >= 90) return "Kim Cương";
  if (level >= 75) return "Tinh Anh";
  if (level >= 60) return "Bạch Kim";
  if (level >= 40) return "Kỳ Thủ Vàng";
  if (level >= 25) return "Kỳ Thủ Bạc";
  if (level >= 10) return "Kỳ Thủ Đồng";
  return "Tân Binh";
}

function buildLevelTable() {
  const rows = [];
  let xp = 0;

  for (let lv = 1; lv <= LEVEL_MAX; lv += 1) {
    const frame = getLevelFrameByLevel(lv);

    rows.push({
      level: lv,
      xp,
      title: getLevelTitleByLevel(lv),
      frame,
      pill: frame
    });

    if (lv < LEVEL_MAX) {
      xp += getLevelXpNeedFromLevel(lv);
    }
  }

  return rows;
}

const SERVER_LEVEL_TABLE = buildLevelTable();

function getLevelInfoByXp(xpValue) {
  const maxXp = SERVER_LEVEL_TABLE[SERVER_LEVEL_TABLE.length - 1].xp;
  const xp = Math.max(0, Math.min(maxXp, Number(xpValue || 0) || 0));

  let current = SERVER_LEVEL_TABLE[0];

  for (const row of SERVER_LEVEL_TABLE) {
    if (xp >= row.xp) current = row;
    else break;
  }

  return current;
}

function buildLevelMeta(meta = {}) {
  const maxXp = SERVER_LEVEL_TABLE[SERVER_LEVEL_TABLE.length - 1].xp;
  const xp = Math.max(0, Math.min(maxXp, Number(meta.xp || 0) || 0));
  const info = getLevelInfoByXp(xp);

  return {
    ...meta,
    xp,
    level: info.level,
    title: info.title,
    frame: info.frame,
    pill: info.pill,
    wins: Math.max(0, Number(meta.wins || 0) || 0),
    losses: Math.max(0, Number(meta.losses || 0) || 0),
    matches: Math.max(0, Number(meta.matches || 0) || 0),
    blockedXpMatches: Math.max(0, Number(meta.blockedXpMatches || 0) || 0)
  };
}

function getTodayKeyVN() {
  const now = new Date(Date.now() + 7 * 60 * 60 * 1000);
  const y = now.getUTCFullYear();
  const m = String(now.getUTCMonth() + 1).padStart(2, "0");
  const d = String(now.getUTCDate()).padStart(2, "0");
  return `${y}-${m}-${d}`;
}

function getRepeatAdjustedXpAbs(pairCount) {
  const n = Math.max(1, Number(pairCount || 1) || 1);

  // Thắng/thua người mới: ±30 EXP.
  // Gặp lại cùng người trong cùng ngày thì giảm dần để chống farm.
  if (n === 1) return 30;
  if (n === 2) return 24;
  if (n === 3) return 20;
  if (n === 4) return 16;
  if (n <= 6) return 12;
  if (n <= 8) return 8;
  if (n <= 10) return 5;
  if (n <= 15) return 3;
  if (n <= 20) return 2;
  return 1;
}

async function reserveServerLevelPairCount(db, walletKey, opponentKey, matchKey) {
  const todayKey = getTodayKeyVN();
  const safeMe = safeWalletKey(walletKey);
  const safeOpp = safeWalletKey(opponentKey);
  const safeMatch = safeWalletKey(matchKey);

  const roomClaimRef = db.ref(
    `levelPairRoomClaimsV3/${safeMe}/${todayKey}/${safeOpp}/${safeMatch}`
  );

  const roomClaimTx = await roomClaimRef.transaction(current => {
    if (current && current.done) return;

    return {
      done: true,
      matchKey: safeMatch,
      at: Date.now()
    };
  });

  if (!roomClaimTx.committed) {
    const snap = await db
      .ref(`levelPairDailyCountsV3/${safeMe}/${todayKey}/${safeOpp}/count`)
      .once("value");

    return {
      count: Number(snap.val() || 1) || 1,
      duplicate: true
    };
  }

  let nextCount = 1;

  const countRef = db.ref(
    `levelPairDailyCountsV3/${safeMe}/${todayKey}/${safeOpp}`
  );

  await countRef.transaction(current => {
    const val = current && typeof current === "object" ? current : {};
    const count = Math.max(0, Number(val.count || 0) || 0);

    nextCount = count + 1;

    return {
      ...val,
      count: nextCount,
      updatedAt: Date.now()
    };
  });

  return {
    count: nextCount,
    duplicate: false
  };
}

async function awardOnePlayerExp(db, roomId, matchKey, walletKey, opponentKey, resultType) {
  const safeKey = safeWalletKey(walletKey);
  const safeRoom = safeWalletKey(matchKey || roomId);

  const claimRef = db.ref(`levelMatchClaimsV3/${safeKey}/${safeRoom}`);

  const claimTx = await claimRef.transaction(current => {
    if (current && current.done) return;

    return {
      done: true,
      at: Date.now(),
      resultType,
      opponentKey: safeWalletKey(opponentKey)
    };
  });

  if (!claimTx.committed) {
    const snap = await db.ref(`wallets/${safeKey}/levelMeta`).once("value");
    return {
      ok: true,
      skipped: true,
      reason: "already_awarded",
      walletKey: safeKey,
      levelMeta: buildLevelMeta(snap.val() || {})
    };
  }

  const pair = await reserveServerLevelPairCount(db, walletKey, opponentKey, matchKey || roomId);
  const absXp = getRepeatAdjustedXpAbs(pair.count);
  const xpDelta = resultType === "win" ? absXp : -absXp;

  let afterMeta = null;

  const levelRef = db.ref(`wallets/${safeKey}/levelMeta`);

  await levelRef.transaction(current => {
    const before = buildLevelMeta(current || {});
    const maxXp = SERVER_LEVEL_TABLE[SERVER_LEVEL_TABLE.length - 1].xp;
    const nextXp = Math.max(0, Math.min(maxXp, before.xp + xpDelta));

    afterMeta = buildLevelMeta({
      ...before,
      xp: nextXp,
      wins: before.wins + (resultType === "win" ? 1 : 0),
      losses: before.losses + (resultType === "lose" ? 1 : 0),
      matches: before.matches + 1,
      lastResult: resultType,
      lastXpDelta: xpDelta,
      lastPairCount: pair.count,
      updatedAt: Date.now()
    });

    return afterMeta;
  });

  await db.ref("levelExpLogsV3").push({
    roomId,
    matchKey: safeRoom,
    walletKey: safeKey,
    opponentKey: safeWalletKey(opponentKey),
    resultType,
    pairCount: pair.count,
    xpDelta,
    afterXp: afterMeta?.xp ?? null,
    afterLevel: afterMeta?.level ?? null,
    createdAt: Date.now(),
    status: "done"
  });

  return {
    ok: true,
    walletKey: safeKey,
    resultType,
    pairCount: pair.count,
    xpDelta,
    levelMeta: afterMeta
  };
}

async function awardMatchExpServer(db, roomId, room) {
  const winnerRaw = String(room?.winner || "").trim().toLowerCase();
  const roundNo = Math.max(1, Math.floor(Number(room?.roundNo || 1) || 1));
  const matchKey = `${roomId}_round_${roundNo}`;

  if (!roomId || !winnerRaw || winnerRaw === "hoa" || winnerRaw === "draw") {
    return null;
  }

  const doPlayer = room.players?.do || {};
  const denPlayer = room.players?.den || {};

  const doWalletKey = String(doPlayer.walletKey || doPlayer.uid || "").trim();
  const denWalletKey = String(denPlayer.walletKey || denPlayer.uid || "").trim();

  if (!doWalletKey || !denWalletKey) {
    return {
      ok: false,
      error: "missing_wallet_for_exp"
    };
  }

  let winnerSide = "";

  if (winnerRaw === "do" || winnerRaw === "red") {
    winnerSide = "do";
  }

  if (winnerRaw === "den" || winnerRaw === "black") {
    winnerSide = "den";
  }

  if (!winnerSide) {
    return {
      ok: false,
      error: "invalid_winner_for_exp"
    };
  }

  const doResult = winnerSide === "do" ? "win" : "lose";
  const denResult = winnerSide === "den" ? "win" : "lose";

  const [doExp, denExp] = await Promise.all([
    awardOnePlayerExp(db, roomId, matchKey, doWalletKey, denWalletKey, doResult),
    awardOnePlayerExp(db, roomId, matchKey, denWalletKey, doWalletKey, denResult)
  ]);

  // Ghi luôn vào room để UI trong trận thấy levelMeta mới khi renderPlayersFromRoom chạy lại.
  const roomUpdate = {};

  if (doExp?.levelMeta) {
    roomUpdate[`matches/${roomId}/players/do/levelMeta`] = doExp.levelMeta;
  }

  if (denExp?.levelMeta) {
    roomUpdate[`matches/${roomId}/players/den/levelMeta`] = denExp.levelMeta;
  }

  if (Object.keys(roomUpdate).length) {
    await db.ref().update(roomUpdate);
  }

  return {
    ok: true,
    do: doExp,
    den: denExp
  };
}

async function adjustPmcWalletByKey(db, walletKey, delta, profile = {}) {
  const ref = db.ref("wallets/" + safeWalletKey(walletKey));

  const result = await ref.transaction(current => {
    const safeCurrent = current && typeof current === "object" ? current : {};

    const currentPi = Number(safeCurrent.balance ?? 0) || 0;
    const currentPmc = Number(safeCurrent.pmcBalance ?? 0) || 0;
    const nextPmc = normalizePmc(currentPmc + (Number(delta || 0) || 0));

    if (nextPmc < -0.000001) return;

    return {
      ...safeCurrent,
      balance: currentPi,
      pmcBalance: Math.max(0, nextPmc),
      updatedAt: Date.now(),
      name: profile.name || safeCurrent.name || "Người chơi",
      photo: profile.photo || safeCurrent.photo || "images/do_tuong.png"
    };
  });

  if (!result.committed) return null;
  return result.snapshot?.val() || null;
}

async function incrementMissionPool(db, amount, roomId) {
  const n = normalizePmc(amount);
  if (n <= 0) {
    return {
      missionPoolPmc: null,
      added: 0
    };
  }

  const ref = db.ref("treasury/missionPoolPmc");
  let nextValue = null;

  const tx = await ref.transaction(current => {
    nextValue = normalizePmc((Number(current || 0) || 0) + n);
    return nextValue;
  });

  await db.ref("treasury/updatedAt").set(Date.now()).catch(() => {});

  await db.ref("missionPoolTransactions").push({
    roomId,
    type: "match_fee_mission_share",
    amountPmc: n,
    missionPoolPmc: tx.snapshot?.val() ?? nextValue,
    source: "pmc_settle_fee20",
    createdAt: Date.now(),
    status: "done"
  }).catch(() => {});

  return {
    missionPoolPmc: tx.snapshot?.val() ?? nextValue,
    added: n
  };
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

    if (body.action === "avatar-skin-buy") {
      try {
        const { handleAvatarSkinBuy } = require("../../lib/avatar-skin-buy");

        const result = await handleAvatarSkinBuy(body, req);
        return res.status(result.status || 200).json(result.json);
      } catch (err) {
        console.error("AVATAR_SKIN_BUY_IN_SETTLE_FAIL", err);

        return res.status(500).json({
          ok: false,
          error:
            "AVATAR_SKIN_BUY_IN_SETTLE_FAIL: " +
            (err && err.stack ? err.stack : err.message || String(err))
        });
      }
    }

    const roomId = String(body.roomId || "").trim();

    if (!roomId) {
      return res.status(400).json({
        ok: false,
        error: "missing_roomId"
      });
    }

    const db = getDatabase(adminBundle.app || adminBundle);
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

    if (!doWalletKey || !denWalletKey) {
      return res.status(400).json({
        ok: false,
        error: "missing_player_walletKey"
      });
    }

    // CHỐT LỖI: EXP không được phụ thuộc stakePMC.
    // Nếu stake bị thiếu / 0 / lỗi thì vẫn cộng-trừ EXP, chỉ bỏ qua chia PMC.
    if (!stake) {
      let expResult = null;

      try {
        expResult = await awardMatchExpServer(db, roomId, room);
        console.log("MATCH EXP ONLY OK =", expResult);
      } catch (expErr) {
        console.error("MATCH EXP ONLY ERROR =", expErr);
        expResult = {
          ok: false,
          error: expErr?.message || "exp_error"
        };
      }

      await settlementRef.update({
        done: true,
        paid: true,
        paidAt: Date.now(),
        route: "exp-only-invalid-stake",
        type: "exp_only",
        stakePMC: stake,
        expResult,
        at: Date.now()
      });

      return res.status(200).json({
        ok: true,
        type: "exp_only",
        warning: "invalid_stakePMC_but_exp_awarded",
        expResult
      });
    }

    // Khóa settle để không chia tiền / cộng EXP 2 lần.
    const lockResult = await settlementRef.transaction(current => {
      if (current?.done || current?.paid || current?.locking) return;

      return {
        locking: true,
        done: false,
        paid: false,
        at: Date.now()
      };
    });

    if (!lockResult.committed) {
      const existedSnap = await settlementRef.once("value");
      const existed = existedSnap.val() || {};
      let expResult = existed.expResult || null;

      // Nếu trước đó PMC đã chốt nhưng thiếu EXP, bù EXP lại.
      if (!expResult && winnerRaw !== "hoa" && winnerRaw !== "draw") {
        try {
          expResult = await awardMatchExpServer(db, roomId, room);

          await settlementRef.update({
            expResult,
            expFixedAt: Date.now(),
            route: "exp-fixed-after-already-settled"
          });
        } catch (expErr) {
          expResult = {
            ok: false,
            error: expErr?.message || "exp_error"
          };
        }
      }

      return res.status(200).json({
        ok: true,
        alreadySettled: true,
        expResult
      });
    }

    // HÒA => hoàn đủ, không ăn phí, không cộng/trừ EXP.
    if (winnerRaw === "hoa" || winnerRaw === "draw") {
      const doAfter = await adjustPmcWalletByKey(db, doWalletKey, stake, {
        name: doPlayer.name || doPlayer.usernameNorm || doPlayer.username || "Người chơi đỏ",
        photo: doPlayer.photo || "images/do_tuong.png"
      });

      const denAfter = await adjustPmcWalletByKey(db, denWalletKey, stake, {
        name: denPlayer.name || denPlayer.usernameNorm || denPlayer.username || "Người chơi đen",
        photo: denPlayer.photo || "images/do_tuong.png"
      });

      await db.ref().update({
        [`matches/${roomId}/players/do/pmcBalance`]: normalizePmc(Number(doAfter?.pmcBalance || 0) || 0),
        [`matches/${roomId}/players/den/pmcBalance`]: normalizePmc(Number(denAfter?.pmcBalance || 0) || 0),
        [`matches/${roomId}/players/do/balance`]: Number(doAfter?.balance || 0) || 0,
        [`matches/${roomId}/players/den/balance`]: Number(denAfter?.balance || 0) || 0
      });

      await settlementRef.set({
        done: true,
        paid: true,
        paidAt: Date.now(),
        type: "draw_refund",
        refundedEach: stake,
        feePmc: 0,
        adminMasterSharePmc: 0,
        missionPoolSharePmc: 0,
        expResult: null,
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
        denPmcBalance: denAfter?.pmcBalance ?? null,
        expResult: null
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

    // THẮNG => cắt phế 20% trên tiền lời/stake, không phải 2% trên tổng pot.
    const grossPot = normalizePmc(stake * 2);
    const feePmc = normalizePmc(stake * MATCH_FEE_RATE_ON_STAKE);
    const adminMasterSharePmc = normalizePmc(feePmc * MATCH_FEE_ADMIN_SHARE_RATE);
    const missionPoolSharePmc = normalizePmc(feePmc * MATCH_FEE_MISSION_SHARE_RATE);
    const winnerReceivePmc = normalizePmc(grossPot - feePmc);

    const winnerAfter = await adjustPmcWalletByKey(
      db,
      winnerWalletKey,
      winnerReceivePmc,
      winnerProfile
    );

    if (!winnerAfter) {
      await settlementRef.remove().catch(() => {});
      return res.status(500).json({
        ok: false,
        error: "winner_wallet_credit_failed"
      });
    }

    const adminAfter = await adjustPmcWalletByKey(
      db,
      ADMIN_WALLET_KEY,
      adminMasterSharePmc,
      {
        name: "Ví phí hệ thống",
        photo: "images/do_tuong.png"
      }
    );

    if (!adminAfter) {
      await settlementRef.update({
        done: false,
        paid: false,
        error: "admin_wallet_credit_failed_after_winner_credit",
        at: Date.now()
      }).catch(() => {});

      return res.status(500).json({
        ok: false,
        error: "admin_wallet_credit_failed_after_winner_credit"
      });
    }

    const missionPoolResult = await incrementMissionPool(db, missionPoolSharePmc, roomId);

    const doWalletAfterSnap = await db.ref("wallets/" + safeWalletKey(doWalletKey)).once("value");
    const denWalletAfterSnap = await db.ref("wallets/" + safeWalletKey(denWalletKey)).once("value");

    const doWalletAfter = doWalletAfterSnap.val() || {};
    const denWalletAfter = denWalletAfterSnap.val() || {};

    const doPmcBalanceAfter = normalizePmc(Number(doWalletAfter.pmcBalance || 0) || 0);
    const denPmcBalanceAfter = normalizePmc(Number(denWalletAfter.pmcBalance || 0) || 0);

    await db.ref().update({
      [`matches/${roomId}/players/do/pmcBalance`]: doPmcBalanceAfter,
      [`matches/${roomId}/players/den/pmcBalance`]: denPmcBalanceAfter,
      [`matches/${roomId}/players/do/balance`]: Number(doWalletAfter.balance || 0) || 0,
      [`matches/${roomId}/players/den/balance`]: Number(denWalletAfter.balance || 0) || 0
    });

    let expResult = null;

    try {
      expResult = await awardMatchExpServer(db, roomId, room);
      console.log("MATCH EXP SERVER OK =", expResult);
    } catch (expErr) {
      console.error("MATCH EXP SERVER ERROR =", expErr);
      expResult = {
        ok: false,
        error: expErr?.message || "exp_error"
      };
    }

    await settlementRef.set({
      done: true,
      paid: true,
      paidAt: Date.now(),
      route: "settle-fee20-admin80-mission20-v4",
      type: "winner_settle",
      stakePMC: stake,
      grossPot,
      feeRateOnStake: MATCH_FEE_RATE_ON_STAKE,
      feePmc,
      adminMasterSharePmc,
      missionPoolSharePmc,
      missionPoolPmc: missionPoolResult.missionPoolPmc,
      winnerReceivePmc,
      winnerWalletKey: safeWalletKey(winnerWalletKey),
      adminWalletKey: ADMIN_WALLET_KEY,
      expResult,
      at: Date.now()
    });

    await db.ref("matchFeeTransactions").push({
      roomId,
      type: "match_fee_pmc",
      stakePMC: stake,
      grossPot,
      feeRateOnStake: MATCH_FEE_RATE_ON_STAKE,
      feePmc,
      adminMasterSharePmc,
      missionPoolSharePmc,
      missionPoolPmc: missionPoolResult.missionPoolPmc,
      winnerReceivePmc,
      winnerWalletKey: safeWalletKey(winnerWalletKey),
      adminWalletKey: ADMIN_WALLET_KEY,
      createdAt: Date.now(),
      status: "done"
    });

    await db.ref("walletTransactions").push({
      type: "match_winner_settle",
      roomId,
      stakePMC: stake,
      grossPot,
      feePmc,
      adminMasterSharePmc,
      missionPoolSharePmc,
      missionPoolPmc: missionPoolResult.missionPoolPmc,
      winnerReceivePmc,
      winnerWalletKey: safeWalletKey(winnerWalletKey),
      adminWalletKey: ADMIN_WALLET_KEY,
      createdAt: Date.now(),
      status: "done"
    });

    return res.status(200).json({
      ok: true,
      type: "winner_settle",
      stakePMC: stake,
      grossPot,
      feePmc,
      adminMasterSharePmc,
      missionPoolSharePmc,
      missionPoolPmc: missionPoolResult.missionPoolPmc,
      winnerReceivePmc,
      winnerWalletKey: safeWalletKey(winnerWalletKey),
      adminWalletKey: ADMIN_WALLET_KEY,
      winnerPmcBalance: winnerAfter?.pmcBalance ?? null,
      adminPmcBalance: adminAfter?.pmcBalance ?? null,
      doPmcBalance: doPmcBalanceAfter,
      denPmcBalance: denPmcBalanceAfter,
      expResult
    });
  } catch (err) {
    console.error("pmc settle error =", err);

    return res.status(500).json({
      ok: false,
      error: err?.message || "server_error"
    });
  }
};