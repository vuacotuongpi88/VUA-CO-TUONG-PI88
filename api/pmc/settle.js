const { getDatabase } = require("firebase-admin/database");
const adminBundle = require("../_firebaseAdmin");

const LEVEL_MAX = 160;

function safeWalletKey(walletKey) {
  return String(walletKey || "").replace(/[.#$\[\]\/]/g, "_");
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

async function reserveServerLevelPairCount(db, walletKey, opponentKey, roomId) {
  const todayKey = getTodayKeyVN();
  const safeMe = safeWalletKey(walletKey);
  const safeOpp = safeWalletKey(opponentKey);
  const safeRoom = safeWalletKey(roomId);

  const roomClaimRef = db.ref(
    `levelPairRoomClaimsV3/${safeMe}/${todayKey}/${safeOpp}/${safeRoom}`
  );

  const roomClaimTx = await roomClaimRef.transaction(current => {
    if (current && current.done) return;

    return {
      done: true,
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

async function awardOnePlayerExp(db, roomId, walletKey, opponentKey, resultType) {
  const safeKey = safeWalletKey(walletKey);
  const safeRoom = safeWalletKey(roomId);

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

  const pair = await reserveServerLevelPairCount(db, walletKey, opponentKey, roomId);
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
    awardOnePlayerExp(db, roomId, doWalletKey, denWalletKey, doResult),
    awardOnePlayerExp(db, roomId, denWalletKey, doWalletKey, denResult)
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
    const currentPmc = Math.floor(Number(safeCurrent.pmcBalance ?? 0) || 0);
    const nextPmc = currentPmc + Math.floor(Number(delta || 0));

    if (nextPmc < 0) return;

    return {
      ...safeCurrent,
      balance: currentPi,
      pmcBalance: nextPmc,
      updatedAt: Date.now(),
      name: profile.name || safeCurrent.name || "Người chơi",
      photo: profile.photo || safeCurrent.photo || "images/do_tuong.png"
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
      if (current?.done || current?.locking) return;

      return {
        locking: true,
        done: false,
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
  [`matches/${roomId}/players/do/pmcBalance`]: Math.floor(Number(doAfter?.pmcBalance || 0) || 0),
  [`matches/${roomId}/players/den/pmcBalance`]: Math.floor(Number(denAfter?.pmcBalance || 0) || 0),
  [`matches/${roomId}/players/do/balance`]: Number(doAfter?.balance || 0) || 0,
  [`matches/${roomId}/players/den/balance`]: Number(denAfter?.balance || 0) || 0
});
      await settlementRef.set({
        done: true,
        type: "draw_refund",
        refundedEach: stake,
        feePmc: 0,
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

    // THẮNG => ăn phí 2% từ tổng pot.
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
    const doWalletAfterSnap = await db.ref("wallets/" + safeWalletKey(doWalletKey)).once("value");
const denWalletAfterSnap = await db.ref("wallets/" + safeWalletKey(denWalletKey)).once("value");

const doWalletAfter = doWalletAfterSnap.val() || {};
const denWalletAfter = denWalletAfterSnap.val() || {};

const doPmcBalanceAfter = Math.floor(Number(doWalletAfter.pmcBalance || 0) || 0);
const denPmcBalanceAfter = Math.floor(Number(denWalletAfter.pmcBalance || 0) || 0);

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
      route: "settle-exp-v3-fixed",
      type: "winner_settle",
      grossPot,
      feePmc,
      winnerReceivePmc,
      winnerWalletKey: safeWalletKey(winnerWalletKey),
      adminWalletKey: "pi_admin_master",
      expResult,
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
