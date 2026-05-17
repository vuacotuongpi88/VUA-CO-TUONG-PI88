const { getDatabase } = require("firebase-admin/database");
const { getAuth } = require("firebase-admin/auth");

let adminBundle;
try {
  // Nếu _firebaseAdmin.js nằm cùng thư mục api/pmc
  adminBundle = require("./_firebaseAdmin.js");
} catch (e1) {
  // Nếu _firebaseAdmin.js nằm ở api/
  adminBundle = require("../_firebaseAdmin.js");
}

const adminApp = adminBundle.app || adminBundle;

const LEVEL_MAX = 160;
const MATCH_FEE_RATE = 0.02; // 2% phí hệ thống. Muốn không lấy phí thì đổi thành 0.

function safeWalletKey(walletKey) {
  return String(walletKey || "").trim().replace(/[.#$\[\]\/]/g, "_");
}

function safeRoomKey(value) {
  return String(value || "").trim().replace(/[.#$\[\]\/]/g, "_");
}

function getLevelXpNeedFromLevel(level) {
  const lv = Math.max(1, Math.min(LEVEL_MAX - 1, Math.floor(Number(level || 1))));
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

async function verifyFirebaseUser(req) {
  const token = String(req.headers.authorization || "")
    .replace(/^Bearer\s+/i, "")
    .trim();

  if (!token) {
    const err = new Error("Thiếu Firebase token");
    err.statusCode = 401;
    throw err;
  }

  return await getAuth(adminApp).verifyIdToken(token);
}

async function reserveServerLevelPairCount(db, walletKey, opponentKey, matchKey) {
  const todayKey = getTodayKeyVN();
  const safeMe = safeWalletKey(walletKey);
  const safeOpp = safeWalletKey(opponentKey);
  const safeMatch = safeWalletKey(matchKey);

  const roomClaimRef = db.ref(
    `levelPairRoomClaimsV3/${safeMe}/${todayKey}/${safeOpp}/${safeMatch}`
  );

  const roomClaimTx = await runTx(roomClaimRef, (current) => {
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

  await runTx(countRef, (current) => {
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

  const claimTx = await runTx(claimRef, (current) => {
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

  const pair = await reserveServerLevelPairCount(
    db,
    walletKey,
    opponentKey,
    matchKey || roomId
  );

  const absXp = getRepeatAdjustedXpAbs(pair.count);
  const xpDelta = resultType === "win" ? absXp : -absXp;

  let afterMeta = null;

  const levelRef = db.ref(`wallets/${safeKey}/levelMeta`);

  await runTx(levelRef, (current) => {
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

  if (winnerRaw === "do" || winnerRaw === "red") winnerSide = "do";
  if (winnerRaw === "den" || winnerRaw === "black") winnerSide = "den";

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
  const safeKey = safeWalletKey(walletKey);
  const ref = db.ref("wallets/" + safeKey);

  const preSnap = await ref.once("value");
  const preWallet = preSnap.val() || {};

  let nextWallet = null;

  const result = await runTx(ref, (current) => {
    const safeCurrent =
      current && typeof current === "object"
        ? current
        : preWallet && typeof preWallet === "object"
          ? { ...preWallet }
          : {};

    const currentPi = Number(safeCurrent.balance ?? safeCurrent.piBalance ?? 0) || 0;
    const currentPmc = Math.floor(Number(safeCurrent.pmcBalance ?? 0) || 0);
    const nextPmc = currentPmc + Math.floor(Number(delta || 0));

    if (nextPmc < 0) return;

    nextWallet = {
      ...safeCurrent,
      balance: currentPi,
      piBalance: Number(safeCurrent.piBalance ?? currentPi) || 0,
      pmcBalance: nextPmc,
      updatedAt: Date.now(),
      name: profile.name || safeCurrent.name || "Người chơi",
      photo: profile.photo || safeCurrent.photo || "images/do_tuong.png"
    };

    return nextWallet;
  });

  if (!result.committed) return null;

  return result.snapshot?.val() || nextWallet || null;
}

function getStakeContribution(lock, stake, fallbackToStake) {
  if (!lock && fallbackToStake) return stake;
  if (!lock || typeof lock !== "object") return 0;
  if (!lock.done && !lock.walletKey) return 0;
  if (lock.isTicketUsed) return 0;

  const lockedStake = Math.max(0, Math.floor(Number(lock.stake || stake || 0) || 0));
  return lockedStake;
}

function getPlayerProfile(player, fallbackName) {
  return {
    name:
      player.name ||
      player.usernameNorm ||
      player.username ||
      player.walletKey ||
      fallbackName,
    photo: player.photo || "images/do_tuong.png"
  };
}

async function writeRoomBalances(db, roomId, doWalletKey, denWalletKey) {
  const [doWalletAfterSnap, denWalletAfterSnap] = await Promise.all([
    db.ref("wallets/" + safeWalletKey(doWalletKey)).once("value"),
    db.ref("wallets/" + safeWalletKey(denWalletKey)).once("value")
  ]);

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

  return {
    doPmcBalanceAfter,
    denPmcBalanceAfter,
    doWalletAfter,
    denWalletAfter
  };
}

async function repairOldSettledRoomIfNeeded(db, roomId, room, existedSettlement) {
  const winnerRaw = String(room.winner || "").trim().toLowerCase();

  let expResult = existedSettlement.expResult || null;

  if (!expResult && winnerRaw && winnerRaw !== "hoa" && winnerRaw !== "draw") {
    try {
      expResult = await awardMatchExpServer(db, roomId, room);
    } catch (expErr) {
      expResult = {
        ok: false,
        error: expErr?.message || "exp_error"
      };
    }
  }

  const patch = {
    paid: true,
    done: true,
    expResult,
    repairedPaidAt: Date.now(),
    route: existedSettlement.route || "repair-old-settlement-paid-flag"
  };

  if (!existedSettlement.paidAt) patch.paidAt = Date.now();

  await db.ref(`matches/${roomId}/settlement`).update(patch);

  return {
    ok: true,
    alreadySettled: true,
    repaired: true,
    expResult
  };
}

module.exports = async function handler(req, res) {
  res.setHeader("Content-Type", "application/json; charset=utf-8");

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

    // Giữ route mua skin cũ của mày.
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

    const decoded = await verifyFirebaseUser(req);
    const uid = decoded.uid;

    const roomId = safeRoomKey(body.roomId);

    if (!roomId) {
      return res.status(400).json({
        ok: false,
        error: "missing_roomId"
      });
    }

    const db = getDatabase(adminApp);
    const roomRef = db.ref(`matches/${roomId}`);
    const settlementRef = db.ref(`matches/${roomId}/settlement`);

    const roomSnap = await roomRef.once("value");
    const room = roomSnap.val() || {};

    if (!room || !room.players) {
      return res.status(404).json({
        ok: false,
        error: "room_not_found"
      });
    }

    const winnerRaw = String(room.winner || "").trim().toLowerCase();
    const stake = Math.max(0, Math.floor(Number(room.stakePMC || 0) || 0));

    const doPlayer = room.players?.do || {};
    const denPlayer = room.players?.den || {};

    const isPlayer =
      String(doPlayer.uid || "") === String(uid) ||
      String(denPlayer.uid || "") === String(uid);

    if (!isPlayer) {
      return res.status(403).json({
        ok: false,
        error: "not_room_player"
      });
    }

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

    const doLock = room?.stakeLocked?.do || null;
    const denLock = room?.stakeLocked?.den || null;

    // Nếu phòng cũ chưa có stakeLocked thì fallback stake*2 để không chết phòng cũ.
    const fallbackOldRoom = !doLock && !denLock && stake > 0;

    const doContribution = getStakeContribution(doLock, stake, fallbackOldRoom);
    const denContribution = getStakeContribution(denLock, stake, fallbackOldRoom);
    const grossPot = Math.max(0, doContribution + denContribution);

    // Nếu không có tiền cược, vẫn cộng/trừ EXP rồi đánh dấu paid để client dọn phòng.
    if (!grossPot || !stake) {
      let expResult = null;

      try {
        expResult = await awardMatchExpServer(db, roomId, room);
        console.log("MATCH_EXP_ONLY_OK", expResult);
      } catch (expErr) {
        console.error("MATCH_EXP_ONLY_ERROR", expErr);
        expResult = {
          ok: false,
          error: expErr?.message || "exp_error"
        };
      }

      await settlementRef.update({
        done: true,
        paid: true,
        route: "exp-only-no-pot",
        type: "exp_only",
        stakePMC: stake,
        grossPot,
        doContribution,
        denContribution,
        expResult,
        paidAt: Date.now(),
        at: Date.now()
      });

      return res.status(200).json({
        ok: true,
        type: "exp_only",
        warning: "no_pot_but_exp_awarded",
        expResult
      });
    }

    // Khóa settle để không chia tiền / cộng EXP 2 lần.
    const lockResult = await runTx(settlementRef, (current) => {
      if (current?.done === true || current?.paid === true) return;
      if (current?.locking === true && current?.lockingAt && Date.now() - Number(current.lockingAt) < 30000) return;

      return {
        locking: true,
        done: false,
        paid: false,
        lockingBy: uid,
        lockingAt: Date.now(),
        at: Date.now()
      };
    });

    if (!lockResult.committed) {
      const existedSnap = await settlementRef.once("value");
      const existed = existedSnap.val() || {};

      if (existed.done === true && existed.paid !== true) {
        const repaired = await repairOldSettledRoomIfNeeded(db, roomId, room, existed);
        return res.status(200).json(repaired);
      }

      let expResult = existed.expResult || null;

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
        settlement: existed,
        expResult
      });
    }

    // HÒA: hoàn đúng phần ai đã nộp PMC. Vé miễn phí thì không hoàn PMC.
    if (winnerRaw === "hoa" || winnerRaw === "draw") {
      const doAfter =
        doContribution > 0
          ? await adjustPmcWalletByKey(db, doWalletKey, doContribution, getPlayerProfile(doPlayer, "Người chơi đỏ"))
          : null;

      const denAfter =
        denContribution > 0
          ? await adjustPmcWalletByKey(db, denWalletKey, denContribution, getPlayerProfile(denPlayer, "Người chơi đen"))
          : null;

      const balances = await writeRoomBalances(db, roomId, doWalletKey, denWalletKey);

      const settlementData = {
        done: true,
        paid: true,
        type: "draw_refund",
        refundedEach: stake,
        doRefund: doContribution,
        denRefund: denContribution,
        grossPot,
        feePmc: 0,
        expResult: null,
        winner: "hoa",
        paidAt: Date.now(),
        at: Date.now()
      };

      await settlementRef.set(settlementData);

      await db.ref("walletTransactions").push({
        type: "match_draw_refund",
        roomId,
        refundedEach: stake,
        doRefund: doContribution,
        denRefund: denContribution,
        doWalletKey: safeWalletKey(doWalletKey),
        denWalletKey: safeWalletKey(denWalletKey),
        createdAt: Date.now(),
        status: "done"
      });

      console.log("MATCH_DRAW_REFUND_OK", {
        roomId,
        doWalletKey,
        denWalletKey,
        doContribution,
        denContribution,
        grossPot
      });

      return res.status(200).json({
        ok: true,
        type: "draw_refund",
        refundedEach: stake,
        doRefund: doContribution,
        denRefund: denContribution,
        doPmcBalance: doAfter?.pmcBalance ?? balances.doPmcBalanceAfter,
        denPmcBalance: denAfter?.pmcBalance ?? balances.denPmcBalanceAfter,
        expResult: null
      });
    }

    let winnerSide = "";
    let winnerWalletKey = "";
    let loserWalletKey = "";
    let winnerProfile = {};

    if (winnerRaw === "do" || winnerRaw === "red") {
      winnerSide = "do";
      winnerWalletKey = doWalletKey;
      loserWalletKey = denWalletKey;
      winnerProfile = getPlayerProfile(doPlayer, "Người chơi đỏ");
    } else if (winnerRaw === "den" || winnerRaw === "black") {
      winnerSide = "den";
      winnerWalletKey = denWalletKey;
      loserWalletKey = doWalletKey;
      winnerProfile = getPlayerProfile(denPlayer, "Người chơi đen");
    } else {
      await settlementRef.update({
        locking: false,
        done: false,
        paid: false,
        error: "invalid_winner_value",
        errorAt: Date.now()
      });

      return res.status(400).json({
        ok: false,
        error: "invalid_winner_value"
      });
    }

    const feePmc = Math.max(0, Math.floor(grossPot * MATCH_FEE_RATE));
    const winnerReceivePmc = Math.max(0, grossPot - feePmc);

    const winnerAfter = await adjustPmcWalletByKey(
      db,
      winnerWalletKey,
      winnerReceivePmc,
      winnerProfile
    );

    if (!winnerAfter) {
      await settlementRef.update({
        locking: false,
        done: false,
        paid: false,
        error: "winner_wallet_update_failed",
        errorAt: Date.now()
      });

      return res.status(500).json({
        ok: false,
        error: "winner_wallet_update_failed"
      });
    }

    let adminAfter = null;

    if (feePmc > 0) {
      adminAfter = await adjustPmcWalletByKey(
        db,
        "pi_admin_master",
        feePmc,
        {
          name: "Ví phí hệ thống",
          photo: "images/do_tuong.png"
        }
      );
    }

    const balances = await writeRoomBalances(db, roomId, doWalletKey, denWalletKey);

    let expResult = null;

    try {
      expResult = await awardMatchExpServer(db, roomId, room);
      console.log("MATCH_EXP_SERVER_OK", expResult);
    } catch (expErr) {
      console.error("MATCH_EXP_SERVER_ERROR", expErr);
      expResult = {
        ok: false,
        error: expErr?.message || "exp_error"
      };
    }

    const settlementData = {
      done: true,
      paid: true,
      route: "backend-settle-v4-paid-flag",
      type: "winner_settle",
      winner: winnerSide,
      grossPot,
      feeRate: MATCH_FEE_RATE,
      feePmc,
      winnerReceivePmc,
      winnerWalletKey: safeWalletKey(winnerWalletKey),
      loserWalletKey: safeWalletKey(loserWalletKey),
      adminWalletKey: "pi_admin_master",
      doContribution,
      denContribution,
      expResult,
      paidAt: Date.now(),
      at: Date.now()
    };

    await settlementRef.set(settlementData);

    await db.ref("matchFeeTransactions").push({
      roomId,
      type: "match_fee_pmc",
      grossPot,
      feeRate: MATCH_FEE_RATE,
      feePmc,
      winnerReceivePmc,
      winnerWalletKey: safeWalletKey(winnerWalletKey),
      loserWalletKey: safeWalletKey(loserWalletKey),
      adminWalletKey: "pi_admin_master",
      createdAt: Date.now(),
      status: "done"
    });

    await db.ref("walletTransactions").push({
      type: "match_winner_settle",
      roomId,
      winner: winnerSide,
      grossPot,
      feePmc,
      winnerReceivePmc,
      winnerWalletKey: safeWalletKey(winnerWalletKey),
      loserWalletKey: safeWalletKey(loserWalletKey),
      adminWalletKey: "pi_admin_master",
      doContribution,
      denContribution,
      createdAt: Date.now(),
      status: "done"
    });

    console.log("MATCH_SETTLE_OK", {
      roomId,
      winnerSide,
      grossPot,
      feePmc,
      winnerReceivePmc,
      winnerWalletKey: safeWalletKey(winnerWalletKey),
      loserWalletKey: safeWalletKey(loserWalletKey),
      doPmcBalance: balances.doPmcBalanceAfter,
      denPmcBalance: balances.denPmcBalanceAfter
    });

    return res.status(200).json({
      ok: true,
      type: "winner_settle",
      winner: winnerSide,
      grossPot,
      feePmc,
      winnerReceivePmc,
      winnerWalletKey: safeWalletKey(winnerWalletKey),
      loserWalletKey: safeWalletKey(loserWalletKey),
      adminWalletKey: "pi_admin_master",
      winnerPmcBalance: winnerAfter?.pmcBalance ?? null,
      adminPmcBalance: adminAfter?.pmcBalance ?? null,
      doPmcBalance: balances.doPmcBalanceAfter,
      denPmcBalance: balances.denPmcBalanceAfter,
      expResult
    });
  } catch (err) {
    console.error("PMC_SETTLE_ERROR", err);

    return res.status(err.statusCode || 500).json({
      ok: false,
      error: err?.message || "server_error"
    });
  }
};