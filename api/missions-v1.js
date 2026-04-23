const { getDatabase } = require('firebase-admin/database');
const adminBundle = require('./_firebaseAdmin.js');

const ADMIN_TREASURY_WALLET_KEY = String(
  process.env.ADMIN_TREASURY_WALLET_KEY || 'pi_admin_master'
).trim();
const TREASURY_SHARE_RATIO = Number(process.env.MISSION_TREASURY_SHARE_RATIO || 0.30);
const VN_OFFSET_MS = 7 * 60 * 60 * 1000;

function safeKey(value) {
  return String(value || '').replace(/[.#$\[\]/]/g, '_');
}

function nowMs() {
  return Date.now();
}

function clamp(n, min, max) {
  return Math.max(min, Math.min(max, n));
}

function localDate(ts = Date.now()) {
  return new Date(ts + VN_OFFSET_MS);
}

function dayStartMs(ts = Date.now()) {
  const d = localDate(ts);
  d.setUTCHours(0, 0, 0, 0);
  return d.getTime() - VN_OFFSET_MS;
}

function monthStartMs(ts = Date.now()) {
  const d = localDate(ts);
  d.setUTCDate(1);
  d.setUTCHours(0, 0, 0, 0);
  return d.getTime() - VN_OFFSET_MS;
}

function weekStartMs(ts = Date.now()) {
  const d = localDate(ts);
  const day = d.getUTCDay() || 7;
  d.setUTCDate(d.getUTCDate() - day + 1);
  d.setUTCHours(0, 0, 0, 0);
  return d.getTime() - VN_OFFSET_MS;
}

function localDayKey(ts = Date.now()) {
  const d = localDate(ts);
  const y = d.getUTCFullYear();
  const m = String(d.getUTCMonth() + 1).padStart(2, '0');
  const day = String(d.getUTCDate()).padStart(2, '0');
  return `${y}${m}${day}`;
}

function localMonthKey(ts = Date.now()) {
  const d = localDate(ts);
  const y = d.getUTCFullYear();
  const m = String(d.getUTCMonth() + 1).padStart(2, '0');
  return `${y}${m}`;
}

function localWeekKey(ts = Date.now()) {
  const start = weekStartMs(ts);
  return `W${localDayKey(start)}`;
}

function countChildren(obj) {
  if (!obj || typeof obj !== 'object') return 0;
  return Object.keys(obj).length;
}

function formatRewardText(amountPmc) {
  return `${Math.max(0, Math.floor(Number(amountPmc) || 0)).toLocaleString('vi-VN')} PMC`;
}
function readPmcBalance(obj) {
  return Math.max(
    0,
    Math.floor(Number(obj?.pmcBalance ?? obj?.pmc ?? 0) || 0)
  );
}
function missionDefinitions() {
  return [
    {
      id: 'daily_login',
      tab: 'day',
      title: 'Lộc đăng nhập',
      desc: 'Vào game hôm nay là có quà mở màn.',
      target: 1,
      metricKey: 'alwaysOne',
      periodType: 'day',
      rewardRate: 0.00015,
      minPmc: 2,
      maxPmc: 8,
      note: 'Quà mở hàng nhẹ, lấy vui mỗi ngày.'
    },
    {
      id: 'daily_play_3',
      tab: 'day',
      title: 'Cày 3 ván',
      desc: 'Hoàn thành 3 ván trong ngày để mở khóa thưởng.',
      target: 3,
      metricKey: 'dayMatches',
      periodType: 'day',
      rewardRate: 0.00035,
      minPmc: 5,
      maxPmc: 18,
      note: 'Chơi đều là có quà, nhưng không đốt quỹ.'
    },
    {
      id: 'daily_win_1',
      tab: 'day',
      title: 'Thắng mở hàng',
      desc: 'Có ít nhất 1 trận thắng hôm nay.',
      target: 1,
      metricKey: 'dayWins',
      periodType: 'day',
      rewardRate: 0.00045,
      minPmc: 6,
      maxPmc: 25,
      note: 'Thắng trận đầu có lộc nhỏ.'
    },
    {
      id: 'weekly_play_10',
      tab: 'week',
      title: 'Chiến thần tuần',
      desc: 'Hoàn thành 10 ván trong tuần.',
      target: 10,
      metricKey: 'weekMatches',
      periodType: 'week',
      rewardRate: 0.0008,
      minPmc: 12,
      maxPmc: 60,
      note: 'Mốc tuần cho người chăm chơi.'
    },
    {
      id: 'weekly_win_5',
      tab: 'week',
      title: '5 chiến thắng',
      desc: 'Thắng 5 trận trong tuần để ăn quỹ lớn hơn.',
      target: 5,
      metricKey: 'weekWins',
      periodType: 'week',
      rewardRate: 0.0010,
      minPmc: 15,
      maxPmc: 80,
      note: 'Thắng nhiều thì thưởng khá hơn.'
    },
    {
      id: 'weekly_active_3',
      tab: 'week',
      title: 'Chuyên cần 3 ngày',
      desc: 'Có ít nhất 3 ngày trong tuần hoàn thành ván cờ.',
      target: 3,
      metricKey: 'weekActiveDays',
      periodType: 'week',
      rewardRate: 0.0009,
      minPmc: 14,
      maxPmc: 70,
      note: 'Giữ nhịp chơi đều là được cộng.'
    },
    {
      id: 'monthly_play_30',
      tab: 'month',
      title: 'Tháng siêng năng',
      desc: 'Hoàn thành 30 ván trong tháng.',
      target: 30,
      metricKey: 'monthMatches',
      periodType: 'month',
      rewardRate: 0.0020,
      minPmc: 40,
      maxPmc: 180,
      note: 'Mốc tháng đáng giá nhưng vẫn sống quỹ.'
    },
    {
      id: 'monthly_win_15',
      tab: 'month',
      title: '15 chiến thắng tháng',
      desc: 'Thắng 15 trận trong tháng để mở khóa thưởng VIP.',
      target: 15,
      metricKey: 'monthWins',
      periodType: 'month',
      rewardRate: 0.0025,
      minPmc: 55,
      maxPmc: 240,
      note: 'Ngon hơn mốc ngày, nhưng không quá tay.'
    },
    {
      id: 'monthly_active_10',
      tab: 'month',
      title: 'Đều đặn 10 ngày',
      desc: 'Có ít nhất 10 ngày trong tháng chơi đủ ván.',
      target: 10,
      metricKey: 'monthActiveDays',
      periodType: 'month',
      rewardRate: 0.0022,
      minPmc: 45,
      maxPmc: 200,
      note: 'Thưởng bền cho người chơi đều.'
    },
    {
      id: 'ref_1',
      tab: 'referral',
      title: 'Mời 1 bạn',
      desc: 'Có 1 bạn bè hợp lệ trong danh sách bạn hữu.',
      target: 1,
      metricKey: 'friendCount',
      periodType: 'lifetime',
      rewardRate: 0.0008,
      minPmc: 8,
      maxPmc: 35,
      note: 'Mốc mở đầu nhẹ, dễ kích hoạt.'
    },
    {
      id: 'ref_10',
      tab: 'referral',
      title: 'Mời 10 bạn',
      desc: 'Đạt 10 bạn bè hợp lệ để mở khóa quỹ lớn.',
      target: 10,
      metricKey: 'friendCount',
      periodType: 'lifetime',
      rewardRate: 0.0020,
      minPmc: 30,
      maxPmc: 150,
      note: 'Có kéo người thật thì thưởng mới dày hơn.'
    },
    {
      id: 'ref_100',
      tab: 'referral',
      title: 'Mời 100 bạn',
      desc: 'Đạt 100 bạn hữu thật để nhận thưởng cộng đồng lớn.',
      target: 100,
      metricKey: 'friendCount',
      periodType: 'lifetime',
      rewardRate: 0.0060,
      minPmc: 120,
      maxPmc: 700,
      note: 'Mốc lớn, nhưng vẫn trong ngưỡng chịu được.'
    },
    {
      id: 'ref_1000',
      tab: 'referral',
      title: 'Mời 1000 bạn',
      desc: 'Đại sứ bàn cờ cấp lớn.',
      target: 1000,
      metricKey: 'friendCount',
      periodType: 'lifetime',
      rewardRate: 0.0120,
      minPmc: 600,
      maxPmc: 1500,
      note: 'Mốc rất khó, thưởng lớn vừa đủ.'
    },
    {
      id: 'ref_10000',
      tab: 'referral',
      title: 'Mời 10000 bạn',
      desc: 'Sứ giả truyền lửa cấp huyền thoại.',
      target: 10000,
      metricKey: 'friendCount',
      periodType: 'lifetime',
      rewardRate: 0.0200,
      minPmc: 4000,
      maxPmc: 8000,
      note: 'Huyền thoại thì có thưởng lớn, nhưng không phá app.'
    }
  ];
}

function periodKeyForMission(def, now = Date.now()) {
  if (def.periodType === 'day') return localDayKey(now);
  if (def.periodType === 'week') return localWeekKey(now);
  if (def.periodType === 'month') return localMonthKey(now);
  return 'lifetime';
}

function missionClaimRef(db, walletKey, def, now = Date.now()) {
  const periodKey = periodKeyForMission(def, now);
  return db.ref(`missionClaimsV1/${walletKey}/${def.id}__${periodKey}`);
}

async function buildMetrics(db, walletKey, now = Date.now()) {
  const dayStart = dayStartMs(now);
  const weekStart = weekStartMs(now);
  const monthStart = monthStartMs(now);

  const [matchesSnap, friendsSnap] = await Promise.all([
    db.ref('matches').once('value'),
    db.ref(`social/friends/${walletKey}`).once('value')
  ]);

  const metrics = {
    alwaysOne: 1,
    friendCount: countChildren(friendsSnap.val()),
    dayMatches: 0,
    dayWins: 0,
    weekMatches: 0,
    weekWins: 0,
    monthMatches: 0,
    monthWins: 0,
    weekActiveDays: 0,
    monthActiveDays: 0
  };

  const weekDays = new Set();
  const monthDays = new Set();

  matchesSnap.forEach(child => {
    const room = child.val() || {};
    const players = room.players || {};
    const doKey = String(players?.do?.walletKey || '').trim();
    const denKey = String(players?.den?.walletKey || '').trim();

    let side = '';
    if (doKey === walletKey) side = 'do';
    if (denKey === walletKey) side = 'den';
    if (!side) return;

    const winner = String(room.winner || '').trim();
    if (!winner) return;

    const eventTs = Number(room.updatedAt || room.createdAt || 0);
    if (!Number.isFinite(eventTs) || eventTs <= 0) return;

    if (eventTs >= dayStart) {
      metrics.dayMatches += 1;
      if (winner === side) metrics.dayWins += 1;
    }

    if (eventTs >= weekStart) {
      metrics.weekMatches += 1;
      weekDays.add(localDayKey(eventTs));
      if (winner === side) metrics.weekWins += 1;
    }

    if (eventTs >= monthStart) {
      metrics.monthMatches += 1;
      monthDays.add(localDayKey(eventTs));
      if (winner === side) metrics.monthWins += 1;
    }
  });

  metrics.weekActiveDays = weekDays.size;
  metrics.monthActiveDays = monthDays.size;
  return metrics;
}

function rewardAmountPmc(def, missionPoolPmc) {
  const raw = Math.floor(Number(missionPoolPmc || 0) * Number(def.rewardRate || 0));
  return clamp(raw, Number(def.minPmc || 0), Number(def.maxPmc || 0));
}

async function buildBoard(db, walletKey, now = Date.now()) {
  const defs = missionDefinitions();
  const [walletSnap, treasurySnap, metrics] = await Promise.all([
    db.ref(`wallets/${walletKey}`).once('value'),
    db.ref(`wallets/${safeKey(ADMIN_TREASURY_WALLET_KEY)}`).once('value'),
    buildMetrics(db, walletKey, now)
  ]);

  const walletVal = walletSnap.val() || {};
  const treasuryVal = treasurySnap.val() || {};
  const treasuryPmc = readPmcBalance(treasuryVal);
  const missionPoolPmc = Math.max(0, Math.floor(treasuryPmc * TREASURY_SHARE_RATIO));

  const tabs = { day: [], week: [], month: [], referral: [] };
  let claimableTotalPmc = 0;
  let claimableCount = 0;

  for (const def of defs) {
    const progress = Math.max(0, Number(metrics[def.metricKey] || 0));
    const rewardPmc = rewardAmountPmc(def, missionPoolPmc);
    const target = Math.max(1, Number(def.target || 1));
    const progressPercent = Math.floor(Math.max(0, Math.min(1, progress / target)) * 100);
    const claimSnap = await missionClaimRef(db, walletKey, def, now).once('value');
    const claimVal = claimSnap.val() || null;
    const claimed = !!(claimVal && claimVal.status === 'done');
    const ready = !claimed && progress >= target && rewardPmc > 0 && treasuryPmc >= rewardPmc;

    if (ready) {
      claimableTotalPmc += rewardPmc;
      claimableCount += 1;
    }

    tabs[def.tab].push({
      id: def.id,
      title: def.title,
      desc: def.desc,
      target,
      progress,
      progressText: `${Math.min(progress, target)}/${target}`,
      progressPercent,
      rewardPmc,
      rewardText: formatRewardText(rewardPmc),
      note: def.note,
      ready,
      claimed,
      claimedAt: claimVal?.claimedAt || null,
      periodKey: periodKeyForMission(def, now)
    });
  }

  return {
    ok: true,
    walletKey,
    walletName: String(walletVal.name || walletVal.username || 'Người chơi'),
    treasury: {
      walletKey: safeKey(ADMIN_TREASURY_WALLET_KEY),
      treasuryPmc,
      missionPoolPmc,
      shareRatio: TREASURY_SHARE_RATIO
    },
    metrics,
    tabs,
    claimableTotalPmc,
    claimableCount,
    generatedAt: now
  };
}

function walletTxnRef(db) {
  return db.ref('walletTransactions');
}

function missionTxnRef(db) {
  return db.ref('missionRewardLogsV1');
}

async function txAdjustPmc(ref, delta, extra = {}, preRead = null) {
  let afterBalance = 0;

  const result = await new Promise((resolve, reject) => {
    ref.transaction(
      current => {
        const baseCurrent =
          current && typeof current === 'object'
            ? current
            : (preRead && typeof preRead === 'object' ? preRead : {});

        const currentPmc = readPmcBalance(baseCurrent);
        const nextPmc = currentPmc + Math.floor(Number(delta || 0));

        if (nextPmc < 0) return;

        afterBalance = nextPmc;

        return {
          ...baseCurrent,
          ...extra,
          pmcBalance: nextPmc,
          updatedAt: nowMs()
        };
      },
      (err, committed) => {
        if (err) return reject(err);
        resolve({ committed, afterBalance });
      },
      false
    );
  });

  return result;
}

async function claimMission(db, walletKey, missionId, now = Date.now()) {
  const def = missionDefinitions().find(item => item.id === missionId);
  if (!def) {
    throw new Error('Không tìm thấy nhiệm vụ.');
  }

  const board = await buildBoard(db, walletKey, now);
  const mission = (board.tabs[def.tab] || []).find(item => item.id === missionId);
  if (!mission) {
    throw new Error('Nhiệm vụ không tồn tại trong bảng hiện tại.');
  }
  if (mission.claimed) {
    throw new Error('Nhiệm vụ này đã nhận rồi.');
  }
  if (!mission.ready) {
    throw new Error('Chưa đủ điều kiện nhận nhiệm vụ này.');
  }

  const claimRef = missionClaimRef(db, walletKey, def, now);
  const lock = await new Promise((resolve, reject) => {
    claimRef.transaction(
      current => {
        if (current && current.status === 'done') return;
        if (current && current.status === 'processing') return;
        return {
          status: 'processing',
          missionId,
          walletKey,
          lockedAt: now
        };
      },
      (err, committed) => {
        if (err) return reject(err);
        resolve({ committed });
      },
      false
    );
  });

  if (!lock.committed) {
    throw new Error('Nhiệm vụ đang được xử lý hoặc đã nhận rồi.');
  }

  const treasuryRef = db.ref(`wallets/${safeKey(ADMIN_TREASURY_WALLET_KEY)}`);
const userRef = db.ref(`wallets/${walletKey}`);

const [treasuryPreSnap, userPreSnap] = await Promise.all([
  treasuryRef.once('value'),
  userRef.once('value')
]);

const treasuryPreRead =
  treasuryPreSnap.val() && typeof treasuryPreSnap.val() === 'object'
    ? treasuryPreSnap.val()
    : {};

const userPreRead =
  userPreSnap.val() && typeof userPreSnap.val() === 'object'
    ? userPreSnap.val()
    : {};

try {
  const treasuryTx = await txAdjustPmc(
    treasuryRef,
    -mission.rewardPmc,
    { name: 'Ví phí hệ thống' },
    treasuryPreRead
  );

    if (!treasuryTx.committed) {
      await claimRef.remove().catch(() => {});
      throw new Error('Ví phí hệ thống hiện không đủ quỹ để trả thưởng.');
    }

    const userTx = await txAdjustPmc(userRef, mission.rewardPmc, {}, userPreRead);
    if (!userTx.committed) {
      await txAdjustPmc(
  treasuryRef,
  mission.rewardPmc,
  { name: 'Ví phí hệ thống' },
  treasuryPreRead
).catch(() => {});
      await claimRef.remove().catch(() => {});
      throw new Error('Không cộng được thưởng vào ví người chơi.');
    }

    const txPayload = {
      type: 'mission_reward_pmc',
      missionId,
      missionTitle: mission.title,
      walletKey,
      sourceWalletKey: safeKey(ADMIN_TREASURY_WALLET_KEY),
      amountPMC: mission.rewardPmc,
      periodKey: mission.periodKey,
      createdAt: now,
      status: 'done'
    };

    await Promise.all([
      walletTxnRef(db).push().set(txPayload),
      missionTxnRef(db).push().set(txPayload),
      claimRef.set({
        status: 'done',
        missionId,
        walletKey,
        amountPmc: mission.rewardPmc,
        periodKey: mission.periodKey,
        claimedAt: now
      })
    ]);

    return {
      ok: true,
      missionId,
      missionTitle: mission.title,
      amountPmc: mission.rewardPmc,
      newPmcBalance: userTx.afterBalance,
      periodKey: mission.periodKey
    };
  } catch (err) {
    await claimRef.remove().catch(() => {});
    throw err;
  }
}

module.exports = async function handler(req, res) {
  if (req.method !== 'POST') {
    return res.status(405).json({ ok: false, error: 'Method not allowed' });
  }

  try {
    const body = typeof req.body === 'string' ? JSON.parse(req.body || '{}') : (req.body || {});
    const action = String(body.action || 'board').trim().toLowerCase();
    const rawWalletKey = String(req.headers['x-wallet-key'] || body.walletKey || '').trim();
    const walletKey = safeKey(rawWalletKey);

    if (!walletKey) {
      return res.status(400).json({ ok: false, error: 'Thiếu walletKey.' });
    }

    const adminApp = adminBundle.app || adminBundle;
    const db = getDatabase(adminApp);

    if (action === 'claim') {
      const missionId = String(body.missionId || '').trim();
      if (!missionId) {
        return res.status(400).json({ ok: false, error: 'Thiếu missionId.' });
      }

      const claimed = await claimMission(db, walletKey, missionId, nowMs());
      return res.status(200).json(claimed);
    }

    const board = await buildBoard(db, walletKey, nowMs());
    return res.status(200).json(board);
  } catch (err) {
    console.error('MISSIONS_V1_FAIL:', err);
    return res.status(500).json({
      ok: false,
      error: err?.message || 'Lỗi hệ thống nhiệm vụ.'
    });
  }
};