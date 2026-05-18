const { getDatabase } = require('firebase-admin/database');
const adminBundle = require('./_firebaseAdmin.js');
const crypto = require('crypto');

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
function roundPmc(value) {
  const n = Number(value || 0);
  if (!Number.isFinite(n)) return 0;
  return Math.max(0, Math.round(n * 1000000) / 1000000);
}

// ===== QUỸ NHIỆM VỤ THEO TUẦN =====
// Quỹ missionPoolPmc dồn trong 1 tuần VN.
// Sang tuần mới, phần còn dư hoàn về ví admin master rồi reset về 0.
function missionPoolWeekKey(ts = Date.now()) {
  return localWeekKey(ts);
}

async function addPmcToAdminWallet(db, walletKey, amount) {
  const add = roundPmc(amount);
  if (add <= 0) {
    return {
      committed: true,
      after: null
    };
  }

  const ref = db.ref(`wallets/${safeKey(walletKey)}`);
  let after = null;

  const tx = await ref.transaction(current => {
    const cur = current && typeof current === 'object' ? current : {};
    const currentPmc = readPmcBalance(cur);
    after = roundPmc(currentPmc + add);

    return {
      ...cur,
      name: cur.name || 'Ví phí hệ thống',
      pmcBalance: after,
      updatedAt: nowMs()
    };
  });

  return {
    committed: !!tx.committed,
    after
  };
}

async function sweepExpiredMissionPoolWeek(db, adminWalletKey = ADMIN_TREASURY_WALLET_KEY, ts = Date.now()) {
  const currentWeekKey = missionPoolWeekKey(ts);
  const metaRef = db.ref('treasury/missionPoolMeta');

  let oldWeekKey = '';
  let shouldSweep = false;

  const metaTx = await metaRef.transaction(current => {
    const meta = current && typeof current === 'object' ? current : {};
    oldWeekKey = String(meta.currentWeekKey || '');

    // Lần đầu chạy bản tuần: chỉ đóng dấu tuần hiện tại, không quét bậy tiền đang có.
    if (!oldWeekKey) {
      return {
        ...meta,
        poolMode: 'week',
        currentWeekKey,
        createdAt: meta.createdAt || nowMs(),
        updatedAt: nowMs()
      };
    }

    if (oldWeekKey === currentWeekKey) {
      return {
        ...meta,
        poolMode: 'week',
        updatedAt: nowMs()
      };
    }

    if (meta.sweepLock) return;

    shouldSweep = true;

    return {
      ...meta,
      poolMode: 'week',
      sweepLock: `${oldWeekKey}_to_${currentWeekKey}`,
      sweepFromWeekKey: oldWeekKey,
      sweepToWeekKey: currentWeekKey,
      sweepStartedAt: nowMs(),
      updatedAt: nowMs()
    };
  });

  if (!metaTx.committed || !shouldSweep) {
    return {
      ok: true,
      swept: false,
      currentWeekKey,
      oldWeekKey,
      amountPmc: 0
    };
  }

  const poolRef = db.ref('treasury/missionPoolPmc');
  let sweptAmount = 0;

  const poolTx = await poolRef.transaction(current => {
    sweptAmount = roundPmc(Number(current || 0) || 0);
    return 0;
  });

  if (!poolTx.committed) {
    await metaRef.update({
      sweepLock: null,
      sweepError: 'pool_transaction_failed',
      updatedAt: nowMs()
    }).catch(() => {});

    throw new Error('Không quét được quỹ nhiệm vụ tuần cũ.');
  }

  let adminPmcAfter = null;

  if (sweptAmount > 0) {
    const adminTx = await addPmcToAdminWallet(db, adminWalletKey, sweptAmount);
    adminPmcAfter = adminTx.after;

    await db.ref('missionPoolSweepLogs').push({
      type: 'mission_pool_weekly_sweep',
      poolMode: 'week',
      fromWeekKey: oldWeekKey,
      toWeekKey: currentWeekKey,
      amountPmc: sweptAmount,
      adminWalletKey: safeKey(adminWalletKey),
      adminPmcAfter,
      createdAt: nowMs(),
      status: 'done'
    }).catch(() => {});
  }

  await metaRef.update({
    poolMode: 'week',
    currentWeekKey,
    previousWeekKey: oldWeekKey,
    lastSweptPmc: sweptAmount,
    lastSweptAt: nowMs(),
    adminWalletKey: safeKey(adminWalletKey),
    adminPmcAfter,
    sweepLock: null,
    sweepFromWeekKey: null,
    sweepToWeekKey: null,
    updatedAt: nowMs()
  }).catch(() => {});

  return {
    ok: true,
    swept: true,
    currentWeekKey,
    oldWeekKey,
    amountPmc: sweptAmount,
    adminPmcAfter
  };
}

async function subtractMissionPoolPmc(db, amount) {
  const sub = roundPmc(amount);
  if (sub <= 0) return { committed: true, after: null };

  const ref = db.ref('treasury/missionPoolPmc');
  let after = 0;

  const tx = await ref.transaction(current => {
    const cur = roundPmc(Number(current || 0) || 0);
    if (cur + 0.000001 < sub) return;

    after = roundPmc(cur - sub);
    return after;
  });

  return {
    committed: !!tx.committed,
    after
  };
}
function countChildren(obj) {
  if (!obj || typeof obj !== 'object') return 0;
  return Object.keys(obj).length;
}

function formatRewardText(amountPmc) {
  return `${Math.max(0, Math.floor(Number(amountPmc) || 0)).toLocaleString('vi-VN')} PMC`;
}

function readPmcBalance(obj) {
  const n = Number(obj?.pmcBalance ?? obj?.pmc ?? 0);
  if (!Number.isFinite(n)) return 0;
  return Math.max(0, Math.round(n * 1000000) / 1000000);
}

function missionDefinitions() {
  return [
    // =====================
    // NGÀY - giữ nhẹ để người mới có cảm giác có quà
    // =====================
    {
      id: 'daily_login',
      tab: 'day',
      title: 'Lộc đăng nhập',
      desc: 'Điểm danh hôm nay để giữ chuỗi nhận mốc tuần/tháng.',
      target: 1,
      metricKey: 'alwaysOne',
      periodType: 'day',
      rewardRate: 0.00015,
      minPmc: 2,
      maxPmc: 8,
      note: 'Quà nhỏ mỗi ngày, quan trọng là cộng ngày điểm danh.'
    },
    {
      id: 'daily_play_5',
      tab: 'day',
      title: 'Cày 5 ván',
      desc: 'Hoàn thành 5 ván trong ngày để mở khóa thưởng.',
      target: 5,
      metricKey: 'dayMatches',
      periodType: 'day',
      rewardRate: 0.00038,
      minPmc: 6,
      maxPmc: 20,
      note: 'Tăng từ 3 lên 5 ván cho đỡ farm quá nhanh.'
    },
    {
      id: 'daily_win_2',
      tab: 'day',
      title: 'Thắng 2 trận',
      desc: 'Có ít nhất 2 trận thắng hôm nay.',
      target: 2,
      metricKey: 'dayWins',
      periodType: 'day',
      rewardRate: 0.00050,
      minPmc: 7,
      maxPmc: 28,
      note: 'Thắng đủ mới có lộc, thưởng nhích nhẹ.'
    },

    // =====================
    // TUẦN - ép chơi đều, không cho 1 buổi ăn hết mốc
    // =====================
    {
      id: 'weekly_checkin_7',
      tab: 'week',
      title: 'Điểm danh đủ tuần',
      desc: 'Điểm danh đủ 7 ngày trong tuần.',
      target: 7,
      metricKey: 'weekCheckinDays',
      periodType: 'week',
      rewardRate: 0.00115,
      minPmc: 22,
      maxPmc: 95,
      note: 'Mốc này bắt buộc quay lại mỗi ngày trong tuần.'
    },
    {
      id: 'weekly_play_45',
      tab: 'week',
      title: 'Chiến thần tuần',
      desc: 'Hoàn thành 45 ván trong tuần.',
      target: 45,
      metricKey: 'weekMatches',
      periodType: 'week',
      rewardRate: 0.00125,
      minPmc: 24,
      maxPmc: 110,
      note: 'Tăng mạnh số ván, thưởng nhích nhẹ.'
    },
    {
      id: 'weekly_win_22',
      tab: 'week',
      title: '22 chiến thắng',
      desc: 'Thắng 22 trận trong tuần để ăn quỹ lớn hơn.',
      target: 22,
      metricKey: 'weekWins',
      periodType: 'week',
      rewardRate: 0.00155,
      minPmc: 32,
      maxPmc: 145,
      note: 'Phải thắng thật nhiều mới mở được mốc này.'
    },
    {
      id: 'weekly_active_5',
      tab: 'week',
      title: 'Chơi đều 5 ngày',
      desc: 'Có ít nhất 5 ngày trong tuần hoàn thành ván cờ.',
      target: 5,
      metricKey: 'weekActiveDays',
      periodType: 'week',
      rewardRate: 0.00110,
      minPmc: 20,
      maxPmc: 100,
      note: 'Không cho cày dồn 1 ngày rồi nghỉ cả tuần.'
    },

    // =====================
    // THÁNG - 3 mốc điểm danh: 7 / 15 / 30 ngày
    // =====================
    {
      id: 'monthly_checkin_7',
      tab: 'month',
      title: 'Điểm danh 7 ngày',
      desc: 'Trong tháng điểm danh đủ 7 ngày để nhận thưởng mốc 1.',
      target: 7,
      metricKey: 'monthCheckinDays',
      periodType: 'month',
      rewardRate: 0.00120,
      minPmc: 25,
      maxPmc: 110,
      note: 'Mốc tháng đầu, dễ vừa đủ để giữ người chơi.'
    },
    {
      id: 'monthly_checkin_15',
      tab: 'month',
      title: 'Điểm danh 15 ngày',
      desc: 'Trong tháng điểm danh đủ 15 ngày để nhận thưởng mốc 2.',
      target: 15,
      metricKey: 'monthCheckinDays',
      periodType: 'month',
      rewardRate: 0.00235,
      minPmc: 55,
      maxPmc: 230,
      note: 'Muốn ăn mốc này phải quay lại nửa tháng.'
    },
    {
      id: 'monthly_checkin_30',
      tab: 'month',
      title: 'Điểm danh 30 ngày',
      desc: 'Trong tháng điểm danh đủ 30 ngày để nhận thưởng lớn.',
      target: 30,
      metricKey: 'monthCheckinDays',
      periodType: 'month',
      rewardRate: 0.00480,
      minPmc: 110,
      maxPmc: 460,
      note: 'Mốc khó nhất, gần như ngày nào cũng phải vào nhận.'
    },
    {
      id: 'monthly_play_180',
      tab: 'month',
      title: 'Tháng siêng năng',
      desc: 'Hoàn thành 180 ván trong tháng.',
      target: 180,
      metricKey: 'monthMatches',
      periodType: 'month',
      rewardRate: 0.00300,
      minPmc: 70,
      maxPmc: 300,
      note: 'Tăng từ 30 lên 180 ván, tránh cày vài buổi ăn sạch.'
    },
    {
      id: 'monthly_win_90',
      tab: 'month',
      title: '90 chiến thắng tháng',
      desc: 'Thắng 90 trận trong tháng để mở khóa thưởng VIP.',
      target: 90,
      metricKey: 'monthWins',
      periodType: 'month',
      rewardRate: 0.00380,
      minPmc: 90,
      maxPmc: 390,
      note: 'Mốc thắng được nâng cao, thưởng tăng nhẹ cho xứng.'
    },
    {
      id: 'monthly_active_22',
      tab: 'month',
      title: 'Chơi đều 22 ngày',
      desc: 'Có ít nhất 22 ngày trong tháng hoàn thành ván cờ.',
      target: 22,
      metricKey: 'monthActiveDays',
      periodType: 'month',
      rewardRate: 0.00360,
      minPmc: 85,
      maxPmc: 360,
      note: 'Mốc này bắt người chơi vừa vào đều, vừa có đánh thật.'
    },

    // =====================
    // GIỚI THIỆU - giữ nguyên để không đụng flow referral
    // =====================
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
  const thisMonthKey = localMonthKey(now);
  const thisWeekKey = localWeekKey(now);

  // Đọc lịch sử ván đấu + bạn bè + claim điểm danh hằng ngày.
  // Mốc điểm danh tháng/tuần tính theo nhiệm vụ daily_login đã claim, không tính theo số ván.
  const [historySnap, friendsSnap, claimsSnap] = await Promise.all([
    db.ref(`wallets/${walletKey}/matchHistoryV2`).once('value'),
    db.ref(`social/friends/${walletKey}`).once('value'),
    db.ref(`missionClaimsV1/${walletKey}`).once('value')
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
    monthActiveDays: 0,
    weekCheckinDays: 0,
    monthCheckinDays: 0
  };

  const weekDays = new Set();
  const monthDays = new Set();
  const weekCheckinDays = new Set();
  const monthCheckinDays = new Set();

  historySnap.forEach(child => {
    const record = child.val() || {};

    // Bỏ qua các ván đang đánh hoặc lỗi
    if (!record.done) return;

    // Lấy thời gian ván đấu kết thúc
    const eventTs = Number(record.at || 0);
    if (!Number.isFinite(eventTs) || eventTs <= 0) return;

    const isWin = record.result === 'win';

    if (eventTs >= dayStart) {
      metrics.dayMatches += 1;
      if (isWin) metrics.dayWins += 1;
    }

    if (eventTs >= weekStart) {
      metrics.weekMatches += 1;
      weekDays.add(localDayKey(eventTs));
      if (isWin) metrics.weekWins += 1;
    }

    if (eventTs >= monthStart) {
      metrics.monthMatches += 1;
      monthDays.add(localDayKey(eventTs));
      if (isWin) metrics.monthWins += 1;
    }
  });

  claimsSnap.forEach(child => {
    const key = String(child.key || '');
    const claim = child.val() || {};

    if (!key.startsWith('daily_login__')) return;
    if (claim.status !== 'done') return;

    const periodKey = String(claim.periodKey || key.replace('daily_login__', '') || '');
    const claimedAt = Number(claim.claimedAt || 0) || 0;

    // daily_login dùng periodKey dạng YYYYMMDD.
    if (/^\d{8}$/.test(periodKey)) {
      if (periodKey.startsWith(thisMonthKey)) {
        monthCheckinDays.add(periodKey);
      }

      const weekKeyOfClaim = claimedAt > 0 ? localWeekKey(claimedAt) : '';
      if (weekKeyOfClaim === thisWeekKey) {
        weekCheckinDays.add(periodKey);
      }
    }
  });

  metrics.weekActiveDays = weekDays.size;
  metrics.monthActiveDays = monthDays.size;
  metrics.weekCheckinDays = weekCheckinDays.size;
  metrics.monthCheckinDays = monthCheckinDays.size;
  return metrics;
}
function rewardAmountPmc(def, missionPoolPmc) {
  const pool = roundPmc(Number(missionPoolPmc || 0) || 0);

  if (pool <= 0) return 0;

  const raw = Math.floor(pool * Number(def.rewardRate || 0));
  const reward = clamp(raw, Number(def.minPmc || 0), Number(def.maxPmc || 0));

  // Quỹ chưa đủ mức thưởng tối thiểu thì chưa cho nhận.
  if (reward > pool) return 0;

  return reward;
}
async function buildBoard(db, walletKey, now = Date.now()) {
  const defs = missionDefinitions();
  const [walletSnap, treasurySnap, missionPoolSnap, metrics] = await Promise.all([
    db.ref(`wallets/${walletKey}`).once('value'),
    db.ref(`wallets/${safeKey(ADMIN_TREASURY_WALLET_KEY)}`).once('value'),
    db.ref('treasury/missionPoolPmc').once('value'), // 🔥 ÉP ĐỌC TỪ FIREBASE ĐÂY NÀY
    buildMetrics(db, walletKey, now)
  ]);

  const walletVal = walletSnap.val() || {};
  const treasuryVal = treasurySnap.val() || {};
  const treasuryPmc = readPmcBalance(treasuryVal);
  
  // 🔥 LẤY ĐÚNG SỐ TRÊN FIREBASE, KHÔNG NHÂN CHIA GÌ NỮA
  const missionPoolPmc = roundPmc(Number(missionPoolSnap.val()) || 0);
  const tabs = { day: [], week: [], month: [], referral: [] };
  let claimableTotalPmc = 0;
  let claimableCount = 0;

  for (const def of defs) {
    const progress = Math.max(0, Number(metrics[def.metricKey] || 0));
    const rewardPmc = rewardAmountPmc(def, missionPoolPmc);
    const target = Math.max(1, Number(def.target || 1));
    const progressPercent = Math.floor(Math.max(0, Math.min(1, progress / target)) * 100);
    // Dùng đúng hàm mầy đã viết để sinh ra cái tên có đuôi chu kỳ (ví dụ: daily_login__20260505)
    const currentPeriodKey = periodKeyForMission(def, now);
    
    // Check thẳng vào ID có chứa cái đuôi đó
    const claimSnap = await db.ref(`missionClaimsV1/${walletKey}/${def.id}__${currentPeriodKey}`).once('value');
    const claimVal = claimSnap.val() || null;
    
    // Nếu ngày/tuần/tháng mới đến, currentPeriodKey sẽ tự động đổi.
    // Lúc đó Firebase check cái ID mới này đéo thấy (vì chưa claim), tự động 'claimed' sẽ = false! Xong bài!
    const claimed = !!(claimVal && claimVal.status === 'done');
    const ready =
  !claimed &&
  progress >= target &&
  rewardPmc > 0 &&
  treasuryPmc >= rewardPmc &&
  missionPoolPmc >= rewardPmc;

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

    // Trừ quỹ nhiệm vụ hiển thị bằng transaction để không âm khi nhiều người claim cùng lúc.
const poolTx = await subtractMissionPoolPmc(db, mission.rewardPmc);
if (!poolTx.committed) {
  await txAdjustPmc(
    treasuryRef,
    mission.rewardPmc,
    { name: 'Ví phí hệ thống' },
    treasuryPreRead
  ).catch(() => {});
  await txAdjustPmc(userRef, -mission.rewardPmc, {}, userPreRead).catch(() => {});
  await claimRef.remove().catch(() => {});
  throw new Error('Quỹ nhiệm vụ hiện không đủ để trả thưởng.');
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
      // BẮT BUỘC lưu với cấu trúc ID kèm Khóa chu kỳ để nó tự động reset
      db.ref(`missionClaimsV1/${walletKey}/${def.id}__${mission.periodKey}`).set({
        status: 'done',
        missionId: def.id,  // Giữ ID gốc để dễ tìm
        fullKey: `${def.id}__${mission.periodKey}`, // Khóa chu kỳ hiện tại
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

// ===== SHOP SKIN + TÚI ĐỒ + RƯƠNG CẤP GỘP VÀO MISSIONS-V1 =====
// Không tạo thêm route /api/cosmetics-v1 để né giới hạn Vercel Hobby.
const SHOP_LEVEL_MAX = 160;
const CHEST_TREASURY_MAX_RATIO = Number(process.env.CHEST_TREASURY_MAX_RATIO || 0.05);

function getShopLevelXpNeedFromLevel(level) {
  const lv = Math.max(1, Math.min(SHOP_LEVEL_MAX - 1, Math.floor(Number(level || 1))));
  return Math.floor(16 * Math.pow(1.035, lv - 1) + lv);
}

function buildShopLevelTable() {
  const rows = [];
  let xp = 0;
  for (let lv = 1; lv <= SHOP_LEVEL_MAX; lv += 1) {
    rows.push({ level: lv, xp });
    if (lv < SHOP_LEVEL_MAX) xp += getShopLevelXpNeedFromLevel(lv);
  }
  return rows;
}

const SHOP_LEVEL_TABLE = buildShopLevelTable();

function shopLevelByXp(xpValue) {
  const xp = Math.max(0, Number(xpValue || 0) || 0);
  let current = SHOP_LEVEL_TABLE[0];
  for (const row of SHOP_LEVEL_TABLE) {
    if (xp >= row.xp) current = row;
    else break;
  }
  return current.level;
}

function shopSkinCatalog() {
  return [
    {
      id: 'none',
      type: 'avatar_skin',
      name: 'Mặc định',
      icon: '⭕',
      pricePmc: 0,
      unlockLevel: 1,
      desc: 'Avatar mặc định.'
    },
    {
      id: 'bronze',
      type: 'avatar_skin',
      name: 'Hào Quang Đồng',
      icon: '🥉',
      pricePmc: 5000,
      unlockLevel: 30,
      desc: 'Vương miện đồng + hoa văn cổ.'
    },
    {
      id: 'jade',
      type: 'avatar_skin',
      name: 'Ngọc Lục Bảo',
      icon: '💚',
      pricePmc: 10000,
      unlockLevel: 60,
      desc: 'Khung ngọc xanh, hoa văn ôm avatar.'
    },
    {
      id: 'dragon',
      type: 'avatar_skin',
      name: 'Long Vương',
      icon: '🐉',
      pricePmc: 20000,
      unlockLevel: 120,
      desc: 'Rồng vàng ôm avatar, skin VIP.'
    },
    {
      id: 'phoenix',
      type: 'avatar_skin',
      name: 'Phượng Hoàng',
      icon: '🔥',
      pricePmc: 50000,
      unlockLevel: 180,
      desc: 'Phượng hoàng tím hồng, skin cao cấp.'
    }
  ];
}

function shopLevelChestMilestones() {
  const arr = [];
  for (let lv = 10; lv <= SHOP_LEVEL_MAX; lv += 10) arr.push(lv);
  return arr;
}

function shopRandomInt(min, max) {
  return crypto.randomInt(min, max + 1);
}

function shopRollChestRewardPmc(treasuryPmc) {
  // Rương vui, jackpot 30k cực hiếm, và bị cap theo ví phí hệ thống để không cháy quỹ.
  const roll = shopRandomInt(1, 10000);
  let raw = 100;

  if (roll <= 7000) raw = shopRandomInt(100, 300);          // 70%
  else if (roll <= 9000) raw = shopRandomInt(301, 1000);    // 20%
  else if (roll <= 9800) raw = shopRandomInt(1001, 5000);   // 8%
  else if (roll <= 9980) raw = shopRandomInt(5001, 15000);  // 1.8%
  else raw = shopRandomInt(15001, 30000);                   // 0.2%

  const safeTreasury = Math.max(0, Math.floor(Number(treasuryPmc || 0) || 0));
  const treasuryCap = Math.max(100, Math.floor(safeTreasury * CHEST_TREASURY_MAX_RATIO));
  return Math.max(100, Math.min(raw, treasuryCap, safeTreasury));
}

async function getShopUserLevelAndWallet(db, walletKey) {
  const snap = await db.ref(`wallets/${walletKey}`).once('value');
  const wallet = snap.val() && typeof snap.val() === 'object' ? snap.val() : {};
  const meta = wallet.levelMeta && typeof wallet.levelMeta === 'object' ? wallet.levelMeta : {};
  const xp = Math.max(0, Number(meta.xp || 0) || 0);

  return {
    wallet,
    pmcBalance: readPmcBalance(wallet),
    level: shopLevelByXp(xp),
    xp
  };
}

async function buildShopBoard(db, walletKey) {
  const [{ wallet, pmcBalance, level }, invSnap, chestSnap] = await Promise.all([
    getShopUserLevelAndWallet(db, walletKey),
    db.ref(`cosmeticsInventoryV1/${walletKey}`).once('value'),
    db.ref(`cosmeticLevelChestClaimsV1/${walletKey}`).once('value')
  ]);

  const inventory = invSnap.val() && typeof invSnap.val() === 'object' ? invSnap.val() : {};
  const chestClaims = chestSnap.val() && typeof chestSnap.val() === 'object' ? chestSnap.val() : {};
  const equippedAvatarSkin = String(wallet.equippedAvatarSkin || wallet.equippedSkin || 'skin_default');

  const catalog = shopSkinCatalog().map(item => {
    const owned = !!inventory[item.id];
    const levelUnlocked = level >= Number(item.unlockLevel || 9999);
    return {
      ...item,
      owned,
      levelUnlocked,
      usable: owned || levelUnlocked,
      equipped: equippedAvatarSkin === item.id
    };
  });

  const levelChests = shopLevelChestMilestones().map(lv => {
    const claim = chestClaims[`lv_${lv}`] || null;
    const claimed = !!(claim && claim.status === 'done');
    const canClaim = level >= lv && !claimed;
    return {
      level: lv,
      claimed,
      canClaim,
      rewardPmc: claim?.rewardPmc || 0,
      claimedAt: claim?.claimedAt || null,
      progressText: `Lv.${level}/${lv}`
    };
  });

  return {
    ok: true,
    walletKey,
    level,
    pmcBalance,
    equippedAvatarSkin,
    catalog,
    inventory,
    levelChests,
    generatedAt: nowMs()
  };
}

async function shopBuyItem(db, walletKey, itemId) {
  const item = shopSkinCatalog().find(x => x.id === itemId);
  if (!item) throw new Error('Skin không tồn tại.');

  const invRef = db.ref(`cosmeticsInventoryV1/${walletKey}/${item.id}`);
  const lock = await new Promise((resolve, reject) => {
    invRef.transaction(
      current => {
        if (current && current.owned) return;
        return { owned: false, processing: true, lockedAt: nowMs() };
      },
      (err, committed) => err ? reject(err) : resolve({ committed }),
      false
    );
  });

  if (!lock.committed) {
    return { ok: true, itemId: item.id, itemName: item.name, alreadyOwned: true };
  }

  const userRef = db.ref(`wallets/${walletKey}`);
  const treasuryRef = db.ref(`wallets/${safeKey(ADMIN_TREASURY_WALLET_KEY)}`);
  const [userPre, treasuryPre] = await Promise.all([
    userRef.once('value'),
    treasuryRef.once('value')
  ]);

  try {
    const debit = await txAdjustPmc(userRef, -item.pricePmc, {}, userPre.val());
    if (!debit.committed) {
      await invRef.remove().catch(() => {});
      throw new Error(`Không đủ PMC để mua ${item.name}.`);
    }

    const credit = await txAdjustPmc(
      treasuryRef,
      item.pricePmc,
      { name: 'Ví phí hệ thống' },
      treasuryPre.val()
    );

    if (!credit.committed) {
      await txAdjustPmc(userRef, item.pricePmc, {}, userPre.val()).catch(() => {});
      await invRef.remove().catch(() => {});
      throw new Error('Không cộng được phí shop vào ví hệ thống, đã hoàn tiền.');
    }

    await invRef.set({
      owned: true,
      itemId: item.id,
      itemName: item.name,
      pricePmc: item.pricePmc,
      boughtAt: nowMs()
    });

    await db.ref('walletTransactions').push().set({
      type: 'cosmetic_shop_buy',
      walletKey,
      itemId: item.id,
      itemName: item.name,
      amountPMC: -item.pricePmc,
      treasuryWalletKey: safeKey(ADMIN_TREASURY_WALLET_KEY),
      createdAt: nowMs(),
      status: 'done'
    });

    await db.ref('cosmeticShopLogsV1').push().set({
      walletKey,
      itemId: item.id,
      itemName: item.name,
      pricePmc: item.pricePmc,
      createdAt: nowMs(),
      status: 'done'
    });
await db.ref('adminLedgerV1').push().set({
  type: 'buy_skin',
  title: `${walletKey} mua skin ${item.name}`,
  detail: `Ví hệ thống nhận +${item.pricePmc} PMC`,
  amountPmc: item.pricePmc,
  missionPoolPmc: 0,
  walletKey,
  itemId: item.id,
  itemName: item.name,
  searchText: `${walletKey} ${item.id} ${item.name} mua skin`.toLowerCase(),
  createdAt: nowMs(),
  status: 'done'
}).catch(() => {});

    return { ok: true, itemId: item.id, itemName: item.name, newPmcBalance: debit.afterBalance };
  } catch (err) {
    await invRef.remove().catch(() => {});
    throw err;
  }
}

async function shopEquipItem(db, walletKey, itemId) {
  const rawItemId = String(itemId || '').trim();

  const aliasMap = {
    default: 'none',
    skin_default: 'none'
  };

  const finalItemId = aliasMap[rawItemId] || rawItemId;
  const item = shopSkinCatalog().find(x => x.id === finalItemId);

  if (!item) {
    throw new Error('Skin không tồn tại.');
  }

  const [{ wallet, level }, invSnap] = await Promise.all([
    getShopUserLevelAndWallet(db, walletKey),
    db.ref(`cosmeticsInventoryV1/${walletKey}/${item.id}`).once('value')
  ]);

  const invVal = invSnap.val();
  const ownedByInventory = !!(
    invVal &&
    (
      invVal === true ||
      invVal.owned === true
    )
  );

  const ownedByWallet = !!(
    wallet &&
    wallet.ownedAvatarSkins &&
    wallet.ownedAvatarSkins[item.id]
  );

  const levelUnlocked = level >= Number(item.unlockLevel || 9999);
  const owned = item.id === 'none' || ownedByInventory || ownedByWallet;

  if (!owned && !levelUnlocked) {
    throw new Error(`Chưa sở hữu skin này. Mua bằng PMC hoặc đạt Lv.${item.unlockLevel}.`);
  }

  const updates = {};

  updates[`wallets/${walletKey}/avatarSkin`] = item.id;
  updates[`wallets/${walletKey}/equippedAvatarSkin`] = item.id;
  updates[`wallets/${walletKey}/equippedSkin`] = item.id;
  updates[`wallets/${walletKey}/updatedAt`] = nowMs();

  // Nếu người chơi mở bằng cấp hoặc đã mua ở hệ thống cũ thì đồng bộ lại túi.
  updates[`wallets/${walletKey}/ownedAvatarSkins/${item.id}`] = true;
  updates[`cosmeticsInventoryV1/${walletKey}/${item.id}`] = {
    owned: true,
    itemId: item.id,
    itemName: item.name,
    pricePmc: item.pricePmc || 0,
    syncedFromEquip: true,
    updatedAt: nowMs()
  };

  updates[`cosmeticsEquippedV1/${walletKey}`] = {
    avatarSkin: item.id,
    equippedAvatarSkin: item.id,
    equippedSkin: item.id,
    itemName: item.name,
    equippedAt: nowMs()
  };

  await db.ref().update(updates);

  return {
    ok: true,
    equippedAvatarSkin: item.id,
    itemName: item.name
  };
}

async function shopOpenLevelChest(db, walletKey, level) {
  const lv = Math.max(0, Math.floor(Number(level || 0) || 0));
  if (!shopLevelChestMilestones().includes(lv)) throw new Error('Mốc rương không hợp lệ.');

  const { level: userLevel } = await getShopUserLevelAndWallet(db, walletKey);
  if (userLevel < lv) throw new Error(`Chưa đạt Lv.${lv}.`);

  const claimRef = db.ref(`cosmeticLevelChestClaimsV1/${walletKey}/lv_${lv}`);
  const lock = await new Promise((resolve, reject) => {
    claimRef.transaction(
      current => {
        if (current && current.status === 'done') return;
        if (current && current.status === 'processing') return;
        return { status: 'processing', walletKey, level: lv, lockedAt: nowMs() };
      },
      (err, committed) => err ? reject(err) : resolve({ committed }),
      false
    );
  });

  if (!lock.committed) throw new Error('Rương này đã mở hoặc đang xử lý.');

  const treasuryRef = db.ref(`wallets/${safeKey(ADMIN_TREASURY_WALLET_KEY)}`);
  const userRef = db.ref(`wallets/${walletKey}`);
  const [treasuryPre, userPre] = await Promise.all([
    treasuryRef.once('value'),
    userRef.once('value')
  ]);

  const treasuryPmc = readPmcBalance(treasuryPre.val() || {});
  if (treasuryPmc < 100) {
    await claimRef.remove().catch(() => {});
    throw new Error('Ví phí hệ thống chưa đủ quỹ mở rương.');
  }

  const rewardPmc = shopRollChestRewardPmc(treasuryPmc);

  try {
    const treasuryTx = await txAdjustPmc(
      treasuryRef,
      -rewardPmc,
      { name: 'Ví phí hệ thống' },
      treasuryPre.val()
    );

    if (!treasuryTx.committed) {
      await claimRef.remove().catch(() => {});
      throw new Error('Ví phí hệ thống không đủ quỹ trả rương.');
    }

    const userTx = await txAdjustPmc(userRef, rewardPmc, {}, userPre.val());

    if (!userTx.committed) {
      await txAdjustPmc(
        treasuryRef,
        rewardPmc,
        { name: 'Ví phí hệ thống' },
        treasuryPre.val()
      ).catch(() => {});
      await claimRef.remove().catch(() => {});
      throw new Error('Không cộng được quà rương vào ví người chơi.');
    }

    const payload = {
      status: 'done',
      walletKey,
      level: lv,
      rewardPmc,
      claimedAt: nowMs(),
      treasuryWalletKey: safeKey(ADMIN_TREASURY_WALLET_KEY)
    };

    await Promise.all([
      claimRef.set(payload),
      db.ref('walletTransactions').push().set({
        type: 'level_chest_reward_pmc',
        walletKey,
        amountPMC: rewardPmc,
        level: lv,
        sourceWalletKey: safeKey(ADMIN_TREASURY_WALLET_KEY),
        createdAt: nowMs(),
        status: 'done'
      }),
      db.ref('levelChestRewardLogsV1').push().set(payload)
    ]);

    return { ok: true, level: lv, rewardPmc, newPmcBalance: userTx.afterBalance };
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

    // PHẢI TẠO db TRƯỚC, rồi mới quét quỹ.
    const adminApp = adminBundle.app || adminBundle;
    const db = getDatabase(adminApp);

    // Quỹ nhiệm vụ dồn theo TUẦN.
    // Sang tuần mới thì phần còn dư tự hoàn về ví admin master.
    await sweepExpiredMissionPoolWeek(
      db,
      safeKey(ADMIN_TREASURY_WALLET_KEY),
      nowMs()
    ).catch(err => {
      console.error('MISSION_POOL_WEEK_SWEEP_FAIL:', err);
    });

    if (action === 'shop_board') {
      const shopBoard = await buildShopBoard(db, walletKey);
      return res.status(200).json(shopBoard);
    }

    if (action === 'shop_buy') {
      const itemId = String(body.itemId || '').trim();
      const bought = await shopBuyItem(db, walletKey, itemId);
      return res.status(200).json(bought);
    }

    if (action === 'shop_equip') {
      const itemId = String(body.itemId || '').trim();
      const equipped = await shopEquipItem(db, walletKey, itemId);
      return res.status(200).json(equipped);
    }

    if (action === 'shop_open_chest') {
      const opened = await shopOpenLevelChest(db, walletKey, body.level);
      return res.status(200).json(opened);
    }

    if (action === 'claim') {
      const missionId = String(body.missionId || '').trim();

      if (!missionId) {
        return res.status(400).json({
          ok: false,
          error: 'Thiếu missionId.'
        });
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