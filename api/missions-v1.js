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
    { id:'skin_bamboo_gold', type:'avatar_skin', name:'Viền Trúc Kim', icon:'🎋', pricePmc:25000, unlockLevel:10, desc:'Viền vàng xanh cho avatar, mốc đầu cho người chăm cày.' },
    { id:'skin_dragon_purple', type:'avatar_skin', name:'Long Ảnh Tím', icon:'🐉', pricePmc:80000, unlockLevel:30, desc:'Hiệu ứng tím cao thủ, mua được nhưng cày Lv.30 cũng mở.' },
    { id:'skin_phoenix_red', type:'avatar_skin', name:'Phượng Hỏa Đỏ', icon:'🔥', pricePmc:150000, unlockLevel:50, desc:'Viền đỏ rực cho người thích nổi bật.' },
    { id:'skin_diamond_blue', type:'avatar_skin', name:'Kim Cương Lam', icon:'💎', pricePmc:300000, unlockLevel:80, desc:'Khung xanh kim cương, dành cho tài khoản mạnh.' },
    { id:'skin_king_rainbow', type:'avatar_skin', name:'Vương Giả Ngũ Sắc', icon:'👑', pricePmc:800000, unlockLevel:120, desc:'Skin nhà giàu hoặc người cày lâu năm.' },
    { id:'skin_god_neon', type:'avatar_skin', name:'Thần Quang Neon', icon:'⚡', pricePmc:1500000, unlockLevel:160, desc:'Mốc tối thượng Lv.160, mua rất đắt để đốt PMC.' }
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

    return { ok: true, itemId: item.id, itemName: item.name, newPmcBalance: debit.afterBalance };
  } catch (err) {
    await invRef.remove().catch(() => {});
    throw err;
  }
}

async function shopEquipItem(db, walletKey, itemId) {
  const item = shopSkinCatalog().find(x => x.id === itemId);
  if (!item) throw new Error('Skin không tồn tại.');

  const [{ level }, invSnap] = await Promise.all([
    getShopUserLevelAndWallet(db, walletKey),
    db.ref(`cosmeticsInventoryV1/${walletKey}/${item.id}`).once('value')
  ]);

  const owned = !!(invSnap.val() && invSnap.val().owned);
  const levelUnlocked = level >= item.unlockLevel;

  if (!owned && !levelUnlocked) {
    throw new Error(`Chưa sở hữu skin này. Mua bằng PMC hoặc đạt Lv.${item.unlockLevel}.`);
  }

  await Promise.all([
    db.ref(`wallets/${walletKey}`).update({ equippedAvatarSkin: item.id, updatedAt: nowMs() }),
    db.ref(`cosmeticsEquippedV1/${walletKey}`).set({
      avatarSkin: item.id,
      itemName: item.name,
      equippedAt: nowMs()
    })
  ]);

  return { ok: true, equippedAvatarSkin: item.id, itemName: item.name };
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

    const adminApp = adminBundle.app || adminBundle;
    const db = getDatabase(adminApp);

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
