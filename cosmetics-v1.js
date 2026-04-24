const { getDatabase } = require('firebase-admin/database');
const adminBundle = require('./_firebaseAdmin.js');
const crypto = require('crypto');

const ADMIN_TREASURY_WALLET_KEY = String(process.env.ADMIN_TREASURY_WALLET_KEY || 'pi_admin_master').trim();
const LEVEL_MAX = 160;
const CHEST_TREASURY_MAX_RATIO = Number(process.env.CHEST_TREASURY_MAX_RATIO || 0.05); // tối đa 5% ví phí/rương để không cháy quỹ

function safeKey(value) {
  return String(value || '').replace(/[.#$\[\]\/]/g, '_');
}

function nowMs() {
  return Date.now();
}

function readPmc(obj = {}) {
  return Math.max(0, Math.floor(Number(obj?.pmcBalance ?? obj?.pmc ?? 0) || 0));
}

function clamp(n, min, max) {
  return Math.max(min, Math.min(max, n));
}

function getLevelXpNeedFromLevel(level) {
  const lv = Math.max(1, Math.min(LEVEL_MAX - 1, Math.floor(Number(level || 1))));
  return Math.floor(16 * Math.pow(1.035, lv - 1) + lv);
}

function buildLevelTable() {
  const rows = [];
  let xp = 0;

  for (let lv = 1; lv <= LEVEL_MAX; lv++) {
    rows.push({ level: lv, xp });
    if (lv < LEVEL_MAX) xp += getLevelXpNeedFromLevel(lv);
  }

  return rows;
}

const LEVEL_TABLE = buildLevelTable();

function levelByXp(xpValue) {
  const xp = Math.max(0, Number(xpValue || 0) || 0);
  let current = LEVEL_TABLE[0];

  for (const row of LEVEL_TABLE) {
    if (xp >= row.xp) current = row;
    else break;
  }

  return current.level;
}

function skinCatalog() {
  return [
    {
      id: 'skin_bamboo_gold',
      type: 'avatar_skin',
      name: 'Viền Trúc Kim',
      icon: '🎋',
      pricePmc: 25000,
      unlockLevel: 10,
      desc: 'Viền vàng xanh cho avatar, mốc đầu cho người chăm cày.'
    },
    {
      id: 'skin_dragon_purple',
      type: 'avatar_skin',
      name: 'Long Ảnh Tím',
      icon: '🐉',
      pricePmc: 80000,
      unlockLevel: 30,
      desc: 'Hiệu ứng tím cao thủ, mua được nhưng cày Lv.30 cũng mở.'
    },
    {
      id: 'skin_phoenix_red',
      type: 'avatar_skin',
      name: 'Phượng Hỏa Đỏ',
      icon: '🔥',
      pricePmc: 150000,
      unlockLevel: 50,
      desc: 'Viền đỏ rực cho người thích nổi bật.'
    },
    {
      id: 'skin_diamond_blue',
      type: 'avatar_skin',
      name: 'Kim Cương Lam',
      icon: '💎',
      pricePmc: 300000,
      unlockLevel: 80,
      desc: 'Khung xanh kim cương, dành cho tài khoản mạnh.'
    },
    {
      id: 'skin_king_rainbow',
      type: 'avatar_skin',
      name: 'Vương Giả Ngũ Sắc',
      icon: '👑',
      pricePmc: 800000,
      unlockLevel: 120,
      desc: 'Skin nhà giàu hoặc người cày lâu năm.'
    },
    {
      id: 'skin_god_neon',
      type: 'avatar_skin',
      name: 'Thần Quang Neon',
      icon: '⚡',
      pricePmc: 1500000,
      unlockLevel: 160,
      desc: 'Mốc tối thượng Lv.160, mua rất đắt để đốt PMC.'
    }
  ];
}

function levelChestMilestones() {
  const arr = [];
  for (let lv = 10; lv <= LEVEL_MAX; lv += 10) arr.push(lv);
  return arr;
}

function randomInt(min, max) {
  return crypto.randomInt(min, max + 1);
}

function rollChestRewardPmc(treasuryPmc) {
  // Tỉ lệ an toàn: jackpot 30k cực hiếm; còn bị cap bởi 5% ví phí để không cháy quỹ.
  const roll = randomInt(1, 10000);

  let raw = 100;

  if (roll <= 7000) raw = randomInt(100, 300);          // 70%
  else if (roll <= 9000) raw = randomInt(301, 1000);    // 20%
  else if (roll <= 9800) raw = randomInt(1001, 5000);   // 8%
  else if (roll <= 9980) raw = randomInt(5001, 15000);  // 1.8%
  else raw = randomInt(15001, 30000);                   // 0.2%

  const treasuryCap = Math.max(
    100,
    Math.floor(Number(treasuryPmc || 0) * CHEST_TREASURY_MAX_RATIO)
  );

  return Math.max(
    100,
    Math.min(raw, treasuryCap, Math.max(0, Math.floor(Number(treasuryPmc || 0))))
  );
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

        const cur = readPmc(baseCurrent);
        const next = cur + Math.floor(Number(delta || 0));

        if (next < 0) return;

        afterBalance = next;

        return {
          ...baseCurrent,
          ...extra,
          pmcBalance: next,
          updatedAt: nowMs()
        };
      },
      (err, committed, snapshot) => {
        if (err) return reject(err);
        resolve({ committed, snapshot, afterBalance });
      },
      false
    );
  });

  return result;
}

async function getUserLevelAndWallet(db, walletKey) {
  const snap = await db.ref(`wallets/${walletKey}`).once('value');
  const wallet = snap.val() && typeof snap.val() === 'object' ? snap.val() : {};
  const meta = wallet.levelMeta && typeof wallet.levelMeta === 'object' ? wallet.levelMeta : {};

  const xp = Math.max(0, Number(meta.xp || 0) || 0);

  return {
    wallet,
    pmcBalance: readPmc(wallet),
    level: levelByXp(xp),
    xp
  };
}

async function buildBoard(db, walletKey) {
  const [{ wallet, pmcBalance, level }, invSnap, chestSnap] = await Promise.all([
    getUserLevelAndWallet(db, walletKey),
    db.ref(`cosmeticsInventoryV1/${walletKey}`).once('value'),
    db.ref(`cosmeticLevelChestClaimsV1/${walletKey}`).once('value')
  ]);

  const inventory = invSnap.val() && typeof invSnap.val() === 'object' ? invSnap.val() : {};
  const chestClaims = chestSnap.val() && typeof chestSnap.val() === 'object' ? chestSnap.val() : {};
  const equippedAvatarSkin = String(wallet.equippedAvatarSkin || wallet.equippedSkin || 'skin_default');

  const catalog = skinCatalog().map(item => {
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

  const levelChests = levelChestMilestones().map(lv => {
    const claim = chestClaims[`lv_${lv}`] || null;
    const claimed = !!(claim && claim.status === 'done');
    const canClaim = level >= lv && !claimed;

    return {
      level: lv,
      claimed,
      canClaim,
      rewardPmc: claim?.rewardPmc || 0,
      claimedAt: claim?.claimedAt || null,
      progressText: level >= lv ? `Lv.${level}/${lv}` : `Lv.${level}/${lv}`
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

async function buyItem(db, walletKey, itemId) {
  const item = skinCatalog().find(x => x.id === itemId);

  if (!item) {
    throw new Error('Skin không tồn tại.');
  }

  const invRef = db.ref(`cosmeticsInventoryV1/${walletKey}/${item.id}`);

  const lock = await new Promise((resolve, reject) => {
    invRef.transaction(
      current => {
        if (current && current.owned) return;

        return {
          owned: false,
          processing: true,
          lockedAt: nowMs()
        };
      },
      (err, committed) => err ? reject(err) : resolve({ committed }),
      false
    );
  });

  if (!lock.committed) {
    return {
      ok: true,
      itemId: item.id,
      itemName: item.name,
      alreadyOwned: true
    };
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

    await db.ref('walletTransactions').push({
      type: 'cosmetic_shop_buy',
      walletKey,
      itemId: item.id,
      itemName: item.name,
      amountPMC: -item.pricePmc,
      treasuryWalletKey: safeKey(ADMIN_TREASURY_WALLET_KEY),
      createdAt: nowMs(),
      status: 'done'
    });

    await db.ref('cosmeticShopLogsV1').push({
      walletKey,
      itemId: item.id,
      itemName: item.name,
      pricePmc: item.pricePmc,
      createdAt: nowMs(),
      status: 'done'
    });

    return {
      ok: true,
      itemId: item.id,
      itemName: item.name,
      newPmcBalance: debit.afterBalance
    };
  } catch (err) {
    await invRef.remove().catch(() => {});
    throw err;
  }
}

async function equipItem(db, walletKey, itemId) {
  const item = skinCatalog().find(x => x.id === itemId);

  if (!item) {
    throw new Error('Skin không tồn tại.');
  }

  const [{ level }, invSnap] = await Promise.all([
    getUserLevelAndWallet(db, walletKey),
    db.ref(`cosmeticsInventoryV1/${walletKey}/${item.id}`).once('value')
  ]);

  const owned = !!(invSnap.val() && invSnap.val().owned);
  const levelUnlocked = level >= item.unlockLevel;

  if (!owned && !levelUnlocked) {
    throw new Error(`Chưa sở hữu skin này. Mua bằng PMC hoặc đạt Lv.${item.unlockLevel}.`);
  }

  await Promise.all([
    db.ref(`wallets/${walletKey}`).update({
      equippedAvatarSkin: item.id,
      updatedAt: nowMs()
    }),
    db.ref(`cosmeticsEquippedV1/${walletKey}`).set({
      avatarSkin: item.id,
      itemName: item.name,
      equippedAt: nowMs()
    })
  ]);

  return {
    ok: true,
    equippedAvatarSkin: item.id,
    itemName: item.name
  };
}

async function openLevelChest(db, walletKey, level) {
  const lv = Math.max(0, Math.floor(Number(level || 0) || 0));

  if (!levelChestMilestones().includes(lv)) {
    throw new Error('Mốc rương không hợp lệ.');
  }

  const { level: userLevel } = await getUserLevelAndWallet(db, walletKey);

  if (userLevel < lv) {
    throw new Error(`Chưa đạt Lv.${lv}.`);
  }

  const claimRef = db.ref(`cosmeticLevelChestClaimsV1/${walletKey}/lv_${lv}`);

  const lock = await new Promise((resolve, reject) => {
    claimRef.transaction(
      current => {
        if (current && current.status === 'done') return;
        if (current && current.status === 'processing') return;

        return {
          status: 'processing',
          walletKey,
          level: lv,
          lockedAt: nowMs()
        };
      },
      (err, committed) => err ? reject(err) : resolve({ committed }),
      false
    );
  });

  if (!lock.committed) {
    throw new Error('Rương này đã mở hoặc đang xử lý.');
  }

  const treasuryRef = db.ref(`wallets/${safeKey(ADMIN_TREASURY_WALLET_KEY)}`);
  const userRef = db.ref(`wallets/${walletKey}`);

  const [treasuryPre, userPre] = await Promise.all([
    treasuryRef.once('value'),
    userRef.once('value')
  ]);

  const treasuryPmc = readPmc(treasuryPre.val() || {});

  if (treasuryPmc < 100) {
    await claimRef.remove().catch(() => {});
    throw new Error('Ví phí hệ thống chưa đủ quỹ mở rương.');
  }

  const rewardPmc = rollChestRewardPmc(treasuryPmc);

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
      db.ref('walletTransactions').push({
        type: 'level_chest_reward_pmc',
        walletKey,
        amountPMC: rewardPmc,
        level: lv,
        sourceWalletKey: safeKey(ADMIN_TREASURY_WALLET_KEY),
        createdAt: nowMs(),
        status: 'done'
      }),
      db.ref('levelChestRewardLogsV1').push(payload)
    ]);

    return {
      ok: true,
      level: lv,
      rewardPmc,
      newPmcBalance: userTx.afterBalance
    };
  } catch (err) {
    await claimRef.remove().catch(() => {});
    throw err;
  }
}

module.exports = async function handler(req, res) {
  if (req.method !== 'POST') {
    return res.status(405).json({
      ok: false,
      error: 'Method not allowed'
    });
  }

  try {
    const body = typeof req.body === 'string'
      ? JSON.parse(req.body || '{}')
      : (req.body || {});

    const action = String(body.action || 'board').trim().toLowerCase();

    const walletKey = safeKey(
      String(req.headers['x-wallet-key'] || body.walletKey || '').trim()
    );

    if (!walletKey) {
      return res.status(400).json({
        ok: false,
        error: 'Thiếu walletKey.'
      });
    }

    const adminApp = adminBundle.app || adminBundle;
    const db = getDatabase(adminApp);

    if (action === 'buy') {
      const itemId = String(body.itemId || '').trim();
      const result = await buyItem(db, walletKey, itemId);
      return res.status(200).json(result);
    }

    if (action === 'equip') {
      const itemId = String(body.itemId || '').trim();
      const result = await equipItem(db, walletKey, itemId);
      return res.status(200).json(result);
    }

    if (action === 'open_chest') {
      const result = await openLevelChest(db, walletKey, body.level);
      return res.status(200).json(result);
    }

    const board = await buildBoard(db, walletKey);
    return res.status(200).json(board);
  } catch (err) {
    console.error('COSMETICS_V1_FAIL:', err);

    return res.status(500).json({
      ok: false,
      error: err?.message || 'Lỗi shop skin.'
    });
  }
};
