const { getDatabase } = require("firebase-admin/database");
const { getAuth } = require("firebase-admin/auth");
const adminBundle = require("./_firebaseAdmin.js");

const adminApp = adminBundle.app || adminBundle;

// CHỐT GIÁ TRÊN SERVER, F12 SỬA HTML CŨNG VÔ DỤNG
const EXP_PACKAGES = {
  exp1: { exp: 1000, price: 1000, tickets: 2 },
  exp2: { exp: 5000, price: 4800, tickets: 4 },
  exp3: { exp: 20000, price: 18500, tickets: 8 },
  exp4: { exp: 100000, price: 85000, tickets: 12 },
  exp5: { exp: 500000, price: 400000, tickets: 16 },
  exp6: { exp: 1650000, price: 1200000, tickets: 18 }
};
const EXP_PACKAGE_ORDER = ["exp1", "exp2", "exp3", "exp4", "exp5", "exp6"];

function getExpPackageRank(pkgId = "") {
  const idx = EXP_PACKAGE_ORDER.indexOf(String(pkgId || "").trim());
  return idx >= 0 ? idx + 1 : 0;
}

function getHighestBoughtExpPackageRank(bought = {}) {
  let highest = 0;

  for (const id of EXP_PACKAGE_ORDER) {
    if (bought && bought[id]) {
      highest = Math.max(highest, getExpPackageRank(id));
    }
  }

  return highest;
}
const LEVEL_MILESTONES = {
  10: { tickets: 2 },
  20: { tickets: 2 },
  30: { tickets: 2, skinId: "bronze" },
  60: { tickets: 2, skinId: "jade" },
  90: { tickets: 2 },
  120: { tickets: 2, skinId: "dragon" },
  180: { tickets: 10, skinId: "phoenix" }
};

function safeKey(value = "") {
  return String(value || "").trim().replace(/[.#$\[\]\/]/g, "_");
}

function calcLevel(exp) {
  const n = Math.max(0, Math.floor(Number(exp || 0)));
  const level = Math.floor((1 + Math.sqrt(1 + (8 * n) / 100)) / 2);
  return Math.max(1, Math.min(180, level));
}

function randomChestReward(targetLevel) {
  let min = 0;
  let max = 0;

  if (targetLevel === 10) {
    min = 2;
    max = 20;
  } else if (targetLevel === 20) {
    min = 4;
    max = 40;
  } else {
    min = targetLevel;
    max = targetLevel * 3;
  }

  const half = Math.floor(max / 2);
  const isLucky = Math.random() < 0.2;

  if (isLucky) {
    return Math.floor(Math.random() * (max - half + 1)) + half;
  }

  return Math.floor(Math.random() * (half - min + 1)) + min;
}

function runTransaction(ref, updater) {
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

async function incrementAdminPmc(db, amount) {
  const n = Math.max(0, Math.floor(Number(amount || 0)));
  if (!n) return;

  const adminPmcRef = db.ref("wallets/pi_admin_master/pmcBalance");

  await runTransaction(adminPmcRef, (current) => {
    return Math.max(0, Math.floor(Number(current || 0))) + n;
  });

  await db.ref("wallets/pi_admin_master/updatedAt").set(Date.now());
}

async function verifyUser(req) {
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

async function loadOwnedWallet(db, walletKey, uid) {
  const safeWalletKey = safeKey(walletKey);

  if (!safeWalletKey) {
    const err = new Error("Thiếu ví người dùng");
    err.statusCode = 400;
    throw err;
  }

  const userRef = db.ref(`wallets/${safeWalletKey}`);
  const snap = await userRef.once("value");
  const wallet = snap.val();

  if (!wallet) {
    const err = new Error("Không tìm thấy ví người dùng");
    err.statusCode = 404;
    throw err;
  }

  if (String(wallet.uid || "") !== String(uid || "")) {
    const err = new Error("Ví này không thuộc tài khoản đang đăng nhập");
    err.statusCode = 403;
    throw err;
  }

  return { safeWalletKey, userRef, wallet };
}

module.exports = async function handler(req, res) {
  res.setHeader("Content-Type", "application/json; charset=utf-8");

  if (req.method !== "POST") {
    return res.status(405).json({
      ok: false,
      error: "Method not allowed"
    });
  }

  try {
    const body =
      typeof req.body === "string"
        ? JSON.parse(req.body || "{}")
        : req.body || {};

    const decoded = await verifyUser(req);
    const uid = decoded.uid;

    const action = String(body.action || "").trim();
    const walletKey = safeKey(body.walletKey);

    const db = getDatabase(adminApp);
    const { safeWalletKey, userRef } = await loadOwnedWallet(db, walletKey, uid);

    // =====================================================
    // 1. MỞ RƯƠNG CẤP ĐỘ
    // =====================================================
    if (action === "claim_chest") {
      const targetLevel = Math.floor(Number(body.targetLevel || 0));
      const milestone = LEVEL_MILESTONES[targetLevel];

      if (!milestone) {
        return res.status(400).json({
          ok: false,
          error: "Mốc rương không tồn tại"
        });
      }

      const rewardPMC = randomChestReward(targetLevel);

      const txResult = await runTransaction(userRef, (current) => {
        if (!current) return current;

        const curLevel = Math.max(1, Math.floor(Number(current.level || 1)));

        if (String(current.uid || "") !== String(uid)) return;
        if (curLevel < targetLevel) return;

        current.claimedLevels = current.claimedLevels || {};
        if (current.claimedLevels[targetLevel]) return;

        current.claimedLevels[targetLevel] = true;

        current.pmcBalance =
          Math.max(0, Math.floor(Number(current.pmcBalance || 0))) + rewardPMC;

        if (milestone.tickets) {
          current.freeTickets =
            Math.max(0, Math.floor(Number(current.freeTickets || 0))) +
            Math.floor(Number(milestone.tickets || 0));
        }

        if (milestone.skinId) {
          current.ownedAvatarSkins = current.ownedAvatarSkins || { none: true };
          current.ownedAvatarSkins[milestone.skinId] = true;
        }

        current.updatedAt = Date.now();

        return current;
      });

      if (!txResult.committed) {
        return res.status(400).json({
          ok: false,
          error: "Chưa đủ cấp hoặc rương này đã nhận rồi"
        });
      }

      const snapData = txResult.snapshot.val() || {};

      await db.ref("walletTransactions").push({
        type: "claim_level_chest",
        uid,
        walletKey: safeWalletKey,
        targetLevel,
        rewardPMC,
        tickets: milestone.tickets || 0,
        skinId: milestone.skinId || "",
        createdAt: Date.now(),
        status: "done"
      });

      return res.status(200).json({
        ok: true,
        action,
        rewardPMC,
        newPmc: Math.max(0, Math.floor(Number(snapData.pmcBalance || 0))),
        newTickets: Math.max(0, Math.floor(Number(snapData.freeTickets || 0))),
        ownedAvatarSkins: snapData.ownedAvatarSkins || {}
      });
    }

    // =====================================================
    // 2. MUA GÓI EXP
    // =====================================================
   if (action === "buy_exp_direct") {
  const pkgId = String(body.pkgId || "").trim();
  const pkg = EXP_PACKAGES[pkgId];

  if (!pkg) {
    return res.status(400).json({
      ok: false,
      error: "Gói EXP không tồn tại"
    });
  }

  const pkgRank = getExpPackageRank(pkgId);

  if (!pkgRank) {
    return res.status(400).json({
      ok: false,
      error: "Gói EXP không hợp lệ"
    });
  }

  const txResult = await runTransaction(userRef, (current) => {
    if (!current) return current;
    if (String(current.uid || "") !== String(uid)) return;

    const livePmc = Math.max(
      0,
      Math.floor(Number(current.pmcBalance || 0))
    );

    if (livePmc < pkg.price) return;

    current.boughtExpPackages = current.boughtExpPackages || {};

    const highestBoughtRank = getHighestBoughtExpPackageRank(current.boughtExpPackages);

    // Đã mua gói bằng hoặc cao hơn rồi thì chặn mua lại gói thấp hơn.
    // Ví dụ mua exp4 rồi thì exp1/exp2/exp3/exp4 đều bị khóa.
    if (highestBoughtRank >= pkgRank) return;

    current.pmcBalance = livePmc - pkg.price;
    current.exp = Math.max(0, Math.floor(Number(current.exp || 0))) + pkg.exp;
    current.level = calcLevel(current.exp);
    current.freeTickets =
      Math.max(0, Math.floor(Number(current.freeTickets || 0))) +
      pkg.tickets;

    // Mua gói cao thì đóng dấu luôn các gói thấp hơn là đã đạt mốc.
    for (let i = 0; i < pkgRank; i++) {
      current.boughtExpPackages[EXP_PACKAGE_ORDER[i]] = true;
    }

    current.lastBoughtExpPackage = pkgId;
    current.lastBoughtExpPackageRank = pkgRank;
    current.updatedAt = Date.now();

    return current;
  });

  if (!txResult.committed) {
    return res.status(400).json({
      ok: false,
      error: "Không đủ PMC hoặc gói này đã bị khóa bởi gói EXP cao hơn."
    });
  }

  await incrementAdminPmc(db, pkg.price);

  await db.ref("walletTransactions").push({
    type: "buy_exp_package",
    uid,
    walletKey: safeWalletKey,
    adminWalletKey: "pi_admin_master",
    pkgId,
    packageRank: pkgRank,
    feePMC: pkg.price,
    expGained: pkg.exp,
    ticketsGained: pkg.tickets,
    createdAt: Date.now(),
    status: "done"
  });

  const snapData = txResult.snapshot.val() || {};

  return res.status(200).json({
    ok: true,
    action,
    pkgId,
    packageRank: pkgRank,
    boughtExpPackages: snapData.boughtExpPackages || {},
    newExp: Math.max(0, Math.floor(Number(snapData.exp || 0))),
    newLevel: Math.max(1, Math.floor(Number(snapData.level || 1))),
    newPmc: Math.max(0, Math.floor(Number(snapData.pmcBalance || 0))),
    newTickets: Math.max(0, Math.floor(Number(snapData.freeTickets || 0)))
  });
}
    // =====================================================
    // 3. MUA LẠI EXP SAU KHI THUA
    // =====================================================
    if (action === "buy_back_exp") {
      const costPMC = Math.floor(Number(body.costPMC || 0));

      if (!Number.isFinite(costPMC) || costPMC <= 0) {
        return res.status(400).json({
          ok: false,
          error: "Số PMC không hợp lệ"
        });
      }

      // Chặn client gửi số quá điên
      if (costPMC > 2000000) {
        return res.status(400).json({
          ok: false,
          error: "Số PMC mua lại EXP quá lớn"
        });
      }

      const txResult = await runTransaction(userRef, (current) => {
        if (!current) return current;
        if (String(current.uid || "") !== String(uid)) return;

        const livePmc = Math.max(
          0,
          Math.floor(Number(current.pmcBalance || 0))
        );

        if (livePmc < costPMC) return;

        current.pmcBalance = livePmc - costPMC;
        current.exp = Math.max(0, Math.floor(Number(current.exp || 0))) + costPMC;
        current.level = calcLevel(current.exp);
        current.updatedAt = Date.now();

        return current;
      });

      if (!txResult.committed) {
        return res.status(400).json({
          ok: false,
          error: "Không đủ PMC để mua lại EXP"
        });
      }

      await incrementAdminPmc(db, costPMC);

      await db.ref("walletTransactions").push({
        type: "buy_back_exp_after_loss",
        uid,
        walletKey: safeWalletKey,
        adminWalletKey: "pi_admin_master",
        feePMC: costPMC,
        expGained: costPMC,
        createdAt: Date.now(),
        status: "done"
      });

      const snapData = txResult.snapshot.val() || {};

      return res.status(200).json({
        ok: true,
        action,
        newExp: Math.max(0, Math.floor(Number(snapData.exp || 0))),
        newLevel: Math.max(1, Math.floor(Number(snapData.level || 1))),
        newPmc: Math.max(0, Math.floor(Number(snapData.pmcBalance || 0)))
      });
    }

    return res.status(400).json({
      ok: false,
      error: "Hành động không hợp lệ"
    });
  } catch (err) {
    console.error("LEVEL_SYSTEM_API_ERROR", err);

    return res.status(err.statusCode || 500).json({
      ok: false,
      error: err?.message || "Lỗi hệ thống Vercel"
    });
  }
};