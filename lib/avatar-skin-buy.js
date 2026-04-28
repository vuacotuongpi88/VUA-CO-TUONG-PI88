const { admin, getAdminDb } = require("./firebase-admin");

const SYSTEM_FEE_WALLET_KEY = "406";
const AVATAR_SKIN_MIN_PRICE = 5000;

const AVATAR_SKINS = {
  bronze: {
    id: "bronze",
    name: "Hào Quang Đồng",
    price: 5000
  },
  jade: {
    id: "jade",
    name: "Ngọc Lục Bảo",
    price: 10000
  },
  dragon: {
    id: "dragon",
    name: "Long Vương",
    price: 20000
  },
  phoenix: {
    id: "phoenix",
    name: "Phượng Hoàng",
    price: 50000
  }
};

function safeKey(value = "") {
  return String(value || "")
    .trim()
    .replace(/[.#$\[\]\/]/g, "_");
}

function normalizeOwnedAvatarSkins(input) {
  const owned = input && typeof input === "object" ? { ...input } : {};
  owned.none = true;
  return owned;
}

function readPmc(value) {
  return Math.max(0, Math.floor(Number(value || 0) || 0));
}

async function handleAvatarSkinBuy(body = {}, req = {}) {
  const db = getAdminDb();

  const skinId = String(body.skinId || "").trim();
  const skin = AVATAR_SKINS[skinId];

  if (!skin) {
    return {
      status: 400,
      json: {
        ok: false,
        error: "Skin không hợp lệ."
      }
    };
  }

  if (skin.price < AVATAR_SKIN_MIN_PRICE) {
    return {
      status: 400,
      json: {
        ok: false,
        error: "Skin trả phí thấp nhất phải từ 5.000 PMC."
      }
    };
  }

  const walletKeyRaw = String(
    body.walletKey ||
    req.headers?.["x-wallet-key"] ||
    ""
  ).trim();

  const walletKey = safeKey(walletKeyRaw);

  if (!walletKey) {
    return {
      status: 400,
      json: {
        ok: false,
        error: "Thiếu walletKey người mua."
      }
    };
  }

  const systemWalletKey = safeKey(SYSTEM_FEE_WALLET_KEY);
  const buyerRef = db.ref("wallets/" + walletKey);

  const beforeSnap = await buyerRef.once("value");
  const before = beforeSnap.val() && typeof beforeSnap.val() === "object" ? beforeSnap.val() : {};
  const beforeOwned = normalizeOwnedAvatarSkins(before.ownedAvatarSkins);

  if (beforeOwned[skin.id]) {
    return {
      status: 200,
      json: {
        ok: true,
        alreadyOwned: true,
        message: "Skin này đã nằm trong túi.",
        newPmcBalance: readPmc(before.pmcBalance),
        avatarSkin: before.avatarSkin || "none",
        ownedAvatarSkins: beforeOwned
      }
    };
  }

  const beforePmc = readPmc(before.pmcBalance);

  if (beforePmc < skin.price) {
    return {
      status: 400,
      json: {
        ok: false,
        error: "Không đủ PMC.",
        currentPmc: beforePmc,
        needPmc: skin.price
      }
    };
  }

  const now = Date.now();

  let txAfter = null;
let txReason = "";

const tx = await buyerRef.transaction(current => {
  const cur =
    current && typeof current === "object"
      ? current
      : (before && typeof before === "object" ? before : {});

  const owned = normalizeOwnedAvatarSkins(cur.ownedAvatarSkins);
  const livePmc = readPmc(cur.pmcBalance);

  if (owned[skin.id]) {
    txAfter = cur;
    txReason = "already_owned";
    return cur;
  }

  if (livePmc < skin.price) {
    txReason = "not_enough_pmc";
    return;
  }

  owned[skin.id] = true;
  owned.none = true;

  txAfter = {
    ...cur,
    balance: Number(cur.balance || 0) || 0,
    pmcBalance: livePmc - skin.price,
    ownedAvatarSkins: owned,
    avatarSkin: cur.avatarSkin || "none",
    name: body.name || cur.name || "Người chơi",
    photo: body.photo || cur.photo || "images/do_tuong.png",
    updatedAt: now
  };

  return txAfter;
}, undefined, false);

if (!tx.committed || !txAfter) {
  const latestSnap = await buyerRef.once("value");
  const latest = latestSnap.val() || {};
  const latestPmc = readPmc(latest.pmcBalance);

  return {
    status: 400,
    json: {
      ok: false,
      error:
        "PMC ví thật trên Firebase không đủ. Ví server đọc được: " +
        latestPmc.toLocaleString("vi-VN") +
        " PMC, cần: " +
        skin.price.toLocaleString("vi-VN") +
        " PMC.",
      reason: txReason || "transaction_cancelled",
      walletKey,
      currentPmc: latestPmc,
      needPmc: skin.price
    }
  };
}

  const after = tx.snapshot.val() || {};
  const afterOwned = normalizeOwnedAvatarSkins(after.ownedAvatarSkins);
  const saleId = db.ref("skinShopSales").push().key;

  await db.ref().update({
    ["wallets/" + systemWalletKey + "/pmcBalance"]:
      admin.database.ServerValue.increment(skin.price),

    ["wallets/" + systemWalletKey + "/walletKey"]: SYSTEM_FEE_WALLET_KEY,
    ["wallets/" + systemWalletKey + "/name"]: "Ví phí hệ thống",
    ["wallets/" + systemWalletKey + "/updatedAt"]:
      admin.database.ServerValue.TIMESTAMP,

    ["skinShopSales/" + saleId]: {
      buyerWalletKey: walletKeyRaw,
      systemFeeWalletKey: SYSTEM_FEE_WALLET_KEY,
      skinId: skin.id,
      skinName: skin.name,
      pricePMC: skin.price,
      at: admin.database.ServerValue.TIMESTAMP
    }
  });

  return {
    status: 200,
    json: {
      ok: true,
      skinId: skin.id,
      skinName: skin.name,
      pricePMC: skin.price,
      newPmcBalance: readPmc(after.pmcBalance),
      avatarSkin: after.avatarSkin || "none",
      ownedAvatarSkins: afterOwned,
      systemFeeWalletKey: SYSTEM_FEE_WALLET_KEY
    }
  };
}

module.exports = {
  handleAvatarSkinBuy
};