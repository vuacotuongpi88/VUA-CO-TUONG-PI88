function loadStellarSdk() {
  try {
    return require("@stellar/stellar-sdk");
  } catch (_) {
    return require("stellar-sdk");
  }
}

const CONFIG = {
  AUTO_WITHDRAW_MAX: Number(process.env.AUTO_WITHDRAW_MAX || 1000),
  MAX_WITHDRAW_PER_DAY_COUNT: Number(process.env.MAX_WITHDRAW_PER_DAY_COUNT || 50),
  LOCK_TTL_MS: Number(process.env.WITHDRAW_LOCK_TTL_MS || 2 * 60 * 1000),
  BURST_WINDOW_MS: Number(process.env.WITHDRAW_BURST_WINDOW_MS || 60 * 1000),
  BURST_MAX_COUNT: Number(process.env.WITHDRAW_BURST_MAX_COUNT || 3)
};

function nowMs() {
  return Date.now();
}

function safeKey(value = "") {
  return String(value || "").trim().replace(/[.#$\[\]\/]/g, "_");
}

function readPiBalance(obj = {}) {
  const n = Number(obj.balance != null ? obj.balance : (obj.piBalance != null ? obj.piBalance : 0));
  if (!Number.isFinite(n)) return 0;
  return Math.max(0, n);
}

function cleanForFirebase(value) {
  if (value === undefined) return null;
  if (typeof value === "number" && !Number.isFinite(value)) return null;
  if (value === null) return null;

  if (Array.isArray(value)) {
    return value.map(cleanForFirebase);
  }

  if (typeof value === "object") {
    const out = {};
    for (const [k, v] of Object.entries(value)) {
      const cleaned = cleanForFirebase(v);
      if (cleaned !== undefined) out[k] = cleaned;
    }
    return out;
  }

  return value;
}

function dayStartMsVN(ts = Date.now()) {
  const offset = 7 * 60 * 60 * 1000;
  const d = new Date(ts + offset);
  d.setUTCHours(0, 0, 0, 0);
  return d.getTime() - offset;
}

function wrapTransaction(ref, updateFn) {
  return new Promise((resolve, reject) => {
    ref.transaction(
      updateFn,
      (err, committed, snap) => {
        if (err) return reject(err);
        resolve({ committed, snap });
      },
      false
    );
  });
}

async function acquireWithdrawLock(lockRef) {
  const now = nowMs();
  const tx = await wrapTransaction(lockRef, current => {
    const cur = current && typeof current === "object" ? current : {};
    const lockedAt = Number(cur.lockedAt || 0) || 0;
    const expired = !lockedAt || now - lockedAt > CONFIG.LOCK_TTL_MS;

    if (cur.locked === true && !expired) return;

    return {
      locked: true,
      lockedAt: now,
      updatedAt: now
    };
  });

  return !!tx.committed;
}

async function releaseWithdrawLock(lockRef, reason = "") {
  await lockRef.update({
    locked: false,
    releasedAt: nowMs(),
    releaseReason: String(reason || ""),
    updatedAt: nowMs()
  }).catch(() => {});
}

async function countTodayWithdraws(db, walletKey) {
  const start = dayStartMsVN();
  const snap = await db
    .ref("piWithdrawRequests")
    .orderByChild("walletKey")
    .equalTo(walletKey)
    .limitToLast(200)
    .once("value");

  let count = 0;

  snap.forEach(child => {
    const item = child.val() || {};
    const createdAt = Number(item.createdAt || 0) || 0;
    const status = String(item.status || "");

    if (createdAt < start) return;
    if (["failed", "cancelled", "rejected"].includes(status)) return;

    count += 1;
  });

  return count;
}

async function inspectWithdrawQueue(db, walletKey) {
  const snap = await db
    .ref("piWithdrawRequests")
    .orderByChild("walletKey")
    .equalTo(walletKey)
    .limitToLast(50)
    .once("value");

  const activeStatuses = new Set([
    "initiated",
    "auto_processing",
    "chain_submitted",
    "chain_submit_missing_txid"
  ]);

  let activeRequest = null;
  let pendingAdminRequest = null;
  let recentCount = 0;
  const now = nowMs();

  snap.forEach(child => {
    const item = child.val() || {};
    const status = String(item.status || "");
    const createdAt = Number(item.createdAt || 0) || 0;

    if (activeStatuses.has(status)) {
      activeRequest = { key: child.key, ...item };
    }

    if (status === "pending_admin") {
      pendingAdminRequest = { key: child.key, ...item };
    }

    if (createdAt && now - createdAt <= CONFIG.BURST_WINDOW_MS) {
      recentCount += 1;
    }
  });

  return {
    activeRequest,
    pendingAdminRequest,
    recentCount
  };
}

function buildRiskFlags({ amount, queueInfo }) {
  return {
    overAutoMax: Number(amount || 0) > CONFIG.AUTO_WITHDRAW_MAX,
    burstRequests: Number(queueInfo?.recentCount || 0) >= CONFIG.BURST_MAX_COUNT,
    hasPendingAdmin: !!queueInfo?.pendingAdminRequest,
    hasActiveRequest: !!queueInfo?.activeRequest
  };
}

function shouldQueueForAdmin(riskFlags = {}) {
  return !!(
    riskFlags.overAutoMax ||
    riskFlags.burstRequests ||
    riskFlags.hasPendingAdmin ||
    riskFlags.hasActiveRequest
  );
}

function normalizeAmount(amount) {
  const n = Number(amount || 0);
  if (!Number.isFinite(n) || n <= 0) {
    throw new Error("Số Pi rút không hợp lệ.");
  }
  return n.toFixed(7);
}

function trimMemoText(text = "") {
  let s = String(text || "Rut Pi").replace(/[^\x20-\x7E]/g, " ").trim();
  if (!s) s = "Rut Pi";

  while (Buffer.byteLength(s, "utf8") > 28) {
    s = s.slice(0, -1);
  }

  return s;
}

async function submitOnChain({ recipientAddress, amount, memo }) {
  const StellarSdk = loadStellarSdk();

  const SOURCE_WALLET_PUBLIC = String(process.env.PI_PUBLIC_KEY_TESTNET || "").trim();
  const SOURCE_WALLET_SECRET = String(process.env.PI_SECRET_KEY_TESTNET || "").trim();

  if (!SOURCE_WALLET_PUBLIC || !SOURCE_WALLET_SECRET) {
    throw new Error("Thiếu PI_PUBLIC_KEY_TESTNET hoặc PI_SECRET_KEY_TESTNET.");
  }

  const HORIZON_URL = String(
    process.env.PI_HORIZON_TESTNET_URL || "https://api.testnet.minepi.com"
  ).trim();

  const NETWORK_PASSPHRASE = String(
    process.env.PI_NETWORK_PASSPHRASE_TESTNET || "Pi Testnet"
  ).trim();

  const ServerCtor = StellarSdk.Horizon?.Server || StellarSdk.Server;
  const server = new ServerCtor(HORIZON_URL);

  const sourceKeypair = StellarSdk.Keypair.fromSecret(SOURCE_WALLET_SECRET);
  const realPublic = sourceKeypair.publicKey();

  if (SOURCE_WALLET_PUBLIC && SOURCE_WALLET_PUBLIC !== realPublic) {
    throw new Error("PI_PUBLIC_KEY_TESTNET không khớp với PI_SECRET_KEY_TESTNET.");
  }

  const destination = String(recipientAddress || "").trim().toUpperCase();
  if (!/^G[A-Z2-7]{55}$/.test(destination)) {
    throw new Error("Địa chỉ ví nhận không hợp lệ.");
  }

  const sourceAccount = await server.loadAccount(realPublic);
  const baseFee = await server.fetchBaseFee();

  const tx = new StellarSdk.TransactionBuilder(sourceAccount, {
    fee: String(baseFee),
    networkPassphrase: NETWORK_PASSPHRASE
  })
    .addOperation(
      StellarSdk.Operation.payment({
        destination,
        asset: StellarSdk.Asset.native(),
        amount: normalizeAmount(amount)
      })
    )
    .addMemo(StellarSdk.Memo.text(trimMemoText(memo)))
    .setTimeout(90)
    .build();

  tx.sign(sourceKeypair);

  const result = await server.submitTransaction(tx);

  return {
    ok: true,
    txid: result?.hash || result?.id || "",
    data: result
  };
}

module.exports = {
  CONFIG,
  nowMs,
  safeKey,
  cleanForFirebase,
  readPiBalance,
  acquireWithdrawLock,
  releaseWithdrawLock,
  submitOnChain,
  countTodayWithdraws,
  inspectWithdrawQueue,
  buildRiskFlags,
  shouldQueueForAdmin
};