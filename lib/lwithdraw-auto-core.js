const StellarSdk = require("stellar-sdk");

const DEV_PUBLIC = String(
  process.env.DEV_PUBLIC ||
    process.env.PI_DEVELOPER_WALLET_PUBLIC_KEY ||
    process.env.PI_WALLET_PUBLIC_KEY ||
    process.env.PI_PUBLIC_KEY ||
    ""
).trim();

const DEV_SECRET = String(
  process.env.DEV_SECRET ||
    process.env.PI_DEVELOPER_WALLET_SECRET_SEED ||
    process.env.PI_WALLET_PRIVATE_KEY ||
    process.env.PI_SECRET_KEY ||
    ""
).trim();

const PI_BLOCKCHAIN_API_URL = String(
  process.env.PI_BLOCKCHAIN_API_URL ||
    process.env.PI_BLOCKCHAIN_URL ||
    process.env.PI_HORIZON_URL ||
    "https://api.testnet.minepi.com"
).trim();

const PI_NETWORK_PASSPHRASE = String(
  process.env.PI_NETWORK_PASSPHRASE || "Pi Testnet"
).trim();

const MAX_WITHDRAW_PER_TX = Number(process.env.MAX_WITHDRAW_PER_TX || 1000);
const MAX_WITHDRAW_PER_DAY_COUNT = Number(
  process.env.MAX_WITHDRAW_PER_DAY_COUNT || 5
);
const AUTO_WITHDRAW_MAX = Number(process.env.AUTO_WITHDRAW_MAX || 2);
const LOCK_TTL_MS = Number(process.env.WITHDRAW_LOCK_TTL_MS || 2 * 60 * 1000);
const WITHDRAW_ACTIVE_WINDOW_MS = Number(
  process.env.WITHDRAW_ACTIVE_WINDOW_MS || 3 * 60 * 1000
);
const WITHDRAW_BURST_WINDOW_MS = Number(
  process.env.WITHDRAW_BURST_WINDOW_MS || 10 * 60 * 1000
);
const WITHDRAW_BURST_COUNT = Number(
  process.env.WITHDRAW_BURST_COUNT || 3
);

function nowMs() {
  return Date.now();
}

function dayStartMs(ts = Date.now()) {
  const d = new Date(ts);
  d.setHours(0, 0, 0, 0);
  return d.getTime();
}

function safeKey(value) {
  return String(value || "").replace(/[.#$/[\]]/g, "_");
}

function pickString(...values) {
  for (const v of values) {
    const s = String(v ?? "").trim();
    if (s) return s;
  }
  return "";
}

function cleanForFirebase(input) {
  if (input === undefined || input === null) return null;

  if (Array.isArray(input)) {
    return input.map((x) => cleanForFirebase(x)).filter((x) => x !== undefined);
  }

  if (typeof input === "object") {
    const out = {};
    for (const [k, v] of Object.entries(input)) {
      if (v === undefined) continue;
      out[k] = cleanForFirebase(v);
    }
    return out;
  }

  return input;
}

function readPiBalance(obj) {
  const raw =
    obj && typeof obj === "object"
      ? obj.piBalance != null
        ? obj.piBalance
        : obj.balance != null
        ? obj.balance
        : 0
      : 0;

  const n = Number(raw || 0);
  return Number.isFinite(n) ? n : 0;
}

function toChainAmount(amount) {
  return Number(amount || 0).toFixed(7);
}

function safeMemoText(text) {
  const s = String(text || "").trim();
  return s.length <= 28 ? s : s.slice(0, 28);
}

async function wrapRefTransaction(ref, updateFn) {
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
  const tx = await wrapRefTransaction(lockRef, (current) => {
    const cur = current && typeof current === "object" ? current : {};
    const active = cur.active === true;
    const expired = now - Number(cur.createdAt || 0) > LOCK_TTL_MS;

    if (active && !expired) return;

    return {
      active: true,
      createdAt: now,
      expiresAt: now + LOCK_TTL_MS
    };
  });

  return !!tx.committed;
}

async function releaseWithdrawLock(lockRef, reason) {
  if (!lockRef) return;
  try {
    await lockRef.set({
      active: false,
      releasedAt: nowMs(),
      reason: String(reason || "")
    });
  } catch (_) {}
}

async function submitOnChain({ recipientAddress, amount, memo }) {
  const server = new StellarSdk.Horizon.Server(PI_BLOCKCHAIN_API_URL);
  const source = await server.loadAccount(DEV_PUBLIC);
  const baseFee = await server.fetchBaseFee();

  const tx = new StellarSdk.TransactionBuilder(source, {
    fee: String(baseFee),
    networkPassphrase: PI_NETWORK_PASSPHRASE
  })
    .addOperation(
      StellarSdk.Operation.payment({
        destination: recipientAddress,
        asset: StellarSdk.Asset.native(),
        amount: toChainAmount(amount)
      })
    )
    .addMemo(StellarSdk.Memo.text(safeMemoText(memo)))
    .setTimeout(180)
    .build();

  tx.sign(StellarSdk.Keypair.fromSecret(DEV_SECRET));

  const submitted = await server.submitTransaction(tx);

  return {
    ok: true,
    txid: pickString(submitted?.hash, submitted?.id),
    data: submitted
  };
}

async function countTodayWithdraws(db, safeWalletKey) {
  const snap = await db.ref("walletTransactions").once("value");
  const start = dayStartMs();
  let count = 0;

  snap.forEach((child) => {
    const v = child.val() || {};
    if (!String(v.type || "").startsWith("wallet_withdraw")) return;
    if (String(v.walletKey || "") !== safeWalletKey) return;
    if (Number(v.createdAt || 0) < start) return;
    count += 1;
  });

  return count;
}

async function inspectWithdrawQueueV2(db, safeWalletKey) {
  const snap = await db.ref("piWithdrawRequestsV2").once("value");
  const now = nowMs();

  let activeRequest = null;
  let pendingAdminRequest = null;
  let recentAttemptCount = 0;

  snap.forEach((child) => {
    const v = child.val() || {};
    if (String(v.walletKey || "") !== safeWalletKey) return;
    if (String(v.type || "") !== "wallet_withdraw_v2") return;

    const updatedAt = Number(v.updatedAt || v.createdAt || 0);
    const age = now - updatedAt;
    const status = String(v.status || "").trim();

    if (age <= WITHDRAW_BURST_WINDOW_MS) {
      recentAttemptCount += 1;
    }

    if (
      age <= WITHDRAW_ACTIVE_WINDOW_MS &&
      ["initiated", "auto_processing", "chain_submitted"].includes(status) &&
      !activeRequest
    ) {
      activeRequest = { key: child.key, value: v };
    }

    if (!pendingAdminRequest && status === "pending_admin") {
      pendingAdminRequest = { key: child.key, value: v };
    }
  });

  return {
    activeRequest,
    pendingAdminRequest,
    recentAttemptCount
  };
}

function buildRiskFlags({ amount, queueInfo }) {
  return {
    overAutoMax: Number(amount || 0) > AUTO_WITHDRAW_MAX,
    burstRequests: Number(queueInfo?.recentAttemptCount || 0) >= WITHDRAW_BURST_COUNT,
    hasPendingAdmin: !!queueInfo?.pendingAdminRequest,
    hasActiveRequest: !!queueInfo?.activeRequest
  };
}

function shouldQueueForAdmin(flags) {
  return !!(
    flags.overAutoMax ||
    flags.burstRequests ||
    flags.hasPendingAdmin ||
    flags.hasActiveRequest
  );
}

async function deductWalletBalance(walletRef, amount) {
  let deductOk = false;
  let newInternalBalance = 0;

  try {
    const deductTx = await wrapRefTransaction(walletRef, (current) => {
      const safeCurrent =
        current && typeof current === "object"
          ? current
          : { piBalance: 0, balance: 0 };

      const currentPi = readPiBalance(safeCurrent);
      if (currentPi < amount) return;

      const nextPi = Number((currentPi - amount).toFixed(7));

      return {
        ...safeCurrent,
        piBalance: nextPi,
        balance: nextPi,
        updatedAt: nowMs()
      };
    });

    if (deductTx.committed) {
      deductOk = true;
      const after = deductTx.snap.val() || {};
      newInternalBalance = readPiBalance(after);
    }
  } catch (_) {}

  if (!deductOk) {
    try {
      const latestSnap = await walletRef.once("value");
      const latestVal = latestSnap.val() || {};
      const latestPi = readPiBalance(latestVal);

      if (latestPi >= amount) {
        const nextPi = Number((latestPi - amount).toFixed(7));
        await walletRef.update(
          cleanForFirebase({
            piBalance: nextPi,
            balance: nextPi,
            updatedAt: nowMs()
          })
        );
        deductOk = true;
        newInternalBalance = nextPi;
      }
    } catch (_) {}
  }

  return { deductOk, newInternalBalance };
}

module.exports = {
  CONFIG: {
    DEV_PUBLIC,
    DEV_SECRET,
    PI_BLOCKCHAIN_API_URL,
    PI_NETWORK_PASSPHRASE,
    MAX_WITHDRAW_PER_TX,
    MAX_WITHDRAW_PER_DAY_COUNT,
    AUTO_WITHDRAW_MAX,
    LOCK_TTL_MS,
    WITHDRAW_ACTIVE_WINDOW_MS,
    WITHDRAW_BURST_WINDOW_MS,
    WITHDRAW_BURST_COUNT
  },
  nowMs,
  safeKey,
  pickString,
  cleanForFirebase,
  readPiBalance,
  acquireWithdrawLock,
  releaseWithdrawLock,
  submitOnChain,
  countTodayWithdraws,
  inspectWithdrawQueueV2,
  buildRiskFlags,
  shouldQueueForAdmin,
  deductWalletBalance
};