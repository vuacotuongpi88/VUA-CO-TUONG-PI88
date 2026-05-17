const StellarSdk = require("stellar-sdk");

function envNumber(...values) {
  for (const value of values) {
    const n = Number(value);
    if (Number.isFinite(n)) return n;
  }
  return 0;
}

const CONFIG = {
  // Bỏ giới hạn số Pi rút mỗi lần
  MAX_WITHDRAW_PER_TX: envNumber(
    process.env.MAX_WITHDRAW_PER_TX,
    process.env.PI_MAX_WITHDRAW_PER_TX,
    999999999
  ),

  // Bỏ giới hạn số lượt rút trong ngày
  MAX_WITHDRAW_PER_DAY_COUNT: envNumber(
    process.env.MAX_WITHDRAW_PER_DAY_COUNT,
    process.env.PI_MAX_WITHDRAW_PER_DAY_COUNT,
    999999
  ),

  // 0 = không đưa lệnh rút Pi cao vào duyệt thủ công
  AUTO_WITHDRAW_MAX: envNumber(
    process.env.AUTO_WITHDRAW_MAX,
    process.env.PI_AUTO_WITHDRAW_MAX,
    0
  ),
  BURST_WINDOW_MS: envNumber(
    process.env.WITHDRAW_BURST_WINDOW_MS,
    3 * 60 * 1000
  ),
  BURST_REQUEST_LIMIT: envNumber(
    process.env.WITHDRAW_BURST_REQUEST_LIMIT,
    3
  ),
  PI_BLOCKCHAIN_API_URL: String(
    process.env.PI_BLOCKCHAIN_API_URL ||
      process.env.PI_HORIZON_URL ||
      "https://api.testnet.minepi.com"
  ).trim(),
  PI_NETWORK_PASSPHRASE: String(
    process.env.PI_NETWORK_PASSPHRASE || "Pi Testnet"
  ).trim()
};

function nowMs() {
  return Date.now();
}

function safeKey(value) {
  return String(value || "").replace(/[.#$/\[\]]/g, "_");
}

function cleanForFirebase(input) {
  if (input === undefined) return null;
  if (input === null) return null;

  if (typeof input === "number" && !Number.isFinite(input)) {
    return null;
  }

  if (Array.isArray(input)) {
    return input.map((item) => cleanForFirebase(item));
  }

  if (typeof input === "object") {
    const out = {};
    for (const [key, value] of Object.entries(input)) {
      if (value === undefined) continue;
      out[key] = cleanForFirebase(value);
    }
    return out;
  }

  return input;
}

function readPiBalance(walletVal) {
  if (!walletVal || typeof walletVal !== "object") return 0;

  const candidates = [
    walletVal.piBalance,
    walletVal.balance,
    walletVal.pi_balance,
    walletVal.pi,
    walletVal.currentBalance
  ];

  for (const value of candidates) {
    const n = Number(value);
    if (Number.isFinite(n)) return n;
  }

  return 0;
}

function startOfTodayMs(ts = Date.now()) {
  const d = new Date(ts);
  d.setHours(0, 0, 0, 0);
  return d.getTime();
}

function refTransaction(ref, updateFn) {
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
  const result = await refTransaction(lockRef, (current) => {
    if (current && current.active === true) return;
    return cleanForFirebase({
      active: true,
      lockedAt: nowMs()
    });
  });

  return !!result.committed;
}

async function releaseWithdrawLock(lockRef, reason = "") {
  try {
    await lockRef.set(
      cleanForFirebase({
        active: false,
        reason: String(reason || ""),
        releasedAt: nowMs()
      })
    );
  } catch (_) {}
}

async function countTodayWithdraws(db, walletKey) {
  const snap = await db.ref("piWithdrawRequests").once("value");
  const start = startOfTodayMs();
  let count = 0;

  snap.forEach((child) => {
    const value = child.val() || {};
    if (String(value.walletKey || "") !== String(walletKey || "")) return;

    const status = String(value.status || "");
    if (status !== "done") return;

    const t = Number(
      value.doneAt || value.updatedAt || value.createdAt || 0
    );

    if (Number.isFinite(t) && t >= start) {
      count += 1;
    }
  });

  return count;
}

async function inspectWithdrawQueue(db, walletKey) {
  const snap = await db.ref("piWithdrawRequests").once("value");
  const recentStart = nowMs() - CONFIG.BURST_WINDOW_MS;

  const ACTIVE_STATUSES = new Set([
    "initiated",
    "auto_processing",
    "chain_submitted",
    "processing",
    "created"
  ]);

  let activeRequest = null;
  let pendingAdminRequest = null;
  let recentCount = 0;

  snap.forEach((child) => {
    const value = child.val() || {};
    if (String(value.walletKey || "") !== String(walletKey || "")) return;

    const status = String(value.status || "");
    const createdAt = Number(value.createdAt || value.updatedAt || 0);

    if (Number.isFinite(createdAt) && createdAt >= recentStart) {
      recentCount += 1;
    }

    if (!activeRequest && ACTIVE_STATUSES.has(status)) {
      activeRequest = {
        key: child.key,
        status,
        data: value
      };
    }

    if (!pendingAdminRequest && status === "pending_admin") {
      pendingAdminRequest = {
        key: child.key,
        status,
        data: value
      };
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
    // Không còn luật: rút Pi cao phải đưa vào duyệt
    overAutoMax: false,

    // Không đưa spam nhanh vào hàng duyệt nữa
    // Chỉ để log nếu sau này cần xem, không dùng để queue admin.
    burstRequests: Number(queueInfo?.recentCount || 0) >= Number(CONFIG.BURST_REQUEST_LIMIT || 999999),

    // Hai cái này vẫn giữ để tránh 1 ví tạo nhiều lệnh rút đang chạy cùng lúc
    hasPendingAdmin: !!queueInfo?.pendingAdminRequest,
    hasActiveRequest: !!queueInfo?.activeRequest,

    // Chỉ để hiển thị/debug, không dùng để queue
    oldPendingOtherWallet: String(queueInfo?.lastOtherWalletHint || "").trim()
  };
}

function shouldQueueForAdmin(riskFlags) {
  // Không đưa vào hàng duyệt vì số Pi cao.
  // Không đưa vào hàng duyệt vì bấm nhanh.
  // Chỉ chặn nếu ví đang có lệnh pending/active để tránh rút trùng.
  return !!(
    riskFlags?.hasPendingAdmin ||
    riskFlags?.hasActiveRequest
  );
}

function normalizeMemo(text) {
  const raw = String(text || "").replace(/\s+/g, " ").trim();
  if (!raw) return "";
  return raw.slice(0, 28);
}

function getStellarServer() {
  const ServerCtor =
    StellarSdk.Server || (StellarSdk.Horizon && StellarSdk.Horizon.Server);

  if (!ServerCtor) {
    throw new Error("Không khởi tạo được Stellar Server.");
  }

  return new ServerCtor(CONFIG.PI_BLOCKCHAIN_API_URL);
}

async function submitOnChain({ recipientAddress, amount, memo }) {
  const sourcePublic = String(
    process.env.DEV_PUBLIC ||
      process.env.PI_DEVELOPER_WALLET_PUBLIC_KEY ||
      process.env.PI_WALLET_PUBLIC_KEY ||
      process.env.PI_PUBLIC_KEY ||
      ""
  ).trim();

  const sourceSecret = String(
    process.env.DEV_SECRET ||
      process.env.PI_DEVELOPER_WALLET_SECRET_SEED ||
      process.env.PI_WALLET_PRIVATE_KEY ||
      process.env.PI_SECRET_KEY ||
      ""
  ).trim();

  if (!sourcePublic || !sourceSecret) {
    throw new Error("Thiếu DEV_PUBLIC/DEV_SECRET.");
  }

  if (!recipientAddress) {
    throw new Error("Thiếu recipientAddress.");
  }

  if (!StellarSdk.StrKey.isValidEd25519PublicKey(sourcePublic)) {
    throw new Error("DEV_PUBLIC không hợp lệ.");
  }

  if (!StellarSdk.StrKey.isValidEd25519PublicKey(recipientAddress)) {
    throw new Error("Ví Pi nhận tiền không hợp lệ.");
  }

  const numericAmount = Number(amount);
  if (!Number.isFinite(numericAmount) || numericAmount <= 0) {
    throw new Error("Số Pi rút không hợp lệ.");
  }

  const amountStr = numericAmount.toFixed(7);
  const sourceKeypair = StellarSdk.Keypair.fromSecret(sourceSecret);

  if (sourceKeypair.publicKey() !== sourcePublic) {
    throw new Error("DEV_PUBLIC không khớp DEV_SECRET.");
  }

  const server = getStellarServer();
  const sourceAccount = await server.loadAccount(sourcePublic);
  const baseFee = await server.fetchBaseFee();

  const txBuilder = new StellarSdk.TransactionBuilder(sourceAccount, {
    fee: String(baseFee),
    networkPassphrase: CONFIG.PI_NETWORK_PASSPHRASE
  }).addOperation(
    StellarSdk.Operation.payment({
      destination: recipientAddress,
      asset: StellarSdk.Asset.native(),
      amount: amountStr
    })
  );

  const safeMemo = normalizeMemo(memo);
  if (safeMemo) {
    txBuilder.addMemo(StellarSdk.Memo.text(safeMemo));
  }

  const tx = txBuilder.setTimeout(30).build();
  tx.sign(sourceKeypair);

  const submitResp = await server.submitTransaction(tx);

  return {
    txid: String(submitResp?.hash || ""),
    data: submitResp
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
