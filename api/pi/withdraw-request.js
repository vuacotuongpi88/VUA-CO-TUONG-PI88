const StellarSdk = require("stellar-sdk");

const PI_API_BASE = String(
  process.env.PI_API_BASE_URL || "https://api.minepi.com"
).trim();

const PI_API_KEY = String(
  process.env.PI_API_KEY ||
    process.env.PI_SERVER_API_KEY ||
    process.env.PI_APIKEY ||
    ""
).trim();

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
const LOCK_TTL_MS = Number(process.env.WITHDRAW_LOCK_TTL_MS || 2 * 60 * 1000);

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
  if (input === undefined) return null;
  if (input === null) return null;

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

async function readResponseData(res) {
  const raw = await res.text();
  if (!raw) return {};
  try {
    return JSON.parse(raw);
  } catch (_) {
    return { raw };
  }
}

function extractApiError(data) {
  return pickString(
    data?.error,
    data?.message,
    data?.error_message,
    data?.raw,
    data?.data?.error,
    data?.data?.message,
    data?.status?.error,
    "Pi API error"
  );
}

function extractPaymentId(data) {
  return pickString(
    data?.identifier,
    data?.paymentId,
    data?.payment_id,
    data?.id,
    data?.payment?.identifier,
    data?.payment?.paymentId,
    data?.payment?.payment_id,
    data?.payment?.id
  );
}

function extractRecipientAddress(data) {
  return pickString(
    data?.recipientAddress,
    data?.recipient_address,
    data?.to,
    data?.to_address,
    data?.recipient,
    data?.payment?.recipientAddress,
    data?.payment?.recipient_address,
    data?.payment?.to,
    data?.payment?.to_address,
    data?.payment?.recipient,
    data?.transaction?.to,
    data?.transaction?.to_address,
    data?.transaction?.destination,
    data?.tx?.to,
    data?.tx?.destination
  );
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

async function countTodayWithdraws(db, safeWalletKey) {
  const snap = await db.ref("walletTransactions").once("value");
  const start = dayStartMs();
  let count = 0;

  snap.forEach((child) => {
    const v = child.val() || {};
    if (String(v.type || "") !== "wallet_withdraw") return;
    if (String(v.walletKey || "") !== safeWalletKey) return;
    if (Number(v.createdAt || 0) < start) return;
    count += 1;
  });

  return count;
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

async function createPiPayment({ amount, piUid, memo, metadata }) {
  const res = await fetch(`${PI_API_BASE}/v2/payments`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Authorization: `Key ${PI_API_KEY}`,
      "Pi-Api-Key": PI_API_KEY
    },
    body: JSON.stringify({
      payment: {
        uid: piUid,
        amount: Number(amount),
        memo,
        metadata
      }
    })
  });

  const data = await readResponseData(res);

  if (!res.ok) {
    return {
      ok: false,
      status: res.status,
      error: extractApiError(data),
      data
    };
  }

  return {
    ok: true,
    status: res.status,
    data
  };
}

async function completePiPayment(paymentId, txid) {
  const res = await fetch(
    `${PI_API_BASE}/v2/payments/${encodeURIComponent(paymentId)}/complete`,
    {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Key ${PI_API_KEY}`,
        "Pi-Api-Key": PI_API_KEY
      },
      body: JSON.stringify({ txid })
    }
  );

  const data = await readResponseData(res);

  if (!res.ok) {
    return {
      ok: false,
      status: res.status,
      error: extractApiError(data),
      data
    };
  }

  return {
    ok: true,
    status: res.status,
    data
  };
}

async function cancelPiPayment(paymentId) {
  const res = await fetch(
    `${PI_API_BASE}/v2/payments/${encodeURIComponent(paymentId)}/cancel`,
    {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Key ${PI_API_KEY}`,
        "Pi-Api-Key": PI_API_KEY
      }
    }
  );

  const data = await readResponseData(res);
  return {
    ok: res.ok,
    status: res.status,
    data
  };
}
async function cleanupOldPendingWithdraw(db, walletKey, piUid) {
  const safeWalletKey = String(walletKey || "").trim();
  const safePiUid = String(piUid || "").trim();

  const snap = await db.ref("piWithdrawRequests").once("value");
  let targetKey = "";
  let target = null;
  let targetPaymentId = "";
  let targetTxid = "";

  snap.forEach((child) => {
    const v = child.val() || {};

    if (String(v.type || "") !== "wallet_withdraw") return;

    const candidateWalletKey = safeKey(
      pickString(
        v.walletKey,
        v?.paymentCreateData?.metadata?.walletKey,
        v?.paymentCreateData?.payment?.metadata?.walletKey,
        v.walletKeyRaw
      )
    );

    const candidatePiUid = String(v.piUid || "").trim();

    // Chỉ đụng request cũ thuộc ĐÚNG ví hiện tại
    if (safeWalletKey && candidateWalletKey !== safeWalletKey) return;

    // Lớp chặn thêm: nếu record có piUid thì phải khớp piUid hiện tại
    if (safePiUid && candidatePiUid && candidatePiUid !== safePiUid) return;

    const nestedPayment = v?.paymentCreateData?.payment || {};
    const nestedStatus = nestedPayment?.status || {};

    const candidatePaymentId = pickString(
      v.paymentId,
      v?.paymentCreateData?.identifier,
      v?.paymentCreateData?.paymentId,
      v?.paymentCreateData?.payment_id,
      nestedPayment?.identifier,
      nestedPayment?.paymentId,
      nestedPayment?.payment_id,
      nestedPayment?.id
    );

    const candidateTxid = pickString(
      v.txid,
      v?.paymentCreateData?.transaction?.txid,
      nestedPayment?.transaction?.txid
    );

    const isDone =
      String(v.status || "").trim() === "done" ||
      nestedStatus.developer_completed === true;

    const isCancelled =
      String(v.status || "").trim().startsWith("cancelled") ||
      nestedStatus.cancelled === true ||
      nestedStatus.user_cancelled === true;

    if (!candidatePaymentId) return;
    if (isDone || isCancelled) return;

    if (!target || Number(v.updatedAt || 0) > Number(target.updatedAt || 0)) {
      targetKey = child.key;
      target = v;
      targetPaymentId = candidatePaymentId;
      targetTxid = candidateTxid;
    }
  });

  if (!targetKey || !targetPaymentId) {
    return { found: false, cleaned: false };
  }

  // Nếu payment cũ đã có txid -> complete trước
  if (targetTxid) {
    const completeRes = await completePiPayment(targetPaymentId, targetTxid);

    await db.ref(`piWithdrawRequests/${targetKey}`).update(
      cleanForFirebase({
        status: completeRes.ok
          ? "completed_old_pending_payment"
          : "complete_old_pending_failed",
        oldPendingPaymentId: targetPaymentId,
        oldPendingTxid: targetTxid,
        completeStatus: completeRes.status,
        completeData: completeRes.data || null,
        updatedAt: nowMs()
      })
    );

    return {
      found: true,
      cleaned: completeRes.ok,
      action: "complete",
      paymentId: targetPaymentId,
      txid: targetTxid,
      status: completeRes.status,
      data: completeRes.data || null
    };
  }

  // Nếu chưa có txid -> cancel
  const cancelRes = await cancelPiPayment(targetPaymentId);

  await db.ref(`piWithdrawRequests/${targetKey}`).update(
    cleanForFirebase({
      status: cancelRes.ok ? "cancelled" : "cancel_old_pending_failed",
      oldPendingPaymentId: targetPaymentId,
      cancelStatus: cancelRes.status,
      cancelData: cancelRes.data || null,
      updatedAt: nowMs()
    })
  );

  return {
    found: true,
    cleaned: cancelRes.ok,
    action: "cancel",
    paymentId: targetPaymentId,
    status: cancelRes.status,
    data: cancelRes.data || null
  };
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

module.exports = async function handler(req, res) {
  if (req.method !== "POST") {
    return res.status(405).json({
      ok: false,
      error: "Method not allowed"
    });
  }

  let stage = "start";
  let requestRef = null;
  let lockRef = null;
  let withdrawId = "";
  let paymentId = "";
  let txid = "";

  try {
    stage = "db-init";
    const adminBundle = require("../_firebaseAdmin.js");
    const { getDatabase } = require("firebase-admin/database");
    const adminApp = adminBundle.app || adminBundle;
    const db = getDatabase(adminApp);

    if (!PI_API_KEY) {
      return res.status(500).json({
        ok: false,
        error: "Thiếu PI_API_KEY."
      });
    }

    if (!DEV_PUBLIC || !DEV_SECRET) {
      return res.status(500).json({
        ok: false,
        error: "Thiếu DEV_PUBLIC/DEV_SECRET."
      });
    }

    stage = "read-body";
    const body =
      typeof req.body === "string"
        ? JSON.parse(req.body || "{}")
        : req.body || {};

    const amount = Number(body.amount || 0);
    const walletKeyRaw = String(
      req.headers["x-wallet-key"] || body.walletKey || ""
    ).trim();

    if (!walletKeyRaw) {
      return res.status(400).json({
        ok: false,
        error: "Thiếu walletKey từ frontend."
      });
    }

    if (!Number.isFinite(amount) || amount <= 0) {
      return res.status(400).json({
        ok: false,
        error: "Số Pi rút không hợp lệ."
      });
    }

    if (amount > MAX_WITHDRAW_PER_TX) {
      return res.status(400).json({
        ok: false,
        error: `Mỗi lần chỉ được rút tối đa ${MAX_WITHDRAW_PER_TX} Pi.`
      });
    }

    const safeWalletKey = safeKey(walletKeyRaw);

    console.log("WITHDRAW_STAGE", {
      stage,
      amount,
      walletKeyRaw,
      safeWalletKey
    });

        stage = "wallet-read";
    const walletRef = db.ref(`wallets/${safeWalletKey}`);
    const walletSnap = await walletRef.once("value");
    const walletVal = walletSnap.val() || {};

    if (!walletVal || typeof walletVal !== "object") {
      return res.status(404).json({
        ok: false,
        error: "Không tìm thấy ví nội bộ."
      });
    }

    // KHÓA CHẶT:
    // Chỉ dùng Pi UID đã link đúng trên CHÍNH walletKey hiện tại.
    // Tuyệt đối không copy piUid từ ví khác theo username / name / piUsername.
    if (walletVal.piVerified !== true || !String(walletVal.piUid || "").trim()) {
      return res.status(400).json({
        ok: false,
        error: "Chưa có verified Pi uid cho đúng ví này. Bấm Liên kết Pi Browser trước."
      });
    }

    const piUid = String(walletVal.piUid || "").trim();
    const piUsername = String(
      walletVal.piUsername || walletVal.username || walletVal.name || ""
    ).trim();
    const currentInternalBalance = readPiBalance(walletVal);

    if (amount > currentInternalBalance) {
      return res.status(400).json({
        ok: false,
        error: `Số dư không đủ. Hiện còn ${currentInternalBalance.toFixed(2)} Pi.`,
        currentBalance: currentInternalBalance
      });
    }

    stage = "daily-limit";
    const todayCount = await countTodayWithdraws(db, safeWalletKey);
    if (todayCount >= MAX_WITHDRAW_PER_DAY_COUNT) {
      return res.status(400).json({
        ok: false,
        error: `Hôm nay đã dùng hết ${MAX_WITHDRAW_PER_DAY_COUNT} lượt rút.`,
        leftToday: 0
      });
    }

    stage = "lock";
    lockRef = db.ref(`wallets/${safeWalletKey}/withdrawLock`);
    const locked = await acquireWithdrawLock(lockRef);

    if (!locked) {
      return res.status(409).json({
        ok: false,
        error: "Đang có lệnh rút khác xử lý. Chờ chút rồi thử lại."
      });
    }
    stage = "cleanup-old-pending";
const cleanupResult = await cleanupOldPendingWithdraw(db, safeWalletKey, piUid);
console.log("CLEANUP_RESULT", cleanupResult);

if (cleanupResult.found && !cleanupResult.cleaned) {
  const verifyErr = String(
    cleanupResult?.data?.verification_error ||
    cleanupResult?.data?.error ||
    ""
  ).trim();

  await releaseWithdrawLock(lockRef, "cleanup_old_pending_failed");

  return res.status(409).json({
    ok: false,
    error:
      verifyErr === "payment_already_linked_with_a_tx"
        ? `Đang kẹt payment Pi cũ ${cleanupResult.paymentId}. Payment này đã gắn với 1 tx cũ, cần xử lý thủ công trước khi rút tiếp.`
        : "Đang còn payment Pi cũ bị pending, app đã thử xử lý nhưng chưa xong.",
    cleanup: cleanupResult
  });
}

stage = "create-request";
    requestRef = db.ref("piWithdrawRequests").push();
    withdrawId = requestRef.key || "";

    const memo = `Rut ${Number(amount).toFixed(2)} Pi tu app`;

    await requestRef.set(
      cleanForFirebase({
        status: "initiated",
        type: "wallet_withdraw",
        walletKey: safeWalletKey,
        walletKeyRaw,
        piUid,
        piUsername,
        amount,
        memo,
        createdAt: nowMs(),
        updatedAt: nowMs()
      })
    );

    console.log("WITHDRAW_REQUEST_CREATED", { withdrawId });

    stage = "create-payment";
    const createResult = await createPiPayment({
      amount,
      piUid,
      memo,
      metadata: {
        kind: "wallet_withdraw",
        withdrawId,
        walletKey: safeWalletKey
      }
    });

    console.log("WITHDRAW_CREATE_RESULT", createResult?.status, createResult?.data);

    if (!createResult.ok) {
  const errText = String(createResult.error || "").trim();

  console.log("WITHDRAW_CREATE_FAIL", {
    errText,
    createData: createResult.data || null
  });

  await requestRef.update(
    cleanForFirebase({
      status: "create_payment_failed",
      paymentCreateStatus: createResult.status,
      paymentCreateData: createResult.data || null,
      failReason: errText || "create_payment_failed",
      updatedAt: nowMs()
    })
  );

  await releaseWithdrawLock(lockRef, "create_payment_failed");

  return res.status(400).json({
  ok: false,
  error:
    errText === "ongoing_payment_found"
      ? "Tài khoản Pi này đang còn payment cũ chưa complete. App đang bị Pi chặn tạo lệnh mới."
      : (errText || "Tạo payout thất bại.")
});
}

    const createData = createResult.data || {};
    paymentId = extractPaymentId(createData);

    const recipientAddress = extractRecipientAddress(createData);
    if (!recipientAddress) {
      await requestRef.update(
        cleanForFirebase({
          status: "missing_recipient_address",
          paymentId,
          paymentCreateData: createData,
          updatedAt: nowMs()
        })
      );

      await releaseWithdrawLock(lockRef, "missing_recipient_address");

      return res.status(500).json({
        ok: false,
        error: "Pi payout đã tạo nhưng thiếu địa chỉ nhận."
      });
    }

    stage = "submit-chain";
    const chainResult = await submitOnChain({
      recipientAddress,
      amount,
      memo
    });

    txid = pickString(chainResult?.txid);

    await requestRef.update(
      cleanForFirebase({
        status: "chain_submitted",
        paymentId,
        txid,
        recipientAddress,
        chainSubmitData: chainResult?.data || null,
        updatedAt: nowMs()
      })
    );

    stage = "complete-payment";
    let completeResult = { ok: true, status: 200, data: {} };
    if (paymentId) {
      completeResult = await completePiPayment(paymentId, txid);
      await requestRef.update(
        cleanForFirebase({
          paymentCompleteStatus: completeResult.status,
          paymentCompleteData: completeResult.data || null,
          updatedAt: nowMs()
        })
      );
    }

    stage = "deduct-internal";
    let deductOk = false;
    let newInternalBalance = currentInternalBalance;

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
          await walletRef.update({
            piBalance: nextPi,
            balance: nextPi,
            updatedAt: nowMs()
          });
          deductOk = true;
          newInternalBalance = nextPi;
        }
      } catch (_) {}
    }

    if (!deductOk) {
      await requestRef.update(
        cleanForFirebase({
          status: "internal_deduct_failed_after_chain_success",
          paymentId,
          txid,
          updatedAt: nowMs()
        })
      );

      await releaseWithdrawLock(lockRef, "internal_deduct_failed_after_chain_success");

      return res.status(200).json({
        ok: true,
        warning: true,
        error:
          "Pi đã về ví ngoài nhưng app chưa trừ nội bộ. Tao đã đánh dấu để xử lý tiếp.",
        withdrawId,
        paymentId,
        txid
      });
    }

    stage = "write-transaction";
    await db.ref("walletTransactions").push().set(
      cleanForFirebase({
        type: "wallet_withdraw",
        walletKey: safeWalletKey,
        walletKeyRaw,
        piUid,
        piUsername,
        amount,
        paymentId,
        txid,
        withdrawId,
        internalBalanceAfter: newInternalBalance,
        createdAt: nowMs()
      })
    );

    stage = "finish-request";
    await requestRef.update(
      cleanForFirebase({
        status: "done",
        paymentId,
        txid,
        internalBalanceAfter: newInternalBalance,
        doneAt: nowMs(),
        updatedAt: nowMs()
      })
    );

    await releaseWithdrawLock(lockRef, "done");

    return res.status(200).json({
      ok: true,
      withdrawId,
      paymentId,
      txid,
      amount,
      leftToday: Math.max(0, MAX_WITHDRAW_PER_DAY_COUNT - (todayCount + 1))
    });
  } catch (err) {
    const msg = err?.message || String(err);

    try {
      if (requestRef) {
        await requestRef.update(
          cleanForFirebase({
            status: "failed",
            failReason: msg,
            stage,
            paymentId,
            txid,
            updatedAt: nowMs()
          })
        );
      }
    } catch (_) {}

    try {
      if (paymentId && !txid) {
        await cancelPiPayment(paymentId);
      }
    } catch (_) {}

    await releaseWithdrawLock(lockRef, "failed");

    return res.status(500).json({
      ok: false,
      error: msg,
      stage
    });
  }
};