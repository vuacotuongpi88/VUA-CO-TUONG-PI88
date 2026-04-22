function loadDeps() {
  const { getDatabase } = require("firebase-admin/database");
  const adminBundle = require("../_firebaseAdmin.js");
  const core = require("../../lib/withdraw-auto-core.js");

  const SOURCE_WALLET_PUBLIC = String(
    process.env.DEV_PUBLIC ||
      process.env.PI_DEVELOPER_WALLET_PUBLIC_KEY ||
      process.env.PI_WALLET_PUBLIC_KEY ||
      process.env.PI_PUBLIC_KEY ||
      ""
  ).trim();

  const SOURCE_WALLET_SECRET = String(
    process.env.DEV_SECRET ||
      process.env.PI_DEVELOPER_WALLET_SECRET_SEED ||
      process.env.PI_WALLET_PRIVATE_KEY ||
      process.env.PI_SECRET_KEY ||
      ""
  ).trim();

  return {
    getDatabase,
    adminBundle,
    SOURCE_WALLET_PUBLIC,
    SOURCE_WALLET_SECRET,
    ...core
  };
}

function pickString(...values) {
  for (const value of values) {
    const s = String(value ?? "").trim();
    if (s) return s;
  }
  return "";
}

function wrapRefTransaction(ref, updateFn) {
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

function pickRecipientAddressInfo(walletVal) {
  const candidates = [
    ["piWalletAddress", walletVal?.piWalletAddress],
    ["linkedWalletAddress", walletVal?.linkedWalletAddress],
    ["walletAddress", walletVal?.walletAddress],
    ["piWallet", walletVal?.piWallet],
    ["piBrowserWalletAddress", walletVal?.piBrowserWalletAddress],
    ["paymentRecipientAddress", walletVal?.paymentRecipientAddress],
    ["linkedWallet.address", walletVal?.linkedWallet?.address],
    ["piLink.walletAddress", walletVal?.piLink?.walletAddress],
    ["piBrowser.address", walletVal?.piBrowser?.address]
  ];

  for (const [field, value] of candidates) {
    const address = pickString(value);
    if (address) {
      return { address, sourceField: field };
    }
  }

  return { address: "", sourceField: "" };
}

function buildPendingAdminMessage(riskFlags, CONFIG) {
  const reasons = [];

  if (riskFlags?.overAutoMax) {
    reasons.push(`Số Pi rút vượt ngưỡng auto ${CONFIG.AUTO_WITHDRAW_MAX} Pi`);
  }
  if (riskFlags?.burstRequests) {
    reasons.push("Ví bấm rút quá nhanh trong thời gian ngắn");
  }
  if (riskFlags?.hasPendingAdmin) {
    reasons.push("Đã có lệnh chờ duyệt trước đó");
  }
  if (riskFlags?.hasActiveRequest) {
    reasons.push("Đang có lệnh xử lý song song");
  }

  return (
    "Lệnh rút đã được đưa vào hàng duyệt thủ công. " +
    (reasons.length ? reasons.join(" | ") : "Admin sẽ xử lý sớm.")
  );
}

async function deductWalletBalance(walletRef, amount, deps) {
  const { readPiBalance, nowMs } = deps;
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

  return { deductOk, newInternalBalance };
}

module.exports = async function handler(req, res) {
  if (req.method !== "POST") {
    return res.status(405).json({
      ok: false,
      error: "Method not allowed"
    });
  }

  let deps;
  try {
    deps = loadDeps();
  } catch (e) {
    return res.status(500).json({
      ok: false,
      error: "loadDeps failed: " + (e?.message || String(e))
    });
  }

  const {
    getDatabase,
    adminBundle,
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
    shouldQueueForAdmin,
    SOURCE_WALLET_PUBLIC,
    SOURCE_WALLET_SECRET
  } = deps;
  
  let stage = "start";
  let requestRef = null;
  let lockRef = null;
  let withdrawId = "";
  let txid = "";

  try {
    stage = "env-check";

    if (!SOURCE_WALLET_PUBLIC || !SOURCE_WALLET_SECRET) {
      return res.status(500).json({
        ok: false,
        error: "Thiếu DEV_PUBLIC/DEV_SECRET cho ví nguồn hệ thống."
      });
    }

    stage = "read-body";
    const body =
      typeof req.body === "string"
        ? JSON.parse(req.body || "{}")
        : req.body || {};

    const amount = Number(body.amount || 0);
    const walletKeyRaw = pickString(
      req.headers["x-wallet-key"],
      body.walletKey
    );
    const safeWalletKey = safeKey(walletKeyRaw);

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

    if (amount > Number(CONFIG.MAX_WITHDRAW_PER_TX || 0)) {
      return res.status(400).json({
        ok: false,
        error: `Mỗi lần chỉ được rút tối đa ${CONFIG.MAX_WITHDRAW_PER_TX} Pi.`
      });
    }

    stage = "db-init";
    const adminApp = adminBundle.app || adminBundle;
    const db = getDatabase(adminApp);

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

    if (walletVal.piVerified !== true || !pickString(walletVal.piUid)) {
      return res.status(400).json({
        ok: false,
        error:
          "Tài khoản này chưa liên kết Pi Browser hoặc chưa verify Pi UID."
      });
    }

    const piUid = pickString(walletVal.piUid, body.piUid);
    const piUsername = pickString(
      walletVal.piUsername,
      walletVal.username,
      walletVal.name,
      body.piUsername
    );

    const { address: recipientAddress, sourceField } =
      pickRecipientAddressInfo(walletVal);

    if (!recipientAddress) {
      return res.status(400).json({
        ok: false,
        error:
          "Tài khoản này chưa có ví Pi nhận tiền. Bấm Liên kết Pi Browser trước."
      });
    }

    if (recipientAddress === SOURCE_WALLET_PUBLIC) {
      return res.status(400).json({
        ok: false,
        error:
          "Ví nhận của khách đang trùng ví nguồn hệ thống. Không cho auto rút kiểu này.",
        recipientAddress,
        sourceWallet: SOURCE_WALLET_PUBLIC
      });
    }

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
    if (todayCount >= Number(CONFIG.MAX_WITHDRAW_PER_DAY_COUNT || 0)) {
      return res.status(400).json({
        ok: false,
        error: `Hôm nay đã dùng hết ${CONFIG.MAX_WITHDRAW_PER_DAY_COUNT} lượt rút.`,
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

    stage = "queue-inspect";
    const queueInfo = await inspectWithdrawQueue(db, safeWalletKey);

    if (queueInfo.activeRequest) {
      await releaseWithdrawLock(lockRef, "active_request_exists");
      return res.status(409).json({
        ok: false,
        error: "Đang có lệnh rút khác xử lý. Chờ chút rồi thử lại.",
        withdrawId: queueInfo.activeRequest.key
      });
    }

    if (queueInfo.pendingAdminRequest) {
      await releaseWithdrawLock(lockRef, "pending_admin_exists");
      return res.status(409).json({
        ok: false,
        pendingAdmin: true,
        error:
          "Ví này đang có lệnh rút chờ duyệt. Không cần bấm thêm, admin sẽ xử lý.",
        withdrawId: queueInfo.pendingAdminRequest.key
      });
    }

    const riskFlags = buildRiskFlags({
      amount,
      queueInfo
    });

    stage = "create-request";
    requestRef = db.ref("piWithdrawRequests").push();
    withdrawId = requestRef.key || "";

    const queueForAdmin = shouldQueueForAdmin(riskFlags);
    const memo = `Rut ${Number(amount).toFixed(2)} Pi tu app`;

    await requestRef.set(
      cleanForFirebase({
        status: queueForAdmin ? "pending_admin" : "initiated",
        type: "wallet_withdraw",
        requestMode: queueForAdmin ? "admin_queue" : "auto_direct_v2",
        walletKey: safeWalletKey,
        walletKeyRaw,
        piUid,
        piUsername,
        amount,
        memo,
        sourceWalletAddress: SOURCE_WALLET_PUBLIC,
        recipientAddress,
        recipientAddressField: sourceField,
        autoEligible: !queueForAdmin,
        riskFlags,
        createdAt: nowMs(),
        updatedAt: nowMs()
      })
    );

    if (queueForAdmin) {
      await releaseWithdrawLock(lockRef, "queued_for_admin");
      return res.status(409).json({
        ok: false,
        pendingAdmin: true,
        withdrawId,
        error: buildPendingAdminMessage(riskFlags, CONFIG),
        riskFlags
      });
    }

    stage = "auto-processing";
    await requestRef.update(
      cleanForFirebase({
        status: "auto_processing",
        updatedAt: nowMs()
      })
    );

    stage = "submit-chain";
    const chainResult = await submitOnChain({
      recipientAddress,
      amount,
      memo
    });

    txid = pickString(chainResult?.txid, chainResult?.data?.hash, chainResult?.data?.id);

    if (!txid) {
      await requestRef.update(
        cleanForFirebase({
          status: "chain_submit_missing_txid",
          chainSubmitData: chainResult?.data || null,
          updatedAt: nowMs()
        })
      );

      await releaseWithdrawLock(lockRef, "chain_submit_missing_txid");

      return res.status(502).json({
        ok: false,
        error: "Submit blockchain không trả txid.",
        withdrawId
      });
    }

    await requestRef.update(
      cleanForFirebase({
        status: "chain_submitted",
        txid,
        sourceWalletAddress: SOURCE_WALLET_PUBLIC,
        recipientAddress,
        chainSubmitData: chainResult?.data || null,
        updatedAt: nowMs()
      })
    );

    stage = "deduct-internal";
    const { deductOk, newInternalBalance } = await deductWalletBalance(
  walletRef,
  amount,
  { readPiBalance, nowMs }
);

    if (!deductOk) {
      await requestRef.update(
        cleanForFirebase({
          status: "internal_deduct_failed_after_chain_success",
          txid,
          updatedAt: nowMs()
        })
      );

      await releaseWithdrawLock(
        lockRef,
        "internal_deduct_failed_after_chain_success"
      );

      return res.status(200).json({
        ok: true,
        warning: true,
        error:
          "Pi đã về ví khách nhưng app chưa trừ nội bộ. Tao đã đánh dấu để xử lý tiếp.",
        withdrawId,
        paymentId: "",
        txid,
        sourceWallet: SOURCE_WALLET_PUBLIC,
        recipientAddress
      });
    }

    stage = "write-transaction";
    const txPayload = cleanForFirebase({
      type: "wallet_withdraw_auto_v2",
      requestMode: "auto_done",
      walletKey: safeWalletKey,
      walletKeyRaw,
      piUid,
      piUsername,
      amount,
      paymentId: "",
      txid,
      withdrawId,
      sourceWalletAddress: SOURCE_WALLET_PUBLIC,
      recipientAddress,
      recipientAddressField: sourceField,
      internalBalanceAfter: newInternalBalance,
      createdAt: nowMs(),
      status: "done"
    });

    await db.ref("walletTransactions").push().set(txPayload);
    await db.ref("walletTransactionsV2").push().set(txPayload);

    stage = "finish-request";
    await requestRef.update(
      cleanForFirebase({
        status: "done",
        requestMode: "auto_done",
        txid,
        sourceWalletAddress: SOURCE_WALLET_PUBLIC,
        recipientAddress,
        internalBalanceAfter: newInternalBalance,
        doneAt: nowMs(),
        updatedAt: nowMs()
      })
    );

    await releaseWithdrawLock(lockRef, "done");

    return res.status(200).json({
      ok: true,
      withdrawId,
      paymentId: "",
      txid,
      amount,
      newBalance: newInternalBalance,
      currentBalance: newInternalBalance,
      leftToday: Math.max(
        0,
        Number(CONFIG.MAX_WITHDRAW_PER_DAY_COUNT || 0) - (todayCount + 1)
      ),
      sourceWallet: SOURCE_WALLET_PUBLIC,
      recipientAddress
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
            txid,
            updatedAt: nowMs()
          })
        );
      }
    } catch (_) {}

    try {
      if (lockRef) {
        await releaseWithdrawLock(lockRef, "failed");
      }
    } catch (_) {}

    return res.status(500).json({
      ok: false,
      error: msg,
      stage
    });
  }
};