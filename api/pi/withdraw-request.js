const {
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
  deductWalletBalance
} = require("../../lib/firebaseAdmin.js")

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
  let txid = "";

  try {
    stage = "env-check";
    if (!CONFIG.DEV_PUBLIC || !CONFIG.DEV_SECRET) {
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

    if (amount > CONFIG.MAX_WITHDRAW_PER_TX) {
      return res.status(400).json({
        ok: false,
        error: `Mỗi lần chỉ được rút tối đa ${CONFIG.MAX_WITHDRAW_PER_TX} Pi.`
      });
    }

    stage = "db-init";
    const adminBundle = require("../../lib/firebaseAdmin.js")
    const { getDatabase } = require("firebase-admin/database");
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

    if (walletVal.piVerified !== true || !String(walletVal.piUid || "").trim()) {
      return res.status(400).json({
        ok: false,
        error: "Tài khoản này chưa liên kết Pi Browser. Bấm Liên kết Pi Browser trước."
      });
    }

    const recipientAddress = String(walletVal.piWalletAddress || "").trim();
    if (!recipientAddress) {
      return res.status(400).json({
        ok: false,
        error: "Tài khoản này chưa có địa chỉ ví Pi để nhận tiền."
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
    if (todayCount >= CONFIG.MAX_WITHDRAW_PER_DAY_COUNT) {
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

    stage = "queue-check";
    const queueInfo = await inspectWithdrawQueue(db, safeWalletKey);

    if (queueInfo.activeRequest) {
      await releaseWithdrawLock(lockRef, "active_request_exists");

      return res.status(409).json({
        ok: false,
        error: "Đang có lệnh rút khác xử lý. Chờ xong rồi thử lại.",
        withdrawId: queueInfo.activeRequest.key
      });
    }

    if (queueInfo.pendingAdminRequest) {
      await releaseWithdrawLock(lockRef, "pending_admin_exists");

      return res.status(409).json({
        ok: false,
        error:
          "Ví này đang có lệnh rút chờ duyệt. Không cần bấm thêm, admin sẽ xử lý.",
        withdrawId: queueInfo.pendingAdminRequest.key,
        pendingAdmin: true
      });
    }

    const riskFlags = buildRiskFlags({
      amount,
      queueInfo
    });

    stage = "create-request";
    requestRef = db.ref("piWithdrawRequests").push();
    withdrawId = requestRef.key || "";

    const memo = `Rut ${Number(amount).toFixed(2)} Pi tu app`;

    const requestBase = cleanForFirebase({
      status: shouldQueueForAdmin(riskFlags) ? "pending_admin" : "initiated",
      type: "wallet_withdraw",
      requestMode: shouldQueueForAdmin(riskFlags) ? "admin_queue" : "auto",
      walletKey: safeWalletKey,
      walletKeyRaw,
      piUid,
      piUsername,
      recipientAddress,
      amount,
      memo,
      autoEligible: !shouldQueueForAdmin(riskFlags),
      riskFlags,
      createdAt: nowMs(),
      updatedAt: nowMs()
    });

    await requestRef.set(requestBase);

    if (shouldQueueForAdmin(riskFlags)) {
      await releaseWithdrawLock(lockRef, "queued_for_admin");

      const reasons = [];
      if (riskFlags.overAutoMax) {
        reasons.push(
          `Số Pi rút vượt ngưỡng auto ${CONFIG.AUTO_WITHDRAW_MAX} Pi`
        );
      }
      if (riskFlags.burstRequests) {
        reasons.push("Ví bấm rút quá nhanh trong thời gian ngắn");
      }
      if (riskFlags.hasPendingAdmin) {
        reasons.push("Đã có lệnh chờ duyệt trước đó");
      }
      if (riskFlags.hasActiveRequest) {
        reasons.push("Đang có lệnh xử lý song song");
      }

      return res.status(409).json({
        ok: false,
        pendingAdmin: true,
        withdrawId,
        error:
          "Lệnh rút đã được đưa vào hàng duyệt thủ công. " +
          (reasons.length ? reasons.join(" | ") : "Admin sẽ xử lý sớm."),
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

    txid = String(chainResult?.txid || "").trim();

    await requestRef.update(
      cleanForFirebase({
        status: "chain_submitted",
        txid,
        chainSubmitData: chainResult?.data || null,
        updatedAt: nowMs()
      })
    );

    stage = "deduct-internal";
    const { deductOk, newInternalBalance } = await deductWalletBalance(
      walletRef,
      amount
    );

    if (!deductOk) {
      await requestRef.update(
        cleanForFirebase({
          status: "internal_deduct_failed_after_chain_success",
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
        txid
      });
    }

    stage = "write-transaction";
    await db.ref("walletTransactions").push().set(
      cleanForFirebase({
        type: "wallet_withdraw",
        mode: "auto",
        walletKey: safeWalletKey,
        walletKeyRaw,
        piUid,
        piUsername,
        recipientAddress,
        amount,
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
        requestMode: "auto_done",
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
      txid,
      amount,
      newBalance: newInternalBalance,
      leftToday: Math.max(0, CONFIG.MAX_WITHDRAW_PER_DAY_COUNT - (todayCount + 1)),
      mode: "auto"
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

    await releaseWithdrawLock(lockRef, "failed");

    return res.status(500).json({
      ok: false,
      error: msg,
      stage
    });
  }
};