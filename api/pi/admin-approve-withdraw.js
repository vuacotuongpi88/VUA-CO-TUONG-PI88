const {
  CONFIG,
  nowMs,
  safeKey,
  cleanForFirebase,
  readPiBalance,
  acquireWithdrawLock,
  releaseWithdrawLock,
  submitOnChain,
  deductWalletBalance
} = require("../../lib/pi/withdraw-core")

const ADMIN_FEE_SECRET = String(
  process.env.ADMIN_FEE_SECRET ||
    process.env.ADMIN_SECRET ||
    ""
).trim();

const ADMIN_ALLOWED_PI_KEYS = String(
  process.env.ADMIN_ALLOWED_PI_KEYS || ""
)
  .split(",")
  .map((x) => String(x || "").trim())
  .filter(Boolean);

module.exports = async function handler(req, res) {
  if (req.method !== "POST") {
    return res.status(405).json({
      ok: false,
      error: "Method not allowed"
    });
  }

  let stage = "start";
  let lockRef = null;
  let requestRef = null;
  let txid = "";

  try {
    stage = "env-check";
    if (!CONFIG.DEV_PUBLIC || !CONFIG.DEV_SECRET) {
      return res.status(500).json({
        ok: false,
        error: "Thiếu DEV_PUBLIC/DEV_SECRET."
      });
    }

    if (!ADMIN_FEE_SECRET) {
      return res.status(500).json({
        ok: false,
        error: "Thiếu ADMIN_FEE_SECRET."
      });
    }

    stage = "read-body";
    const body =
      typeof req.body === "string"
        ? JSON.parse(req.body || "{}")
        : req.body || {};

    const action = String(body.action || "approve").trim().toLowerCase();
    const requestId = String(
      body.requestId || body.withdrawId || body.id || ""
    ).trim();
    const adminSecret = String(body.adminSecret || "").trim();
    const adminNote = String(body.note || "").trim();
    const requesterWalletKeyRaw = String(req.headers["x-wallet-key"] || "").trim();
    const requesterWalletKey = safeKey(requesterWalletKeyRaw);

    if (adminSecret !== ADMIN_FEE_SECRET) {
      return res.status(401).json({
        ok: false,
        error: "Sai adminSecret."
      });
    }

    if (
      ADMIN_ALLOWED_PI_KEYS.length > 0 &&
      requesterWalletKey &&
      !ADMIN_ALLOWED_PI_KEYS.includes(requesterWalletKey)
    ) {
      return res.status(403).json({
        ok: false,
        error: `Ví admin ${requesterWalletKeyRaw} không có quyền duyệt rút.`
      });
    }

    if (!requestId) {
      return res.status(400).json({
        ok: false,
        error: "Thiếu requestId."
      });
    }

    if (!["approve", "reject"].includes(action)) {
      return res.status(400).json({
        ok: false,
        error: "action chỉ nhận approve hoặc reject."
      });
    }

    stage = "db-init";
    const adminBundle = require("../_firebaseAdmin.js");
    const { getDatabase } = require("firebase-admin/database");
    const adminApp = adminBundle.app || adminBundle;
    const db = getDatabase(adminApp);

    stage = "request-read";
    requestRef = db.ref(`piWithdrawRequests/${requestId}`);
    const requestSnap = await requestRef.once("value");
    const requestVal = requestSnap.val() || {};

    if (!requestVal || typeof requestVal !== "object") {
      return res.status(404).json({
        ok: false,
        error: "Không tìm thấy withdraw request."
      });
    }

    const requestStatus = String(requestVal.status || "").trim();
    if (requestStatus === "done") {
      return res.status(200).json({
        ok: true,
        alreadyDone: true,
        withdrawId: requestId,
        txid: String(requestVal.txid || "").trim()
      });
    }

    if (requestStatus !== "pending_admin") {
      return res.status(409).json({
        ok: false,
        error: `Request này đang ở trạng thái ${requestStatus || "unknown"}, không duyệt kiểu này được.`
      });
    }

    if (action === "reject") {
      await requestRef.update(
        cleanForFirebase({
          status: "rejected_admin",
          adminAction: "reject",
          rejectedByWalletKey: requesterWalletKeyRaw,
          rejectedNote: adminNote || "Admin từ chối lệnh rút",
          rejectedAt: nowMs(),
          updatedAt: nowMs()
        })
      );

      return res.status(200).json({
        ok: true,
        rejected: true,
        withdrawId: requestId
      });
    }

    const walletKeyRaw = String(
      requestVal.walletKeyRaw || requestVal.walletKey || ""
    ).trim();
    const safeWalletKey = safeKey(walletKeyRaw);
    if (!safeWalletKey) {
      return res.status(400).json({
        ok: false,
        error: "Request thiếu walletKey đích."
      });
    }

    stage = "wallet-read";
    const walletRef = db.ref(`wallets/${safeWalletKey}`);
    const walletSnap = await walletRef.once("value");
    const walletVal = walletSnap.val() || {};

    if (!walletVal || typeof walletVal !== "object") {
      return res.status(404).json({
        ok: false,
        error: "Không tìm thấy ví nội bộ của request."
      });
    }

    const amount = Number(requestVal.amount || 0);
    if (!Number.isFinite(amount) || amount <= 0) {
      return res.status(400).json({
        ok: false,
        error: "Amount trong request không hợp lệ."
      });
    }

    const recipientAddress = String(
      requestVal.recipientAddress || walletVal.piWalletAddress || ""
    ).trim();

    if (!recipientAddress) {
      return res.status(400).json({
        ok: false,
        error: "Ví đích chưa có piWalletAddress."
      });
    }

    const currentInternalBalance = readPiBalance(walletVal);
    if (amount > currentInternalBalance) {
      return res.status(400).json({
        ok: false,
        error: `Số dư không đủ để duyệt. Hiện còn ${currentInternalBalance.toFixed(2)} Pi.`
      });
    }

    stage = "lock";
    lockRef = db.ref(`wallets/${safeWalletKey}/withdrawLock`);
    const locked = await acquireWithdrawLock(lockRef);

    if (!locked) {
      return res.status(409).json({
        ok: false,
        error: "Ví đang có lệnh rút khác xử lý. Chờ chút rồi duyệt lại."
      });
    }

    const memo = String(
      requestVal.memo || `Rut ${amount.toFixed(2)} Pi tu app`
    ).trim();

    await requestRef.update(
      cleanForFirebase({
        status: "auto_processing",
        adminAction: "approve",
        approvedByWalletKey: requesterWalletKeyRaw,
        approvedNote: adminNote || "Admin duyệt lệnh rút",
        approvedAt: nowMs(),
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
        withdrawId: requestId,
        txid
      });
    }

    stage = "write-transaction";
    await db.ref("walletTransactions").push().set(
      cleanForFirebase({
        type: "wallet_withdraw",
        mode: "admin_approved",
        walletKey: safeWalletKey,
        walletKeyRaw,
        requesterWalletKey: requesterWalletKeyRaw,
        piUid: String(requestVal.piUid || walletVal.piUid || "").trim(),
        piUsername: String(
          requestVal.piUsername ||
            walletVal.piUsername ||
            walletVal.username ||
            walletVal.name ||
            ""
        ).trim(),
        recipientAddress,
        amount,
        txid,
        withdrawId: requestId,
        internalBalanceAfter: newInternalBalance,
        createdAt: nowMs()
      })
    );

    stage = "finish-request";
    await requestRef.update(
      cleanForFirebase({
        status: "done",
        requestMode: "admin_done",
        txid,
        internalBalanceAfter: newInternalBalance,
        doneAt: nowMs(),
        updatedAt: nowMs()
      })
    );

    await releaseWithdrawLock(lockRef, "done");

    return res.status(200).json({
      ok: true,
      approved: true,
      withdrawId: requestId,
      txid,
      amount,
      recipientAddress,
      newBalance: newInternalBalance
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