module.exports = async function handler(req, res) {
  let stage = "start";

  if (req.method !== "POST") {
    return res.status(405).json({
      ok: false,
      error: "Method not allowed"
    });
  }

  function safeKey(value = "") {
    return String(value || "").replace(/[.#$[\]/]/g, "_");
  }

  function readPiBalance(obj = {}) {
    return Number(
      obj.balance != null
        ? obj.balance
        : (obj.piBalance != null ? obj.piBalance : 0)
    ) || 0;
  }

  async function runDbTransaction(ref, updater) {
    return await new Promise((resolve, reject) => {
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

  async function parseJsonResponse(fetchRes) {
    const raw = await fetchRes.text();
    let data = {};
    try {
      data = raw ? JSON.parse(raw) : {};
    } catch (_) {
      data = { raw };
    }
    return data;
  }

  async function callPiComplete(piApiKey, paymentId, txid) {
    const fetchRes = await fetch(`https://api.minepi.com/v2/payments/${paymentId}/complete`, {
      method: "POST",
      headers: {
        Authorization: `Key ${piApiKey}`,
        "Content-Type": "application/json"
      },
      body: JSON.stringify({ txid })
    });

    const data = await parseJsonResponse(fetchRes);
    return { ok: fetchRes.ok, status: fetchRes.status, data };
  }

  function getRequestBody(req) {
    if (!req.body) return {};
    if (typeof req.body === "string") {
      try {
        return JSON.parse(req.body);
      } catch (_) {
        return {};
      }
    }
    return req.body;
  }

  let getDatabase;
  let adminApp;

  try {
    stage = "load-deps";
    const adminBundle = require("../_firebaseAdmin.js");
    ({ getDatabase } = require("firebase-admin/database"));
    adminApp = adminBundle.app || adminBundle;
  } catch (e) {
    return res.status(500).json({
      ok: false,
      error: "load deps failed: " + (e?.message || String(e))
    });
  }

  try {
    stage = "read-env";
    const PI_API_KEY = String(process.env.PI_API_KEY || "").trim();
    if (!PI_API_KEY) {
      return res.status(500).json({
        ok: false,
        error: "Thiếu PI_API_KEY trên Vercel."
      });
    }

    stage = "read-body";
    const body = getRequestBody(req);
    const withdrawId = String(body.withdrawId || "").trim();

    if (!withdrawId) {
      return res.status(400).json({
        ok: false,
        error: "Thiếu withdrawId."
      });
    }

    stage = "db-init";
    const db = getDatabase(adminApp);
    const requestRef = db.ref("piWithdrawRequests/" + withdrawId);
    const snap = await requestRef.once("value");
    const snapValue = snap.val();
const data = snapValue && typeof snapValue === "object" ? snapValue : null;

    if (!data) {
      return res.status(404).json({
        ok: false,
        error: "Không tìm thấy withdraw request."
      });
    }

    if (data.status === "done") {
      return res.status(200).json({
        ok: true,
        withdrawId,
        paymentId: data.paymentId || "",
        txid: data.txid || "",
        newBalance: Number(data.newInternalBalance || 0)
      });
    }

    const paymentId = String(data.paymentId || "").trim();
    const txid = String(data.txid || "").trim();
    const walletKey = safeKey(data.walletKey || "");
    const amount = Number(data.amount || 0);

    if (!paymentId || !txid || !walletKey || !amount) {
      return res.status(400).json({
        ok: false,
        error: "Withdraw request thiếu dữ liệu để retry complete."
      });
    }

    if (
      data.status !== "pi_complete_failed" &&
      data.status !== "internal_deduct_failed_after_chain_success" &&
      data.status !== "chain_submitted" &&
      data.status !== "pi_completed"
    ) {
      return res.status(400).json({
        ok: false,
        error: `Status hiện tại không cần retry: ${data.status || "unknown"}`
      });
    }

    if (data.status !== "pi_completed" && data.status !== "internal_deduct_failed_after_chain_success") {
      stage = "pi-complete";
      const completeResult = await callPiComplete(PI_API_KEY, paymentId, txid);

      const completeText = JSON.stringify(completeResult.data || {});
      const alreadyCompleted =
        !completeResult.ok &&
        /already|complete|completed/i.test(completeText);

      if (!completeResult.ok && !alreadyCompleted) {
        await requestRef.update({
          retryCompleteStatus: completeResult.status,
          retryCompleteData: completeResult.data,
          retryFailedAt: Date.now()
        });

        return res.status(502).json({
          ok: false,
          error: "Retry complete vẫn thất bại.",
          status: completeResult.status,
          data: completeResult.data
        });
      }

      await requestRef.update({
        status: "pi_completed",
        retryCompleteData: completeResult.data,
        retryCompletedAt: Date.now()
      });
    }

    stage = "deduct-internal";
    const walletRef = db.ref("wallets/" + walletKey);
    const walletSnap = await walletRef.once("value");
    const walletData =
      walletSnap.val() && typeof walletSnap.val() === "object"
        ? walletSnap.val()
        : {};

    const deductTx = await runDbTransaction(walletRef, current => {
      const safeCurrent = current && typeof current === "object" ? current : walletData;
      const currentBalance = readPiBalance(safeCurrent);

      if (currentBalance < amount) {
        return;
      }

      const nextBalance = currentBalance - amount;

      return {
        ...safeCurrent,
        balance: nextBalance,
        piBalance: nextBalance,
        updatedAt: Date.now()
      };
    });

    if (!deductTx.committed) {
      await requestRef.update({
        status: "internal_deduct_failed_after_chain_success",
        manualActionNeeded: true,
        updatedAt: Date.now()
      });

      return res.status(409).json({
        ok: false,
        error: "Retry complete ok nhưng vẫn không trừ được số dư nội bộ.",
        manualActionNeeded: true
      });
    }

    const finalWallet = deductTx.snapshot?.val() || {};
    const newBalance = readPiBalance(finalWallet);

    await db.ref("walletTransactions").push({
      type: "pi_withdraw_a2u_retry_complete",
      walletKey,
      amount,
      paymentId,
      txid,
      withdrawId,
      createdAt: Date.now(),
      status: "done"
    });

    await requestRef.update({
      status: "done",
      newInternalBalance: newBalance,
      completedAt: Date.now()
    });

    return res.status(200).json({
      ok: true,
      withdrawId,
      paymentId,
      txid,
      newBalance
    });
  } catch (err) {
    console.error("WITHDRAW COMPLETE ERROR stage =", stage, err);
    return res.status(500).json({
      ok: false,
      error: err?.message || "withdraw complete error",
      stage
    });
  }
};