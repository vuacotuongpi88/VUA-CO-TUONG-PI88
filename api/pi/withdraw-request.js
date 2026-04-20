const { getDatabase } = require("firebase-admin/database");
const StellarSdk = require("stellar-sdk");
const adminBundle = require("./_firebaseAdmin.js");

const adminApp = adminBundle.app || adminBundle;

const MAX_WITHDRAW_PER_TX = 1000;
const LOCK_TTL_MS = 2 * 60 * 1000;

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

function nowMs() {
  return Date.now();
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

async function parseJsonResponse(res) {
  const raw = await res.text();
  let data = {};
  try {
    data = raw ? JSON.parse(raw) : {};
  } catch (_) {
    data = { raw };
  }
  return data;
}

async function callPiCreatePayment(apiKey, body) {
  const res = await fetch("https://api.minepi.com/v2/payments", {
    method: "POST",
    headers: {
      Authorization: `Key ${apiKey}`,
      "Content-Type": "application/json"
    },
    body: JSON.stringify(body)
  });

  const data = await parseJsonResponse(res);
  return { ok: res.ok, status: res.status, data };
}

async function callPiComplete(apiKey, paymentId, txid) {
  const res = await fetch(`https://api.minepi.com/v2/payments/${paymentId}/complete`, {
    method: "POST",
    headers: {
      Authorization: `Key ${apiKey}`,
      "Content-Type": "application/json"
    },
    body: JSON.stringify({ txid })
  });

  const data = await parseJsonResponse(res);
  return { ok: res.ok, status: res.status, data };
}

module.exports = async function handler(req, res) {
  let stage = "start";

  if (req.method !== "POST") {
    return res.status(405).json({
      ok: false,
      error: "Method not allowed"
    });
  }

  try {
    stage = "read-env";
    const PI_API_KEY = String(process.env.PI_API_KEY || "").trim();
    const DEV_PUBLIC = String(process.env.PI_DEVELOPER_WALLET_PUBLIC_KEY || "").trim();
    const DEV_SECRET = String(process.env.PI_DEVELOPER_WALLET_SECRET_SEED || "").trim();
    const BLOCKCHAIN_URL = String(process.env.PI_BLOCKCHAIN_API_URL || "https://api.testnet.minepi.com").trim();
    const NETWORK_PASSPHRASE = String(process.env.PI_NETWORK_PASSPHRASE || "Pi Testnet").trim();

    if (!PI_API_KEY) {
      return res.status(500).json({ ok: false, error: "Thiếu PI_API_KEY trên Vercel." });
    }
    if (!DEV_PUBLIC) {
      return res.status(500).json({ ok: false, error: "Thiếu PI_DEVELOPER_WALLET_PUBLIC_KEY trên Vercel." });
    }
    if (!DEV_SECRET) {
      return res.status(500).json({ ok: false, error: "Thiếu PI_DEVELOPER_WALLET_SECRET_SEED trên Vercel." });
    }

    stage = "read-body";
    const amount = Number(req.body?.amount || 0);
    const piUid = String(req.body?.piUid || "").trim();
    const piUsername = String(req.body?.piUsername || "").trim();
    const walletKeyRaw = String(
      req.headers["x-wallet-key"] ||
      req.body?.walletKey ||
      ""
    ).trim();

    if (!walletKeyRaw) {
      return res.status(401).json({ ok: false, error: "Thiếu walletKey." });
    }

    if (!piUid) {
      return res.status(400).json({ ok: false, error: "Thiếu verified Pi uid của người nhận." });
    }

    if (!Number.isFinite(amount) || amount <= 0) {
      return res.status(400).json({ ok: false, error: "Số Pi rút không hợp lệ." });
    }

    if (amount > MAX_WITHDRAW_PER_TX) {
      return res.status(400).json({
        ok: false,
        error: `Mỗi lần chỉ được rút tối đa ${MAX_WITHDRAW_PER_TX} Pi.`
      });
    }

    stage = "db-init";
    const db = getDatabase(adminApp);
    const safeWalletKey = safeKey(walletKeyRaw);

    const walletRef = db.ref("wallets/" + safeWalletKey);
    const requestRef = db.ref("piWithdrawRequests").push();
    const withdrawId = requestRef.key;
    const lockRef = db.ref(`wallets/${safeWalletKey}/withdrawLock`);

    if (!withdrawId) {
      return res.status(500).json({ ok: false, error: "Không tạo được withdrawId." });
    }

    stage = "lock-wallet";
    const lockTx = await runDbTransaction(lockRef, current => {
      const safeCurrent = current && typeof current === "object" ? current : null;
      const active = !!safeCurrent?.active;
      const expiresAt = Number(safeCurrent?.expiresAt || 0);

      if (active && expiresAt > nowMs()) {
        return;
      }

      return {
        active: true,
        withdrawId,
        at: nowMs(),
        expiresAt: nowMs() + LOCK_TTL_MS
      };
    });

    if (!lockTx.committed) {
      return res.status(409).json({
        ok: false,
        error: "Đang có lệnh rút khác chạy rồi. Đợi chút rồi thử lại."
      });
    }

    let paymentId = "";
    let txid = "";

    try {
      stage = "read-wallet-balance";
      const walletSnap = await walletRef.once("value");
      const walletData =
        walletSnap.val() && typeof walletSnap.val() === "object"
          ? walletSnap.val()
          : {};

      const currentBalance = readPiBalance(walletData);

      if (amount > currentBalance) {
        await requestRef.set({
          status: "rejected_insufficient_internal_balance",
          walletKey: safeWalletKey,
          piUid,
          piUsername,
          amount,
          currentBalance,
          createdAt: nowMs()
        });

        return res.status(400).json({
          ok: false,
          error: `Số dư không đủ. Hiện còn ${currentBalance.toFixed(2)} Pi.`
        });
      }

      stage = "create-request-record";
      await requestRef.set({
        status: "initiated",
        walletKey: safeWalletKey,
        piUid,
        piUsername,
        amount,
        internalBalanceBefore: currentBalance,
        createdAt: nowMs()
      });

      stage = "pi-create-payment";
      const createResult = await callPiCreatePayment(PI_API_KEY, {
        uid: piUid,
        amount,
        memo: `Rut ${amount.toFixed(2)} Pi tu app`,
        metadata: {
          kind: "wallet_withdraw",
          withdrawId,
          walletKey: safeWalletKey,
          piUsername
        }
      });

      if (!createResult.ok) {
        await requestRef.update({
          status: "pi_create_payment_failed",
          piCreateStatus: createResult.status,
          piCreateData: createResult.data,
          failedAt: nowMs()
        });

        return res.status(502).json({
          ok: false,
          error: createResult.data?.error || "Tạo payment rút Pi thất bại.",
          withdrawId,
          debug: createResult.data
        });
      }

      paymentId = String(
        createResult.data?.identifier ||
        createResult.data?.paymentId ||
        createResult.data?.payment?.identifier ||
        ""
      ).trim();

      const recipientAddress = String(
        createResult.data?.recipient_address ||
        createResult.data?.recipient ||
        createResult.data?.to_address ||
        createResult.data?.payment?.recipient_address ||
        createResult.data?.payment?.to_address ||
        ""
      ).trim();

      if (!paymentId || !recipientAddress) {
        await requestRef.update({
          status: "pi_create_payment_missing_fields",
          piCreateData: createResult.data,
          failedAt: nowMs()
        });

        return res.status(502).json({
          ok: false,
          error: "Pi API không trả đủ paymentId/recipientAddress.",
          withdrawId,
          debug: createResult.data
        });
      }

      await requestRef.update({
        status: "payment_created",
        paymentId,
        recipientAddress,
        piCreateData: createResult.data,
        updatedAt: nowMs()
      });

      stage = "load-stellar";
      const HorizonServer =
        StellarSdk.Horizon && StellarSdk.Horizon.Server
          ? StellarSdk.Horizon.Server
          : StellarSdk.Server;

      const server = new HorizonServer(BLOCKCHAIN_URL);
      const sourceAccount = await server.loadAccount(DEV_PUBLIC);
      const baseFee = await server.fetchBaseFee();

      stage = "build-transaction";
      const tx = new StellarSdk.TransactionBuilder(sourceAccount, {
        fee: String(baseFee),
        networkPassphrase: NETWORK_PASSPHRASE
      })
        .addOperation(
          StellarSdk.Operation.payment({
            destination: recipientAddress,
            asset: StellarSdk.Asset.native(),
            amount: amount.toString()
          })
        )
        .addMemo(StellarSdk.Memo.text(paymentId.slice(0, 28)))
        .setTimeout(180)
        .build();

      stage = "sign-transaction";
      const keypair = StellarSdk.Keypair.fromSecret(DEV_SECRET);
      tx.sign(keypair);

      stage = "submit-transaction";
      const submitResult = await server.submitTransaction(tx);
      txid = String(submitResult?.hash || submitResult?.id || "").trim();

      if (!txid) {
        await requestRef.update({
          status: "blockchain_submit_missing_txid",
          submitResult,
          failedAt: nowMs()
        });

        return res.status(502).json({
          ok: false,
          error: "Submit blockchain không trả txid.",
          withdrawId,
          debug: submitResult
        });
      }

      await requestRef.update({
        status: "chain_submitted",
        txid,
        submitResult,
        updatedAt: nowMs()
      });

      stage = "pi-complete";
      const completeResult = await callPiComplete(PI_API_KEY, paymentId, txid);

      if (!completeResult.ok) {
        await requestRef.update({
          status: "pi_complete_failed",
          piCompleteStatus: completeResult.status,
          piCompleteData: completeResult.data,
          updatedAt: nowMs()
        });

        return res.status(502).json({
          ok: false,
          error: "Blockchain đã submit nhưng complete với Pi server thất bại. Đừng bấm rút lại, hãy chạy retry complete.",
          withdrawId,
          paymentId,
          txid,
          debug: completeResult.data
        });
      }

      await requestRef.update({
        status: "pi_completed",
        piCompleteData: completeResult.data,
        updatedAt: nowMs()
      });

      stage = "deduct-internal-balance";
      const deductTx = await runDbTransaction(walletRef, current => {
        const safeCurrent =
          current && typeof current === "object" ? current : walletData;

        const balanceNow = readPiBalance(safeCurrent);
        if (balanceNow < amount) {
          return;
        }

        const nextBalance = balanceNow - amount;

        return {
          ...safeCurrent,
          balance: nextBalance,
          piBalance: nextBalance,
          updatedAt: nowMs()
        };
      });

      if (!deductTx.committed) {
        await requestRef.update({
          status: "internal_deduct_failed_after_chain_success",
          txid,
          paymentId,
          updatedAt: nowMs(),
          manualActionNeeded: true
        });

        return res.status(409).json({
          ok: false,
          error: "Đã trả Pi ra ví ngoài nhưng trừ số dư nội bộ thất bại. Cần xử lý tay ngay.",
          withdrawId,
          paymentId,
          txid,
          manualActionNeeded: true
        });
      }

      const finalWallet = deductTx.snapshot?.val() || {};
      const newBalance = readPiBalance(finalWallet);

      stage = "write-logs";
      await db.ref("walletTransactions").push({
        type: "pi_withdraw_a2u",
        walletKey: safeWalletKey,
        piUid,
        piUsername,
        amount,
        paymentId,
        txid,
        withdrawId,
        createdAt: nowMs(),
        status: "done"
      });

      await requestRef.update({
        status: "done",
        newInternalBalance: newBalance,
        completedAt: nowMs()
      });

      return res.status(200).json({
        ok: true,
        withdrawId,
        paymentId,
        txid,
        newBalance
      });
    } finally {
      await runDbTransaction(lockRef, current => {
        if (!current || current.withdrawId !== withdrawId) return current || null;
        return null;
      }).catch(() => {});
    }
  } catch (err) {
    console.error("WITHDRAW REQUEST ERROR stage =", stage, err);
    return res.status(500).json({
      ok: false,
      error: err?.message || "withdraw request error",
      stage
    });
  }
};