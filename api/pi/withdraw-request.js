const StellarSdk = require("stellar-sdk");

const PI_API_KEY = String(
  process.env.PI_API_KEY ||
    process.env.PI_SERVER_API_KEY ||
    process.env.PI_APIKEY ||
    ""
).trim();

const DEV_PUBLIC = String(
  process.env.DEV_PUBLIC ||
    process.env.PI_WALLET_PUBLIC_KEY ||
    process.env.PI_PUBLIC_KEY ||
    ""
).trim();

const DEV_SECRET = String(
  process.env.DEV_SECRET ||
    process.env.PI_WALLET_PRIVATE_KEY ||
    process.env.PI_SECRET_KEY ||
    ""
).trim();

const BLOCKCHAIN_URL = String(
  process.env.PI_BLOCKCHAIN_URL ||
    process.env.PI_HORIZON_URL ||
    "https://api.mainnet.minepi.com"
).trim();

const NETWORK_PASSPHRASE = String(
  process.env.PI_NETWORK_PASSPHRASE || "Pi Network"
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
  return String(value || "").replace(/[.#$\[\]/]/g, "_");
}

function readPiBalance(obj) {
  return Number(
    obj && obj.balance != null
      ? obj.balance
      : obj && obj.piBalance != null
        ? obj.piBalance
        : 0
  ) || 0;
}

function cleanForFirebase(value) {
  try {
    return JSON.parse(JSON.stringify(value ?? null));
  } catch (_) {
    return value == null ? null : String(value);
  }
}

async function parseJsonResponse(fetchRes) {
  const raw = await fetchRes.text();
  let data = {};
  try {
    data = raw ? JSON.parse(raw) : {};
  } catch (_) {
    data = { raw };
  }
  return { raw, data };
}

async function callPiCreatePayment(apiKey, payload) {
  const res = await fetch("https://api.minepi.com/v2/payments", {
    method: "POST",
    headers: {
      Authorization: `Key ${apiKey}`,
      "Content-Type": "application/json"
    },
    body: JSON.stringify(payload)
  });

  const { raw, data } = await parseJsonResponse(res);
  return {
    ok: res.ok,
    status: res.status,
    raw,
    data
  };
}

async function callPiCompletePayment(apiKey, paymentId, txid) {
  const res = await fetch(
    `https://api.minepi.com/v2/payments/${encodeURIComponent(paymentId)}/complete`,
    {
      method: "POST",
      headers: {
        Authorization: `Key ${apiKey}`,
        "Content-Type": "application/json"
      },
      body: JSON.stringify({ txid })
    }
  );

  const { raw, data } = await parseJsonResponse(res);
  return {
    ok: res.ok,
    status: res.status,
    raw,
    data
  };
}

async function countTodayWithdraws(safeWalletKey) {
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
  let db = null;
module.exports = async function handler(req, res) {
  if (req.method !== "POST") {
    return res.status(405).json({
      ok: false,
      error: "Method not allowed"
    });
  }

  let stage = "start";
  let requestRef = null;
  let withdrawId = "";
  let lockRef = null;


  try {
    stage = "db-init";
    try {
      const adminBundle = require("../_firebaseAdmin.js");
      const { getDatabase } = require("firebase-admin/database");
      const adminApp = adminBundle.app || adminBundle;
      db = getDatabase(adminApp);
    } catch (e) {
      return res.status(500).json({
        ok: false,
        error: "load_firebaseAdmin failed: " + (e?.message || String(e)),
        stage
      });
    }

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
    const piUid = String(body.piUid || "").trim();
    const piUsername = String(body.piUsername || "").trim();
    const walletKeyRaw = String(
      req.headers["x-wallet-key"] || body.walletKey || ""
    ).trim();

    if (!walletKeyRaw) {
      return res.status(401).json({
        ok: false,
        error: "Thiếu định danh ví."
      });
    }

    if (!piUid) {
      return res.status(400).json({
        ok: false,
        error: "Thiếu verified Pi uid của người nhận."
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

    stage = "daily-limit";
    const todayCount = await countTodayWithdraws(safeWalletKey);
    if (todayCount >= MAX_WITHDRAW_PER_DAY_COUNT) {
      return res.status(400).json({
        ok: false,
        error: `Hôm nay đã dùng hết ${MAX_WITHDRAW_PER_DAY_COUNT} lượt rút.`,
        leftToday: 0
      });
    }

    stage = "wallet-read";
    const walletRef = db.ref("wallets/" + safeWalletKey);
    const walletSnap = await walletRef.once("value");
    const walletVal = walletSnap.val() || {};
    const currentBalance = readPiBalance(walletVal);

    if (amount > currentBalance) {
      return res.status(400).json({
        ok: false,
        error: `Số dư không đủ. Hiện còn ${currentBalance.toFixed(2)} Pi.`,
        currentBalance
      });
    }

    stage = "create-request-record";
    requestRef = db.ref("piWithdrawRequests").push();
    withdrawId = requestRef.key || "";

    await requestRef.set({
      status: "initiated",
      walletKey: safeWalletKey,
      walletKeyRaw,
      piUid,
      piUsername,
      amount,
      internalBalanceBefore: currentBalance,
      createdAt: nowMs()
    });

    stage = "lock-wallet";
    lockRef = db.ref("wallets/" + safeWalletKey + "/withdrawLock");

    const lockResult = await wrapRefTransaction(lockRef, (current) => {
      const safeCurrent =
        current && typeof current === "object" ? current : null;
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

    if (!lockResult.committed) {
      return res.status(409).json({
        ok: false,
        error: "Đang có yêu cầu rút Pi khác đang xử lý."
      });
    }

    let paymentId = "";
    let txid = "";
    let recipientAddress = "";

    let activeWithdrawId = withdrawId;
    let activeRequestRef = requestRef;

    let resumeFromPending = false;
    let skipSubmitChain = false;

    let createResult = null;
    let pending = null;

    stage = "pi-create-payment";
    createResult = await callPiCreatePayment(PI_API_KEY, {
      payment: {
        uid: piUid,
        amount,
        memo: `Rut ${amount.toFixed(2)} Pi tu app`,
        metadata: {
          kind: "wallet_withdraw",
          withdrawId,
          walletKey: safeWalletKey,
          piUsername
        }
      }
    });

    if (!createResult.ok) {
      const createErr = String(createResult.data?.error || "").trim();

      await requestRef.update({
        status: createErr || "pi_create_payment_failed",
        piCreateStatus: createResult.status,
        piCreateData: cleanForFirebase(createResult.data),
        failedAt: nowMs()
      });

      if (createErr === "ongoing_payment_found") {
        const allSnap = await db.ref("piWithdrawRequests").once("value");

        allSnap.forEach((child) => {
          const v = child.val() || {};
          if (child.key === withdrawId) return;
          if (String(v.walletKey || "") !== safeWalletKey) return;
          if (String(v.piUid || "") !== piUid) return;
          if (Number(v.amount || 0) !== amount) return;

          const st = String(v.status || "");
          if (
            ![
              "payment_created",
              "chain_submitted",
              "pi_complete_failed",
              "blockchain_submit_missing_txid",
              "linked_to_pending_payment"
            ].includes(st)
          ) {
            return;
          }

          if (
            !pending ||
            Number(v.createdAt || 0) > Number(pending.createdAt || 0)
          ) {
            pending = { id: child.key, ...v };
          }
        });

        if (pending) {
          paymentId = String(pending.paymentId || "").trim();
          txid = String(pending.txid || "").trim();
          recipientAddress = String(
            pending.recipientAddress ||
              pending.piCreateData?.recipient_address ||
              pending.piCreateData?.recipient ||
              pending.piCreateData?.to_address ||
              pending.piCreateData?.payment?.recipient_address ||
              pending.piCreateData?.payment?.to_address ||
              pending.piCreateData?.transaction?.to ||
              pending.piCreateData?.transaction?.to_address ||
              ""
          ).trim();

          if (paymentId && (recipientAddress || txid)) {
            await requestRef.update({
              status: "linked_to_pending_payment",
              linkedWithdrawId: pending.id,
              paymentId,
              txid,
              recipientAddress,
              updatedAt: nowMs()
            });

            activeWithdrawId = pending.id;
            activeRequestRef = db.ref("piWithdrawRequests/" + pending.id);
            resumeFromPending = true;
            skipSubmitChain = !!txid;
          }
        }
      }

      if (!resumeFromPending) {
        return res.status(createErr === "ongoing_payment_found" ? 409 : 502).json({
          ok: false,
          error: createErr || "Tạo payment rút Pi thất bại.",
          pendingWithdrawId: pending?.id || "",
          pendingStatus: pending?.status || "",
          paymentId: pending?.paymentId || "",
          txid: pending?.txid || "",
          withdrawId,
          debug: createResult.data
        });
      }
    }

    if (!resumeFromPending) {
      paymentId = String(
        createResult.data?.identifier ||
          createResult.data?.paymentId ||
          createResult.data?.payment?.identifier ||
          ""
      ).trim();

      recipientAddress = String(
        createResult.data?.recipient_address ||
          createResult.data?.recipient ||
          createResult.data?.to_address ||
          createResult.data?.payment?.recipient_address ||
          createResult.data?.payment?.to_address ||
          createResult.data?.transaction?.to ||
          createResult.data?.transaction?.to_address ||
          ""
      ).trim();

      if (!paymentId || !recipientAddress) {
        await requestRef.update({
          status: "pi_create_payment_missing_fields",
          piCreateData: cleanForFirebase(createResult.data),
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
        piCreateData: cleanForFirebase(createResult.data),
        updatedAt: nowMs()
      });
    }

    if (!skipSubmitChain) {
    stage = "load-stellar";

let StellarSdk = null;
try {
  try {
    StellarSdk = require("@stellar/stellar-sdk");
  } catch (_) {
    StellarSdk = require("stellar-sdk");
  }
} catch (e) {
  return res.status(500).json({
    ok: false,
    error: "Không load được Stellar SDK: " + (e?.message || String(e)),
    stage
  });
}

const S = StellarSdk?.default || StellarSdk;
const Keypair = S?.Keypair;
const TransactionBuilder = S?.TransactionBuilder;
const Operation = S?.Operation;
const Asset = S?.Asset;
const Networks = S?.Networks;
const ServerCtor = S?.Horizon?.Server || S?.Server;
const BASE_FEE = Number(S?.BASE_FEE || 100000);

if (
  !Keypair ||
  !TransactionBuilder ||
  !Operation ||
  !Asset ||
  !Networks ||
  !ServerCtor
) {
  return res.status(500).json({
    ok: false,
    error: "Stellar SDK load thiếu object cần thiết.",
    stage
  });
}

const server = new ServerCtor(PI_BLOCKCHAIN_API_URL);
const sourceKeypair = Keypair.fromSecret(DEV_SECRET);
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
        await activeRequestRef.update({
          status: "blockchain_submit_missing_txid",
          submitResult: cleanForFirebase(submitResult),
          failedAt: nowMs()
        });

        return res.status(502).json({
          ok: false,
          error: "Submit blockchain không trả txid.",
          withdrawId: activeWithdrawId,
          debug: submitResult
        });
      }

      await activeRequestRef.update({
        status: "chain_submitted",
        txid,
        submitResult: cleanForFirebase(submitResult),
        updatedAt: nowMs()
      });
    }

    stage = "pi-complete";
    const completeResult = await callPiCompletePayment(
      PI_API_KEY,
      paymentId,
      txid
    );

    if (!completeResult.ok) {
      await activeRequestRef.update({
        status: "pi_complete_failed",
        piCompleteStatus: completeResult.status,
        piCompleteData: cleanForFirebase(completeResult.data),
        updatedAt: nowMs()
      });

      return res.status(502).json({
        ok: false,
        error: String(completeResult.data?.error || "pi_complete_failed"),
        withdrawId: activeWithdrawId,
        paymentId,
        txid,
        debug: completeResult.data
      });
    }

    await activeRequestRef.update({
      status: "pi_completed",
      piCompleteData: cleanForFirebase(completeResult.data),
      updatedAt: nowMs()
    });

    stage = "internal-deduct";
    const deductResult = await wrapRefTransaction(walletRef, (current) => {
      const safeCurrent =
        current && typeof current === "object" ? current : { piBalance: 0 };

      const currentPi = readPiBalance(safeCurrent);
      if (currentPi < amount) return;

      const nextPi = currentPi - amount;

      return {
        ...safeCurrent,
        piBalance: nextPi,
        balance: nextPi,
        updatedAt: nowMs()
      };
    });

    if (!deductResult.committed) {
      await activeRequestRef.update({
        status: "internal_deduct_failed_after_chain_success",
        updatedAt: nowMs()
      });

      return res.status(409).json({
        ok: false,
        error: "Chain đã chạy xong nhưng trừ số dư nội bộ thất bại.",
        withdrawId: activeWithdrawId,
        paymentId,
        txid
      });
    }

    const walletAfter = deductResult.snap.val() || {};
    const newInternalBalance = readPiBalance(walletAfter);

    await db.ref("walletTransactions").push({
      type: "wallet_withdraw",
      walletKey: safeWalletKey,
      walletKeyRaw,
      piUid,
      piUsername,
      amount,
      paymentId,
      txid,
      withdrawId: activeWithdrawId,
      internalBalanceAfter: newInternalBalance,
      createdAt: nowMs()
    });

    await activeRequestRef.update({
      status: "done",
      internalBalanceAfter: newInternalBalance,
      doneAt: nowMs()
    });

    return res.status(200).json({
      ok: true,
      withdrawId: activeWithdrawId,
      paymentId,
      txid,
      amount,
      leftToday: Math.max(0, MAX_WITHDRAW_PER_DAY_COUNT - (todayCount + 1)),
      currentBalance: newInternalBalance
    });
  } catch (err) {
    try {
      if (requestRef) {
        await requestRef.update({
          status: "exception",
          stage,
          error: err?.message || String(err),
          failedAt: nowMs()
        });
      }
    } catch (_) {}

    return res.status(500).json({
      ok: false,
      error: err?.message || "server error",
      stage
    });
  } finally {
    try {
      if (lockRef && withdrawId) {
        await wrapRefTransaction(lockRef, (current) => {
          const safeCurrent =
            current && typeof current === "object" ? current : null;

          if (!safeCurrent) return null;
          if (String(safeCurrent.withdrawId || "") !== String(withdrawId)) {
            return;
          }

          return null;
        });
      }
    } catch (_) {}
  }
};