const PMC_PER_PI = 1000;

module.exports = async function handler(req, res) {
  console.log("exchange-pmc-to-pi HIT", req.method);
  let stage = "start";

  if (req.method !== "POST") {
    return res.status(405).json({
      ok: false,
      error: "Method not allowed"
    });
  }

  let getDatabase;
  let adminApp;
  let adminDatabaseURL;

  try {
    const adminBundle = require("./_firebaseAdmin.js");
    ({ getDatabase } = require("firebase-admin/database"));
    adminApp = adminBundle.app;
    adminDatabaseURL = adminBundle.databaseURL;

    console.log("firebaseAdmin loaded OK");
    console.log("adminDatabaseURL =", adminDatabaseURL);
  } catch (e) {
    console.error("load _firebaseAdmin failed =", e);
    return res.status(500).json({
      ok: false,
      error: "load _firebaseAdmin failed: " + (e?.message || String(e))
    });
  }

  try {
    stage = "read-body";
    const { pmcAmount, walletKey: bodyWalletKey } = req.body || {};
    const safePmc = Math.max(0, Math.floor(Number(pmcAmount || 0) || 0));

    if (!safePmc || safePmc <= 0) {
      return res.status(400).json({
        ok: false,
        error: "Nhập số PMC muốn đổi trước đã."
      });
    }

    stage = "read-wallet-key";
    const walletKey = req.headers["x-wallet-key"] || bodyWalletKey;
    if (!walletKey) {
      return res.status(401).json({
        ok: false,
        error: "Thiếu định danh ví."
      });
    }

    stage = "get-db";
    const db = getDatabase(adminApp);

    stage = "build-wallet-path";
const safeWalletKey = String(walletKey || "").replace(/[.#$\[\]\/]/g, "_");
const walletPath = "wallets/" + safeWalletKey;
const walletRef = db.ref(walletPath);

console.log("admin db ref url =", db.ref().toString());
console.log("wallet ref url =", walletRef.toString());
console.log("exchange-pmc-to-pi pmcAmount =", safePmc);
console.log("exchange-pmc-to-pi walletKey =", walletKey);
console.log("walletPath =", walletPath);

stage = "pre-read-wallet";
const preSnap = await walletRef.once("value");
const preRead = preSnap.val();

console.log("PRE-READ wallet url =", walletRef.toString());
console.log("PRE-READ wallet value =", preRead);

   stage = "transaction";
let exchangeResult = null;
let serverSeen = {
  rootUrl: db.ref().toString(),
  walletUrl: walletRef.toString(),
  preRead,
  currentPi: null,
  currentPmc: null,
  rawCurrent: null
};

const txResult = await walletRef.transaction(current => {
  const baseCurrent =
    current && typeof current === "object"
      ? current
      : (preRead && typeof preRead === "object" ? preRead : {});

  const currentPi = Number(baseCurrent.balance ?? 0) || 0;
  const currentPmc = Math.floor(Number(baseCurrent.pmcBalance ?? 0) || 0);

  serverSeen = {
    rootUrl: db.ref().toString(),
    walletUrl: walletRef.toString(),
    preRead,
    currentPi,
    currentPmc,
    rawCurrent: baseCurrent
  };

  if (currentPmc < safePmc) {
    return;
  }

  const piAmount = safePmc / PMC_PER_PI;
  const newPmcBalance = currentPmc - safePmc;
  const newPiBalance = currentPi + piAmount;

  exchangeResult = {
    piAmount,
    newPmcBalance,
    newPiBalance
  };

  return {
    ...baseCurrent,
    balance: newPiBalance,
    pmcBalance: newPmcBalance,
    updatedAt: Date.now()
  };
});
    console.log("txResult committed =", txResult?.committed);
    console.log("exchangeResult =", exchangeResult);

    if (!exchangeResult) {
      return res.status(400).json({
        ok: false,
        error: "PMC không đủ hoặc giao dịch không hợp lệ.",
        debug: {
          walletKey,
          safeWalletKey,
          walletPath,
          safePmc,
          txCommitted: !!txResult?.committed,
          serverSeen
        }
      });
    }

    stage = "write-history";
    await db.ref("walletTransactions").push({
      type: "pmc_to_pi",
      walletKey: safeWalletKey,
      pmcAmount: safePmc,
      piAmount: exchangeResult.piAmount,
      rate: PMC_PER_PI,
      createdAt: Date.now(),
      status: "done"
    });

    stage = "done";
    return res.status(200).json({
      ok: true,
      pmcAmount: safePmc,
      piAmount: exchangeResult.piAmount,
      newPmcBalance: exchangeResult.newPmcBalance,
      newPiBalance: exchangeResult.newPiBalance
    });
  } catch (err) {
    console.error("exchange-pmc-to-pi crash stage =", stage);
    console.error("exchange-pmc-to-pi error =", err);

    return res.status(500).json({
      ok: false,
      error: err.message || "Server error"
    });
  }
};