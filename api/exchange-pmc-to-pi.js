const PMC_PER_PI = 1000;

export default async function handler(req, res) {
  console.log("exchange-pmc-to-pi HIT", req.method);
  let stage = "start";

  if (req.method !== "POST") {
    return res.status(405).json({
      ok: false,
      error: "Method not allowed"
    });
  }
let firebaseAdmin;

try {
 const mod = await import("./_firebaseAdmin.js");
  firebaseAdmin = mod.default || mod;
  console.log("firebaseAdmin loaded OK");
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
    const { getDatabase } = await import("firebase-admin/database");
const db = getDatabase();

    stage = "build-wallet-path";
    const safeWalletKey = String(walletKey || "").replace(/[.#$\[\]\/]/g, "_");
    const walletPath = "wallets/" + safeWalletKey;
    const walletRef = db.ref(walletPath);

    console.log("exchange-pmc-to-pi pmcAmount =", safePmc);
    console.log("exchange-pmc-to-pi walletKey =", walletKey);
    console.log("walletPath =", walletPath);

    stage = "transaction";
    let exchangeResult = null;
    let serverSeen = {
  currentPi: null,
  currentPmc: null
};
    const txResult = await walletRef.transaction(current => {
      const safeCurrent = current && typeof current === "object" ? current : {};

      const currentPi = Number(safeCurrent.balance ?? 0) || 0;
      const currentPmc = Math.floor(Number(safeCurrent.pmcBalance ?? 0) || 0);
      serverSeen = {
  currentPi,
  currentPmc
};
      console.log("server currentPmc =", currentPmc);
      console.log("server safePmc =", safePmc);
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
        ...safeCurrent,
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
}