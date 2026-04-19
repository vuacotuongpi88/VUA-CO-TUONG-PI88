// api/exchange-pmc-to-pi.js
import firebaseAdmin from "./firebaseAdmin.js";

const PI_TO_PMC_RATE = 1000;
const MIN_PMC_TO_PI = 100;

export default async function handler(req, res) {
  if (req.method !== "POST") {
    return res.status(405).json({ ok: false, error: "Method not allowed" });
  }

  try {
    const { pmcAmount } = req.body || {};
    const safePmc = Math.max(0, Math.floor(Number(pmcAmount || 0) || 0));

    if (safePmc < MIN_PMC_TO_PI) {
      return res.status(400).json({
        ok: false,
        error: `Tối thiểu phải đổi ${MIN_PMC_TO_PI} PMC`
      });
    }

    // TODO: thay bằng user thật từ auth / session / token Pi / Firebase Auth
     const walletKey = req.headers["x-wallet-key"] || req.body?.walletKey;
    if (!walletKey) {
      return res.status(401).json({ ok: false, error: "Thiếu định danh ví." });
    }
    console.log("exchange-pmc-to-pi pmcAmount =", pmcAmount);
    console.log("exchange-pmc-to-pi walletKey =", walletKey);
    const db = firebaseAdmin.database();
   const walletPath = "wallets/" + String(walletKey).replace(/[.#$\[\]/]/g, "_");
    const walletRef = db.ref(walletPath);

    let exchangeResult = null;

    await walletRef.transaction(current => {
      console.log("exchangeResult =", exchangeResult);
      const safeCurrent = current && typeof current === "object" ? current : {};

      const currentPi = Number(safeCurrent.balance ?? 0) || 0;
      const currentPmc = Math.floor(Number(safeCurrent.pmcBalance ?? 0) || 0);

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

    if (!exchangeResult) {
      return res.status(400).json({ ok: false, error: "PMC không đủ hoặc giao dịch không hợp lệ." });
    }

    await db.ref("walletTransactions").push({
      type: "pmc_to_pi",
      walletKey,
      pmcAmount: safePmc,
      piAmount: exchangeResult.piAmount,
      rate: PMC_PER_PI,
      createdAt: Date.now(),
      status: "done"
    });

    return res.status(200).json({
      ok: true,
      pmcAmount: safePmc,
      piAmount: exchangeResult.piAmount,
      newPmcBalance: exchangeResult.newPmcBalance,
      newPiBalance: exchangeResult.newPiBalance
    });
  } catch (err) {
    console.error("exchange-pmc-to-pi error:", err);
    return res.status(500).json({ ok: false, error: "Lỗi server khi đổi PMC sang Pi." });
    console.error("exchange-pmc-to-pi error full:", err);
return res.status(500).json({
    ok: false,
    error: err?.message || "Lỗi server khi đổi PMC sang Pi."
});
  }
  
}