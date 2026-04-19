import firebaseAdmin from "./firebaseAdmin.js";

const PMC_PER_PI = 1000;
const MIN_PMC_TO_PI = 0;

export default async function handler(req, res) {
  if (req.method !== "POST") {
    return res.status(405).json({ ok: false, error: "Method not allowed" });
  }

  try {
    const { pmcAmount, walletKey: bodyWalletKey } = req.body || {};
    const safePmc = Math.max(0, Math.floor(Number(pmcAmount || 0) || 0));

    if (!safePmc || safePmc <= 0) {
      return res.status(400).json({
        ok: false,
        error: "Nhập số PMC muốn đổi trước đã."
      });
    }

    if (safePmc < MIN_PMC_TO_PI) {
      return res.status(400).json({
        ok: false,
        error: `Tối thiểu phải đổi ${MIN_PMC_TO_PI} PMC`
      });
    }

    const walletKey = req.headers["x-wallet-key"] || bodyWalletKey;
    if (!walletKey) {
      return res.status(401).json({
        ok: false,
        error: "Thiếu định danh ví."
      });
    }

    console.log("exchange-pmc-to-pi pmcAmount =", pmcAmount);
    console.log("exchange-pmc-to-pi walletKey =", walletKey);

    const db = firebaseAdmin.database();
    const safeWalletKey = String(walletKey || "").replace(/[.#$\[\]/]/g, "_");
    const walletPath = "wallets/" + safeWalletKey;
    const walletRef = db.ref(walletPath);

    console.log("walletPath =", walletPath);

    let exchangeResult = null;

    await walletRef.transaction(current => {
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

    console.log("exchangeResult =", exchangeResult);

    if (!exchangeResult) {
      return res.status(400).json({
        ok: false,
        error: "PMC không đủ hoặc giao dịch không hợp lệ."
      });
    }

    await db.ref("walletTransactions").push({
      type: "pmc_to_pi",
      walletKey: safeWalletKey,
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
    console.error("exchange-pmc-to-pi error full:", err);
    return res.status(500).json({
      ok: false,
      error: err?.message || "Lỗi server khi đổi PMC sang Pi."
    });
  }
}