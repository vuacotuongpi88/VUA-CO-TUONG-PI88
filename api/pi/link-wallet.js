const { getDatabase } = require("firebase-admin/database");

let adminBundle;
try {
  adminBundle = require("./_firebaseAdmin.js");
} catch (e1) {
  adminBundle = require("../_firebaseAdmin.js");
}

function safeKey(value = "") {
  return String(value || "").trim().replace(/[.#$\[\]\/]/g, "_");
}

function cleanText(value = "") {
  return String(value || "").trim();
}

function normalizePiAddress(value = "") {
  return String(value || "").trim().toUpperCase();
}

function isValidPiAddress(address = "") {
  // Địa chỉ ví Pi/Stellar thường dài 56 ký tự, bắt đầu bằng G
  return /^G[A-Z2-7]{55}$/.test(String(address || "").trim().toUpperCase());
}

module.exports = async function handler(req, res) {
  res.setHeader("Content-Type", "application/json; charset=utf-8");

  if (req.method !== "POST" && req.method !== "GET") {
    return res.status(405).json({
      ok: false,
      error: "Method not allowed"
    });
  }

  try {
    const adminApp = adminBundle.app || adminBundle;
    const db = getDatabase(adminApp);

    const body =
      typeof req.body === "string"
        ? JSON.parse(req.body || "{}")
        : req.body || {};

    const walletKey = safeKey(
      req.headers["x-wallet-key"] ||
      body.walletKey ||
      ""
    );

    if (!walletKey) {
      return res.status(400).json({
        ok: false,
        error: "Thiếu walletKey."
      });
    }

    const walletRef = db.ref("wallets/" + walletKey);

    // GET: đọc trạng thái liên kết
    if (req.method === "GET") {
      const snap = await walletRef.once("value");
      const wallet = snap.val() || {};

      return res.status(200).json({
        ok: true,
        walletKey,
        piVerified: wallet.piVerified === true,
        piUid: cleanText(wallet.piUid || ""),
        piUsername: cleanText(wallet.piUsername || ""),
        piWalletAddress: cleanText(
          wallet.piWalletAddress ||
          wallet.linkedWalletAddress ||
          wallet.piBrowserWalletAddress ||
          wallet.withdrawWalletAddress ||
          ""
        )
      });
    }

    const piUid = cleanText(body.piUid || body.uid || "");
    const piUsername = cleanText(body.piUsername || body.username || "");
    const piWalletAddress = normalizePiAddress(
      body.piWalletAddress ||
      body.walletAddress ||
      body.linkedWalletAddress ||
      body.withdrawWalletAddress ||
      ""
    );

    if (!piUid) {
      return res.status(400).json({
        ok: false,
        error: "Thiếu Pi UID."
      });
    }

    if (!piUsername) {
      return res.status(400).json({
        ok: false,
        error: "Thiếu username Pi."
      });
    }

    if (!piWalletAddress) {
      return res.status(400).json({
        ok: false,
        error: "Thiếu địa chỉ ví Pi nhận tiền."
      });
    }

    if (!isValidPiAddress(piWalletAddress)) {
      return res.status(400).json({
        ok: false,
        error: "Địa chỉ ví Pi không hợp lệ. Địa chỉ phải bắt đầu bằng G và dài 56 ký tự."
      });
    }

    const now = Date.now();
    let finalWallet = null;

    const tx = await walletRef.transaction(current => {
      const wallet = current && typeof current === "object" ? current : {};

      const oldPiUid = cleanText(wallet.piUid || "");
      const oldAddress = normalizePiAddress(
        wallet.piWalletAddress ||
        wallet.linkedWalletAddress ||
        wallet.piBrowserWalletAddress ||
        wallet.withdrawWalletAddress ||
        ""
      );

      // Chặn lấy ví game của người khác gắn Pi UID khác
      if (oldPiUid && oldPiUid !== piUid) {
        return;
      }

      // Cho phép đổi địa chỉ nếu cùng Pi UID, vì Pi Browser đôi khi không trả ví tự động
      finalWallet = {
        ...wallet,

        walletKey,
        piUid,
        piUsername,
        piVerified: true,
        verifiedAt: wallet.verifiedAt || now,
        piLinkSource: "api_pi_browser",

        piWalletAddress,
        linkedWalletAddress: piWalletAddress,
        piBrowserWalletAddress: piWalletAddress,
        withdrawWalletAddress: piWalletAddress,
        withdrawAddress: piWalletAddress,

        linkedWallet: {
          ...(wallet.linkedWallet || {}),
          address: piWalletAddress,
          linkedAt: now,
          source: "api_pi_browser"
        },

        piLink: {
          ...(wallet.piLink || {}),
          piUid,
          piUsername,
          walletAddress: piWalletAddress,
          linkedAt: now,
          source: "api_pi_browser"
        },

        updatedAt: now
      };

      return finalWallet;
    });

    if (!tx.committed || !finalWallet) {
      return res.status(409).json({
        ok: false,
        error: "Ví này đã liên kết với Pi UID khác, không cho ghi đè."
      });
    }

    await db.ref("piWalletLinkLogs").push({
      walletKey,
      piUid,
      piUsername,
      piWalletAddress,
      oldSource: "pi_browser_link",
      createdAt: now,
      status: "done"
    }).catch(() => {});

    return res.status(200).json({
      ok: true,
      walletKey,
      piVerified: true,
      piUid,
      piUsername,
      piWalletAddress
    });
  } catch (err) {
    console.error("PI_LINK_WALLET_ERROR:", err);

    return res.status(500).json({
      ok: false,
      error: err?.message || "Lỗi server khi lưu địa chỉ ví Pi."
    });
  }
};