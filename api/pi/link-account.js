const { getDatabase } = require("firebase-admin/database");
const adminBundle = require("../_firebaseAdmin.js");

function safeKey(value) {
  return String(value || "").replace(/[.#$/\[\]]/g, "_");
}

function cleanForFirebase(input) {
  if (input === undefined || input === null) return null;

  if (Array.isArray(input)) {
    return input.map((x) => cleanForFirebase(x)).filter((x) => x !== undefined);
  }

  if (typeof input === "object") {
    const out = {};
    for (const [k, v] of Object.entries(input)) {
      if (v === undefined) continue;
      out[k] = cleanForFirebase(v);
    }
    return out;
  }

  return input;
}

function pickIncomingWalletAddress(body = {}) {
  const candidates = [
    body.walletAddress,
    body.publicKey,
    body.piWalletAddress,
    body.piBrowserWalletAddress,
    body.linkedWalletAddress,
    body?.wallet?.address,
    body?.piLink?.walletAddress,
    body?.piBrowser?.address
  ];

  for (const raw of candidates) {
    const s = String(raw || "").trim();
    if (s) return s;
  }

  return "";
}

function isValidPiWalletAddress(address) {
  return /^G[A-Z2-7]{55}$/.test(String(address || "").trim());
}

module.exports = async function handler(req, res) {
  if (req.method !== "POST") {
    return res.status(405).json({
      ok: false,
      error: "Phương thức không được phép"
    });
  }

  try {
    const body =
      typeof req.body === "string"
        ? JSON.parse(req.body || "{}")
        : (req.body || {});

    const walletKeyRaw = String(
      body.walletKey ||
      req.headers["x-wallet-key"] ||
      req.headers["wallet-key"] ||
      ""
    ).trim();

    const walletKey = safeKey(walletKeyRaw);
    if (!walletKey) {
      return res.status(400).json({
        ok: false,
        error: "Thiếu walletKey nội bộ"
      });
    }

    const piUid = String(body.piUid || "").trim();
    const piUsername = String(body.piUsername || "").trim();
    const recipientAddress = pickIncomingWalletAddress(body);

    if (!piUid) {
      return res.status(400).json({
        ok: false,
        error: "Thiếu piUid"
      });
    }

    if (!piUsername) {
      return res.status(400).json({
        ok: false,
        error: "Thiếu piUsername"
      });
    }

    if (!recipientAddress) {
      return res.status(400).json({
        ok: false,
        error: "Thiếu địa chỉ ví Pi nhận tiền"
      });
    }

    if (!isValidPiWalletAddress(recipientAddress)) {
      return res.status(400).json({
        ok: false,
        error: "Địa chỉ ví Pi không hợp lệ"
      });
    }

    const db = getDatabase(adminBundle.adminApp);
    const walletRef = db.ref(`wallets/${walletKey}`);
    const now = Date.now();

    await walletRef.update(
      cleanForFirebase({
        walletKey,
        piUid,
        piUsername,
        piVerified: true,
        verifiedAt: now,
        piLinkSource: "pi-browser",

        piWalletAddress: recipientAddress,
        linkedWalletAddress: recipientAddress,
        piBrowserWalletAddress: recipientAddress,

        linkedWallet: {
          address: recipientAddress,
          linkedAt: now,
          source: "pi-browser"
        },

        piLink: {
          walletAddress: recipientAddress,
          linkedAt: now,
          source: "pi-browser"
        },

        updatedAt: now
      })
    );

    return res.status(200).json({
      ok: true,
      walletKey,
      piUid,
      piUsername,
      recipientAddress,
      message: "Đã lưu ví Pi nhận tiền thành công"
    });
  } catch (err) {
    return res.status(500).json({
      ok: false,
      error: err?.message || String(err)
    });
  }
};