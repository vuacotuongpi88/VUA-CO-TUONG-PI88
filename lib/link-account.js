function safeKey(value) {
  return String(value || "").trim().replace(/[.#$\[\]/]/g, "_");
}

function loadFirebaseAdmin() {
  try {
    return require("../_firebaseAdmin.js");
  } catch (e) {
    throw new Error("load_firebaseAdmin failed: " + (e.message || String(e)));
  }
}

module.exports = async function handler(req, res) {
  if (req.method !== "POST") {
    return res.status(405).json({
      ok: false,
      error: "Method not allowed"
    });
  }

  try {
    const body =
      typeof req.body === "string"
        ? JSON.parse(req.body || "{}")
        : (req.body || {});

    const accessToken = String(body.accessToken || "").trim();
    const walletKeyRaw = String(
      req.headers["x-wallet-key"] || body.walletKey || ""
    ).trim();

    if (!accessToken) {
      return res.status(400).json({
        ok: false,
        error: "Thiếu accessToken."
      });
    }

    if (!walletKeyRaw) {
      return res.status(400).json({
        ok: false,
        error: "Thiếu walletKey."
      });
    }

    const meRes = await fetch("https://api.minepi.com/v2/me", {
      method: "GET",
      headers: {
        Authorization: `Bearer ${accessToken}`
      }
    });

    const raw = await meRes.text();
    let me = {};
    try {
      me = raw ? JSON.parse(raw) : {};
    } catch (_) {
      me = {};
    }

    if (!meRes.ok) {
      return res.status(401).json({
        ok: false,
        error: "Xác minh Pi /me thất bại.",
        raw
      });
    }

    const piUid = String(me.uid || "").trim();
    const piUsername = String(me.username || "").trim();
    const piWalletAddress = String(
      me.wallet_address || me.walletAddress || ""
    ).trim();

    if (!piUid) {
      return res.status(400).json({
        ok: false,
        error: "Pi /me không trả về uid hợp lệ."
      });
    }

    const adminBundle = loadFirebaseAdmin();
    const { getDatabase } = require("firebase-admin/database");
    const adminApp = adminBundle.app || adminBundle;
    const db = getDatabase(adminApp);

    const walletKey = safeKey(walletKeyRaw);
    const walletRef = db.ref("wallets/" + walletKey);
    const snap = await walletRef.once("value");
    const prev = snap.val() || {};

    const updateData = {
      ...prev,
      piVerified: true,
      piUid,
      piUsername: piUsername || prev.piUsername || "",
      verifiedAt: Date.now(),
      piLinkSource: "pi_browser"
    };

    if (piWalletAddress) {
      updateData.piWalletAddress = piWalletAddress;
    }

    await walletRef.update(updateData);

    return res.status(200).json({
      ok: true,
      piVerified: true,
      piUid,
      piUsername: updateData.piUsername || "",
      piWalletAddress: updateData.piWalletAddress || ""
    });
  } catch (err) {
    return res.status(500).json({
      ok: false,
      error: err?.message || String(err)
    });
  }
};