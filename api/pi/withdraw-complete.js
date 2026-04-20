module.exports = async function handler(req, res) {
  try {
    if (req.method !== "POST") {
      return res.status(405).json({
        ok: false,
        error: "Method not allowed"
      });
    }

    let adminBundle;
    let getDatabase;

    try {
      adminBundle = require("../_firebaseAdmin.js");
      ({ getDatabase } = require("firebase-admin/database"));
    } catch (e) {
      return res.status(500).json({
        ok: false,
        error: "load deps failed: " + (e?.message || String(e))
      });
    }

    const adminApp = adminBundle.app || adminBundle;
    const db = getDatabase(adminApp);

    return res.status(200).json({
      ok: true,
      msg: "withdraw-complete route OK",
      rootRef: db.ref().toString()
    });
  } catch (err) {
    return res.status(500).json({
      ok: false,
      error: err?.message || "server error"
    });
  }
};