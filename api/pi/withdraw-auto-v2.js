module.exports = async function handler(req, res) {
  if (req.method !== "POST") {
    return res.status(405).json({
      ok: false,
      error: "Method not allowed"
    });
  }

  try {
    let adminBundle = null;
    let core = null;

    try {
      adminBundle = require("../_firebaseAdmin.js");
    } catch (e) {
      return res.status(500).json({
        ok: false,
        stage: "load_firebase_admin",
        error: e.message || String(e),
        stack: String(e.stack || "").slice(0, 1200)
      });
    }

    try {
      core = require("../../lib/withdraw-auto-core.js");
    } catch (e) {
      return res.status(500).json({
        ok: false,
        stage: "load_withdraw_auto_core",
        error: e.message || String(e),
        stack: String(e.stack || "").slice(0, 1200)
      });
    }

    return res.status(200).json({
      ok: true,
      message: "withdraw-auto-v2 alive",
      loaded: {
        firebaseAdmin: !!adminBundle,
        withdrawAutoCore: !!core
      }
    });
  } catch (e) {
    return res.status(500).json({
      ok: false,
      stage: "outer_handler",
      error: e.message || String(e),
      stack: String(e.stack || "").slice(0, 1200)
    });
  }
};