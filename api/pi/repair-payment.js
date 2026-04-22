const PI_API_BASE = String(
  process.env.PI_API_BASE_URL || "https://api.minepi.com"
).trim();

const PI_API_KEY = String(
  process.env.PI_API_KEY ||
    process.env.PI_SERVER_API_KEY ||
    process.env.PI_APIKEY ||
    ""
).trim();

const REPAIR_API_KEY = String(process.env.REPAIR_API_KEY || "").trim();

function nowMs() {
  return Date.now();
}

function safeKey(value) {
  return String(value || "").replace(/[.#$/[\]]/g, "_");
}

function pickString(...values) {
  for (const v of values) {
    const s = String(v ?? "").trim();
    if (s) return s;
  }
  return "";
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

async function readResponseData(res) {
  const raw = await res.text();
  if (!raw) return {};
  try {
    return JSON.parse(raw);
  } catch (_) {
    return { raw };
  }
}

function extractApiError(data) {
  return pickString(
    data?.error,
    data?.message,
    data?.error_message,
    data?.raw,
    data?.data?.error,
    data?.data?.message,
    data?.status?.error,
    "Pi API error"
  );
}

async function completePiPayment(paymentId, txid) {
  const res = await fetch(
    `${PI_API_BASE}/v2/payments/${encodeURIComponent(paymentId)}/complete`,
    {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Key ${PI_API_KEY}`,
        "Pi-Api-Key": PI_API_KEY
      },
      body: JSON.stringify({ txid })
    }
  );

  const data = await readResponseData(res);

  if (!res.ok) {
    return {
      ok: false,
      status: res.status,
      error: extractApiError(data),
      data
    };
  }

  return {
    ok: true,
    status: res.status,
    data
  };
}

module.exports = async function handler(req, res) {
  if (req.method !== "POST") {
    return res.status(405).json({
      ok: false,
      error: "Method not allowed"
    });
  }

  try {
    if (!PI_API_KEY) {
      return res.status(500).json({
        ok: false,
        error: "Thiếu PI_API_KEY."
      });
    }

    if (!REPAIR_API_KEY) {
      return res.status(500).json({
        ok: false,
        error: "Thiếu REPAIR_API_KEY."
      });
    }

    const sentRepairKey = String(req.headers["x-repair-key"] || "").trim();
    if (!sentRepairKey || sentRepairKey !== REPAIR_API_KEY) {
      return res.status(401).json({
        ok: false,
        error: "Sai x-repair-key."
      });
    }

    const body =
      typeof req.body === "string"
        ? JSON.parse(req.body || "{}")
        : req.body || {};

    const paymentId = String(body.paymentId || "").trim();
    const txid = String(body.txid || "").trim();
    const walletKeyRaw = String(body.walletKey || "").trim();
    const oldPendingWalletKeyRaw = String(body.oldPendingWalletKey || "").trim();
    const note = String(body.note || "manual repair old pending payment").trim();

    if (!paymentId) {
      return res.status(400).json({
        ok: false,
        error: "Thiếu paymentId."
      });
    }

    if (!txid) {
      return res.status(400).json({
        ok: false,
        error: "Thiếu txid."
      });
    }

    const adminBundle = require("../_firebaseAdmin.js");
    const { getDatabase } = require("firebase-admin/database");
    const adminApp = adminBundle.app || adminBundle;
    const db = getDatabase(adminApp);

    const completeRes = await completePiPayment(paymentId, txid);

    const verifyErr = String(
      completeRes?.data?.verification_error ||
        completeRes?.data?.error ||
        ""
    ).trim();

    const repaired =
      completeRes.ok || verifyErr === "payment_already_linked_with_a_tx";

    const safeWalletKey = safeKey(walletKeyRaw);
    const safeOldPendingWalletKey = safeKey(oldPendingWalletKeyRaw);

    const snap = await db.ref("piWithdrawRequests").once("value");
    const matchedKeySet = new Set();

    snap.forEach((child) => {
      const v = child.val() || {};

      const recordWalletKey = safeKey(
        pickString(v.walletKey, v.walletKeyRaw)
      );

      const recordOldPendingWalletKey = safeKey(
        pickString(
          v.oldPendingWalletKey,
          v?.paymentCreateData?.payment?.metadata?.walletKey,
          v?.paymentCreateData?.metadata?.walletKey
        )
      );

      const recordPaymentId = pickString(
        v.paymentId,
        v.oldPendingPaymentId,
        v?.paymentCreateData?.identifier,
        v?.paymentCreateData?.paymentId,
        v?.paymentCreateData?.payment_id,
        v?.paymentCreateData?.payment?.identifier,
        v?.paymentCreateData?.payment?.paymentId,
        v?.paymentCreateData?.payment?.payment_id
      );

      const recordTxid = pickString(
        v.txid,
        v.oldPendingTxid,
        v?.paymentCreateData?.transaction?.txid,
        v?.paymentCreateData?.payment?.transaction?.txid
      );

      const paymentMatch = !!paymentId && recordPaymentId === paymentId;
      const txMatch = !txid || !recordTxid || recordTxid === txid;

      const currentWalletMatch =
        !!safeWalletKey &&
        (recordWalletKey === safeWalletKey ||
          recordOldPendingWalletKey === safeWalletKey);

      const oldWalletMatch =
        !!safeOldPendingWalletKey &&
        (recordWalletKey === safeOldPendingWalletKey ||
          recordOldPendingWalletKey === safeOldPendingWalletKey);

      const walletHintMatch = currentWalletMatch || oldWalletMatch;

      if ((paymentMatch && txMatch) || (walletHintMatch && (paymentMatch || txMatch))) {
        matchedKeySet.add(child.key);
      }
    });

    const matchedKeys = Array.from(matchedKeySet);

    const patch = cleanForFirebase({
      status: repaired
        ? "completed_old_pending_payment"
        : "manual_repair_failed",
      paymentId,
      txid,
      oldPendingPaymentId: paymentId,
      oldPendingTxid: txid,
      oldPendingWalletKey: oldPendingWalletKeyRaw || "",
      repairStatus: completeRes.status,
      repairData: completeRes.data || null,
      verifyErr,
      cleanupNote: repaired
        ? "manual repair: payment cũ đã được complete / linked tx, cho qua để không chặn rút mãi"
        : "",
      repairNote: note,
      repairedAt: nowMs(),
      updatedAt: nowMs()
    });

    for (const key of matchedKeys) {
      await db.ref(`piWithdrawRequests/${key}`).update(patch);
    }

    return res.status(repaired ? 200 : 409).json({
      ok: repaired,
      repaired,
      paymentId,
      txid,
      walletKey: walletKeyRaw,
      oldPendingWalletKey: oldPendingWalletKeyRaw,
      verifyErr,
      matchedKeys,
      repairStatus: completeRes.status,
      repairData: completeRes.data || null
    });
  } catch (err) {
    return res.status(500).json({
      ok: false,
      error: err?.message || String(err)
    });
  }
};