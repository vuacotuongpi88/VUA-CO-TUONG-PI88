const VN_OFFSET_MS = 7 * 60 * 60 * 1000;

function nowMs() {
  return Date.now();
}

function localDate(ts = Date.now()) {
  return new Date(ts + VN_OFFSET_MS);
}

function localDayKey(ts = Date.now()) {
  const d = localDate(ts);
  const y = d.getUTCFullYear();
  const m = String(d.getUTCMonth() + 1).padStart(2, "0");
  const day = String(d.getUTCDate()).padStart(2, "0");
  return `${y}${m}${day}`;
}

function weekStartMs(ts = Date.now()) {
  const d = localDate(ts);
  const day = d.getUTCDay() || 7; // Thứ 2 là đầu tuần
  d.setUTCDate(d.getUTCDate() - day + 1);
  d.setUTCHours(0, 0, 0, 0);
  return d.getTime() - VN_OFFSET_MS;
}

function localWeekKey(ts = Date.now()) {
  return `W${localDayKey(weekStartMs(ts))}`;
}

function roundPmc(value) {
  const n = Number(value || 0);
  if (!Number.isFinite(n)) return 0;
  return Math.max(0, Math.round(n * 1000000) / 1000000);
}

function safeKey(value = "") {
  return String(value || "").trim().replace(/[.#$\[\]\/]/g, "_");
}

async function txAddPmcBalance(db, walletKey, amount) {
  const add = roundPmc(amount);
  if (add <= 0) {
    return {
      committed: true,
      after: null
    };
  }

  const ref = db.ref(`wallets/${safeKey(walletKey)}`);
  let after = null;

  const tx = await ref.transaction(current => {
    const cur = current && typeof current === "object" ? current : {};
    const oldPmc = Number(cur.pmcBalance || 0) || 0;
    after = roundPmc(oldPmc + add);

    return {
      ...cur,
      name: cur.name || "Ví phí hệ thống",
      pmcBalance: after,
      updatedAt: nowMs()
    };
  });

  return {
    committed: !!tx.committed,
    after
  };
}

// Tên hàm giữ nguyên để không gãy require cũ,
// nhưng logic bên trong đổi thành quỹ TUẦN.
async function sweepExpiredMissionPool(db, adminWalletKey = "pi_admin_master", ts = Date.now()) {
  const currentWeekKey = localWeekKey(ts);
  const metaRef = db.ref("treasury/missionPoolMeta");

  let oldWeekKey = "";
  let shouldSweep = false;

  const metaTx = await metaRef.transaction(current => {
    const meta = current && typeof current === "object" ? current : {};
    oldWeekKey = String(meta.currentWeekKey || "");

    // Lần đầu chạy bản tuần: chỉ đóng dấu tuần hiện tại, không quét bậy tiền đang có.
    if (!oldWeekKey) {
      return {
        ...meta,
        poolMode: "week",
        currentWeekKey,
        createdAt: meta.createdAt || nowMs(),
        updatedAt: nowMs()
      };
    }

    // Cùng tuần thì không hoàn quỹ.
    if (oldWeekKey === currentWeekKey) {
      return {
        ...meta,
        poolMode: "week",
        updatedAt: nowMs()
      };
    }

    // Có máy khác đang quét thì thôi.
    if (meta.sweepLock) {
      return;
    }

    shouldSweep = true;

    return {
      ...meta,
      poolMode: "week",
      sweepLock: `${oldWeekKey}_to_${currentWeekKey}`,
      sweepFromWeekKey: oldWeekKey,
      sweepToWeekKey: currentWeekKey,
      sweepStartedAt: nowMs(),
      updatedAt: nowMs()
    };
  });

  if (!metaTx.committed || !shouldSweep) {
    return {
      ok: true,
      swept: false,
      poolMode: "week",
      currentWeekKey,
      oldWeekKey,
      amountPmc: 0
    };
  }

  const poolRef = db.ref("treasury/missionPoolPmc");
  let sweptAmount = 0;

  const poolTx = await poolRef.transaction(current => {
    sweptAmount = roundPmc(Number(current || 0) || 0);
    return 0;
  });

  if (!poolTx.committed) {
    await metaRef.update({
      sweepLock: null,
      sweepError: "pool_transaction_failed",
      updatedAt: nowMs()
    }).catch(() => {});

    throw new Error("Không quét được quỹ nhiệm vụ tuần cũ.");
  }

  let adminAfter = null;

  if (sweptAmount > 0) {
    const adminTx = await txAddPmcBalance(db, adminWalletKey, sweptAmount);
    adminAfter = adminTx?.after ?? null;

    await db.ref("missionPoolSweepLogs").push({
      type: "mission_pool_weekly_sweep",
      poolMode: "week",
      fromWeekKey: oldWeekKey,
      toWeekKey: currentWeekKey,
      amountPmc: sweptAmount,
      adminWalletKey: safeKey(adminWalletKey),
      adminPmcAfter: adminAfter,
      createdAt: nowMs(),
      status: "done"
    }).catch(() => {});
  }

  await metaRef.update({
    poolMode: "week",
    currentWeekKey,
    previousWeekKey: oldWeekKey,
    lastSweptPmc: sweptAmount,
    lastSweptAt: nowMs(),
    adminWalletKey: safeKey(adminWalletKey),
    adminPmcAfter: adminAfter,

    // Xóa dấu vết bản ngày cũ cho sạch
    currentDayKey: null,
    previousDayKey: null,
    sweepFromDayKey: null,
    sweepToDayKey: null,

    sweepLock: null,
    sweepFromWeekKey: null,
    sweepToWeekKey: null,
    updatedAt: nowMs()
  }).catch(() => {});

  return {
    ok: true,
    swept: true,
    poolMode: "week",
    currentWeekKey,
    oldWeekKey,
    amountPmc: sweptAmount,
    adminPmcAfter: adminAfter
  };
}

async function addMissionPoolPmc(db, amount, adminWalletKey = "pi_admin_master", meta = {}) {
  await sweepExpiredMissionPool(db, adminWalletKey, Date.now());

  const add = roundPmc(amount);

  if (add <= 0) {
    const snap = await db.ref("treasury/missionPoolPmc").once("value");
    return {
      ok: true,
      added: 0,
      missionPoolPmc: roundPmc(snap.val() || 0),
      poolMode: "week",
      weekKey: localWeekKey()
    };
  }

  const poolRef = db.ref("treasury/missionPoolPmc");
  let after = 0;

  const tx = await poolRef.transaction(current => {
    after = roundPmc((Number(current || 0) || 0) + add);
    return after;
  });

  const finalPool = roundPmc(tx.snapshot?.val() ?? after);

  await db.ref("treasury/missionPoolMeta").update({
    poolMode: "week",
    currentWeekKey: localWeekKey(),
    updatedAt: nowMs()
  }).catch(() => {});

  await db.ref("missionPoolTransactions").push({
    type: meta.type || "mission_pool_add",
    source: meta.source || "unknown",
    roomId: meta.roomId || "",
    amountPmc: add,
    missionPoolPmc: finalPool,
    poolMode: "week",
    weekKey: localWeekKey(),
    createdAt: nowMs(),
    status: "done"
  }).catch(() => {});

  return {
    ok: true,
    added: add,
    missionPoolPmc: finalPool,
    poolMode: "week",
    weekKey: localWeekKey()
  };
}

module.exports = {
  localDayKey,
  localWeekKey,
  roundPmc,
  sweepExpiredMissionPool,
  addMissionPoolPmc
};