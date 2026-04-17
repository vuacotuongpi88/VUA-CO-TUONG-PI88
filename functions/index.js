const { onValueCreated } = require("firebase-functions/database");
const { logger } = require("firebase-functions");
const admin = require("firebase-admin");

admin.initializeApp();

exports.settleMatchPmc = onValueCreated(
  {
    ref: "/matches/{roomId}/winner",
    region: "us-central1"
  },
  async (event) => {
    const roomId = event.params.roomId;
    const winner = event.data.val();

    if (!roomId || !winner) {
      logger.warn("Thiếu roomId hoặc winner", { roomId, winner });
      return;
    }

    const db = admin.database();
    const roomRef = db.ref(`matches/${roomId}`);
    const settlementRef = db.ref(`matches/${roomId}/settlement`);

    const roomSnap = await roomRef.get();
    if (!roomSnap.exists()) {
      logger.error("Không tìm thấy room", { roomId });
      return;
    }

    const room = roomSnap.val() || {};
    const stake = Math.max(0, Math.floor(Number(room.stakePMC || 0) || 0));
    const doPlayer = room.players?.do || {};
    const denPlayer = room.players?.den || {};

    if (!stake) {
      await settlementRef.update({
        done: false,
        paid: false,
        error: "stake_zero",
        errorAt: Date.now()
      });
      logger.warn("stakePMC = 0, bỏ qua payout", { roomId, stake });
      return;
    }

    const lock = await settlementRef.transaction((current) => {
      if (current && current.done) return;
      return {
        ...(current || {}),
        processing: true,
        done: false,
        paid: false,
        winner,
        stakePMC: stake,
        at: Date.now(),
        by: "cloud-function"
      };
    });

    if (!lock.committed) {
      logger.info("Settlement đã được xử lý trước đó", { roomId });
      return;
    }

    try {
      if (winner === "hoa") {
        const targets = [
          { side: "do", walletKey: doPlayer.walletKey || "" },
          { side: "den", walletKey: denPlayer.walletKey || "" }
        ].filter(x => x.walletKey);

        if (targets.length !== 2) {
          throw new Error("draw_missing_wallet");
        }

        for (const t of targets) {
          const walletRef = db.ref(`wallets/${t.walletKey}`);
          await walletRef.transaction((wallet) => {
            const next = wallet || {};
            next.pmcBalance = Math.max(
              0,
              Math.floor(Number(next.pmcBalance || 0) || 0) + stake
            );
            next.updatedAt = Date.now();
            return next;
          });
        }
      } else {
        const winnerPlayer = winner === "do" ? doPlayer : denPlayer;
        const winnerWalletKey = winnerPlayer.walletKey || "";

        if (!winnerWalletKey) {
          throw new Error("winner_wallet_missing");
        }

        const walletRef = db.ref(`wallets/${winnerWalletKey}`);
        await walletRef.transaction((wallet) => {
          const next = wallet || {};
          next.pmcBalance = Math.max(
            0,
            Math.floor(Number(next.pmcBalance || 0) || 0) + (stake * 2)
          );
          next.updatedAt = Date.now();
          return next;
        });
      }

      await settlementRef.update({
        processing: false,
        done: true,
        paid: true,
        paidAt: Date.now(),
        winner,
        stakePMC: stake,
        by: "cloud-function"
      });

      logger.info("Payout PMC thành công", { roomId, winner, stake });
    } catch (err) {
      await settlementRef.update({
        processing: false,
        done: false,
        paid: false,
        error: String(err?.message || err || "settle_failed"),
        errorAt: Date.now(),
        winner,
        stakePMC: stake,
        by: "cloud-function"
      });

      logger.error("Payout PMC thất bại", {
        roomId,
        winner,
        stake,
        error: String(err?.message || err || err)
      });
    }
  }
);