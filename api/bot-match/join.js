const { db } = require("../../firebaseAdmin"); 

module.exports = async (req, res) => {
    if (req.method !== 'POST') return res.status(405).json({ ok: false, error: 'Method Not Allowed' });

    try {
        const { roomId } = req.body;
        const hostWalletKey = req.headers['x-wallet-key'];

        if (!roomId || !hostWalletKey) {
            return res.status(400).json({ ok: false, error: 'Thiếu thông tin.' });
        }

        const roomRef = db.ref(`matches/${roomId}`);
        const snap = await roomRef.once("value");
        const room = snap.val();

        if (!room) return res.status(404).json({ ok: false, error: 'Phòng không tồn tại.' });
        if (room.status !== "waiting" || room.players?.den) {
            return res.status(400).json({ ok: false, error: 'Phòng không ở trạng thái trống để Bot vào.' });
        }

        // KỊCH BẢN: TẠO PROFILE VÔ DANH LÃO TẨU (CẤP 100)
        const botProfile = {
            uid: "bot_master_100",
            walletKey: "bot_master_100",
            name: "Vô Danh Lão Tẩu",
            photo: "https://i.imgur.com/QhT8A4G.png", 
            level: 100,
            isBot: true, 
            pmcBalance: 99999999, 
            avatarSkin: "dragon", 
            statsV2: { wins: 9999, losses: 1, matches: 10000 } 
        };

        const updates = {};
        // 1. Mời Bot ngồi vào ghế Đen
        updates[`matches/${roomId}/players/den`] = botProfile;
        
        // 2. Ép Bot khóa cược luôn
        const stake = room.stakePMC || 0;
        updates[`matches/${roomId}/stakeLocked/den`] = {
            done: true,
            walletKey: "bot_master_100",
            stake: stake,
            at: Date.now(),
            uid: "bot_master_100",
            isBotStake: true // Cờ đánh dấu để đéo hoàn tiền cho Bot
        };
        
        // 3. Bot Sẵn Sàng luôn!
        updates[`matches/${roomId}/ready/den`] = true;
        
        // 4. Khóa cửa phòng
        updates[`matches/${roomId}/lobbyOpen`] = false;

        await db.ref().update(updates);

        return res.status(200).json({ ok: true, message: "Bot đã nhập tiệc!" });

    } catch (err) {
        console.error("Lỗi Bot Join:", err);
        return res.status(500).json({ ok: false, error: 'Lỗi server.' });
    }
};