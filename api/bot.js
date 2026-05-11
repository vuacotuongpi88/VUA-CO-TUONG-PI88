const admin = require('firebase-admin');

if (!admin.apps.length) {
    try {
        admin.initializeApp({
            credential: admin.credential.cert({
                "projectId": process.env.FIREBASE_PROJECT_ID,
                "private_key": process.env.FIREBASE_PRIVATE_KEY?.replace(/\\n/g, '\n'),
                "client_email": process.env.FIREBASE_CLIENT_EMAIL
            }),
            databaseURL: "https://co-tuong-bd072-default-rtdb.asia-southeast1.firebasedatabase.app"
        });
    } catch (e) {
        console.error("Lỗi khởi tạo Firebase Admin:", e);
    }
}

const db = admin.database();

module.exports = async (req, res) => {
    res.setHeader('Access-Control-Allow-Origin', '*');
    res.setHeader('Access-Control-Allow-Methods', 'POST, OPTIONS');
    res.setHeader('Access-Control-Allow-Headers', 'Content-Type');

    if (req.method === 'OPTIONS') return res.status(200).end();
    if (req.method !== 'POST') return res.status(405).json({ error: 'Method Not Allowed' });

    const { action, roomId, hostWallet } = req.body;

    if (!roomId) return res.status(400).json({ ok: false, error: 'Thiếu Room ID' });

    // HỒ SƠ BOT
    const botProfile = {
        uid: "bot_master_100",
        walletKey: "bot_master_100",
        name: "Vô Danh Lão Tẩu",
        photo: "https://i.imgur.com/QhT8A4O.png",
        level: 100,
        isBot: true,
        pmcBalance: 99999999,
        avatarSkin: "dragon",
        statsV2: { wins: 9999, losses: 1, matches: 10000 },
        updatedAt: admin.database.ServerValue.TIMESTAMP
    };

    try {
        const roomRef = db.ref(`matches/${roomId}`);
        const snap = await roomRef.once('value');
        const room = snap.val();

        if (!room) return res.status(404).json({ ok: false, error: 'Phòng không tồn tại.' });

        // CHỈ XỬ LÝ ACTION 'JOIN' ĐỂ CHO BOT VÀO BÀN
        if (action === 'join') {
            if (room.status !== "waiting" || room.players?.den) {
                return res.status(400).json({ ok: false, error: 'Phòng đéo trống.' });
            }

            // BẢO VỆ 1: Mức cược tối đa
            const stake = Math.max(0, Number(room.stakePMC || 0));
            if (stake > 500) {
                return res.status(400).json({ ok: false, error: 'Lão phu nghèo, đéo đánh cược lớn.' });
            }

            // BẢO VỆ 2: Giới hạn 3 ván/ngày
            const hostBotKey = `bot_quota_${hostWallet}_${new Date().toISOString().split('T')[0]}`;
            const quotaSnap = await db.ref(`system_limits/${hostBotKey}`).once('value');
            const matchesToday = quotaSnap.val() || 0;
            
            if (matchesToday >= 3) {
                return res.status(400).json({ ok: false, error: 'Nay lão phu mỏi lưng rồi, tha cho lão.' });
            }

            const updates = {};
            updates[`matches/${roomId}/players/den`] = botProfile;
            updates[`matches/${roomId}/stakeLocked/den`] = {
                done: true,
                walletKey: "bot_master_100",
                stake: stake,
                at: Date.now(),
                uid: "bot_master_100",
                isBotStake: true 
            };
            updates[`matches/${roomId}/ready/den`] = true;
            updates[`matches/${roomId}/lobbyOpen`] = false;
            updates[`system_limits/${hostBotKey}`] = admin.database.ServerValue.increment(1);

            await db.ref().update(updates);
            return res.status(200).json({ ok: true, message: 'Bot đã nhập tiệc!' });
        }

        return res.status(400).json({ ok: false, error: 'Action không hợp lệ.' });

    } catch (error) {
        console.error("Bot API Error:", error);
        return res.status(500).json({ ok: false, error: 'Lỗi server nội bộ của Bot.' });
    }
};