const { getDatabase } = require('firebase-admin/database');
const adminBundle = require('./_firebaseAdmin.js');

// CHỐT GIÁ VÀ PHẦN THƯỞNG CỨNG TRÊN SERVER, HACKER CÓ F12 ĐỔI GIÁ TRÊN HTML CŨNG VÔ DỤNG!
const EXP_PACKAGES = {
    "exp1": { exp: 1000, price: 1000, tickets: 2 },
    "exp2": { exp: 5000, price: 4800, tickets: 4 },
    "exp3": { exp: 20000, price: 18500, tickets: 8 },
    "exp4": { exp: 100000, price: 85000, tickets: 12 },
    "exp5": { exp: 500000, price: 400000, tickets: 16 },
    "exp6": { exp: 1650000, price: 1200000, tickets: 18 }
};

const LEVEL_MILESTONES = {
    10: { tickets: 2 },
    20: { tickets: 2 },
    30: { tickets: 2, skinId: 'bronze' },
    60: { tickets: 2, skinId: 'jade' },
    90: { tickets: 2 },
    120: { tickets: 2, skinId: 'dragon' },
    180: { tickets: 10, skinId: 'phoenix' }
};

module.exports = async function handler(req, res) {
    if (req.method !== 'POST') return res.status(405).json({ ok: false, error: 'Method not allowed' });

    try {
        const body = typeof req.body === "string" ? JSON.parse(req.body || "{}") : (req.body || {});
        const { action, walletKey } = body;
        const targetLevel = body.targetLevel;

        if (!walletKey) return res.status(400).json({ ok: false, error: "Thiếu ví người dùng" });

        const adminApp = adminBundle.app || adminBundle;
        const db = getDatabase(adminApp);
        const userRef = db.ref(`wallets/${walletKey}`);
        const adminRef = db.ref(`wallets/pi_admin_master`);

        // ==========================================
        // 1. TÍNH NĂNG: MỞ RƯƠNG CẤP ĐỘ
        // ==========================================
        if (action === 'claim_chest') {
            const milestone = LEVEL_MILESTONES[targetLevel];
            if (!milestone) return res.status(400).json({ ok: false, error: "Mốc rương không tồn tại!" });

            const txResult = await new Promise((resolve) => {
                userRef.transaction((current) => {
                    if (!current) return current;
                    
                    let curLevel = Number(current.level || 1);
                    if (curLevel < targetLevel) return; // Chưa đủ cấp

                    current.claimedLevels = current.claimedLevels || {};
                    if (current.claimedLevels[targetLevel]) return; // Đã nhận rồi

                    // Tính thưởng PMC ngẫu nhiên
                    let min = 0, max = 0;
                    if (targetLevel === 10) { min = 2; max = 20; }
                    else if (targetLevel === 20) { min = 4; max = 40; }
                    else { min = targetLevel; max = targetLevel * 3; }

                    let isLucky = Math.random() < 0.2;
                    let rewardPMC = isLucky ? (Math.floor(Math.random() * (max - (max/2) + 1)) + Math.floor(max/2)) 
                                            : (Math.floor(Math.random() * ((max/2) - min + 1)) + min);

                    // Trả thưởng
                    current.claimedLevels[targetLevel] = true;
                    current.pmcBalance = (Number(current.pmcBalance || 0)) + rewardPMC;
                    if (milestone.tickets) current.freeTickets = (Number(current.freeTickets || 0)) + milestone.tickets;
                    if (milestone.skinId) {
                        current.ownedAvatarSkins = current.ownedAvatarSkins || { none: true };
                        current.ownedAvatarSkins[milestone.skinId] = true;
                    }
                    current.updatedAt = Date.now();

                    // Đính kèm data để trả về cho Client
                    current._rewardPMC = rewardPMC; 
                    return current;
                }, (error, committed, snapshot) => resolve({ error, committed, snapshot }), false);
            });

            if (!txResult.committed) return res.status(400).json({ ok: false, error: "Điều kiện không hợp lệ hoặc đã mở rương này rồi!" });

            const snapData = txResult.snapshot.val();
            return res.status(200).json({ 
                ok: true, 
                rewardPMC: snapData._rewardPMC, 
                newPmc: snapData.pmcBalance,
                newTickets: snapData.freeTickets
            });
        }

        // ==========================================
        // 2. TÍNH NĂNG: MUA GÓI EXP TRONG SHOP
        // ==========================================
        if (action === 'buy_exp_direct') {
            const pkgId = String(body.pkgId);
            const pkg = EXP_PACKAGES[pkgId];
            if (!pkg) return res.status(400).json({ ok: false, error: "Gói EXP không tồn tại!" });

            const txResult = await new Promise((resolve) => {
                userRef.transaction((current) => {
                    if (!current) return current;
                    
                    let livePmc = Number(current.pmcBalance || 0);
                    if (livePmc < pkg.price) return; // Đéo đủ tiền

                    current.boughtExpPackages = current.boughtExpPackages || {};
                    if (current.boughtExpPackages[pkgId]) return; // Mua rồi cấm mua lại

                    // Trừ tiền, Cộng EXP, Cộng vé
                    current.pmcBalance = livePmc - pkg.price;
                    current.exp = (Number(current.exp || 0)) + pkg.exp;
                    
                    // Tính lại cấp độ
                    let newLevel = Math.floor((1 + Math.sqrt(1 + 8 * current.exp / 100)) / 2);
                    current.level = Math.min(180, newLevel);

                    current.freeTickets = (Number(current.freeTickets || 0)) + pkg.tickets;
                    current.boughtExpPackages[pkgId] = true;
                    current.updatedAt = Date.now();

                    return current;
                }, (error, committed, snapshot) => resolve({ error, committed, snapshot }), false);
            });

            if (!txResult.committed) return res.status(400).json({ ok: false, error: "Không đủ PMC hoặc đã mua gói này rồi!" });

            // Trích tiền về quỹ Admin
            await adminRef.child('pmcBalance').set({
                ".sv": { "increment": pkg.price }
            });
            await db.ref("walletTransactions").push({
                type: "buy_exp_package", walletKey, adminWalletKey: "pi_admin_master", pkgId,
                feePMC: pkg.price, expGained: pkg.exp, createdAt: Date.now(), status: "done"
            });

            const snapData = txResult.snapshot.val();
            return res.status(200).json({ ok: true, newExp: snapData.exp, newLevel: snapData.level, newPmc: snapData.pmcBalance });
        }

        // ==========================================
        // 3. TÍNH NĂNG: MUA LẠI EXP KHI THUA TRẬN
        // ==========================================
        if (action === 'buy_back_exp') {
            const costPMC = Math.floor(Number(body.costPMC));
            if (costPMC <= 0) return res.status(400).json({ ok: false, error: "Số PMC không hợp lệ" });

            const txResult = await new Promise((resolve) => {
                userRef.transaction((current) => {
                    if (!current) return current;
                    let livePmc = Number(current.pmcBalance || 0);
                    if (livePmc < costPMC) return;

                    current.pmcBalance = livePmc - costPMC;
                    current.exp = (Number(current.exp || 0)) + costPMC; // Trả lại đúng số EXP đã mất
                    
                    let newLevel = Math.floor((1 + Math.sqrt(1 + 8 * current.exp / 100)) / 2);
                    current.level = Math.min(180, newLevel);
                    current.updatedAt = Date.now();

                    return current;
                }, (error, committed, snapshot) => resolve({ error, committed, snapshot }), false);
            });

            if (!txResult.committed) return res.status(400).json({ ok: false, error: "Không đủ PMC để mua lại EXP!" });

            // Bắn tiền về quỹ Admin
            await adminRef.child('pmcBalance').set({ ".sv": { "increment": costPMC } });
            await db.ref("walletTransactions").push({
                type: "buy_back_exp_after_loss", walletKey, adminWalletKey: "pi_admin_master",
                feePMC: costPMC, createdAt: Date.now(), status: "done"
            });

            return res.status(200).json({ ok: true, newExp: txResult.snapshot.val().exp, newPmc: txResult.snapshot.val().pmcBalance });
        }

        return res.status(400).json({ ok: false, error: 'Hành động không hợp lệ' });

    } catch (err) {
        console.error("Lỗi Level System API:", err);
        return res.status(500).json({ ok: false, error: "Lỗi hệ thống Vercel" });
    }
};