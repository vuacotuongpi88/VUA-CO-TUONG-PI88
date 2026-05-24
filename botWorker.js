// ==========================================
// FILE: botWorker.js (CHẠY NGẦM TRÊN MÁY KHÁCH)
// BẢN TỐI THƯỢNG: CHỐNG PHÁO ĐẦU + BẢO VỆ TƯỚNG + GÀI THẾ + NÃO CỜ ÚP BỚT NGU
// ==========================================

let positionHistory = []; 
let killerMoves = Array(100).fill(null).map(() => []);
let transTable = new Map();
let ponderTable = new Map();
let ponderBusy = false;

const MAX_TRANS_TABLE = 80000;
const MAX_PONDER_TABLE = 300;
let BOT_SEARCH_DEADLINE = 0;
let BOT_SEARCH_TIMED_OUT = false;
let BOT_NODE_COUNT = 0;

function clampBotLevel(level) {
    level = Number(level || 6);
    if (!Number.isFinite(level)) level = 6;
    return Math.max(1, Math.min(10, Math.floor(level)));
}

function getBotLevelConfig(level, pieceCount, isCoUp) {
    level = clampBotLevel(level);

    const table = {
        1:  { maxDepth: 1, maxMs: 120,  rootLimit: 8,   cloud: false, cloudMs: 0,   blunder: 0.45 },
        2:  { maxDepth: 2, maxMs: 180,  rootLimit: 10,  cloud: false, cloudMs: 0,   blunder: 0.35 },
        3:  { maxDepth: 2, maxMs: 260,  rootLimit: 14,  cloud: false, cloudMs: 0,   blunder: 0.25 },
        4:  { maxDepth: 3, maxMs: 380,  rootLimit: 18,  cloud: false, cloudMs: 0,   blunder: 0.16 },
        5:  { maxDepth: 3, maxMs: 520,  rootLimit: 24,  cloud: false, cloudMs: 0,   blunder: 0.10 },
        6:  { maxDepth: 4, maxMs: 720,  rootLimit: 32,  cloud: false, cloudMs: 0,   blunder: 0.05 },
        7:  { maxDepth: 4, maxMs: 950,  rootLimit: 40,  cloud: true,  cloudMs: 250, blunder: 0.02 },
        8:  { maxDepth: 5, maxMs: 1250, rootLimit: 52,  cloud: true,  cloudMs: 320, blunder: 0.00 },
        9:  { maxDepth: 6, maxMs: 1650, rootLimit: 70,  cloud: true,  cloudMs: 420, blunder: 0.00 },
        10: { maxDepth: 7, maxMs: 2200, rootLimit: 999, cloud: true,  cloudMs: 500, blunder: 0.00 }
    };

    const cfg = { level, ...table[level] };

    if (pieceCount <= 12 && level >= 6) cfg.maxDepth += 1;
    if (pieceCount <= 7 && level >= 7) cfg.maxDepth += 1;

    if (isCoUp && cfg.maxDepth > 3) cfg.maxDepth -= 1;

    return cfg;
}

function botTimeUp() {
    BOT_NODE_COUNT++;
    if ((BOT_NODE_COUNT & 1023) !== 0) return false;

    if (BOT_SEARCH_DEADLINE && Date.now() >= BOT_SEARCH_DEADLINE) {
        BOT_SEARCH_TIMED_OUT = true;
        return true;
    }

    return false;
}

function boardArrayHash(boardArray, botSide, isCoUp) {
    return getBoardHash(buildMatrix(boardArray)) + "|" + botSide + "|" + (isCoUp ? "up" : "normal");
}

function trimBigMap(map, maxSize) {
    if (map.size <= maxSize) return;
    const removeCount = Math.floor(maxSize * 0.25);
    let i = 0;
    for (const key of map.keys()) {
        map.delete(key);
        i++;
        if (i >= removeCount) break;
    }
}

function moveKey(m) {
    return `${m.from.c},${m.from.r}-${m.to.c},${m.to.r}`;
}

const PIECE_VALUES = {
    '帥': 1000000, '將': 1000000,
    '俥': 1200, '車': 1200,
    '炮': 550,  '砲': 550,
    '傌': 500,  '馬': 500,  
    '相': 250,  '象': 250,
    '仕': 250,  '士': 250,
    '兵': 120,  '卒': 120
};

function getBoardHash(mt) {
    let hash = "";
    for(let r=0; r<10; r++) {
        for(let c=0; c<9; c++) {
            if(mt[r][c]) hash += r + "" + c + mt[r][c].type + (mt[r][c].isUp ? "U" : "D");
        }
    }
    return hash;
}

function buildMatrix(boardArray) {
    let mt = Array(10).fill(null).map(() => Array(9).fill(null));
    boardArray.forEach(p => { mt[p.r][p.c] = p; });
    return mt;
}

function countV(c1, r1, c2, r2, mt) {
    let count = 0;
    if (c1 === c2) {
        for (let r = Math.min(r1, r2) + 1; r < Math.max(r1, r2); r++) {
            if (mt[r][c1]) count++;
        }
    } else {
        for (let c = Math.min(c1, c2) + 1; c < Math.max(c1, c2); c++) {
            if (mt[r1][c]) count++;
        }
    }
    return count;
}

function checkLuatWorker(type, side, c, r, tc, tr, mt, isCoUp) {
    if (tc < 0 || tc > 8 || tr < 0 || tr > 9) return false;
    if (mt[tr][tc] && mt[tr][tc].side === side) return false; 

    const dx = Math.abs(tc - c), dy = Math.abs(tr - r);
    let luatType = type;

    if (isCoUp && mt[r][c] && mt[r][c].isUp) {
        if (r === 0 || r === 9) {
            if (c === 0 || c === 8) luatType = (side === 'do' ? '俥' : '車');
            else if (c === 1 || c === 7) luatType = (side === 'do' ? '傌' : '馬');
            else if (c === 2 || c === 6) luatType = (side === 'do' ? '相' : '象');
            else if (c === 3 || c === 5) luatType = (side === 'do' ? '仕' : '士');
        } else if ((r === 2 && side === 'den') || (r === 7 && side === 'do')) {
            if (c === 1 || c === 7) luatType = (side === 'do' ? '炮' : '砲');
        } else if ((r === 3 && side === 'den') || (r === 6 && side === 'do')) {
            if (c % 2 === 0) luatType = (side === 'do' ? '兵' : '卒');
        }
    }

    switch (luatType) {
        case '帥':
        case '將':
            return dx + dy === 1 && tc >= 3 && tc <= 5 && (side === 'do' ? tr >= 7 : tr <= 2);

        case '仕':
        case '士':
            if (isCoUp && mt[r][c] && !mt[r][c].isUp) return dx === 1 && dy === 1;
            return dx === 1 && dy === 1 && tc >= 3 && tc <= 5 && (side === 'do' ? tr >= 7 : tr <= 2);

        case '相':
        case '象':
            if (!isCoUp && (side === 'do' ? tr < 5 : tr > 4)) return false;
            if (dx !== 2 || dy !== 2) return false;
            return !mt[(r + tr) / 2][(c + tc) / 2];

        case '傌':
        case '馬':
            if (!((dx === 1 && dy === 2) || (dx === 2 && dy === 1))) return false;
            return !mt[r + (dy === 2 ? (tr > r ? 1 : -1) : 0)][c + (dx === 2 ? (tc > c ? 1 : -1) : 0)];

        case '俥':
        case '車':
            if (dx !== 0 && dy !== 0) return false;
            return countV(c, r, tc, tr, mt) === 0;

        case '炮':
        case '砲': {
            const v = countV(c, r, tc, tr, mt);
            return (mt[tr][tc] ? v === 1 : v === 0) && (dx === 0 || dy === 0);
        }

        case '兵':
        case '卒': {
            const daQuaSong = (side === 'do') ? (r <= 4) : (r >= 5);
            const diTien = (side === 'do') ? (dx === 0 && dy === 1 && tr < r) : (dx === 0 && dy === 1 && tr > r);
            const diNgang = (dx === 1 && dy === 0);
            if (!daQuaSong) return diTien;
            return diTien || diNgang;
        }
    }

    return false;
}

function laBiChieuWorker(side, mt, isCoUp) {
    let tPos = null;

    for (let r = 0; r < 10; r++) {
        for (let c = 0; c < 9; c++) {
            if (mt[r][c] && mt[r][c].side === side && (mt[r][c].type === '帥' || mt[r][c].type === '將')) {
                tPos = { c, r };
                break;
            }
        }
        if (tPos) break;
    }

    if (!tPos) return false;

    for (let r = 0; r < 10; r++) {
        for (let c = 0; c < 9; c++) {
            if (mt[r][c] && mt[r][c].side !== side) {
                if (checkLuatWorker(mt[r][c].type, mt[r][c].side, c, r, tPos.c, tPos.r, mt, isCoUp)) {
                    return true;
                }
            }
        }
    }

    return false;
}

function haiTuongDoiMat(mt) {
    let tDo = null, tDen = null;

    for(let r=0; r<10; r++) {
        for(let c=0; c<9; c++) {
            if(mt[r][c] && mt[r][c].type === '帥') tDo = {c, r};
            if(mt[r][c] && mt[r][c].type === '將') tDen = {c, r};
        }
    }

    if(!tDo || !tDen) return false;
    if(tDo.c !== tDen.c) return false; 

    let count = 0;
    let minR = Math.min(tDo.r, tDen.r);
    let maxR = Math.max(tDo.r, tDen.r);

    for(let r = minR + 1; r < maxR; r++) {
        if(mt[r][tDo.c]) count++;
    }

    return count === 0;
}

function otherSide(side) {
    return side === 'do' ? 'den' : 'do';
}

function isKingPiece(type) {
    return type === '帥' || type === '將';
}

function isRookPiece(type) {
    return type === '俥' || type === '車';
}

function isCannonPiece(type) {
    return type === '炮' || type === '砲';
}

function isHorsePiece(type) {
    return type === '傌' || type === '馬';
}

function isGuardPiece(type) {
    return type === '仕' || type === '士' || type === '相' || type === '象';
}

function getPieceBaseValue(type) {
    return PIECE_VALUES[type] || 10;
}

function getEffectiveMoveType(type, side, c, r, mt, isCoUp) {
    let luatType = type;

    if (isCoUp && mt[r][c] && mt[r][c].isUp) {
        if (r === 0 || r === 9) {
            if (c === 0 || c === 8) luatType = (side === 'do' ? '俥' : '車');
            else if (c === 1 || c === 7) luatType = (side === 'do' ? '傌' : '馬');
            else if (c === 2 || c === 6) luatType = (side === 'do' ? '相' : '象');
            else if (c === 3 || c === 5) luatType = (side === 'do' ? '仕' : '士');
        } else if ((r === 2 && side === 'den') || (r === 7 && side === 'do')) {
            if (c === 1 || c === 7) luatType = (side === 'do' ? '炮' : '砲');
        } else if ((r === 3 && side === 'den') || (r === 6 && side === 'do')) {
            if (c % 2 === 0) luatType = (side === 'do' ? '兵' : '卒');
        }
    }

    return luatType;
}

// Kiểm tra một quân có kiểm soát/bảo vệ một ô không.
// Khác checkLuatWorker: ô có quân mình vẫn tính là được bảo vệ.
function canPieceControlSquare(type, side, c, r, tc, tr, mt, isCoUp) {
    if (tc < 0 || tc > 8 || tr < 0 || tr > 9) return false;
    if (c === tc && r === tr) return false;

    const dx = Math.abs(tc - c), dy = Math.abs(tr - r);
    const luatType = getEffectiveMoveType(type, side, c, r, mt, isCoUp);

    switch (luatType) {
        case '帥':
        case '將':
            return dx + dy === 1 && tc >= 3 && tc <= 5 && (side === 'do' ? tr >= 7 : tr <= 2);

        case '仕':
        case '士':
            if (isCoUp && mt[r][c] && !mt[r][c].isUp) return dx === 1 && dy === 1;
            return dx === 1 && dy === 1 && tc >= 3 && tc <= 5 && (side === 'do' ? tr >= 7 : tr <= 2);

        case '相':
        case '象':
            if (!isCoUp && (side === 'do' ? tr < 5 : tr > 4)) return false;
            if (dx !== 2 || dy !== 2) return false;
            return !mt[(r + tr) / 2][(c + tc) / 2];

        case '傌':
        case '馬':
            if (!((dx === 1 && dy === 2) || (dx === 2 && dy === 1))) return false;
            return !mt[r + (dy === 2 ? (tr > r ? 1 : -1) : 0)][c + (dx === 2 ? (tc > c ? 1 : -1) : 0)];

        case '俥':
        case '車':
            if (dx !== 0 && dy !== 0) return false;
            return countV(c, r, tc, tr, mt) === 0;

        case '炮':
        case '砲': {
            if (dx !== 0 && dy !== 0) return false;
            const v = countV(c, r, tc, tr, mt);
            return mt[tr][tc] ? v === 1 : v === 0;
        }

        case '兵':
        case '卒': {
            const daQuaSong = (side === 'do') ? (r <= 4) : (r >= 5);
            const diTien = (side === 'do') ? (dx === 0 && dy === 1 && tr < r) : (dx === 0 && dy === 1 && tr > r);
            const diNgang = (dx === 1 && dy === 0);
            if (!daQuaSong) return diTien;
            return diTien || diNgang;
        }
    }

    return false;
}

function countSquareControls(mt, side, tc, tr, isCoUp) {
    let count = 0;

    for (let r = 0; r < 10; r++) {
        for (let c = 0; c < 9; c++) {
            const p = mt[r][c];
            if (!p || p.side !== side) continue;
            if (canPieceControlSquare(p.type, p.side, c, r, tc, tr, mt, isCoUp)) count++;
        }
    }

    return count;
}

function locateKing(mt, side) {
    for (let r = 0; r < 10; r++) {
        for (let c = 0; c < 9; c++) {
            const p = mt[r][c];
            if (p && p.side === side && isKingPiece(p.type)) return { c, r };
        }
    }
    return null;
}

function doTempMove(mt, from, to) {
    const moving = mt[from.r][from.c];
    const target = mt[to.r][to.c];
    const wasUp = moving ? !!moving.isUp : false;

    mt[to.r][to.c] = moving;
    mt[from.r][from.c] = null;

    // Cờ úp: quân vừa đi phải lật ra ngay. Tính thử cũng phải lật.
    if (moving && moving.isUp) moving.isUp = false;

    return { moving, target, wasUp };
}

function undoTempMove(mt, from, to, state) {
    if (state.moving) state.moving.isUp = state.wasUp;
    mt[from.r][from.c] = state.moving;
    mt[to.r][to.c] = state.target;
}

function findEnemyHeadCannon(mt, side) {
    const enemy = otherSide(side);
    const myKing = locateKing(mt, side);
    if (!myKing) return null;

    let best = null;

    for (let r = 0; r < 10; r++) {
        const p = mt[r][myKing.c];
        if (!p || p.side !== enemy || !isCannonPiece(p.type)) continue;

        const screens = countV(myKing.c, myKing.r, myKing.c, r, mt);

        // 1 ngòi là đang chiếu, 2 ngòi là pháo đầu đang ép trung lộ, 0 ngòi là đã phá thủng lộ.
        if (screens <= 2) {
            const pressure = (screens === 1 ? 3 : screens === 0 ? 2 : 1);
            if (!best || pressure > best.pressure) {
                best = { c: myKing.c, r, screens, pressure };
            }
        }
    }

    return best;
}

function evaluateKingSafety(mt, side, isCoUp) {
    const king = locateKing(mt, side);
    if (!king) return -50000;

    const enemy = otherSide(side);
    let score = 0;

    if (laBiChieuWorker(side, mt, isCoUp)) score -= 18000;

    const frontDir = side === 'do' ? -1 : 1;
    const frontR = king.r + frontDir;

    if (frontR >= 0 && frontR <= 9) {
        const shield = mt[frontR][king.c];
        if (shield && shield.side === side) score += 420;
        else score -= 520;
    }

    // Giữ Sĩ/Tượng/Mã gần cung để chống Pháo đầu.
    const palaceRows = side === 'do' ? [7, 8, 9] : [0, 1, 2];

    for (const r of palaceRows) {
        for (let c = 3; c <= 5; c++) {
            const p = mt[r][c];
            if (!p || p.side !== side) continue;
            if (isGuardPiece(p.type)) score += 170;
            if (isHorsePiece(p.type)) score += 130;
            if (isCannonPiece(p.type) || isRookPiece(p.type)) score += 70;
        }
    }

    const headCannon = findEnemyHeadCannon(mt, side);
    if (headCannon) score -= 900 * headCannon.pressure;

    // Trung lộ bị đối thủ kiểm soát quá nhiều thì trừ nặng.
    for (const rr of [king.r, frontR]) {
        if (rr < 0 || rr > 9) continue;

        const enemyCtrl = countSquareControls(mt, enemy, king.c, rr, isCoUp);
        const ownCtrl = countSquareControls(mt, side, king.c, rr, isCoUp);

        if (enemyCtrl > ownCtrl) score -= (enemyCtrl - ownCtrl) * 260;
        if (ownCtrl > 0) score += Math.min(ownCtrl, 3) * 120;
    }

    return score;
}

function scoreThreatsCreatedByPiece(mt, side, c, r, isCoUp) {
    const p = mt[r][c];
    if (!p) return 0;

    const enemy = otherSide(side);
    let score = 0;

    for (let tr = 0; tr < 10; tr++) {
        for (let tc = 0; tc < 9; tc++) {
            const target = mt[tr][tc];
            if (!target || target.side !== enemy) continue;

            if (!canPieceControlSquare(p.type, p.side, c, r, tc, tr, mt, isCoUp)) continue;

            const targetVal = getPieceBaseValue(target.type);
            const myVal = getPieceBaseValue(p.type);
            const defenders = countSquareControls(mt, enemy, tc, tr, isCoUp);
            const attackers = countSquareControls(mt, side, tc, tr, isCoUp);

            if (isKingPiece(target.type)) score += 9000;
            else score += Math.min(2600, targetVal * 2);

            // Gài thế: quân địch bị mình đánh nhiều hơn nó đỡ.
            if (attackers > defenders) {
                score += Math.min(2200, targetVal + (attackers - defenders) * 240);
            }

            if (myVal < targetVal) score += 260;
        }
    }

    return score;
}

function scoreStrategicMove(mt, move, side, isCoUp, depth) {
    const enemy = otherSide(side);
    const movingPiece = mt[move.from.r][move.from.c];
    if (!movingPiece) return -999999;

    const tempState = doTempMove(mt, move.from, move.to);

    let score = 0;
    const movedVal = getPieceBaseValue(movingPiece.type);

    if (laBiChieuWorker(side, mt, isCoUp) || haiTuongDoiMat(mt)) {
        score = -999999;
    } else {
        const givesCheck = laBiChieuWorker(enemy, mt, isCoUp);
        if (givesCheck) score += 12000 + depth * 100;

        const defenders = countSquareControls(mt, side, move.to.c, move.to.r, isCoUp);
        const attackers = countSquareControls(mt, enemy, move.to.c, move.to.r, isCoUp);

        if (defenders > 0) score += 260 + Math.min(defenders, 3) * 120;
        if (attackers > defenders) score -= Math.min(6500, movedVal * (attackers - defenders + 1));
        if (attackers === 0) score += 120;

        // Đừng ham lôi Xe/Pháo/Mã ra ăn nếu sau đó treo không ai đỡ.
        if ((isRookPiece(movingPiece.type) || isCannonPiece(movingPiece.type) || isHorsePiece(movingPiece.type)) && attackers > defenders) {
            score -= 1400;
        }

        score += scoreThreatsCreatedByPiece(mt, side, move.to.c, move.to.r, isCoUp);

        const myKing = locateKing(mt, side);
        if (myKing) {
            const distHome = Math.abs(move.to.c - myKing.c) + Math.abs(move.to.r - myKing.r);

            if (distHome <= 3 && (isHorsePiece(movingPiece.type) || isGuardPiece(movingPiece.type))) {
                score += 550;
            }

            if (move.to.c === myKing.c && distHome <= 3) {
                score += 260;
            }
        }

        const headCannon = findEnemyHeadCannon(mt, side);
        if (!headCannon) {
            score += 900;
        } else if (headCannon.pressure <= 1) {
            score += 350;
        }
    }

    undoTempMove(mt, move.from, move.to, tempState);
    return score;
}

function makePlanMove(mt, side, from, to, isCoUp) {
    const p = mt[from.r]?.[from.c];
    if (!p || p.side !== side) return null;

    if (!checkLuatWorker(p.type, side, from.c, from.r, to.c, to.r, mt, isCoUp)) {
        return null;
    }

    const tempState = doTempMove(mt, from, to);
    const safe = !laBiChieuWorker(side, mt, isCoUp) && !haiTuongDoiMat(mt);
    undoTempMove(mt, from, to, tempState);

    if (!safe) return null;
    return { from, to, pieceObj: p };
}

function chooseBestPlanMove(mt, side, isCoUp, plans) {
    let best = null;
    let bestScore = -Infinity;

    for (const plan of plans) {
        const m = makePlanMove(mt, side, plan.f, plan.t, isCoUp);
        if (!m) continue;

        const score = scoreStrategicMove(mt, m, side, isCoUp, 3) + (plan.bonus || 0);

        if (score > bestScore) {
            bestScore = score;
            best = m;
        }
    }

    return best;
}

function chooseAntiHeadCannonMove(mt, side, isCoUp) {
    if (isCoUp) return null;

    const pressure = findEnemyHeadCannon(mt, side);
    if (!pressure) return null;

    const plans = side === 'den'
        ? [
            {f:{c:1, r:0}, t:{c:2, r:2}, bonus: 2400},
            {f:{c:7, r:0}, t:{c:6, r:2}, bonus: 2350},
            {f:{c:2, r:0}, t:{c:4, r:2}, bonus: 2100},
            {f:{c:6, r:0}, t:{c:4, r:2}, bonus: 2050},
            {f:{c:3, r:0}, t:{c:4, r:1}, bonus: 1400},
            {f:{c:5, r:0}, t:{c:4, r:1}, bonus: 1400}
          ]
        : [
            {f:{c:1, r:9}, t:{c:2, r:7}, bonus: 2400},
            {f:{c:7, r:9}, t:{c:6, r:7}, bonus: 2350},
            {f:{c:2, r:9}, t:{c:4, r:7}, bonus: 2100},
            {f:{c:6, r:9}, t:{c:4, r:7}, bonus: 2050},
            {f:{c:3, r:9}, t:{c:4, r:8}, bonus: 1400},
            {f:{c:5, r:9}, t:{c:4, r:8}, bonus: 1400}
          ];

    return chooseBestPlanMove(mt, side, isCoUp, plans);
}

function evaluateBoard(mt, botSide) {
    let botMaterial = 0;
    let oppMaterial = 0;
    let tBot = null, tOpp = null;
    const oppSide = otherSide(botSide);
    
    let isCoUp = false;

    for (let r = 0; r < 10; r++) {
        for (let c = 0; c < 9; c++) {
            if (mt[r][c] && mt[r][c].isUp) isCoUp = true;
        }
    }

    for (let r = 0; r < 10; r++) {
        for (let c = 0; c < 9; c++) {
            const p = mt[r][c];
            if (!p) continue;

            const val = getPieceBaseValue(p.type);

            if (!p.isUp) {
                if (p.side === botSide) botMaterial += val;
                else oppMaterial += val;
            }

            if (isKingPiece(p.type)) {
                if (p.side === botSide) tBot = { r, c };
                else tOpp = { r, c };
            }
        }
    }

    const isWinning = (botMaterial - oppMaterial) > 400;
    const isLosing = (oppMaterial - botMaterial) > 400;
    let score = 0;

    // Sống còn: bị chiếu phạt cực nặng, chiếu được đối phương thì thưởng.
    score += evaluateKingSafety(mt, botSide, isCoUp);
    score -= evaluateKingSafety(mt, oppSide, isCoUp);

    if (laBiChieuWorker(botSide, mt, isCoUp)) score -= 22000;
    if (laBiChieuWorker(oppSide, mt, isCoUp)) score += 12000;

    for (let r = 0; r < 10; r++) {
        for (let c = 0; c < 9; c++) {
            const p = mt[r][c];
            if (!p) continue;

            let val = getPieceBaseValue(p.type);
            const isMyPiece = (p.side === botSide);
            const pieceSide = p.side;
            const enemySide = otherSide(pieceSide);

            if (isCoUp && p.isUp) {
                // Cờ úp: bỏ kiểu nhìn xuyên rồi cứ lật quân to ghi điểm.
                const effective = getEffectiveMoveType(p.type, p.side, c, r, mt, isCoUp);

                if (isRookPiece(effective) || isCannonPiece(effective) || isHorsePiece(effective)) val += 180;
                else if (isGuardPiece(effective)) val += 120;
                else val += 90;

                const ownKing = locateKing(mt, p.side);
                if (ownKing) {
                    const distHome = Math.abs(c - ownKing.c) + Math.abs(r - ownKing.r);
                    if (distHome <= 3) val += 100;
                }
            } else {
                if (p.type === '兵' || p.type === '卒') {
                    if (p.side === 'do' && r <= 4) {
                        val += 50 + (4 - r) * 30;
                        if (c >= 3 && c <= 5) val += 50;
                    }

                    if (p.side === 'den' && r >= 5) {
                        val += 50 + (r - 5) * 30;
                        if (c >= 3 && c <= 5) val += 50;
                    }
                }

                if (isHorsePiece(p.type)) {
                    if (c === 0 || c === 8) val -= 80;
                    if (c >= 2 && c <= 6 && r >= 2 && r <= 7) val += 110;

                    if (p.side === 'do' && (r === 7 || r === 8) && (c === 2 || c === 6)) val += 230;
                    if (p.side === 'den' && (r === 1 || r === 2) && (c === 2 || c === 6)) val += 230;
                }

                if (isCannonPiece(p.type)) {
                    if (c === 4) val += 100;
                    if (c === 3 || c === 5) val += 50;
                }

                if (isRookPiece(p.type)) {
                    if (c === 3 || c === 4 || c === 5) val += 90;

                    if (p.side === 'do' && r === 9 && (c === 0 || c === 8)) val -= 160;
                    if (p.side === 'den' && r === 0 && (c === 0 || c === 8)) val -= 160;

                    if (p.side === 'do' && r === 6) val += 60;
                    if (p.side === 'den' && r === 3) val += 60;
                }
            }

            if (isKingPiece(p.type)) {
                if (p.side === 'do' && r !== 9) val -= 180;
                if (p.side === 'den' && r !== 0) val -= 180;
            }

            // Quân được bảo vệ thì thưởng, quân treo thì phạt nặng.
            if (!isKingPiece(p.type)) {
                const attackers = countSquareControls(mt, enemySide, c, r, isCoUp);
                const defenders = countSquareControls(mt, pieceSide, c, r, isCoUp);
                const base = Math.min(2600, getPieceBaseValue(p.type));

                if (isMyPiece) {
                    if (defenders > 0) val += 55 + Math.min(defenders, 3) * 45;
                    if (attackers > defenders) val -= Math.min(4200, base * (attackers - defenders + 1));
                    if (attackers > 0 && defenders === 0) val -= 850;
                } else {
                    if (attackers > defenders) val -= Math.min(3500, base * (attackers - defenders + 1));
                    if (attackers > 0 && defenders === 0) val -= 650;
                }
            }

            // Đòn ép Tướng.
            if (isMyPiece && tOpp && !p.isUp && (isRookPiece(p.type) || isCannonPiece(p.type) || isHorsePiece(p.type))) {
                const distToEnemyKing = Math.abs(r - tOpp.r) + Math.abs(c - tOpp.c);
                val += Math.max(0, (14 - distToEnemyKing) * 16);
            }

            // Đang thua thì kéo quân về giữ Tướng.
            if (isLosing && isMyPiece && !p.isUp && tBot) {
                const distToHomeKing = Math.abs(r - tBot.r) + Math.abs(c - tBot.c);

                if (distToHomeKing <= 3) val += 150;
                else if (isRookPiece(p.type) || isCannonPiece(p.type)) val -= 80;
            }

            // Đang lời thì được đổi quân, nhưng không đổi kiểu tự treo.
            if (isWinning && isMyPiece && !isKingPiece(p.type) && p.type !== '兵' && p.type !== '卒') {
                const attackers = countSquareControls(mt, oppSide, c, r, isCoUp);
                const defenders = countSquareControls(mt, botSide, c, r, isCoUp);

                if (defenders >= attackers) val = Math.floor(val * 0.97);
            }

            if (isMyPiece) score += val;
            else score -= val;
        }
    }

    return score;
}

function generateAllMoves(mt, side, isCoUp, depth, onlyCaptures = false) {
    let moves = [];

    for (let r = 0; r < 10; r++) {
        for (let c = 0; c < 9; c++) {
            const p = mt[r][c];
            if (!p || p.side !== side) continue;

            for (let tr = 0; tr < 10; tr++) {
                for (let tc = 0; tc < 9; tc++) {
                    if (onlyCaptures && !mt[tr][tc]) continue;

                    if (!checkLuatWorker(p.type, side, c, r, tc, tr, mt, isCoUp)) continue;

                    const targetPiece = mt[tr][tc];
                    const from = { c, r };
                    const to = { c: tc, r: tr };

                    const tempState = doTempMove(mt, from, to);
                    const legal = !laBiChieuWorker(side, mt, isCoUp) && !haiTuongDoiMat(mt);
                    undoTempMove(mt, from, to, tempState);

                    if (legal) {
                        moves.push({
                            from,
                            to,
                            pieceObj: p,
                            captured: targetPiece
                        });
                    }
                }
            }
        }
    }

    const kMoves = killerMoves[depth] || [];

    moves.forEach(m => {
        let s = 0;

        if (m.captured) {
            // Ăn quân vẫn tốt, nhưng scoreStrategicMove sẽ phạt nếu tự treo.
            s += (getPieceBaseValue(m.captured.type) * 10) - getPieceBaseValue(m.pieceObj.type) + 100000;
        }

        if (kMoves.some(k => k.from.r === m.from.r && k.from.c === m.from.c && k.to.r === m.to.r && k.to.c === m.to.c)) {
            s += 50000;
        }

        if (!onlyCaptures) {
            s += scoreStrategicMove(mt, m, side, isCoUp, depth || 0);
        }

        m.score = s;
    });

    moves.sort((a, b) => b.score - a.score);
    return moves;
}

function quiesce(mt, alpha, beta, isMaximizing, botSide, isCoUp, qDepth) {
    if (botTimeUp()) return evaluateBoard(mt, botSide);

    let stand_pat = evaluateBoard(mt, botSide);
    if (qDepth > 5) return stand_pat; 

    if (isMaximizing) {
        if (stand_pat >= beta) return beta;
        if (alpha < stand_pat) alpha = stand_pat;
    } else {
        if (stand_pat <= alpha) return alpha;
        if (beta > stand_pat) beta = stand_pat;
    }

    let currentSide = isMaximizing ? botSide : otherSide(botSide);
    let moves = generateAllMoves(mt, currentSide, isCoUp, 0, true); 

    if (isMaximizing) {
        for (let move of moves) {
            const tempState = doTempMove(mt, move.from, move.to);
            let score = quiesce(mt, alpha, beta, false, botSide, isCoUp, qDepth + 1);
            undoTempMove(mt, move.from, move.to, tempState);

            if (score >= beta) return beta;
            if (score > alpha) alpha = score;
        }

        return alpha;
    } else {
        for (let move of moves) {
            const tempState = doTempMove(mt, move.from, move.to);
            let score = quiesce(mt, alpha, beta, true, botSide, isCoUp, qDepth + 1);
            undoTempMove(mt, move.from, move.to, tempState);

            if (score <= alpha) return alpha;
            if (score < beta) beta = score;
        }

        return beta;
    }
}

function minimax(mt, depth, alpha, beta, isMaximizing, botSide, isCoUp, isNullMove = false) {
    if (botTimeUp()) return evaluateBoard(mt, botSide);
    if (depth <= 0) return quiesce(mt, alpha, beta, isMaximizing, botSide, isCoUp, 0);

    const originalAlpha = alpha;
    const originalBeta = beta;
    const currentSideForKey = isMaximizing ? botSide : otherSide(botSide);
    const ttKey = getBoardHash(mt) + "|" + currentSideForKey + "|" + depth + "|" + (isMaximizing ? "max" : "min") + "|" + (isCoUp ? "up" : "normal");

    const cached = transTable.get(ttKey);
    if (cached && cached.depth >= depth) {
        if (cached.flag === "EXACT") return cached.value;
        if (cached.flag === "LOWER") alpha = Math.max(alpha, cached.value);
        if (cached.flag === "UPPER") beta = Math.min(beta, cached.value);
        if (alpha >= beta) return cached.value;
    }

    let currentSide = isMaximizing ? botSide : otherSide(botSide);
    
    // Null move pruning.
    let R = 2;
    if (depth >= 3 && !isNullMove && !laBiChieuWorker(currentSide, mt, isCoUp)) {
        let nmpScore = minimax(mt, depth - 1 - R, alpha, beta, !isMaximizing, botSide, isCoUp, true);

        if (isMaximizing && nmpScore >= beta) return beta;
        if (!isMaximizing && nmpScore <= alpha) return alpha;
    }

    let moves = generateAllMoves(mt, currentSide, isCoUp, depth, false);
    if (moves.length === 0) return isMaximizing ? -99999 + depth : 99999 - depth;

    if (isMaximizing) {
        let maxEval = -Infinity;

        for (let move of moves) {
            const tempState = doTempMove(mt, move.from, move.to);
            let ev = minimax(mt, depth - 1, alpha, beta, false, botSide, isCoUp, false);
            undoTempMove(mt, move.from, move.to, tempState);

            if (ev > maxEval) maxEval = ev;
            if (ev > alpha) alpha = ev;

            if (beta <= alpha) {
                if (!tempState.target) {
                    if (!killerMoves[depth]) killerMoves[depth] = [];
                    killerMoves[depth].unshift(move);
                    if (killerMoves[depth].length > 2) killerMoves[depth].pop();
                }
                break; 
            }
        }

        let flag = "EXACT";
        if (maxEval <= originalAlpha) flag = "UPPER";
        else if (maxEval >= originalBeta) flag = "LOWER";

        transTable.set(ttKey, { depth, value: maxEval, flag });
        trimBigMap(transTable, MAX_TRANS_TABLE);

        return maxEval;
    } else {
        let minEval = Infinity;

        for (let move of moves) {
            const tempState = doTempMove(mt, move.from, move.to);
            let ev = minimax(mt, depth - 1, alpha, beta, true, botSide, isCoUp, false);
            undoTempMove(mt, move.from, move.to, tempState);

            if (ev < minEval) minEval = ev;
            if (ev < beta) beta = ev;

            if (beta <= alpha) {
                if (!tempState.target) {
                    if (!killerMoves[depth]) killerMoves[depth] = [];
                    killerMoves[depth].unshift(move);
                    if (killerMoves[depth].length > 2) killerMoves[depth].pop();
                }
                break;
            }
        }

        let flag = "EXACT";
        if (minEval <= originalAlpha) flag = "UPPER";
        else if (minEval >= originalBeta) flag = "LOWER";

        transTable.set(ttKey, { depth, value: minEval, flag });
        trimBigMap(transTable, MAX_TRANS_TABLE);

        return minEval;
    }
}

function calculateLocalMove(boardArray, botSide, isCoUp, forceDepth = null, botLevel = 6, maxMsOverride = null) {
    killerMoves = Array(100).fill(null).map(() => []);

    let mt = buildMatrix(boardArray);
    let moves = generateAllMoves(mt, botSide, isCoUp, 10, false);
    if (moves.length === 0) return null;

    const pieceCount = boardArray.length;
    let cfg = getBotLevelConfig(botLevel, pieceCount, isCoUp);

    if (forceDepth) {
        cfg.maxDepth = forceDepth;
        cfg.rootLimit = Math.min(cfg.rootLimit, 18);
        cfg.blunder = 0;
    }

    if (maxMsOverride) {
        cfg.maxMs = maxMsOverride;
    }

    moves = moves.slice(0, cfg.rootLimit);

    BOT_SEARCH_DEADLINE = Date.now() + cfg.maxMs;
    BOT_SEARCH_TIMED_OUT = false;
    BOT_NODE_COUNT = 0;

    let bestMove = moves[0];
    let bestScore = -Infinity;
    let bestMoves = [moves[0]];
    let completedDepth = 0;

    for (let depth = 1; depth <= cfg.maxDepth; depth++) {
        if (botTimeUp()) break;

        let depthBestScore = -Infinity;
        let depthBestMoves = [];

        for (let move of moves) {
            if (botTimeUp()) break;

            const tempState = doTempMove(mt, move.from, move.to);

            let currentHash = getBoardHash(mt);
            let oppSide = otherSide(botSide);
            let isChecking = laBiChieuWorker(oppSide, mt, isCoUp);
            let repeatCount = positionHistory.filter(h => h.hash === currentHash && h.isCheck).length;

            let score = 0;

            if (isChecking && repeatCount >= 3) {
                score = -100000;
            } else {
                score = minimax(mt, depth - 1, -Infinity, Infinity, false, botSide, isCoUp, false);
            }

            undoTempMove(mt, move.from, move.to, tempState);

            if (BOT_SEARCH_TIMED_OUT) break;

            move.__lastScore = score;

            if (score > depthBestScore) {
                depthBestScore = score;
                depthBestMoves = [move];
            } else if (score === depthBestScore) {
                depthBestMoves.push(move);
            }
        }

        if (!BOT_SEARCH_TIMED_OUT && depthBestMoves.length) {
            completedDepth = depth;
            bestScore = depthBestScore;
            bestMoves = depthBestMoves;
            bestMove = bestMoves[Math.floor(Math.random() * bestMoves.length)];

            moves.sort((a, b) => {
                if (a === bestMove) return -1;
                if (b === bestMove) return 1;
                return (b.__lastScore || b.score || 0) - (a.__lastScore || a.score || 0);
            });
        } else {
            break;
        }
    }

    BOT_SEARCH_DEADLINE = 0;

    if (!forceDepth && cfg.blunder > 0 && Math.random() < cfg.blunder && moves.length > 1) {
        const poolSize = Math.min(moves.length, Math.max(2, 7 - cfg.level));
        const pool = moves.slice(1, poolSize);

        if (pool.length) {
            bestMove = pool[Math.floor(Math.random() * pool.length)];
        }
    }

    bestMove.__botLevel = cfg.level;
    bestMove.__searchDepth = completedDepth;
    bestMove.__searchMs = cfg.maxMs;
    bestMove.__score = bestScore;

    return bestMove;
}

function applyMoveToBoardArray(boardArray, move, isCoUp, side) {
    const moving = boardArray.find(p => p.c === move.from.c && p.r === move.from.r);
    if (!moving) return boardArray;

    let movedPiece = {
        ...moving,
        c: move.to.c,
        r: move.to.r
    };

    if (isCoUp && movedPiece.isUp) {
        movedPiece.isUp = false;

        const names = {
            '車':'xe','馬':'ma','象':'tuong','士':'si','將':'tuong_soai','砲':'phao','卒':'tot',
            '俥':'xe','傌':'ma','相':'tuong','仕':'si','帥':'tuong_soai','炮':'phao','兵':'tot'
        };

        if (names[movedPiece.type]) movedPiece.src = `images/${side}_${names[movedPiece.type]}.png`;
    }

    const newBoardArray = boardArray.filter(p => {
        if (p.c === move.from.c && p.r === move.from.r) return false;
        if (p.c === move.to.c && p.r === move.to.r) return false;
        return true;
    });

    newBoardArray.push(movedPiece);
    return newBoardArray;
}

function buildFinalMovePayload(boardArray, bestMoveObject, botSide, isCoUp) {
    let movedPiece = {
        ...bestMoveObject.pieceObj,
        c: bestMoveObject.to.c,
        r: bestMoveObject.to.r
    };

    if (isCoUp && movedPiece.isUp) {
        movedPiece.isUp = false;

        const names = {
            '車':'xe','馬':'ma','象':'tuong','士':'si','將':'tuong_soai','砲':'phao','卒':'tot',
            '俥':'xe','傌':'ma','相':'tuong','仕':'si','帥':'tuong_soai','炮':'phao','兵':'tot'
        };

        if (names[movedPiece.type]) movedPiece.src = `images/${botSide}_${names[movedPiece.type]}.png`;
    }

    let newBoardArray = boardArray.filter(p => {
        if (p.c === bestMoveObject.from.c && p.r === bestMoveObject.from.r) return false;
        if (p.c === bestMoveObject.to.c && p.r === bestMoveObject.to.r) return false;
        return true;
    });

    newBoardArray.push(movedPiece);

    let tempMt = buildMatrix(newBoardArray);
    let oppSide = otherSide(botSide);

    return {
        from: bestMoveObject.from,
        to: bestMoveObject.to,
        newBoard: newBoardArray,
        hash: getBoardHash(tempMt),
        isCheck: laBiChieuWorker(oppSide, tempMt, isCoUp)
    };
}

function boardToFEN(mt, botSide) {
    const mapRed = {
        '俥':'R', '車':'R',
        '傌':'N', '馬':'N',
        '相':'B', '象':'B',
        '仕':'A', '士':'A',
        '帥':'K', '將':'K',
        '炮':'C', '砲':'C',
        '兵':'P', '卒':'P'
    };

    const mapBlack = {
        '俥':'r', '車':'r',
        '傌':'n', '馬':'n',
        '相':'b', '象':'b',
        '仕':'a', '士':'a',
        '帥':'k', '將':'k',
        '炮':'c', '砲':'c',
        '兵':'p', '卒':'p'
    };

    let fen = "";

    for (let r = 0; r < 10; r++) {
        let emptyCount = 0;

        for (let c = 0; c < 9; c++) {
            const p = mt[r][c];

            if (!p) {
                emptyCount++;
            } else {
                if (emptyCount > 0) {
                    fen += emptyCount;
                    emptyCount = 0;
                }

                fen += p.side === 'do' ? mapRed[p.type] : mapBlack[p.type];
            }
        }

        if (emptyCount > 0) fen += emptyCount;
        if (r < 9) fen += "/";
    }

    let turn = botSide === 'do' ? 'w' : 'b'; 
    return fen + " " + turn + " - - 0 1";
}

function parseUCIMove(uci) {
    const f1 = uci.charCodeAt(0) - 97; 
    const r1 = 9 - parseInt(uci.charAt(1)); 
    const f2 = uci.charCodeAt(2) - 97; 
    const r2 = 9 - parseInt(uci.charAt(3)); 

    return {
        from: {c: f1, r: r1},
        to: {c: f2, r: r2}
    };
}

async function fetchCloudMove(fen, timeoutMs = 450) {
    const url = `https://www.chessdb.cn/cdb.php?action=queryall&board=${encodeURIComponent(fen)}`;

    try {
        const controller = new AbortController();
        const timer = setTimeout(() => controller.abort(), timeoutMs);

        const res = await fetch(url, { signal: controller.signal });
        clearTimeout(timer);

        const text = await res.text();

        if (text.startsWith("move:")) {
            const bestMoveUCI = text.split(",")[0].split(":")[1];
            return parseUCIMove(bestMoveUCI);
        }
    } catch(e) {
        return null;
    }

    return null;
}
const VPS_ENGINE_URL = "http://171.244.63.167/bestmove";

async function fetchVpsMove(fen, level = 8, timeoutMs = 6500) {
    try {
        const controller = new AbortController();
        const timer = setTimeout(() => controller.abort(), timeoutMs);

        const res = await fetch(VPS_ENGINE_URL, {
            method: "POST",
            headers: {
                "Content-Type": "application/json"
            },
            body: JSON.stringify({
                fen,
                level
            }),
            signal: controller.signal
        });

        clearTimeout(timer);

        const data = await res.json();

        if (data && data.ok && data.bestmove) {
            return parseUCIMove(data.bestmove);
        }
    } catch (e) {
        return null;
    }

    return null;
}
self.onmessage = async function(e) {
    const data = e.data;

    if (data.action === "ponder") {
        if (ponderBusy) return;

        ponderBusy = true;

        try {
            const humanSide = otherSide(data.botSide);
            const mt = buildMatrix(data.boardState);

            const botLevel = clampBotLevel(data.botLevel || data.level || 6);
            const ponderCfg = getBotLevelConfig(botLevel, data.boardState.length, data.isCoUp);

            const humanMoves = generateAllMoves(mt, humanSide, data.isCoUp, 1, false)
                .slice(0, Math.min(4, ponderCfg.rootLimit));

            for (const humanMove of humanMoves) {
                const afterHumanBoard = applyMoveToBoardArray(
                    data.boardState,
                    humanMove,
                    data.isCoUp,
                    humanSide
                );

                const key = boardArrayHash(afterHumanBoard, data.botSide, data.isCoUp);

                if (ponderTable.has(key)) continue;

                const reply = calculateLocalMove(
                    afterHumanBoard,
                    data.botSide,
                    data.isCoUp,
                    Math.min(3, ponderCfg.maxDepth),
                    botLevel,
                    Math.min(260, ponderCfg.maxMs)
                );

                if (reply) {
                    const payload = buildFinalMovePayload(
                        afterHumanBoard,
                        reply,
                        data.botSide,
                        data.isCoUp
                    );

                    ponderTable.set(key, payload);
                    trimBigMap(ponderTable, MAX_PONDER_TABLE);
                }
            }
        } catch (err) {
            // Ponder lỗi thì tới lượt thật vẫn tính lại.
        } finally {
            ponderBusy = false;
        }

        return;
    }

    if (data.action === "think") {
        if (data.recentHistory) positionHistory = data.recentHistory;

        const botLevel = clampBotLevel(data.botLevel || data.level || 6);
        const botCfg = getBotLevelConfig(botLevel, data.boardState.length, data.isCoUp);

        const readyKey = boardArrayHash(data.boardState, data.botSide, data.isCoUp);
        const readyMove = ponderTable.get(readyKey);

        if (readyMove) {
            ponderTable.delete(readyKey);
            postMessage({ action: "done", move: readyMove, fromPonder: true });
            return;
        }

        let bestMoveObject = null;
        let mt = buildMatrix(data.boardState);

        // ==========================================
        // KHAI CUỘC + CHỐNG PHÁO ĐẦU
        // ==========================================
        let botMoveCount = (data.recentHistory || []).filter(h => h && h.side === data.botSide).length;
        if (!botMoveCount) botMoveCount = Math.floor((data.recentHistory || []).length / 2);
// Level cao + cờ thường: gọi não VPS Pikafish trước.
// Cờ úp không gọi VPS vì Pikafish không biết luật quân úp.
if (!bestMoveObject && !data.isCoUp && botCfg.cloud) {
    const fen = boardToFEN(mt, data.botSide);
    const vpsMove = await fetchVpsMove(fen, botLevel, 6500);

    if (vpsMove) {
        bestMoveObject = makePlanMove(mt, data.botSide, vpsMove.from, vpsMove.to, false);
    }
}
        // Nếu đối thủ vào Pháo đầu, ưu tiên Mã/Tượng/Sĩ đỡ trước.
        if (!bestMoveObject && botMoveCount < 8) {
    bestMoveObject = chooseAntiHeadCannonMove(mt, data.botSide, data.isCoUp);
}

        // Nếu không bị Pháo đầu ép thì đi khai cuộc lành mạnh.
        if (!bestMoveObject && botMoveCount < 4 && !data.isCoUp) {
            const OPENING_MOVES_DEN = [
                {f:{c:1, r:0}, t:{c:2, r:2}, bonus: 800},
                {f:{c:7, r:0}, t:{c:6, r:2}, bonus: 780},
                {f:{c:1, r:2}, t:{c:4, r:2}, bonus: 520},
                {f:{c:7, r:2}, t:{c:4, r:2}, bonus: 500},
                {f:{c:8, r:0}, t:{c:8, r:1}, bonus: 260},
                {f:{c:0, r:0}, t:{c:0, r:1}, bonus: 250}
            ];

            const OPENING_MOVES_DO = [
                {f:{c:1, r:9}, t:{c:2, r:7}, bonus: 800},
                {f:{c:7, r:9}, t:{c:6, r:7}, bonus: 780},
                {f:{c:1, r:7}, t:{c:4, r:7}, bonus: 520},
                {f:{c:7, r:7}, t:{c:4, r:7}, bonus: 500},
                {f:{c:8, r:9}, t:{c:8, r:8}, bonus: 260},
                {f:{c:0, r:9}, t:{c:0, r:8}, bonus: 250}
            ];

            bestMoveObject = chooseBestPlanMove(
                mt,
                data.botSide,
                data.isCoUp,
                data.botSide === 'den' ? OPENING_MOVES_DEN : OPENING_MOVES_DO
            );
        }

        // Cờ thường level cao thì có thể gọi ChessDB.
        if (!bestMoveObject && !data.isCoUp && botCfg.cloud) {
            let fen = boardToFEN(mt, data.botSide);
            const cloudMove = await fetchCloudMove(fen, botCfg.cloudMs);

            if (cloudMove) {
                bestMoveObject = makePlanMove(mt, data.botSide, cloudMove.from, cloudMove.to, false);
            }
        }

        if (!bestMoveObject) {
            bestMoveObject = calculateLocalMove(data.boardState, data.botSide, data.isCoUp, null, botLevel, botCfg.maxMs);
        }

        if (bestMoveObject) {
            let movedPiece = {
                ...bestMoveObject.pieceObj,
                c: bestMoveObject.to.c,
                r: bestMoveObject.to.r
            };

            if (data.isCoUp && movedPiece.isUp) {
                movedPiece.isUp = false;

                const names = {
                    '車':'xe','馬':'ma','象':'tuong','士':'si','將':'tuong_soai','砲':'phao','卒':'tot',
                    '俥':'xe','傌':'ma','相':'tuong','仕':'si','帥':'tuong_soai','炮':'phao','兵':'tot'
                };

                if (names[movedPiece.type]) movedPiece.src = `images/${data.botSide}_${names[movedPiece.type]}.png`;
            }

            let newBoardArray = data.boardState.filter(p => {
                if (p.c === bestMoveObject.from.c && p.r === bestMoveObject.from.r) return false;
                if (p.c === bestMoveObject.to.c && p.r === bestMoveObject.to.r) return false;
                return true;
            });

            newBoardArray.push(movedPiece);

            let tempMt = buildMatrix(newBoardArray);
            let oppSide = otherSide(data.botSide);
            
            const finalMoveData = {
                from: bestMoveObject.from,
                to: bestMoveObject.to,
                newBoard: newBoardArray,
                hash: getBoardHash(tempMt),
                isCheck: laBiChieuWorker(oppSide, tempMt, data.isCoUp)
            };
            
            postMessage({ action: "done", move: finalMoveData });
        } else {
            postMessage({ action: "done", move: null }); 
        }
    }
};