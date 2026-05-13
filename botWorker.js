// ==========================================
// FILE: botWorker.js (BẢN SIÊU CẤP MA GIÁO V5)
// CHẾ ĐỘ: HACK NHÌN XUYÊN THẤU + DIỄN VIÊN ĐIỆN ẢNH
// ==========================================

let positionHistory = []; 
let killerMoves = Array(100).fill(null).map(() => []); 
let currentMatchMoveCount = 0; // Biến đếm nước đi nội bộ của não

const PIECE_VALUES = {
    '帥': 1000000, '將': 1000000,
    '俥': 1200, '車': 1200,
    '炮': 550,  '砲': 550,
    '傌': 500,  '馬': 500,  
    '相': 250,  '象': 250,
    '仕': 250,  '士': 250,
    '兵': 120,  '卒': 120
};

function buildMatrix(boardArray) {
    let mt = Array(10).fill(null).map(() => Array(9).fill(null));
    boardArray.forEach(p => { mt[p.r][p.c] = p; });
    return mt;
}

function getBoardHash(mt) {
    let hash = "";
    for(let r=0; r<10; r++) {
        for(let c=0; c<9; c++) {
            if(mt[r][c]) hash += r + "" + c + mt[r][c].type + (mt[r][c].isUp ? "U" : "D");
        }
    }
    return hash;
}

function countV(c1, r1, c2, r2, mt) {
    let count = 0;
    if (c1 === c2) {
        for (let r = Math.min(r1, r2) + 1; r < Math.max(r1, r2); r++) { if (mt[r][c1]) count++; }
    } else {
        for (let c = Math.min(c1, c2) + 1; c < Math.max(c1, c2); c++) { if (mt[r1][c]) count++; }
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
        case '帥': case '將': return dx + dy === 1 && tc >= 3 && tc <= 5 && (side === 'do' ? tr >= 7 : tr <= 2);
        case '仕': case '士': 
            if (isCoUp && mt[r][c] && !mt[r][c].isUp) return dx === 1 && dy === 1;
            return dx === 1 && dy === 1 && tc >= 3 && tc <= 5 && (side === 'do' ? tr >= 7 : tr <= 2);
        case '相': case '象':
            if (dx !== 2 || dy !== 2) return false;
            if (isCoUp && mt[r][c] && !mt[r][c].isUp) return !mt[(r + tr) / 2][(c + tc) / 2];
            if (side === 'do' ? tr < 5 : tr > 4) return false;
            return !mt[(r + tr) / 2][(c + tc) / 2];
        case '傌': case '馬':
            if (!((dx === 1 && dy === 2) || (dx === 2 && dy === 1))) return false;
            return !mt[r + (dy === 2 ? (tr > r ? 1 : -1) : 0)][c + (dx === 2 ? (tc > c ? 1 : -1) : 0)];
        case '俥': case '車':
            if (dx !== 0 && dy !== 0) return false;
            return countV(c, r, tc, tr, mt) === 0;
        case '炮': case '砲':
            const v = countV(c, r, tc, tr, mt);
            return (mt[tr][tc] ? v === 1 : v === 0) && (dx === 0 || dy === 0);
        case '兵': case '卒':
            const daQuaSong = (side === 'do') ? (r <= 4) : (r >= 5);
            const diTien = (side === 'do') ? (dx === 0 && dy === 1 && tr < r) : (dx === 0 && dy === 1 && tr > r);
            const diNgang = (dx === 1 && dy === 0);
            return daQuaSong ? (diTien || diNgang) : diTien;
    }
    return false;
}

function laBiChieuWorker(side, mt, isCoUp) {
    let tPos = null;
    for (let r = 0; r < 10; r++) {
        for (let c = 0; c < 9; c++) {
            if (mt[r][c] && mt[r][c].side === side && (mt[r][c].type === '帥' || mt[r][c].type === '將')) {
                tPos = { c, r }; break;
            }
        }
        if (tPos) break;
    }
    if (!tPos) return false;
    for (let r = 0; r < 10; r++) {
        for (let c = 0; c < 9; c++) {
            if (mt[r][c] && mt[r][c].side !== side) {
                if (checkLuatWorker(mt[r][c].type, mt[r][c].side, c, r, tPos.c, tPos.r, mt, isCoUp)) return true;
            }
        }
    }
    return false;
}

function isSquareAttackedWorker(tc, tr, enemySide, mt, isCoUp) {
    for (let r = 0; r < 10; r++) {
        for (let c = 0; c < 9; c++) {
            const p = mt[r][c];
            if (p && p.side === enemySide) {
                if (checkLuatWorker(p.type, enemySide, c, r, tc, tr, mt, isCoUp)) return true;
            }
        }
    }
    return false;
}

function isSquareDefendedWorker(tc, tr, mySide, mt, isCoUp) {
    let temp = mt[tr][tc]; mt[tr][tc] = null;
    let isDefended = false;
    for (let r = 0; r < 10; r++) {
        for (let c = 0; c < 9; c++) {
            const p = mt[r][c];
            if (p && p.side === mySide) {
                if (checkLuatWorker(p.type, mySide, c, r, tc, tr, mt, isCoUp)) { isDefended = true; break; }
            }
        }
    }
    mt[tr][tc] = temp; return isDefended;
}

function evaluateBoard(mt, botSide, isCoUp) {
    let score = 0;
    let oppSide = botSide === 'do' ? 'den' : 'do';
    let tBot = null, tOpp = null;
    let myMaterial = 0, oppMaterial = 0;

    for(let r=0; r<10; r++) {
        for(let c=0; c<9; c++) {
            const p = mt[r][c];
            if(!p) continue;
            let val = PIECE_VALUES[p.type] || 0;
            if(p.side === botSide) {
                myMaterial += val;
                if(p.type === '帥' || p.type === '將') tBot = {r, c};
            } else {
                oppMaterial += val;
                if(p.type === '帥' || p.type === '將') tOpp = {r, c};
            }
        }
    }

    const isWinning = (myMaterial - oppMaterial) > 400;

    for(let r=0; r<10; r++) {
        for(let c=0; c<9; c++) {
            const p = mt[r][c];
            if(!p) continue;
            let isMyPiece = (p.side === botSide);
            let val = PIECE_VALUES[p.type] || 10;

            if (isCoUp && p.isUp) {
                // 🔥 HACK NHÌN XUYÊN THẤU + DIỄN KỊCH 🔥
                let actualType = p.type;
                if (currentMatchMoveCount < 6) {
                    // Giả ngu: Lật Sĩ/Tượng/Tốt trước
                    if (['兵','卒','仕','士','相','象'].includes(actualType)) val = 400;
                    else val = 50; 
                } else {
                    // Lộ diện: Lật Xe/Pháo/Mã
                    if (['俥','車','炮','砲','傌','馬'].includes(actualType)) val = 800;
                }
                // Nếu lật lên mà CHIẾU được hoặc ĂN XỊN thì lật luôn đéo diễn nữa
                if (isSquareAttackedWorker(c, r, isMyPiece ? oppSide : botSide, mt, false)) val += 1000;
            }

            if (!p.isUp && p.type !== '帥' && p.type !== '將') {
                let attacked = isSquareAttackedWorker(c, r, isMyPiece ? oppSide : botSide, mt, isCoUp);
                if (attacked) {
                    let defended = isSquareDefendedWorker(c, r, p.side, mt, isCoUp);
                    val -= isWinning ? (val * 0.8) : (val * 0.4);
                    if (defended) val += (val * 0.2);
                }
            }

            if (isMyPiece && tOpp && !p.isUp) {
                let dist = Math.abs(r - tOpp.r) + Math.abs(c - tOpp.c);
                val += Math.max(0, (14 - dist) * 15);
            }

            if (isMyPiece) score += val; else score -= val;
        }
    }
    return score;
}

function generateAllMoves(mt, side, isCoUp, depth, onlyCaptures = false) {
    let moves = [];
    for (let r = 0; r < 10; r++) {
        for (let c = 0; c < 9; c++) {
            let p = mt[r][c];
            if (p && p.side === side) {
                for (let tr = 0; tr < 10; tr++) {
                    for (let tc = 0; tc < 9; tc++) {
                        if (onlyCaptures && !mt[tr][tc]) continue; 
                        if (checkLuatWorker(p.type, side, c, r, tc, tr, mt, isCoUp)) {
                            let target = mt[tr][tc];
                            mt[tr][tc] = p; mt[r][c] = null;
                            if (!laBiChieuWorker(side, mt, isCoUp)) {
                                moves.push({from: {c, r}, to: {c: tc, r: tr}, pieceObj: p, captured: target});
                            }
                            mt[r][c] = p; mt[tr][tc] = target;
                        }
                    }
                }
            }
        }
    }
    return moves;
}

function minimax(mt, depth, alpha, beta, isMaximizing, botSide, isCoUp) {
    if (depth <= 0) return evaluateBoard(mt, botSide, isCoUp);
    let currentSide = isMaximizing ? botSide : (botSide === 'do' ? 'den' : 'do');
    let moves = generateAllMoves(mt, currentSide, isCoUp, depth);
    if (moves.length === 0) return isMaximizing ? -1000000 : 1000000;

    if (isMaximizing) {
        let maxEval = -Infinity;
        for (let m of moves) {
            let target = mt[m.to.r][m.to.c];
            let wasUp = m.pieceObj.isUp;
            mt[m.to.r][m.to.c] = m.pieceObj; mt[m.from.r][m.from.c] = null;
            if (isCoUp && wasUp) m.pieceObj.isUp = false; 
            let ev = minimax(mt, depth - 1, alpha, beta, false, botSide, isCoUp);
            if (isCoUp && wasUp) m.pieceObj.isUp = true;
            mt[m.from.r][m.from.c] = m.pieceObj; mt[m.to.r][m.to.c] = target;
            maxEval = Math.max(maxEval, ev); alpha = Math.max(alpha, ev);
            if (beta <= alpha) break;
        }
        return maxEval;
    } else {
        let minEval = Infinity;
        for (let m of moves) {
            let target = mt[m.to.r][m.to.c];
            let wasUp = m.pieceObj.isUp;
            mt[m.to.r][m.to.c] = m.pieceObj; mt[m.from.r][m.from.c] = null;
            if (isCoUp && wasUp) m.pieceObj.isUp = false;
            let ev = minimax(mt, depth - 1, alpha, beta, true, botSide, isCoUp);
            if (isCoUp && wasUp) m.pieceObj.isUp = true;
            mt[m.from.r][m.from.c] = m.pieceObj; mt[m.to.r][m.to.c] = target;
            minEval = Math.min(minEval, ev); beta = Math.min(beta, ev);
            if (beta <= alpha) break;
        }
        return minEval;
    }
}

// ==========================================
// CÁC HÀM CƯỚP BIỂN (CHESSDB)
// ==========================================
function boardToFEN(mt, botSide) {
    const mapRed = {'俥':'R', '車':'R', '傌':'N', '馬':'N', '相':'B', '象':'B', '仕':'A', '士':'A', '帥':'K', '將':'K', '炮':'C', '砲':'C', '兵':'P', '卒':'P'};
    const mapBlack = {'俥':'r', '車':'r', '傌':'n', '馬':'n', '相':'b', '象':'b', '仕':'a', '士':'a', '帥':'k', '將':'k', '炮':'c', '砲':'c', '兵':'p', '卒':'p'};
    let fen = "";
    for (let r = 0; r < 10; r++) {
        let empty = 0;
        for (let c = 0; c < 9; c++) {
            const p = mt[r][c];
            if (!p) empty++;
            else {
                if (empty > 0) { fen += empty; empty = 0; }
                fen += p.side === 'do' ? mapRed[p.type] : mapBlack[p.type];
            }
        }
        if (empty > 0) fen += empty;
        if (r < 9) fen += "/";
    }
    return fen + " " + (botSide === 'do' ? 'w' : 'b') + " - - 0 1";
}

async function fetchCloudMove(fen) {
    try {
        const res = await fetch(`https://www.chessdb.cn/cdb.php?action=queryall&board=${encodeURIComponent(fen)}`);
        const text = await res.text();
        if (text.startsWith("move:")) {
            const uci = text.split(",")[0].split(":")[1];
            return { from: {c: uci.charCodeAt(0)-97, r: 9-parseInt(uci[1])}, to: {c: uci.charCodeAt(2)-97, r: 9-parseInt(uci[3])} };
        }
    } catch(e) {} return null;
}

self.onmessage = async function(e) {
    const data = e.data;
    if (data.action === "think") {
        currentMatchMoveCount++;
        let mt = buildMatrix(data.boardState);
        let bestMove = null;

        if (!data.isCoUp) {
            bestMove = await fetchCloudMove(boardToFEN(mt, data.botSide));
        }

        if (!bestMove) {
            let moves = generateAllMoves(mt, data.botSide, data.isCoUp, 4);
            let bestScore = -Infinity;
            let depth = data.boardState.length <= 10 ? 6 : 4;

            for (let m of moves) {
                let target = mt[m.to.r][m.to.c];
                mt[m.to.r][m.to.c] = m.pieceObj; mt[m.from.r][m.from.c] = null;
                let score = minimax(mt, depth - 1, -Infinity, Infinity, false, data.botSide, data.isCoUp);
                mt[m.from.r][m.from.c] = m.pieceObj; mt[m.to.r][m.to.c] = target;
                if (score > bestScore) { bestScore = score; bestMove = m; }
            }
        }

        if (bestMove) {
            let moved = { ...bestMove.pieceObj, c: bestMove.to.c, r: bestMove.to.r };
            if (data.isCoUp && moved.isUp) {
                moved.isUp = false;
                const names = {'車':'xe','馬':'ma','象':'tuong','士':'si','將':'tuong_soai','砲':'phao','卒':'tot','俥':'xe','傌':'ma','相':'tuong','仕':'si','帥':'tuong_soai','炮':'phao','兵':'tot'};
                moved.src = `images/${data.botSide}_${names[moved.type]}.png`;
            }
            let newBoard = data.boardState.filter(p => !(p.c === bestMove.from.c && p.r === bestMove.from.r) && !(p.c === bestMove.to.c && p.r === bestMove.to.r));
            newBoard.push(moved);
            postMessage({ action: "done", move: { from: bestMove.from, to: bestMove.to, newBoard } });
        }
    }
};