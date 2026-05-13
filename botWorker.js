// ==========================================
// FILE: botWorker.js (BẢN SIÊU CẤP V8 - CHỐT HẠ)
// TỔNG HỢP: HACK NHÌN XUYÊN + SÁCH TÀU + CHỐNG CHIẾU NHÂY + CỨU QUÂN VIP
// ==========================================

let positionHistory = []; 
let currentMatchMoveCount = 0; 

const PIECE_VALUES = {
    '帥': 1000000, '將': 1000000,
    '俥': 1500, '車': 1500,
    '炮': 650,  '砲': 650,
    '傌': 600,  '馬': 600,  
    '相': 300,  '象': 250,
    '仕': 300,  '士': 250,
    '兵': 150,  '卒': 150
};

// --- HÀM BỔ TRỢ HỆ THỐNG ---
function buildMatrix(boardArray) {
    let mt = Array(10).fill(null).map(() => Array(9).fill(null));
    boardArray.forEach(p => { if(p) mt[p.r][p.c] = p; });
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

// --- LUẬT ĐI QUÂN CHUẨN CỜ ÚP ---
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
            if (mt[(r + tr) / 2][(c + tc) / 2]) return false;
            if (isCoUp && mt[r][c] && !mt[r][c].isUp) return true;
            return (side === 'do' ? tr >= 5 : tr <= 4);
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
            if (!daQuaSong) return dx === 0 && dy === 1 && (side === 'do' ? tr < r : tr > r);
            return (dx + dy === 1) && (side === 'do' ? tr <= r : tr >= r);
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

function isSquareAttacked(tc, tr, enemySide, mt, isCoUp) {
    for(let r=0; r<10; r++) for(let c=0; c<9; c++) {
        const p = mt[r][c];
        if(p && p.side === enemySide && checkLuatWorker(p.type, enemySide, c, r, tc, tr, mt, isCoUp)) return true;
    }
    return false;
}

function isSquareDefended(tc, tr, mySide, mt, isCoUp) {
    let temp = mt[tr][tc]; mt[tr][tc] = null;
    let def = false;
    for(let r=0; r<10; r++) for(let c=0; c<9; c++) {
        const p = mt[r][c];
        if(p && p.side === mySide && checkLuatWorker(p.type, mySide, c, r, tc, tr, mt, isCoUp)) { def = true; break; }
    }
    mt[tr][tc] = temp; return def;
}

function haiTuongDoiMat(mt) {
    let tDo = null, tDen = null;
    for(let r=0; r<10; r++) for(let c=0; c<9; c++) {
        if(mt[r][c] && mt[r][c].type === '帥') tDo = {c, r};
        if(mt[r][c] && mt[r][c].type === '將') tDen = {c, r};
    }
    if(!tDo || !tDen) return false;
    if(tDo.c !== tDen.c) return false; 
    let count = 0;
    for(let r = Math.min(tDo.r, tDen.r) + 1; r < Math.max(tDo.r, tDen.r); r++) {
        if(mt[r][tDo.c]) count++;
    }
    return count === 0;
}

// --- HÀM ĐÁNH GIÁ V8: RADAR + BẢO VỆ QUÂN + DIỄN VIÊN ---
function evaluateBoard(mt, botSide, isCoUp) {
    let score = 0;
    let oppSide = botSide === 'do' ? 'den' : 'do';
    let myMaterial = 0, oppMaterial = 0;

    for(let r=0; r<10; r++) {
        for(let c=0; c<9; c++) {
            const p = mt[r][c];
            if(!p) continue;
            let val = PIECE_VALUES[p.type] || 0;
            if(p.side === botSide) myMaterial += val;
            else oppMaterial += val;
        }
    }

    for(let r=0; r<10; r++) {
        for(let c=0; c<9; c++) {
            const p = mt[r][c];
            if(!p) continue;
            let isMyPiece = (p.side === botSide);
            let val = PIECE_VALUES[p.type] || 10;

            // RADAR DÒ NẮP ÚP & DIỄN KỊCH
            if (isCoUp && p.isUp) {
                if (isMyPiece) {
                    if (currentMatchMoveCount < 6) val = (['兵','卒','仕','士','相','象'].includes(p.type)) ? 600 : 100;
                    else val = (['俥','車','炮','砲','傌','馬'].includes(p.type)) ? 1500 : 300;
                } else {
                    if (['俥','車','炮','砲','傌','馬'].includes(p.type) && isSquareAttacked(c, r, botSide, mt, false)) {
                        score += 800; // Thèm khát ăn nắp úp xịn của địch
                    }
                }
            }

            // BẢO VỆ QUÂN THEO TRỌNG ĐIỂM
            if (!p.isUp || !isMyPiece) {
                let attacked = isSquareAttacked(c, r, isMyPiece ? oppSide : botSide, mt, isCoUp);
                let defended = isSquareDefended(c, r, p.side, mt, isCoUp);
                if (isMyPiece && attacked) {
                    val -= defended ? (val * 0.2) : (val * 0.95); // Xe đéo có bảo kê trừ gần hết điểm để nó chạy
                } else if (!isMyPiece && attacked) {
                    val += defended ? 50 : (val * 0.5); // Ưu tiên ăn quân hở của địch
                }
            }

            if (p.type === '帥' || p.type === '將') {
                if (isMyPiece && isSquareAttacked(c, r, oppSide, mt, isCoUp)) val -= 20000;
            }

            if (isMyPiece) score += val; else score -= val;
        }
    }
    return score;
}

function generateAllMoves(mt, side, isCoUp) {
    let moves = [];
    for(let r=0; r<10; r++) for(let c=0; c<9; c++) {
        let p = mt[r][c];
        if(p && p.side === side) {
            for(let tr=0; tr<10; tr++) for(let tc=0; tc<9; tc++) {
                if(checkLuatWorker(p.type, side, c, r, tc, tr, mt, isCoUp)) {
                    let target = mt[tr][tc];
                    mt[tr][tc] = p; mt[r][c] = null;
                    if(!laBiChieuWorker(side, mt, isCoUp) && !haiTuongDoiMat(mt)) {
                        moves.push({from: {c, r}, to: {c: tc, r: tr}, pieceObj: p});
                    }
                    mt[r][c] = p; mt[tr][tc] = target;
                }
            }
        }
    }
    return moves;
}

function minimax(mt, depth, alpha, beta, isMaximizing, botSide, isCoUp) {
    if (depth <= 0) return evaluateBoard(mt, botSide, isCoUp);
    let currentSide = isMaximizing ? botSide : (botSide === 'do' ? 'den' : 'do');
    let moves = generateAllMoves(mt, currentSide, isCoUp);
    if (moves.length === 0) return isMaximizing ? -2000000 : 2000000;

    for (let m of moves) {
        let target = mt[m.to.r][m.to.c];
        let wasUp = m.pieceObj.isUp;
        mt[m.to.r][m.to.c] = m.pieceObj; mt[m.from.r][m.from.c] = null;
        if (isCoUp && wasUp) m.pieceObj.isUp = false; 
        let ev = minimax(mt, depth-1, alpha, beta, !isMaximizing, botSide, isCoUp);
        if (isCoUp && wasUp) m.pieceObj.isUp = true;
        mt[m.from.r][m.from.c] = m.pieceObj; mt[m.to.r][m.to.c] = target;
        if (isMaximizing) {
            alpha = Math.max(alpha, ev);
            if (beta <= alpha) break;
        } else {
            beta = Math.min(beta, ev);
            if (beta <= alpha) break;
        }
    }
    return isMaximizing ? alpha : beta;
}

// --- CƯỚP BIỂN CHESSDB ---
function boardToFEN(mt, botSide) {
    const mapRed = {'俥':'R','車':'R','傌':'N','馬':'N','相':'B','象':'B','仕':'A','士':'A','帥':'K','將':'K','炮':'C','砲':'C','兵':'P','卒':'P'};
    const mapBlack = {'俥':'r','車':'r','傌':'n','馬':'n','相':'b','象':'b','仕':'a','士':'a','帥':'k','將':'k','炮':'c','砲':'c','兵':'p','卒':'p'};
    let fen = "";
    for(let r=0; r<10; r++) {
        let empty = 0;
        for(let c=0; c<9; c++) {
            const p = mt[r][c];
            if(!p) empty++;
            else { if(empty>0){fen+=empty; empty=0;} fen += p.side==='do'?mapRed[p.type]:mapBlack[p.type]; }
        }
        if(empty>0) fen += empty; if(r<9) fen += "/";
    }
    return fen + " " + (botSide==='do'?'w':'b') + " - - 0 1";
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

// --- TỔNG TRẠM ĐIỀU PHỐI (FIX CHIẾU NHÂY) ---
self.onmessage = async function(e) {
    const data = e.data;
    if (data.action === "think") {
        currentMatchMoveCount++;
        let mt = buildMatrix(data.boardState);
        let bestMove = null;

        const nguaCount = data.boardState.filter(p => p && !p.isUp).length;
        if (!data.isCoUp || nguaCount > 10) {
            bestMove = await fetchCloudMove(boardToFEN(mt, data.botSide));
        }

        if (!bestMove) {
            let moves = generateAllMoves(mt, data.botSide, data.isCoUp);
            let bestScore = -Infinity;
            let depth = data.boardState.length <= 12 ? 5 : 4;
            
            for (let m of moves) {
                let target = mt[m.to.r][m.to.c];
                let movingPiece = mt[m.from.r][m.from.c];
                mt[m.to.r][m.to.c] = movingPiece; mt[m.from.r][m.from.c] = null;
                
                // CHỐT CHẶN CHIẾU NHÂY
                let currentHash = getBoardHash(mt);
                let isChecking = laBiChieuWorker(data.botSide === 'do' ? 'den' : 'do', mt, data.isCoUp);
                let repeatCount = (data.recentHistory || []).filter(h => h.hash === currentHash && h.isCheck).length;

                let score = 0;
                if (isChecking && repeatCount >= 2) score = -1000000;
                else score = minimax(mt, depth-1, -Infinity, Infinity, false, data.botSide, data.isCoUp);

                mt[m.from.r][m.from.c] = movingPiece; mt[m.to.r][m.to.c] = target;
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
            let newBoard = data.boardState.filter(p => p && !(p.c === bestMove.from.c && p.r === bestMove.from.r) && !(p.c === bestMove.to.c && p.r === bestMove.to.r));
            newBoard.push(moved);
            
            // Ghi nhận hash để chống chiếu nhây ván sau
            let finalMt = buildMatrix(newBoard);
            let finalHash = getBoardHash(finalMt);
            let finalIsCheck = laBiChieuWorker(data.botSide === 'do' ? 'den' : 'do', finalMt, data.isCoUp);

            postMessage({ action: "done", move: { from: bestMove.from, to: bestMove.to, newBoard, hash: finalHash, isCheck: finalIsCheck } });
        }
    }
};