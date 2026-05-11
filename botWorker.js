// ==========================================
// FILE: botWorker.js (CHẠY NGẦM TRÊN MÁY KHÁCH)
// NÃO BỘ: MINIMAX DEPTH 3 + TRẬN PHÁP + CHỐNG CHIẾU NHÂY
// ==========================================

let positionHistory = []; // Bộ nhớ ngắn hạn của Bot

const PIECE_VALUES = {
    '帥': 10000, '將': 10000,
    '俥': 900, '車': 900,
    '炮': 450, '砲': 450,
    '傌': 400, '馬': 400,
    '相': 200, '象': 200,
    '仕': 200, '士': 200,
    '兵': 100, '卒': 100
};

// Hàm tạo mã băm (Hash) để ghi nhớ bàn cờ
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
        case '帥': case '將': 
            return dx + dy === 1 && tc >= 3 && tc <= 5 && (side === 'do' ? tr >= 7 : tr <= 2);
        case '仕': case '士':
            return dx === 1 && dy === 1 && tc >= 3 && tc <= 5 && (side === 'do' ? tr >= 7 : tr <= 2);
        case '相': case '象':
            if (!isCoUp && (side === 'do' ? tr < 5 : tr > 4)) return false;
            if (dx !== 2 || dy !== 2) return false;
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
            if (!daQuaSong) return diTien;
            return diTien || diNgang;
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

// Chấm điểm thế trận
function evaluateBoard(mt, botSide) {
    let score = 0;
    for(let r=0; r<10; r++) {
        for(let c=0; c<9; c++) {
            const p = mt[r][c];
            if(p) {
                let val = PIECE_VALUES[p.type] || 10;
                
                if ((p.type === '兵' || p.type === '卒') && !p.isUp) {
                    if (p.side === 'do' && r <= 4) {
                        val += 30 + (4 - r) * 15;
                        if (c >= 3 && c <= 5) val += 30;
                    }
                    if (p.side === 'den' && r >= 5) {
                        val += 30 + (r - 5) * 15;
                        if (c >= 3 && c <= 5) val += 30;
                    }
                }
                
                if (p.type === '傌' || p.type === '馬') {
                    if (c >= 2 && c <= 6 && r >= 2 && r <= 7) val += 35;
                }
                
                if (p.type === '炮' || p.type === '砲') {
                    if (c === 4) val += 40;
                    if (c === 3 || c === 5) val += 20;
                }
                
                if (p.type === '俥' || p.type === '車') {
                    if (c === 3 || c === 4 || c === 5) val += 30;
                    if (p.side === 'do' && r <= 2) val += 45;
                    if (p.side === 'den' && r >= 7) val += 45;
                }

                if (p.isUp) val += 60; 

                if(p.side === botSide) score += val;
                else score -= val;
            }
        }
    }
    return score;
}

function generateAllMoves(mt, side, isCoUp) {
    let moves = [];
    for (let r = 0; r < 10; r++) {
        for (let c = 0; c < 9; c++) {
            let p = mt[r][c];
            if (p && p.side === side) {
                for (let tr = 0; tr < 10; tr++) {
                    for (let tc = 0; tc < 9; tc++) {
                        if (checkLuatWorker(p.type, side, c, r, tc, tr, mt, isCoUp)) {
                            let targetPiece = mt[tr][tc];
                            mt[tr][tc] = mt[r][c];
                            mt[r][c] = null;
                            if (!laBiChieuWorker(side, mt, isCoUp) && !haiTuongDoiMat(mt)) {
                                moves.push({from: {c, r}, to: {c: tc, r: tr}, pieceObj: p, captured: targetPiece});
                            }
                            mt[r][c] = mt[tr][tc];
                            mt[tr][tc] = targetPiece;
                        }
                    }
                }
            }
        }
    }

    moves.sort((a,b) => {
        let scoreA = a.captured ? PIECE_VALUES[a.captured.type] : 0;
        let scoreB = b.captured ? PIECE_VALUES[b.captured.type] : 0;
        if (scoreA === 0 && scoreB === 0) {
            let advanceA = side === 'do' ? (a.from.r - a.to.r) : (a.to.r - a.from.r);
            let advanceB = side === 'do' ? (b.from.r - b.to.r) : (b.to.r - b.from.r);
            return advanceB - advanceA; 
        }
        return scoreB - scoreA;
    });

    return moves;
}

function minimax(mt, depth, alpha, beta, isMaximizing, botSide, isCoUp) {
    if (depth === 0) {
        return evaluateBoard(mt, botSide);
    }

    let currentSide = isMaximizing ? botSide : (botSide === 'do' ? 'den' : 'do');
    let moves = generateAllMoves(mt, currentSide, isCoUp);

    if (moves.length === 0) {
        return isMaximizing ? -99999 + depth : 99999 - depth;
    }

    if (isMaximizing) {
        let maxEval = -Infinity;
        for (let move of moves) {
            let target = mt[move.to.r][move.to.c];
            mt[move.to.r][move.to.c] = mt[move.from.r][move.from.c];
            mt[move.from.r][move.from.c] = null;

            let ev = minimax(mt, depth - 1, alpha, beta, false, botSide, isCoUp);

            mt[move.from.r][move.from.c] = mt[move.to.r][move.to.c];
            mt[move.to.r][move.to.c] = target;

            maxEval = Math.max(maxEval, ev);
            alpha = Math.max(alpha, ev);
            if (beta <= alpha) break;
        }
        return maxEval;
    } else {
        let minEval = Infinity;
        for (let move of moves) {
            let target = mt[move.to.r][move.to.c];
            mt[move.to.r][move.to.c] = mt[move.from.r][move.from.c];
            mt[move.from.r][move.from.c] = null;

            let ev = minimax(mt, depth - 1, alpha, beta, true, botSide, isCoUp);

            mt[move.from.r][move.from.c] = mt[move.to.r][move.to.c];
            mt[move.to.r][move.to.c] = target;

            minEval = Math.min(minEval, ev);
            beta = Math.min(beta, ev);
            if (beta <= alpha) break;
        }
        return minEval;
    }
}

function calculateMove(boardArray, botSide, isCoUp) {
    let mt = buildMatrix(boardArray);
    let moves = generateAllMoves(mt, botSide, isCoUp);
    
    if (moves.length === 0) return null;

    let bestScore = -Infinity;
    let bestMoves = [];
    const DEPTH = 3; 

    for (let move of moves) {
        let target = mt[move.to.r][move.to.c];
        mt[move.to.r][move.to.c] = mt[move.from.r][move.from.c];
        mt[move.from.r][move.from.c] = null;

        // BỘ LỌC CHỐNG CHIẾU NHÂY
        let currentHash = getBoardHash(mt);
        let oppSide = botSide === 'do' ? 'den' : 'do';
        let isChecking = laBiChieuWorker(oppSide, mt, isCoUp);
        
        let repeatCount = positionHistory.filter(h => h.hash === currentHash && h.isCheck).length;

        let score = 0;
        if (isChecking && repeatCount >= 3) {
            score = -100000; // Phạt chết cụ nó nếu lặp lại chiếu quá 3 lần
        } else {
            score = minimax(mt, DEPTH - 1, -Infinity, Infinity, false, botSide, isCoUp);
        }

        mt[move.from.r][move.from.c] = mt[move.to.r][move.to.c];
        mt[move.to.r][move.to.c] = target;

        if (score > bestScore) {
            bestScore = score;
            bestMoves = [move];
        } else if (score === bestScore) {
            bestMoves.push(move);
        }
    }

    const chosenMove = bestMoves[Math.floor(Math.random() * bestMoves.length)];

    let movedPiece = { ...chosenMove.pieceObj, c: chosenMove.to.c, r: chosenMove.to.r };
    if (isCoUp && movedPiece.isUp) {
        movedPiece.isUp = false;
        const names = {'車':'xe','馬':'ma','象':'tuong','士':'si','將':'tuong_soai','砲':'phao','卒':'tot'};
        if (names[movedPiece.type]) movedPiece.src = `images/${botSide}_${names[movedPiece.type]}.png`;
    }

    let newBoardArray = boardArray.filter(p => {
        if (p.c === chosenMove.from.c && p.r === chosenMove.from.r) return false;
        if (p.c === chosenMove.to.c && p.r === chosenMove.to.r) return false;
        return true;
    });
    newBoardArray.push(movedPiece);

    // Tạo giả lập bàn cờ sau khi đi để lấy hash gửi lên Client ghi nhớ
    let tempMt = buildMatrix(newBoardArray);
    let resultHash = getBoardHash(tempMt);
    let oppSide = botSide === 'do' ? 'den' : 'do';
    let resultIsCheck = laBiChieuWorker(oppSide, tempMt, isCoUp);

    return {
        from: chosenMove.from,
        to: chosenMove.to,
        newBoard: newBoardArray,
        hash: resultHash,
        isCheck: resultIsCheck
    };
}

self.onmessage = function(e) {
    const data = e.data;
    if (data.action === "think") {
        if (data.recentHistory) positionHistory = data.recentHistory;
        const bestMove = calculateMove(data.boardState, data.botSide, data.isCoUp);
        postMessage({ action: "done", move: bestMove });
    }
};