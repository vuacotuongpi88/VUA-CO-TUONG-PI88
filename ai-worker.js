// ==========================================
// FILE: botWorker.js (CHẠY NGẦM TRÊN MÁY KHÁCH)
// NÃO BỘ: MINIMAX DYNAMIC DEPTH (10) + TRẬN PHÁP + CHỐNG CHIẾU NHÂY
// ==========================================

let positionHistory = []; 

const PIECE_VALUES = {
    '帥': 20000, '將': 20000,
    '俥': 1000, '車': 1000,
    '炮': 450,  '砲': 450,
    '傌': 420,  '馬': 420, 
    '相': 250,  '象': 250,
    '仕': 250,  '士': 250,
    '兵': 100,  '卒': 100
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

// --- HÀM ĐÁNH GIÁ NÂNG CAO CHO CỜ ÚP ---
function evaluateBoard(mt, botSide, isCoUp) {
    let score = 0;
    let oppSide = botSide === 'do' ? 'den' : 'do';
    let tOpp = null;

    // Tìm tướng địch trước để tính sát khí
    for (let r = 0; r < 10; r++) {
        for (let c = 0; c < 9; c++) {
            if (mt[r][c] && mt[r][c].side === oppSide && (mt[r][c].type === '帥' || mt[r][c].type === '將')) {
                tOpp = { r, c }; break;
            }
        }
    }

    for (let r = 0; r < 10; r++) {
        for (let c = 0; c < 9; c++) {
            const p = mt[r][c];
            if (!p) continue;

            let val = PIECE_VALUES[p.type] || 10;
            let isMyPiece = (p.side === botSide);

            // --- CHIẾN THUẬT CỜ ÚP ĐẶC BIỆT ---
            if (isCoUp && p.isUp) {
                // Nếu quân đang úp, giá trị trung bình là 300 điểm (giá trị kỳ vọng)
                val = 300; 
                
                // Phạt nặng nếu vác nắp úp đi vào chỗ chết (nơi địch đang canh giữ)
                if (isSquareAttackedWorker(c, r, isMyPiece ? oppSide : botSide, mt, isCoUp)) {
                    val -= 150; 
                }
            }

            // --- BÙA BẢO KÊ & SINH TỒN ---
            if (!p.isUp && p.type !== '帥' && p.type !== '將') {
                let isAttacked = isSquareAttackedWorker(c, r, isMyPiece ? oppSide : botSide, mt, isCoUp);
                let isDefended = isSquareDefendedWorker(c, r, p.side, mt, isCoUp);

                if (isAttacked) {
                    // Nếu bị tấn công mà ĐÉO có bảo kê -> Trừ 70% giá trị (ép nó phải chạy)
                    // Nếu bị tấn công mà CÓ bảo kê -> Chỉ trừ 15% (dám đứng lại đổi quân)
                    val -= isDefended ? (val * 0.15) : (val * 0.7);
                } else if (isDefended) {
                    // Thưởng điểm cho việc các quân đứng gần nhau bảo vệ nhau
                    val += 30;
                }
            }

            // Cộng điểm vị trí (Xe, Pháo, Mã...) như cũ nhưng gắt hơn
            if (p.type === '俥' || p.type === '車') val += 50; 
            if (isMyPiece && tOpp) {
                let dist = Math.abs(r - tOpp.r) + Math.abs(c - tOpp.c);
                val += Math.max(0, (14 - dist) * 10);
            }

            if (isMyPiece) score += val;
            else score -= val;
        }
    }
    return score;
}

// --- MINIMAX VỚI TẦM NHÌN XUYÊN THẤU CỜ ÚP ---
function minimax(mt, depth, alpha, beta, isMaximizing, botSide, isCoUp, isNullMove = false) {
    if (depth <= 0) return quiesce(mt, alpha, beta, isMaximizing, botSide, isCoUp, 0);

    let currentSide = isMaximizing ? botSide : (botSide === 'do' ? 'den' : 'do');
    let moves = generateAllMoves(mt, currentSide, isCoUp, depth, false);
    
    if (moves.length === 0) return isMaximizing ? -1000000 + depth : 1000000 - depth;

    if (isMaximizing) {
        let maxEval = -Infinity;
        for (let move of moves) {
            let target = mt[move.to.r][move.to.c];
            let movingPiece = mt[move.from.r][move.from.c];
            let wasUp = movingPiece.isUp;

            mt[move.to.r][move.to.c] = movingPiece;
            mt[move.from.r][move.from.c] = null;
            
            // 🔥 GIẢ LẬP LẬT CỜ: Bot tính toán dựa trên việc lật con cờ đó ra
            if (isCoUp && wasUp) movingPiece.isUp = false; 

            let ev = minimax(mt, depth - 1, alpha, beta, false, botSide, isCoUp, false);

            if (isCoUp && wasUp) movingPiece.isUp = true; 
            mt[move.from.r][move.from.c] = movingPiece;
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
            let movingPiece = mt[move.from.r][move.from.c];
            let wasUp = movingPiece.isUp;

            mt[move.to.r][move.to.c] = movingPiece;
            mt[move.from.r][move.from.c] = null;
            
            if (isCoUp && wasUp) movingPiece.isUp = false;

            let ev = minimax(mt, depth - 1, alpha, beta, true, botSide, isCoUp, false);

            if (isCoUp && wasUp) movingPiece.isUp = true;
            mt[move.from.r][move.from.c] = movingPiece;
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

    // ĐẾM SỐ QUÂN CÒN LẠI ĐỂ SANG SỐ (ÉP XUNG DEPTH 8-9-10)
    let pieceCount = boardArray.length;
    let DEPTH = 4; // Mặc định khai cuộc, đẩy lên mức 4 (Mức 5 lúc 32 quân JS chạy sẽ mất khoảng 10-20 giây)

    if (pieceCount <= 5) {
        // TÀN CUỘC VẮNG VẺ: Mở khóa siêu trí tuệ DEPTH = 10.
        // Nhìn thấu 10 bước (Tao đi -> Mày đỡ -> ... 5 vòng lặp). Nó sẽ vắt kiệt CPU để dồn Tướng mày vào góc chết!
        DEPTH = 10; 
    } else if (pieceCount <= 8) {
        // CÒN 8 QUÂN: Mức 8. Đã đủ khôn để nhìn ra mọi đòn hy sinh quân cạm bẫy.
        DEPTH = 8;
    } else if (pieceCount <= 14) {
        // CÒN KHOẢNG NỬA BÀN CỜ: Mức 6. Bắt đầu ép sân.
        DEPTH = 6;
    } else if (pieceCount <= 22) {
        // RỤNG ĐƯỢC VÀI QUÂN: Mức 5.
        DEPTH = 5;
    } else {
        // KHAI CUỘC (Hơn 22 quân): Giữ mức 4 để tránh đơ app quá 30 giây ngay nước đầu.
        DEPTH = 4; 
    }

    let bestScore = -Infinity;
    let bestMoves = [];

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

    let tempMt = buildMatrix(newBoardArray);
    let resultHash = getBoardHash(tempMt);
    let oppSideCheck = botSide === 'do' ? 'den' : 'do';
    let resultIsCheck = laBiChieuWorker(oppSideCheck, tempMt, isCoUp);

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