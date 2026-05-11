// ==========================================
// FILE: botWorker.js (CHẠY NGẦM TRÊN MÁY KHÁCH)
// BẢN CAO THỦ: BIẾT TRẬN PHÁP (NGỌA TÀO, KẸP NÁCH, PHÁO ĐẦU) + ENDGAME
// ==========================================

let positionHistory = []; 

// Chỉnh lại giá trị quân cờ chuẩn chỉ hơn
const PIECE_VALUES = {
    '帥': 20000, '將': 20000,
    '俥': 1000, '車': 1000,
    '炮': 450,  '砲': 450,
    '傌': 420,  '馬': 420, // Mã đầu game hơi yếu, nhưng tàn cuộc rất mạnh
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

// --- BỘ NÃO ĐỊNH GIÁ TRẬN PHÁP CỰC ĐỘC ---
function evaluateBoard(mt, botSide) {
    let score = 0;
    for(let r=0; r<10; r++) {
        for(let c=0; c<9; c++) {
            const p = mt[r][c];
            if(p) {
                let val = PIECE_VALUES[p.type] || 10;
                
                // 1. TỐT QUA SÔNG LÀ MÃNH HỔ
                if ((p.type === '兵' || p.type === '卒') && !p.isUp) {
                    if (p.side === 'do' && r <= 4) {
                        val += 50 + (4 - r) * 20; // Càng chui sâu càng nhiều điểm
                        if (c >= 3 && c <= 5) val += 40; // Áp sát Tướng
                    }
                    if (p.side === 'den' && r >= 5) {
                        val += 50 + (r - 5) * 20;
                        if (c >= 3 && c <= 5) val += 40;
                    }
                }
                
                // 2. MÃ NGỌA TÀO & MÃ TRUNG TÂM
                if (p.type === '傌' || p.type === '馬') {
                    // Cấm Mã nằm góc xó xỉnh (Trừ điểm)
                    if (c === 0 || c === 8) val -= 30;
                    
                    // Mã phi lên giữa bàn cờ (Trục 2->6)
                    if (c >= 2 && c <= 6 && r >= 2 && r <= 7) val += 60;

                    // MÃ NGỌA TÀO (Vị trí sát thủ hiểm độc nhất)
                    if (p.side === 'do' && (r === 1 || r === 2) && (c === 2 || c === 6)) val += 150;
                    if (p.side === 'den' && (r === 7 || r === 8) && (c === 2 || c === 6)) val += 150;
                }
                
                // 3. PHÁO ĐẦU & PHÁO GIĂNG
                if (p.type === '炮' || p.type === '砲') {
                    // Pháo đầu (Đường số 4) -> Trấn áp Tướng địch
                    if (c === 4) val += 80;
                    // Pháo khống chế 2 trục Sĩ (Đường 3 và 5)
                    if (c === 3 || c === 5) val += 50;
                    // Pháo lùi về hàng đáy thủ
                    if (p.side === 'do' && r === 9) val += 30;
                    if (p.side === 'den' && r === 0) val += 30;
                }
                
                // 4. XE KẸP NÁCH TƯỚNG
                if (p.type === '俥' || p.type === '車') {
                    // Xe chiếm đường thông (Đường 3,4,5)
                    if (c === 3 || c === 4 || c === 5) val += 60;
                    
                    // Xe cắm xuống tận nách Tướng địch
                    if (p.side === 'do' && r <= 2 && c >= 3 && c <= 5) val += 100;
                    if (p.side === 'den' && r >= 7 && c >= 3 && c <= 5) val += 100;
                }

                // 5. BẢO VỆ TƯỚNG (Núp trong cung)
                if (p.type === '帥' || p.type === '將') {
                    if (p.side === 'do' && r !== 9) val -= 50; // Tướng đỏ bò lên cao -> Trừ điểm
                    if (p.side === 'den' && r !== 0) val -= 50; // Tướng đen bò lên cao -> Trừ điểm
                }

                // 6. CỜ ÚP: Ưu tiên mở những quân bị úp chặn đường
                if (p.isUp) val += 150; // Kích thích Bot mở cờ thay vì chạy loanh quanh

                // CỘNG TRỪ TỔNG ĐIỂM
                if(p.side === botSide) score += val;
                else score -= val;
            }
        }
    }
    return score;
}

// Sắp xếp nước đi thông minh để tăng tốc độ Alpha-Beta (Nước xịn tính trước)
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
            // Khuyến khích lao lên tấn công
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

    // ĐẾM SỐ QUÂN CÒN LẠI ĐỂ SANG SỐ (ÉP ĐỘ SÂU MINIMAX)
    let pieceCount = boardArray.length;
    let DEPTH = 3; // Mặc định đầu game đông quân

    if (pieceCount <= 6) {
        // CỜ TÀN KHỐC: Cả bàn cờ còn lèo tèo vài con (ví dụ 1 Xe, 1 Pháo, 2 Tướng, 2 Tốt)
        // Số trường hợp rất ít -> Mở khóa siêu trí tuệ DEPTH = 6. Cắm đầu truy sát Tướng!
        DEPTH = 6; 
    } else if (pieceCount <= 10) {
        // CÒN ÍT QUÂN: Nâng não lên mức 5
        DEPTH = 5;
    } else if (pieceCount <= 16) {
        // TRUNG CUỘC: Tăng lên mức 4
        DEPTH = 4;
    } else {
        // KHAI CUỘC: Đầy bàn cờ (32 quân), giữ nguyên mức 3 để chống cháy nổ điện thoại
        DEPTH = 3; 
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
            score = -100000; // Mày thích nhây tao cho mày ăn đạn!
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