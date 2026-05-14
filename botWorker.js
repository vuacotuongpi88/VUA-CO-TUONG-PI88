// ==========================================
// FILE: botWorker.js (CHẠY NGẦM TRÊN MÁY KHÁCH)
// BẢN TỐI THƯỢNG: BÙA TĨNH TÂM + KILLER MOVES + NULL MOVE PRUNING
// ==========================================

let positionHistory = []; 
let killerMoves = Array(100).fill(null).map(() => []); // Sổ tay ghi nhớ đòn sát thủ

const PIECE_VALUES = {
    '帥': 1000000, '將': 1000000,
    '俥': 1200, '車': 1200, // Tăng giá xe lên xíu
    '炮': 550,  '砲': 550,  // Pháo cực kỳ quan trọng
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
        case '仕': case '士': return dx === 1 && dy === 1 && tc >= 3 && tc <= 5 && (side === 'do' ? tr >= 7 : tr <= 2);
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

function evaluateBoard(mt, botSide) {
    let botMaterial = 0;
    let oppMaterial = 0;
    let tBot = null, tOpp = null;

    // QUÉT BÀN CỜ ĐỂ BẮT MẠCH TRẬN ĐẤU & TÌM TỌA ĐỘ 2 TƯỚNG
    for(let r=0; r<10; r++) {
        for(let c=0; c<9; c++) {
            const p = mt[r][c];
            if(p) {
                let val = PIECE_VALUES[p.type] || 10;
                // Cộng tổng tài sản 2 bên (không tính cờ úp chưa lật)
                if (!p.isUp) {
                    if(p.side === botSide) botMaterial += val;
                    else oppMaterial += val;
                }
                
                // Ghi nhớ tọa độ Tướng để lát ám sát
                if(p.type === '帥' || p.type === '將') {
                    if(p.side === botSide) tBot = {r, c};
                    else tOpp = {r, c};
                }
            }
        }
    }

    // NHẬN DIỆN THẾ TRẬN (Lệch 400 điểm tương đương 1 con Mã/Pháo)
    let isWinning = (botMaterial - oppMaterial) > 400; 
    let isLosing = (oppMaterial - botMaterial) > 400;  

    let score = 0;
    for(let r=0; r<10; r++) {
        for(let c=0; c<9; c++) {
            const p = mt[r][c];
            if(p) {
                let val = PIECE_VALUES[p.type] || 10;
                let isMyPiece = (p.side === botSide);

                // 1. Lính qua sông giá trị x3
                if ((p.type === '兵' || p.type === '卒') && !p.isUp) {
                    if (p.side === 'do' && r <= 4) {
                        val += 50 + (4 - r) * 30; 
                        if (c >= 3 && c <= 5) val += 50; 
                    }
                    if (p.side === 'den' && r >= 5) {
                        val += 50 + (r - 5) * 30;
                        if (c >= 3 && c <= 5) val += 50;
                    }
                }

                // 2. Khen Mã ngọa tào, chê Mã góc
                if (p.type === '傌' || p.type === '馬') {
                    if (c === 0 || c === 8) val -= 60; 
                    if (c >= 2 && c <= 6 && r >= 2 && r <= 7) val += 90; 
                    if (p.side === 'do' && (r === 1 || r === 2) && (c === 2 || c === 6)) val += 200;
                    if (p.side === 'den' && (r === 7 || r === 8) && (c === 2 || c === 6)) val += 200;
                }

                // 3. Pháo đầu vô đối
                if (p.type === '炮' || p.type === '砲') {
                    if (c === 4) val += 120; 
                    if (c === 3 || c === 5) val += 60; 
                }

                // 4. Ép Xe xuất cung
                if (p.type === '俥' || p.type === '車') {
                    if (c === 3 || c === 4 || c === 5) val += 150; 
                    if (p.side === 'do' && r === 9 && (c === 0 || c === 8)) val -= 200;
                    if (p.side === 'den' && r === 0 && (c === 0 || c === 8)) val -= 200;
                    if (p.side === 'do' && r === 6) val += 80; 
                    if (p.side === 'den' && r === 3) val += 80;
                }

                // 5. Tướng giữ gầm
                if (p.type === '帥' || p.type === '將') {
                    if (p.side === 'do' && r !== 9) val -= 100; 
                    if (p.side === 'den' && r !== 0) val -= 100; 
                }

                // ==========================================
                // 🔥 BÙA SÁT KHÍ (ĐÒN KẾT LIỄU): TRUY CÙNG DIỆT TẬN
                // ==========================================
                if (isMyPiece && tOpp && (p.type === '俥' || p.type === '車' || p.type === '炮' || p.type === '砲' || p.type === '傌' || p.type === '馬')) {
                    // Tính khoảng cách Mahattan từ quân tao tới cổ Tướng địch
                    let distToEnemyKing = Math.abs(r - tOpp.r) + Math.abs(c - tOpp.c);
                    // Càng gần Tướng địch càng cộng điểm bạo (Tối đa +200 điểm nếu kề sát)
                    val += Math.max(0, (14 - distToEnemyKing) * 15);
                }

                // ==========================================
                // 🐢 BÙA QUY TỨC CÔNG (TỬ THỦ CẦU HÒA)
                // ==========================================
                if (isLosing && isMyPiece) {
                    if (tBot) {
                        let distToHomeKing = Math.abs(r - tBot.r) + Math.abs(c - tBot.c);
                        if (distToHomeKing <= 3) {
                            val += 80; // Trọng thưởng cho những đứa lùi về bảo vệ Tướng
                        } else {
                            val -= 50; // Phạt nặng mấy con chạy rông bên sân địch để nộp mạng
                        }
                    }
                }

                // ==========================================
                // ⚔️ BÙA LƯỠNG BẠI CÂU THƯƠNG (ÉP ĐỔI QUÂN KHI ĐANG LỜI)
                // ==========================================
                if (isWinning && isMyPiece && p.type !== '兵' && p.type !== '卒' && p.type !== '帥' && p.type !== '將') {
                    // Giảm nhẹ 5% giá trị quân mình. Về mặt thuật toán Minimax, 
                    // điều này kích thích nó chủ động nhào vô đổi Xe, đổi Pháo để dọn dẹp bàn cờ!
                    val = Math.floor(val * 0.95);
                }

                // Ưu tiên lật cờ úp
                if (p.isUp) val += 200; 

                // Cộng trừ vào tổng gia tài
                if (isMyPiece) score += val;
                else score -= val;
            }
        }
    }
    return score;
}
// SINH NƯỚC ĐI VÀ SẮP XẾP MVV-LVA + KILLER MOVES
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
    
    // Tối ưu hóa thứ tự nhánh (Move Ordering)
    let kMoves = killerMoves[depth] || [];
    moves.forEach(m => {
        if (m.captured) {
            // Đòn MVV-LVA (Quân bèo ăn quân xịn)
            m.score = (PIECE_VALUES[m.captured.type] * 10) - PIECE_VALUES[m.pieceObj.type] + 100000;
        } else {
            m.score = 0;
            // Khen thưởng đòn sát thủ (Killer Moves)
            if (kMoves.some(k => k.from.r === m.from.r && k.from.c === m.from.c && k.to.r === m.to.r && k.to.c === m.to.c)) {
                m.score = 50000; 
            }
        }
    });

    moves.sort((a,b) => b.score - a.score);
    return moves;
}

// BÙA TĨNH TÂM (Chống chết ngu do tầm nhìn ngắn)
function quiesce(mt, alpha, beta, isMaximizing, botSide, isCoUp, qDepth) {
    let stand_pat = evaluateBoard(mt, botSide);
    if (qDepth > 5) return stand_pat; 

    if (isMaximizing) {
        if (stand_pat >= beta) return beta;
        if (alpha < stand_pat) alpha = stand_pat;
    } else {
        if (stand_pat <= alpha) return alpha;
        if (beta > stand_pat) beta = stand_pat;
    }

    let currentSide = isMaximizing ? botSide : (botSide === 'do' ? 'den' : 'do');
    let moves = generateAllMoves(mt, currentSide, isCoUp, 0, true); 

    if (isMaximizing) {
        for (let move of moves) {
            let target = mt[move.to.r][move.to.c];
            mt[move.to.r][move.to.c] = mt[move.from.r][move.from.c];
            mt[move.from.r][move.from.c] = null;
            let score = quiesce(mt, alpha, beta, false, botSide, isCoUp, qDepth + 1);
            mt[move.from.r][move.from.c] = mt[move.to.r][move.to.c];
            mt[move.to.r][move.to.c] = target;

            if (score >= beta) return beta;
            if (score > alpha) alpha = score;
        }
        return alpha;
    } else {
        for (let move of moves) {
            let target = mt[move.to.r][move.to.c];
            mt[move.to.r][move.to.c] = mt[move.from.r][move.from.c];
            mt[move.from.r][move.from.c] = null;
            let score = quiesce(mt, alpha, beta, true, botSide, isCoUp, qDepth + 1);
            mt[move.from.r][move.from.c] = mt[move.to.r][move.to.c];
            mt[move.to.r][move.to.c] = target;

            if (score <= alpha) return alpha;
            if (score < beta) beta = score;
        }
        return beta;
    }
}

function minimax(mt, depth, alpha, beta, isMaximizing, botSide, isCoUp, isNullMove = false) {
    if (depth <= 0) return quiesce(mt, alpha, beta, isMaximizing, botSide, isCoUp, 0);

    let currentSide = isMaximizing ? botSide : (botSide === 'do' ? 'den' : 'do');
    
    // --- LĂNG BA VI BỘ (NULL MOVE PRUNING) ---
    // Giả vờ nhường đối thủ đi 1 nước, nếu thế cờ vẫn tốt -> Cắt nhánh đéo cần tính thêm!
    let R = 2; // Giảm độ sâu 2 bậc
    if (depth >= 3 && !isNullMove && !laBiChieuWorker(currentSide, mt, isCoUp)) {
        let nmpScore = minimax(mt, depth - 1 - R, alpha, beta, !isMaximizing, botSide, isCoUp, true);
        if (isMaximizing && nmpScore >= beta) return beta;
        if (!isMaximizing && nmpScore <= alpha) return alpha;
    }
    // ----------------------------------------

    let moves = generateAllMoves(mt, currentSide, isCoUp, depth, false);
    if (moves.length === 0) return isMaximizing ? -99999 + depth : 99999 - depth;

    if (isMaximizing) {
        let maxEval = -Infinity;
        for (let move of moves) {
            let target = mt[move.to.r][move.to.c];
            mt[move.to.r][move.to.c] = mt[move.from.r][move.from.c];
            mt[move.from.r][move.from.c] = null;

            let ev = minimax(mt, depth - 1, alpha, beta, false, botSide, isCoUp, false);

            mt[move.from.r][move.from.c] = mt[move.to.r][move.to.c];
            mt[move.to.r][move.to.c] = target;

            if (ev > maxEval) maxEval = ev;
            if (ev > alpha) alpha = ev;
            if (beta <= alpha) {
                // Đòn này hiểm quá tạo ra cắt tỉa -> Lưu vào Sổ tay sát thủ
                if (!target) {
                    if (!killerMoves[depth]) killerMoves[depth] = [];
                    killerMoves[depth].unshift(move);
                    if (killerMoves[depth].length > 2) killerMoves[depth].pop();
                }
                break; 
            }
        }
        return maxEval;
    } else {
        let minEval = Infinity;
        for (let move of moves) {
            let target = mt[move.to.r][move.to.c];
            mt[move.to.r][move.to.c] = mt[move.from.r][move.from.c];
            mt[move.from.r][move.from.c] = null;

            let ev = minimax(mt, depth - 1, alpha, beta, true, botSide, isCoUp, false);

            mt[move.from.r][move.from.c] = mt[move.to.r][move.to.c];
            mt[move.to.r][move.to.c] = target;

            if (ev < minEval) minEval = ev;
            if (ev < beta) beta = ev;
            if (beta <= alpha) {
                if (!target) {
                    if (!killerMoves[depth]) killerMoves[depth] = [];
                    killerMoves[depth].unshift(move);
                    if (killerMoves[depth].length > 2) killerMoves[depth].pop();
                }
                break;
            }
        }
        return minEval;
    }
}

// BỘ MÁY FALLBACK: CHẠY BẰNG RAM CỦA KHÁCH
function calculateLocalMove(boardArray, botSide, isCoUp) {
    killerMoves = Array(100).fill(null).map(() => []); // Reset sổ tay
    let mt = buildMatrix(boardArray);
    let moves = generateAllMoves(mt, botSide, isCoUp, 10, false);
    if (moves.length === 0) return null;

    let pieceCount = boardArray.length;
    let DEPTH = 4; 

    // Bờm Null Move Pruning giúp máy tính nhàn hơn, tao TĂNG DEPTH LÊN 1 BẬC SO VỚI BẢN TRƯỚC
    if (pieceCount <= 6) DEPTH = 8; 
    else if (pieceCount <= 12) DEPTH = 6;
    else if (pieceCount <= 20) DEPTH = 5;
    else DEPTH = 4; // Khai cuộc vẫn giữ mượt mà

    let bestScore = -Infinity;
    let bestMoves = [];

    for (let move of moves) {
        let target = mt[move.to.r][move.to.c];
        mt[move.to.r][move.to.c] = mt[move.from.r][move.from.c];
        mt[move.from.r][move.from.c] = null;

        let currentHash = getBoardHash(mt);
        let oppSide = botSide === 'do' ? 'den' : 'do';
        let isChecking = laBiChieuWorker(oppSide, mt, isCoUp);
        let repeatCount = positionHistory.filter(h => h.hash === currentHash && h.isCheck).length;

        let score = 0;
        if (isChecking && repeatCount >= 3) {
            score = -100000; 
        } else {
            score = minimax(mt, DEPTH - 1, -Infinity, Infinity, false, botSide, isCoUp, false);
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
    return bestMoves[Math.floor(Math.random() * bestMoves.length)];
}

// ==========================================
// KHO ĐỘNG CƠ CƯỚP BIỂN: DỊCH FEN VÀ GỌI CHESSDB
// ==========================================
function boardToFEN(mt, botSide) {
    const mapRed = {'俥':'R', '車':'R', '傌':'N', '馬':'N', '相':'B', '象':'B', '仕':'A', '士':'A', '帥':'K', '將':'K', '炮':'C', '砲':'C', '兵':'P', '卒':'P'};
    const mapBlack = {'俥':'r', '車':'r', '傌':'n', '馬':'n', '相':'b', '象':'b', '仕':'a', '士':'a', '帥':'k', '將':'k', '炮':'c', '砲':'c', '兵':'p', '卒':'p'};

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

async function fetchCloudMove(fen) {
    const url = `https://www.chessdb.cn/cdb.php?action=queryall&board=${encodeURIComponent(fen)}`;
    try {
        const res = await fetch(url);
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

// ==========================================
// TỔNG TRẠM ĐIỀU PHỐI (CHẠY KHI ĐƯỢC GỌI)
// ==========================================
self.onmessage = async function(e) {
    const data = e.data;
    if (data.action === "think") {
        if (data.recentHistory) positionHistory = data.recentHistory;

        let bestMoveObject = null;
        let mt = buildMatrix(data.boardState);

        // ==========================================
        // 📚 BÍ KÍP KHAI CUỘC SIÊU TỐC (3 NƯỚC ĐẦU)
        // ==========================================
        let botMoveCount = (data.recentHistory || []).length;
        
        // Nếu Bot mới đi chưa quá 3 nước -> Giở sách ra đi nhanh đéo cần nghĩ
        if (botMoveCount < 3) {
            // Thứ tự ưu tiên: Pháo đầu -> Mã đội (Mã lên) -> Ra Xe
            const OPENING_MOVES_DEN = [
                {f:{c:7, r:2}, t:{c:4, r:2}}, // 1. Pháo phải vào giữa
                {f:{c:7, r:0}, t:{c:6, r:2}}, // 2. Mã phải nhảy lên lộ 6
                {f:{c:8, r:0}, t:{c:8, r:1}}, // 3. Xe phải tiến 1 (Hoành xa)
                {f:{c:1, r:2}, t:{c:4, r:2}}, // (Dự phòng) Pháo trái
                {f:{c:1, r:0}, t:{c:2, r:2}}  // (Dự phòng) Mã trái
            ];
            const OPENING_MOVES_DO = [
                {f:{c:7, r:7}, t:{c:4, r:7}}, // 1. Pháo phải vào giữa
                {f:{c:7, r:9}, t:{c:6, r:7}}, // 2. Mã phải nhảy lên lộ 6
                {f:{c:8, r:9}, t:{c:8, r:8}}, // 3. Xe phải tiến 1 (Hoành xa)
                {f:{c:1, r:7}, t:{c:4, r:7}}, // (Dự phòng) Pháo trái
                {f:{c:1, r:9}, t:{c:2, r:7}}  // (Dự phòng) Mã trái
            ];

            let script = data.botSide === 'den' ? OPENING_MOVES_DEN : OPENING_MOVES_DO;

            // Quét từ trên xuống dưới xem chiêu nào dùng được thì múc luôn
            for (let plan of script) {
                let p = mt[plan.f.r][plan.f.c];
                
                // Kiểm tra xem có quân của mình ở ô xuất phát không và đi có đúng luật không
                if (p && p.side === data.botSide && checkLuatWorker(p.type, data.botSide, plan.f.c, plan.f.r, plan.t.c, plan.t.r, mt, data.isCoUp)) {
                    
                    // Đi nháp xem Tướng có bị đối thủ ngắm bắn không
                    let tempTarget = mt[plan.t.r][plan.t.c];
                    mt[plan.t.r][plan.t.c] = p;
                    mt[plan.f.r][plan.f.c] = null;
                    
                    let safe = !laBiChieuWorker(data.botSide, mt, data.isCoUp) && !haiTuongDoiMat(mt);
                    
                    // Trả lại bàn cờ
                    mt[plan.f.r][plan.f.c] = p;
                    mt[plan.t.r][plan.t.c] = tempTarget;

                    if (safe) {
                        bestMoveObject = { from: plan.f, to: plan.t, pieceObj: p };
                        break; // Tìm được nước đi ngon thì dừng vòng lặp, bốc nước này đi luôn!
                    }
                }
            }
        }
        // ==========================================

        // NẾU HẾT 3 NƯỚC ĐẦU (HOẶC KHÔNG CÓ TRONG SÁCH) THÌ DÙNG CHESSDB CLOUD
        if (!bestMoveObject && !data.isCoUp) {
            let fen = boardToFEN(mt, data.botSide);
            const cloudMove = await fetchCloudMove(fen);
            
            if (cloudMove) {
                let p = mt[cloudMove.from.r][cloudMove.from.c];
                if (p && p.side === data.botSide && checkLuatWorker(p.type, data.botSide, cloudMove.from.c, cloudMove.from.r, cloudMove.to.c, cloudMove.to.r, mt, false)) {
                    bestMoveObject = {
                        from: cloudMove.from,
                        to: cloudMove.to,
                        pieceObj: p
                    };
                }
            }
        }

        // TỰ ĐỘNG KÍCH HOẠT NÃO BỘ TÍNH TOÁN (TỪ NƯỚC THỨ 4 TRỞ ĐI)
        if (!bestMoveObject) {
            bestMoveObject = calculateLocalMove(data.boardState, data.botSide, data.isCoUp);
        }

        // TRẢ KẾT QUẢ VỀ CHO MÀN HÌNH CHÍNH
        if (bestMoveObject) {
            let movedPiece = { ...bestMoveObject.pieceObj, c: bestMoveObject.to.c, r: bestMoveObject.to.r };
            if (data.isCoUp && movedPiece.isUp) {
                movedPiece.isUp = false;
                // Có fix lại đoạn này cho đủ bộ từ điển cả Đỏ lẫn Đen
                const names = {'車':'xe','馬':'ma','象':'tuong','士':'si','將':'tuong_soai','砲':'phao','卒':'tot', '俥':'xe','傌':'ma','相':'tuong','仕':'si','帥':'tuong_soai','炮':'phao','兵':'tot'};
                if (names[movedPiece.type]) movedPiece.src = `images/${data.botSide}_${names[movedPiece.type]}.png`;
            }

            let newBoardArray = data.boardState.filter(p => {
                if (p.c === bestMoveObject.from.c && p.r === bestMoveObject.from.r) return false;
                if (p.c === bestMoveObject.to.c && p.r === bestMoveObject.to.r) return false;
                return true;
            });
            newBoardArray.push(movedPiece);

            let tempMt = buildMatrix(newBoardArray);
            let oppSide = data.botSide === 'do' ? 'den' : 'do';
            
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