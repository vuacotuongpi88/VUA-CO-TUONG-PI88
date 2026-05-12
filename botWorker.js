// ==========================================
// FILE: botWorker.js (CHẠY NGẦM TRÊN MÁY KHÁCH)
// BẢN HYBRID CLOUD: BÚ ĐÁM MÂY TÀU KHỰA (CHESSDB) + FALLBACK MINIMAX
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

// ==========================================
// KHO ĐỘNG CƠ CƠ BẢN CỦA JS (GIỮ NGUYÊN BẢN CŨ)
// ==========================================
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
    let score = 0;
    for(let r=0; r<10; r++) {
        for(let c=0; c<9; c++) {
            const p = mt[r][c];
            if(p) {
                let val = PIECE_VALUES[p.type] || 10;
                if ((p.type === '兵' || p.type === '卒') && !p.isUp) {
                    if (p.side === 'do' && r <= 4) {
                        val += 50 + (4 - r) * 20; 
                        if (c >= 3 && c <= 5) val += 40; 
                    }
                    if (p.side === 'den' && r >= 5) {
                        val += 50 + (r - 5) * 20;
                        if (c >= 3 && c <= 5) val += 40;
                    }
                }
                if (p.type === '傌' || p.type === '馬') {
                    if (c === 0 || c === 8) val -= 30;
                    if (c >= 2 && c <= 6 && r >= 2 && r <= 7) val += 60;
                    if (p.side === 'do' && (r === 1 || r === 2) && (c === 2 || c === 6)) val += 150;
                    if (p.side === 'den' && (r === 7 || r === 8) && (c === 2 || c === 6)) val += 150;
                }
                if (p.type === '炮' || p.type === '砲') {
                    if (c === 4) val += 80;
                    if (c === 3 || c === 5) val += 50;
                    if (p.side === 'do' && r === 9) val += 30;
                    if (p.side === 'den' && r === 0) val += 30;
                }
                if (p.type === '俥' || p.type === '車') {
                    if (c === 3 || c === 4 || c === 5) val += 60;
                    if (p.side === 'do' && r <= 2 && c >= 3 && c <= 5) val += 100;
                    if (p.side === 'den' && r >= 7 && c >= 3 && c <= 5) val += 100;
                }
                if (p.type === '帥' || p.type === '將') {
                    if (p.side === 'do' && r !== 9) val -= 50; 
                    if (p.side === 'den' && r !== 0) val -= 50; 
                }
                if (p.isUp) val += 150; 

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
    if (depth === 0) return evaluateBoard(mt, botSide);

    let currentSide = isMaximizing ? botSide : (botSide === 'do' ? 'den' : 'do');
    let moves = generateAllMoves(mt, currentSide, isCoUp);

    if (moves.length === 0) return isMaximizing ? -99999 + depth : 99999 - depth;

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

// BỘ MÁY FALLBACK: CHẠY BẰNG RAM CỦA KHÁCH
function calculateLocalMove(boardArray, botSide, isCoUp) {
    let mt = buildMatrix(boardArray);
    let moves = generateAllMoves(mt, botSide, isCoUp);
    if (moves.length === 0) return null;

    let pieceCount = boardArray.length;
    let DEPTH = 3; 

    if (pieceCount <= 5) {
        DEPTH = 8; 
    } else if (pieceCount <= 8) {
        DEPTH = 6;
    } else if (pieceCount <= 14) {
        DEPTH = 5;
    } else if (pieceCount <= 22) {
        DEPTH = 4;
    } else {
        DEPTH = 3; 
    }

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
        // Server trả về: move:h2e2,score:123...
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
        let isCloudUsed = false;
        let mt = buildMatrix(data.boardState);

        // 1. NẾU LÀ CỜ TƯỚNG THƯỜNG -> DÙNG PHÉP TRIỆU HỒI SIÊU MÁY TÍNH
        if (!data.isCoUp) {
            let fen = boardToFEN(mt, data.botSide);
            const cloudMove = await fetchCloudMove(fen);
            
            if (cloudMove) {
                // Kiểm tra tính hợp lệ của nước Cloud (Phòng hờ API lỗi)
                let p = mt[cloudMove.from.r][cloudMove.from.c];
                if (p && p.side === data.botSide && checkLuatWorker(p.type, data.botSide, cloudMove.from.c, cloudMove.from.r, cloudMove.to.c, cloudMove.to.r, mt, false)) {
                    bestMoveObject = {
                        from: cloudMove.from,
                        to: cloudMove.to,
                        pieceObj: p
                    };
                    isCloudUsed = true;
                }
            }
        }

        // 2. NẾU CỜ ÚP HOẶC CLOUD TỊT NGÒI -> ÉP XUNG CPU BẰNG JAVASCRIPT
        if (!bestMoveObject) {
            bestMoveObject = calculateLocalMove(data.boardState, data.botSide, data.isCoUp);
        }

        // 3. ĐÓNG GÓI KẾT QUẢ GỬI VỀ CHO UI
        if (bestMoveObject) {
            let movedPiece = { ...bestMoveObject.pieceObj, c: bestMoveObject.to.c, r: bestMoveObject.to.r };
            if (data.isCoUp && movedPiece.isUp) {
                movedPiece.isUp = false;
                const names = {'車':'xe','馬':'ma','象':'tuong','士':'si','將':'tuong_soai','砲':'phao','卒':'tot'};
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
            postMessage({ action: "done", move: null }); // Bí lù
        }
    }
};