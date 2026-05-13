// ==========================================
// FILE: botWorker.js (BẢN CHỐT HẠ V6 - VIP)
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

function checkLuatWorker(type, side, c, r, tc, tr, mt, isCoUp) {
    if (tc < 0 || tc > 8 || tr < 0 || tr > 9) return false;
    if (mt[tr][tc] && mt[tr][tc].side === side) return false; 

    const dx = Math.abs(tc - c), dy = Math.abs(tr - r);
    let luatType = type;

    // --- 🚨 KHỐI SIẾT LUẬT CỜ ÚP NƯỚC ĐẦU (QUAN TRỌNG) 🚨 ---
    if (isCoUp && mt[r][c] && mt[r][c].isUp) {
        // Nếu quân đang ÚP, ép nó đi theo luật của cái ô nó đang đứng ban đầu
        if (r === 0 || r === 9) {
            if (c === 0 || c === 8) luatType = (side === 'do' ? '俥' : '車'); // Ô Xe
            else if (c === 1 || c === 7) luatType = (side === 'do' ? '傌' : '馬'); // Ô Mã
            else if (c === 2 || c === 6) luatType = (side === 'do' ? '相' : '象'); // Ô Tượng
            else if (c === 3 || c === 5) luatType = (side === 'do' ? '仕' : '士'); // Ô Sĩ
        } else if ((r === 2 && side === 'den') || (r === 7 && side === 'do')) {
            if (c === 1 || c === 7) luatType = (side === 'do' ? '炮' : '砲'); // Ô Pháo
        } else if ((r === 3 && side === 'den') || (r === 6 && side === 'do')) {
            if (c % 2 === 0) luatType = (side === 'do' ? '兵' : '卒'); // Ô Tốt
        }
    }

    switch (luatType) {
        case '帥': case '將': return dx + dy === 1 && tc >= 3 && tc <= 5 && (side === 'do' ? tr >= 7 : tr <= 2);
        case '仕': case '士': 
            // Nếu đã lật (ngửa) thì đi chéo tự do, nếu đang ÚP thì chỉ được đi trong cung
            if (isCoUp && mt[r][c] && !mt[r][c].isUp) return dx === 1 && dy === 1;
            return dx === 1 && dy === 1 && tc >= 3 && tc <= 5 && (side === 'do' ? tr >= 7 : tr <= 2);
        case '相': case '象':
            if (dx !== 2 || dy !== 2) return false;
            if (mt[(r + tr) / 2][(c + tc) / 2]) return false; // Bị cản chân tượng
            // Nếu đã lật -> Qua sông thoải mái. Nếu đang ÚP -> Cấm qua sông
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
    for(let r=0; r<10; r++) {
        for(let c=0; c<9; c++) {
            const p = mt[r][c];
            if(p && p.side === enemySide && checkLuatWorker(p.type, enemySide, c, r, tc, tr, mt, isCoUp)) return true;
        }
    }
    return false;
}

function isSquareDefended(tc, tr, mySide, mt, isCoUp) {
    let temp = mt[tr][tc]; mt[tr][tc] = null;
    let def = false;
    for(let r=0; r<10; r++) {
        for(let c=0; c<9; c++) {
            const p = mt[r][c];
            if(p && p.side === mySide && checkLuatWorker(p.type, mySide, c, r, tc, tr, mt, isCoUp)) { def = true; break; }
        }
    }
    mt[tr][tc] = temp; return def;
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
    for(let r = Math.min(tDo.r, tDen.r) + 1; r < Math.max(tDo.r, tDen.r); r++) {
        if(mt[r][tDo.c]) count++;
    }
    return count === 0;
}

// --- HÀM ĐÁNH GIÁ V8: BẢN NĂNG SINH TỒN & SĂN NẮP ÚP ---
function evaluateBoard(mt, botSide, isCoUp) {
    let score = 0;
    let oppSide = botSide === 'do' ? 'den' : 'do';
    let myMaterial = 0, oppMaterial = 0;
    let tBot = null, tOpp = null;

    // Quét tổng tài sản
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

    const isWinning = (myMaterial - oppMaterial) > 300;

    for(let r=0; r<10; r++) {
        for(let c=0; c<9; c++) {
            const p = mt[r][c];
            if(!p) continue;

            let isMyPiece = (p.side === botSide);
            let val = PIECE_VALUES[p.type] || 10;

            // --- 🚨 LOGIC SINH TỒN & ĐE DỌA (ÁP DỤNG CHO CẢ QUÂN ÚP LẪN NGỬA) 🚨 ---
            // checkLuatWorker đã có bùa "đi theo chân nắp úp", nên isSquareAttacked sẽ tự hiểu nắp úp có thể ăn quân.
            let attackedByEnemy = isSquareAttacked(c, r, isMyPiece ? oppSide : botSide, mt, isCoUp);
            let defendedByMe = isSquareDefended(c, r, p.side, mt, isCoUp);

            if (isMyPiece) {
                if (attackedByEnemy) {
                    // 1. CỨU QUÂN CÓ TRỌNG TÂM: Bị dọa ăn -> Trừ điểm theo độ quý hiếm (Xe trừ nặng nhất để nó lo mà chạy)
                    // Nếu có bảo kê (defendedByMe) thì trừ nhẹ (20%), đéo có bảo kê trừ 95% (buộc phải vọt)
                    val -= defendedByMe ? (val * 0.2) : (val * 0.95);
                }
            } else {
                // ĐỐI VỚI QUÂN ĐỊCH (MÀY)
                if (attackedByEnemy) { // Nghĩa là đang bị Bot rình ăn
                    if (isCoUp && p.isUp) {
                        // 2. SÁT THỦ DIỆT NẮP: Thấy nắp úp hớ hênh -> Tăng điểm thèm khát để Bot nhào vô cắn
                        val += 400; 
                    } else {
                        // Thấy quân ngửa hớ hênh -> Múc theo điểm số
                        val += defendedByMe ? 50 : (val * 0.5);
                    }
                }
            }

            // --- 🧠 HACK NHÌN XUYÊN THẤU (GIỮ LẠI TỪ V7) ---
            if (isCoUp && p.isUp && isMyPiece) {
                if (currentMatchMoveCount < 6) {
                    val += (['兵','卒','仕','士','相','象'].includes(p.type)) ? 300 : -200;
                } else {
                    val += (['俥','車','炮','砲','傌','馬'].includes(p.type)) ? 800 : 0;
                }
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
                        
                        // 🔥 ĐÂY: checkLuatWorker bây giờ sẽ ép quân úp đi đúng chân ô đứng
                        if (checkLuatWorker(p.type, side, c, r, tc, tr, mt, isCoUp)) {
                            let target = mt[tr][tc];
                            mt[tr][tc] = p; mt[r][c] = null;
                            if (!laBiChieuWorker(side, mt, isCoUp) && !haiTuongDoiMat(mt)) {
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
    let moves = generateAllMoves(mt, currentSide, isCoUp);
    if (moves.length === 0) return isMaximizing ? -2000000 : 2000000;

    if (isMaximizing) {
        let maxEval = -Infinity;
        for (let m of moves) {
            let target = mt[m.to.r][m.to.c];
            mt[m.to.r][m.to.c] = m.pieceObj; mt[m.from.r][m.from.c] = null;
            let ev = minimax(mt, depth-1, alpha, beta, false, botSide, isCoUp);
            mt[m.from.r][m.from.c] = m.pieceObj; mt[m.to.r][m.to.c] = target;
            maxEval = Math.max(maxEval, ev); alpha = Math.max(alpha, ev);
            if (beta <= alpha) break;
        }
        return maxEval;
    } else {
        let minEval = Infinity;
        for (let m of moves) {
            let target = mt[m.to.r][m.to.c];
            mt[m.to.r][m.to.c] = m.pieceObj; mt[m.from.r][m.from.c] = null;
            let ev = minimax(mt, depth-1, alpha, beta, true, botSide, isCoUp);
            mt[m.from.r][m.from.c] = m.pieceObj; mt[m.to.r][m.to.c] = target;
            minEval = Math.min(minEval, ev); beta = Math.min(beta, ev);
            if (beta <= alpha) break;
        }
        return minEval;
    }
}

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
    const url = `https://www.chessdb.cn/cdb.php?action=queryall&board=${encodeURIComponent(fen)}`;
    try {
        const controller = new AbortController();
        const timeoutId = setTimeout(() => controller.abort(), 1500); // CHỐT 1.5 GIÂY
        
        const res = await fetch(url, { signal: controller.signal });
        clearTimeout(timeoutId);
        
        const text = await res.text();
        if (text.startsWith("move:")) {
            const bestMoveUCI = text.split(",")[0].split(":")[1];
            return { from: {c: bestMoveUCI.charCodeAt(0)-97, r: 9-parseInt(bestMoveUCI[1])}, to: {c: bestMoveUCI.charCodeAt(2)-97, r: 9-parseInt(bestMoveUCI[3])} };
        }
    } catch(e) { return null; }
    return null;
}
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
            // BÙA GIẢM TẢI CPU: Quân đông thì tính nông, quân ít thì tính sâu
        let depth = 3; // Mặc định tính 3 bước (Cực nhanh)
        if (data.boardState.length <= 16) depth = 4; // Còn nửa bàn cờ tính 4 bước
        if (data.boardState.length <= 8) depth = 6;  // Cờ tàn tính 6 bước để dứt điểm
            for (let m of moves) {
                let target = mt[m.to.r][m.to.c];
                mt[m.to.r][m.to.c] = m.pieceObj; mt[m.from.r][m.from.c] = null;
                let score = minimax(mt, depth-1, -Infinity, Infinity, false, data.botSide, data.isCoUp);
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
            let newBoard = data.boardState.filter(p => p && !(p.c === bestMove.from.c && p.r === bestMove.from.r) && !(p.c === bestMove.to.c && p.r === bestMove.to.r));
            newBoard.push(moved);
            postMessage({ action: "done", move: { from: bestMove.from, to: bestMove.to, newBoard } });
        }
    }
};