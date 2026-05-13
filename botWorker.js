// ==========================================
// FILE: botWorker.js (BẢN SIÊU CẤP V9 - BÍ KÍP TRUYỀN THỐNG)
// TÍNH NĂNG: SÁCH KHAI CUỘC + HACK NHÌN XUYÊN + CHỐNG CHIẾU NHÂY
// ==========================================

let positionHistory = []; 
let currentMatchMoveCount = 0; 

// --- 📚 QUYỂN SÁCH KHAI CUỘC CỜ ÚP (CỦA CAO THỦ) ---
// Định dạng: "HashBànCờ": {from:{c,r}, to:{c,r}}
// --- 📚 QUYỂN SÁCH KHAI CUỘC CỜ ÚP (CỦA CAO THỦ) ---
// Định dạng: "HashBànCờ": {from:{c,r}, to:{c,r}}
const UP_OPENING_BOOK = {
    // Nước 1: Bot cầm Đen. Sửa lại tọa độ cho đi chuẩn chân quân.
    "den_uu_tien": [
        // Lật Pháo trái (Bốc con pháo ở c:1, r:2 tiến lên 1 ô)
        { from: {c:1, r:2}, to: {c:1, r:3} }, 
        
        // Lật Pháo phải (Bốc con pháo ở c:7, r:2 tiến lên 1 ô)
        { from: {c:7, r:2}, to: {c:7, r:3} }, 
        
        // Lật Xe biên trái (Bốc con xe c:0, r:0 tiến lên 1 ô)
        { from: {c:0, r:0}, to: {c:0, r:1} }, 
        
        // Lật Chốt giữa (Bốc con chốt c:4, r:3 tiến lên 1 ô)
        { from: {c:4, r:3}, to: {c:4, r:4} },

        // THÍCH LẬT MÃ THÌ ĐÂY: Nhảy đúng chân Mã chữ L
        { from: {c:1, r:0}, to: {c:2, r:2} }, // Mã trái nhảy lên
        { from: {c:7, r:0}, to: {c:6, r:2} }  // Mã phải nhảy lên
    ]
};

const PIECE_VALUES = {
    '帥': 1000000, '將': 1000000,
    '俥': 1500, '車': 1500,
    '炮': 650,  '砲': 650,
    '傌': 600,  '馬': 600,  
    '相': 300,  '象': 250,
    '仕': 300,  '士': 250,
    '兵': 150,  '卒': 150
};

// --- HÀM HỆ THỐNG (GIỮ NGUYÊN LOGIC CHUẨN) ---
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
    
    // Mặc định dùng type truyền vào (quân thật hoặc quân giả lập)
    let luatType = type;

    // --- 🚨 BẢN FIX SIẾT LUẬT CHÂN QUÂN (CẤM SĨ ÚP RA CUNG) 🚨 ---
    if (isCoUp && mt[r][c] && mt[r][c].isUp) {
        // ÉP BOT ĐI THEO CHÂN QUÂN TẠI Ô ĐANG ĐỨNG
        if (r === 0 || r === 9) {
            if (c === 0 || c === 8) luatType = (side === 'do' ? '俥' : '車');
            else if (c === 1 || c === 7) luatType = (side === 'do' ? '傌' : '馬');
            else if (c === 2 || c === 6) luatType = (side === 'do' ? '相' : '象');
            else if (c === 3 || c === 5) luatType = (side === 'do' ? '仕' : '士');
            else if (c === 4) luatType = (side === 'do' ? '帥' : '將');
        } else if ((r === 2 && side === 'den') || (r === 7 && side === 'do')) {
            if (c === 1 || c === 7) luatType = (side === 'do' ? '炮' : '砲');
            else luatType = 'NONE'; // Không phải chân pháo thì không cho đi
        } else if ((r === 3 && side === 'den') || (r === 6 && side === 'do')) {
            if (c === 0 || c === 2 || c === 4 || c === 6 || c === 8) luatType = (side === 'do' ? '兵' : '卒');
            else luatType = 'NONE';
        } else {
            luatType = 'NONE'; // Nếu quân úp nằm ở vị trí không hợp lệ thì đứng im
        }
        if (luatType === 'NONE') return false;
    }

    switch (luatType) {
        case '帥': case '將': 
            return dx + dy === 1 && tc >= 3 && tc <= 5 && (side === 'do' ? tr >= 7 : tr <= 2);
        
        case '仕': case '士': 
            // Nếu đã lật (ngửa) -> Đi chéo tự do (đặc quyền cờ úp)
            if (isCoUp && mt[r][c] && !mt[r][c].isUp) return dx === 1 && dy === 1;
            // Nếu đang ÚP -> BUỘC phải ở trong cung và đi chéo 1 ô
            return dx === 1 && dy === 1 && tc >= 3 && tc <= 5 && (side === 'do' ? tr >= 7 : tr <= 2);

        case '相': case '象':
            if (dx !== 2 || dy !== 2) return false;
            if (mt[(r + tr) / 2][(c + tc) / 2]) return false; // Bị cản chân
            // Nếu đã lật -> Qua sông thoải mái
            if (isCoUp && mt[r][c] && !mt[r][c].isUp) return true;
            // Nếu đang ÚP -> CẤM qua sông
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
            // Nếu chưa qua sông: Chỉ đi thẳng 1 bước
            if (!daQuaSong) return dx === 0 && dy === 1 && (side === 'do' ? tr < r : tr > r);
            // Đã qua sông: Đi thẳng hoặc đi ngang 1 bước
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

// --- HÀM ĐÁNH GIÁ V9: KẾT HỢP SÁCH & HẬU THUẪN ---
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
            let baseVal = PIECE_VALUES[p.type] || 10;

            // 1. MA GIÁO: ÉP BOT BIẾT TIẾC QUÂN ÚP XỊN CỦA MÌNH
            if (isCoUp && p.isUp) {
                // Hack nhìn xuyên: Biết bên trong là hàng xịn thì định giá cực cao để nó giấu như mả tổ
                if (['俥','車','炮','砲','傌','馬'].includes(p.type)) {
                    baseVal = 2000; 
                } else {
                    baseVal = 250; // Hàng cùi (tốt, sĩ, tượng) thì cho giá thấp
                }
            }

            // ĐÁNH GIÁ TÌNH TRẠNG BỊ TẤN CÔNG / BẢO KÊ
            let attacked = isSquareAttacked(c, r, isMyPiece ? oppSide : botSide, mt, isCoUp);
            let defended = isSquareDefended(c, r, p.side, mt, isCoUp);

            // --- XỬ LÝ ĐẶC BIỆT CHO TƯỚNG (Bị chiếu là rén ngang) ---
            if (p.type === '帥' || p.type === '將') {
                if (isMyPiece && attacked) score -= 500000; // Tướng mình bị đánh: Phạt sấp mặt
                if (!isMyPiece && attacked) score += 500000; // Chiếu tướng địch: Thưởng bung nóc
                if (isMyPiece) score += baseVal; else score -= baseVal;
                continue; // Tính xong Tướng thì qua quân khác luôn
            }

            // --- NHỒI NÃO KỸ NĂNG CHẠY TRỐN & ĂN CHÙA ---
            let pieceScore = baseVal;

            if (isMyPiece) {
                // NẾU LÀ QUÂN CỦA BOT
                if (attacked) {
                    if (!defended) {
                        // Bị địch ngắm mà KHÔNG CÓ BẢO KÊ -> Trừ x4 giá trị để hoảng loạn mà né ngay
                        pieceScore -= (baseVal * 4); 
                    } else {
                        // CÓ BẢO KÊ (Kèo đổi quân): 
                        if (baseVal >= 600) {
                            // Quân xịn (Xe, Pháo, Mã) thì cấm đổi bậy bạ, phạt x1.5 ép nó né
                            pieceScore -= (baseVal * 1.5); 
                        } else {
                            // Quân cùi (Tốt, Sĩ) thì trừ nhẹ, cho phép lấy chốt đổi chốt
                            pieceScore -= (baseVal * 0.2); 
                        }
                    }
                }
                score += pieceScore;
            } else {
                // NẾU LÀ QUÂN CỦA NGƯỜI CHƠI (ĐỊCH)
                if (attacked) {
                    if (!defended) {
                        // Địch hở sườn không bảo vệ -> Giảm mạnh giá trị của nó để Bot khoái lao vào húp
                        pieceScore -= (baseVal * 1.5); 
                    } else {
                        // Địch có bảo vệ -> Giảm nhẹ thôi để ưu tiên đớp nếu kèo thơm
                        pieceScore -= 100;
                    }
                }
                score -= pieceScore;
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
                        // 1. Kiểm tra luật đi cơ bản
                        if (checkLuatWorker(p.type, side, c, r, tc, tr, mt, isCoUp)) {
                            let target = mt[tr][tc];
                            
                            // 2. GIẢ LẬP ĐI THỬ
                            mt[tr][tc] = p; 
                            mt[r][c] = null;

                            // 3. KIỂM TRA SAU KHI ĐI: Tướng mình có bị đối phương ăn không?
                            // Phải check cả "Lộ mặt tướng" và "Bị quân địch chiếu"
                            let hopLe = !laBiChieuWorker(side, mt, isCoUp) && !haiTuongDoiMat(mt);

                            // 4. HOÀN TÁC GIẢ LẬP
                            mt[r][c] = p; 
                            mt[tr][tc] = target;

                            if (hopLe) {
                                moves.push({ from: { c, r }, to: { c: tc, r: tr }, pieceObj: p });
                            }
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

    // --- BÍ KÍP: SẮP XẾP NƯỚC ĐI (ÉP BOT TÍNH NƯỚC ĂN QUÂN & LẬT CỜ TRƯỚC) ---
    moves.sort((a, b) => {
        let scoreA = 0, scoreB = 0;
        // Nếu nước đi A có ăn quân địch -> Ưu tiên A
        if (mt[a.to.r][a.to.c]) scoreA += PIECE_VALUES[mt[a.to.r][a.to.c].type] || 100;
        // Nếu nước đi B có ăn quân địch -> Ưu tiên B
        if (mt[b.to.r][b.to.c]) scoreB += PIECE_VALUES[mt[b.to.r][b.to.c].type] || 100;
        
        // Ưu tiên các nước lật cờ úp
        if (isCoUp) {
            if (a.pieceObj.isUp) scoreA += 50;
            if (b.pieceObj.isUp) scoreB += 50;
        }
        return scoreB - scoreA; // Sắp xếp giảm dần để bốc nước ngon nhất ra tính trước
    });

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
            if (beta <= alpha) break; // Cắt tỉa siêu nhanh nhờ sắp xếp ở trên
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

// --- TRẠM ĐIỀU PHỐI NÃO BỘ ---
self.onmessage = async function(e) {
    const data = e.data;
    if (data.action === "think") {
        currentMatchMoveCount++;
        let mt = buildMatrix(data.boardState);
        let bestMove = null;

        // 1. KIỂM TRA SÁCH KHAI CUỘC NỘI BỘ (3 NƯỚC ĐẦU)
        if (data.isCoUp && currentMatchMoveCount <= 2) {
            const bookMoves = UP_OPENING_BOOK["den_uu_tien"];
            const randomMove = bookMoves[Math.floor(Math.random() * bookMoves.length)];
            const p = mt[randomMove.from.r][randomMove.from.c];
            if (p && p.side === data.botSide) {
                bestMove = { from: randomMove.from, to: randomMove.to, pieceObj: p };
            }
        }

       // 2. NẾU KHÔNG CÓ TRONG SÁCH NỘI BỘ -> GỌI SÁCH TÀU 
        if (!bestMove) {
            // CẤM GỌI CHESSDB KHI ĐÁNH CỜ ÚP (Vì nó không hiểu luật lật cờ)
            if (!data.isCoUp) { 
                bestMove = await fetchCloudMove(boardToFEN(mt, data.botSide));
                if (bestMove) {
                    bestMove.pieceObj = mt[bestMove.from.r][bestMove.from.c];
                }
            }
        }

        // 3. NẾU VẪN KHÔNG CÓ -> TỰ TÍNH TOÁN MINIMAX
        if (!bestMove) {
            let moves = generateAllMoves(mt, data.botSide, data.isCoUp);
            let bestScore = -Infinity;
            // Tính nhanh hơn rồi nên cho nó nhìn xa thêm 1 nước (Cực kỳ khôn)
let depth = data.boardState.length <= 12 ? 6 : 5;
            
            for (let m of moves) {
                let target = mt[m.to.r][m.to.c];
                let movingPiece = mt[m.from.r][m.from.c];
                mt[m.to.r][m.to.c] = movingPiece; mt[m.from.r][m.from.c] = null;
                
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
            
            let finalMt = buildMatrix(newBoard);
            let finalHash = getBoardHash(finalMt);
            let finalIsCheck = laBiChieuWorker(data.botSide === 'do' ? 'den' : 'do', finalMt, data.isCoUp);

            postMessage({ action: "done", move: { from: bestMove.from, to: bestMove.to, newBoard, hash: finalHash, isCheck: finalIsCheck } });
        }
    }
};