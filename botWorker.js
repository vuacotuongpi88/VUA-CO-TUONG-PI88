// ==========================================
// FILE: botWorker.js (CHẠY NGẦM TRÊN MÁY KHÁCH)
// BẢN FIX LỖI: BOT BIẾT BẢO VỆ TƯỚNG VÀ TRÁNH LỘ MẶT TƯỚNG
// ==========================================

const PIECE_VALUES = {
    '帥': 10000, '將': 10000,
    '俥': 900, '車': 900,
    '炮': 450, '砲': 450,
    '傌': 400, '馬': 400,
    '相': 200, '象': 200,
    '仕': 200, '士': 200,
    '兵': 100, '卒': 100
};

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

// Hàm 1: Kiểm tra xem Tướng của phe "side" có đang bị địch chĩa súng vào không
function laBiChieuWorker(side, mt, isCoUp) {
    let tPos = null;
    // Tìm vị trí Tướng
    for (let r = 0; r < 10; r++) {
        for (let c = 0; c < 9; c++) {
            if (mt[r][c] && mt[r][c].side === side && (mt[r][c].type === '帥' || mt[r][c].type === '將')) {
                tPos = { c, r }; break;
            }
        }
        if (tPos) break;
    }
    if (!tPos) return false;

    // Quét toàn bộ quân địch xem có con nào táng được vào mặt Tướng không
    for (let r = 0; r < 10; r++) {
        for (let c = 0; c < 9; c++) {
            if (mt[r][c] && mt[r][c].side !== side) {
                if (checkLuatWorker(mt[r][c].type, mt[r][c].side, c, r, tPos.c, tPos.r, mt, isCoUp)) return true;
            }
        }
    }
    return false;
}

// Hàm 2: Kiểm tra luật "Lộ Mặt Tướng" (Hai con tướng nhìn nhau đéo có quân cản)
function haiTuongDoiMat(mt) {
    let tDo = null, tDen = null;
    for(let r=0; r<10; r++) {
        for(let c=0; c<9; c++) {
            if(mt[r][c] && mt[r][c].type === '帥') tDo = {c, r};
            if(mt[r][c] && mt[r][c].type === '將') tDen = {c, r};
        }
    }
    if(!tDo || !tDen) return false;
    
    // Nếu không cùng cột thì kệ mẹ nó
    if(tDo.c !== tDen.c) return false; 
    
    // Nếu cùng cột, đếm số quân ở giữa
    let count = 0;
    let minR = Math.min(tDo.r, tDen.r);
    let maxR = Math.max(tDo.r, tDen.r);
    for(let r = minR + 1; r < maxR; r++) {
        if(mt[r][tDo.c]) count++;
    }
    return count === 0; // Nếu bằng 0 nghĩa là 2 tướng nhìn thấy nhau -> Bất hợp pháp
}

function calculateMove(boardArray, botSide, isCoUp) {
    let mt = buildMatrix(boardArray);
    let validMoves = [];

    // Quét bàn cờ tìm quân Bot
    for (let r = 0; r < 10; r++) {
        for (let c = 0; c < 9; c++) {
            let piece = mt[r][c];
            if (piece && piece.side === botSide) {
                
                // Quét 90 ô xem đi được ô nào
                for (let tr = 0; tr < 10; tr++) {
                    for (let tc = 0; tc < 9; tc++) {
                        
                        // 1. Phải đi đúng quy tắc của quân cờ
                        if (checkLuatWorker(piece.type, botSide, c, r, tc, tr, mt, isCoUp)) {
                            
                            // GIẢ LẬP ĐI NƯỚC CỜ ĐỂ KIỂM TRA MẠNG SỐNG CỦA TƯỚNG
                            let tempTarget = mt[tr][tc]; // Lưu lại quân bị đớp (nếu có)
                            mt[tr][tc] = mt[r][c];       // Chuyển quân Bot đến chỗ mới
                            mt[r][c] = null;             // Xóa chỗ cũ
                            
                            // 2. CHECK XEM ĐI XONG CÓ BỊ ĐỊCH CHIẾU KHÔNG VÀ CÓ BỊ LỘ MẶT TƯỚNG KHÔNG?
                            let isSafe = !laBiChieuWorker(botSide, mt, isCoUp) && !haiTuongDoiMat(mt);

                            // TRẢ BÀN CỜ VỀ NHƯ CŨ (Undo) ĐỂ THỬ NƯỚC KHÁC
                            mt[r][c] = mt[tr][tc];
                            mt[tr][tc] = tempTarget;

                            // NẾU TƯỚNG AN TOÀN THÌ MỚI LƯU VÀO SỔ TAY
                            if (isSafe) {
                                let score = tempTarget && tempTarget.side !== botSide ? (PIECE_VALUES[tempTarget.type] || 10) : 0;
                                validMoves.push({
                                    from: { c, r },
                                    to: { c: tc, r: tr },
                                    score: score,
                                    pieceObj: piece
                                });
                            }
                        }
                    }
                }
            }
        }
    }

    // Nếu đéo có nước nào đi được (bị chiếu bí)
    if (validMoves.length === 0) return null;

    // Phân loại điểm, đớp con nào béo nhất
    validMoves.sort((a, b) => b.score - a.score);
    const maxScore = validMoves[0].score;
    const bestMoves = validMoves.filter(m => m.score === maxScore);
    
    // Chọn random 1 nước ngon nhất để nó đi khác nhau
    const chosenMove = bestMoves[Math.floor(Math.random() * bestMoves.length)];

    // Lật cờ úp
    let movedPiece = { ...chosenMove.pieceObj, c: chosenMove.to.c, r: chosenMove.to.r };
    if (isCoUp && movedPiece.isUp) {
        movedPiece.isUp = false;
        const names = {'車':'xe','馬':'ma','象':'tuong','士':'si','將':'tuong_soai','砲':'phao','卒':'tot'};
        if (names[movedPiece.type]) movedPiece.src = `images/${botSide}_${names[movedPiece.type]}.png`;
    }

    // Tạo mảng mới báo lại cho Client
    let newBoardArray = boardArray.filter(p => {
        if (p.c === chosenMove.from.c && p.r === chosenMove.from.r) return false;
        if (p.c === chosenMove.to.c && p.r === chosenMove.to.r) return false;
        return true;
    });
    newBoardArray.push(movedPiece);

    return {
        from: chosenMove.from,
        to: chosenMove.to,
        newBoard: newBoardArray
    };
}

// Lắng nghe lệnh từ index.html
self.onmessage = function(e) {
    const data = e.data;
    if (data.action === "think") {
        const bestMove = calculateMove(data.boardState, data.botSide, data.isCoUp);
        postMessage({ action: "done", move: bestMove });
    }
};