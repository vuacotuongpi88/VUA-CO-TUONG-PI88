const AI_TT = new Map();
const AI_CACHE_MAX = 6000;

function otherSide(side) {
  return side === "do" ? "den" : "do";
}

function cloneMatrix(mt) {
  return mt.map(row => row.map(p => p ? { side: p.side, type: p.type } : null));
}

function inBoard(c, r) {
  return c >= 0 && c < 9 && r >= 0 && r < 10;
}

function kind(type) {
  if (["帥", "帅", "將", "将"].includes(type)) return "king";
  if (["俥", "車", "车"].includes(type)) return "rook";
  if (["傌", "馬", "马"].includes(type)) return "horse";
  if (["相", "象"].includes(type)) return "elephant";
  if (["仕", "士"].includes(type)) return "advisor";
  if (["炮", "砲"].includes(type)) return "cannon";
  if (["兵", "卒"].includes(type)) return "pawn";
  return "unknown";
}

function valueOf(type) {
  const k = kind(type);
  return {
    king: 100000,
    rook: 950,
    cannon: 520,
    horse: 480,
    elephant: 230,
    advisor: 230,
    pawn: 120,
    unknown: 40
  }[k] || 40;
}

function isPalace(side, c, r) {
  if (c < 3 || c > 5) return false;
  if (side === "do") return r >= 7 && r <= 9;
  return r >= 0 && r <= 2;
}

function countBetween(mt, c1, r1, c2, r2) {
  let n = 0;

  if (c1 === c2) {
    const a = Math.min(r1, r2) + 1;
    const b = Math.max(r1, r2) - 1;
    for (let r = a; r <= b; r++) {
      if (mt[r][c1]) n++;
    }
    return n;
  }

  if (r1 === r2) {
    const a = Math.min(c1, c2) + 1;
    const b = Math.max(c1, c2) - 1;
    for (let c = a; c <= b; c++) {
      if (mt[r1][c]) n++;
    }
    return n;
  }

  return 999;
}

function canMove(type, side, c1, r1, c2, r2, mt) {
  if (!inBoard(c1, r1) || !inBoard(c2, r2)) return false;

  const p = mt[r1][c1];
  const t = mt[r2][c2];

  if (!p || p.side !== side) return false;
  if (t && t.side === side) return false;

  const dc = c2 - c1;
  const dr = r2 - r1;
  const ax = Math.abs(dc);
  const ay = Math.abs(dr);
  const k = kind(type);

  if (k === "king") {
    if (!isPalace(side, c2, r2)) return false;
    return ax + ay === 1;
  }

  if (k === "advisor") {
    if (!isPalace(side, c2, r2)) return false;
    return ax === 1 && ay === 1;
  }

  if (k === "elephant") {
    if (!(ax === 2 && ay === 2)) return false;

    if (side === "do" && r2 < 5) return false;
    if (side === "den" && r2 > 4) return false;

    const bc = c1 + dc / 2;
    const br = r1 + dr / 2;

    return !mt[br][bc];
  }

  if (k === "horse") {
    if (!((ax === 2 && ay === 1) || (ax === 1 && ay === 2))) return false;

    let bc = c1;
    let br = r1;

    if (ax === 2) bc = c1 + Math.sign(dc);
    if (ay === 2) br = r1 + Math.sign(dr);

    return !mt[br][bc];
  }

  if (k === "rook") {
    if (c1 !== c2 && r1 !== r2) return false;
    return countBetween(mt, c1, r1, c2, r2) === 0;
  }

  if (k === "cannon") {
    if (c1 !== c2 && r1 !== r2) return false;

    const between = countBetween(mt, c1, r1, c2, r2);

    if (t) return between === 1;
    return between === 0;
  }

  if (k === "pawn") {
    if (side === "do") {
      if (dc === 0 && dr === -1) return true;
      if (r1 <= 4 && dr === 0 && ax === 1) return true;
      return false;
    }

    if (dc === 0 && dr === 1) return true;
    if (r1 >= 5 && dr === 0 && ax === 1) return true;
    return false;
  }

  return false;
}

function findKing(side, mt) {
  const keys = side === "do" ? ["帥", "帅"] : ["將", "将"];

  for (let r = 0; r < 10; r++) {
    for (let c = 0; c < 9; c++) {
      const p = mt[r][c];
      if (p && p.side === side && keys.includes(p.type)) {
        return { c, r };
      }
    }
  }

  return null;
}

function twoKingsFacing(mt) {
  const a = findKing("do", mt);
  const b = findKing("den", mt);

  if (!a || !b) return false;
  if (a.c !== b.c) return false;

  const from = Math.min(a.r, b.r) + 1;
  const to = Math.max(a.r, b.r) - 1;

  for (let r = from; r <= to; r++) {
    if (mt[r][a.c]) return false;
  }

  return true;
}

function inCheck(side, mt) {
  if (twoKingsFacing(mt)) return true;

  const king = findKing(side, mt);
  if (!king) return true;

  const enemy = otherSide(side);

  for (let r = 0; r < 10; r++) {
    for (let c = 0; c < 9; c++) {
      const p = mt[r][c];
      if (!p || p.side !== enemy) continue;

      if (canMove(p.type, p.side, c, r, king.c, king.r, mt)) {
        return true;
      }
    }
  }

  return false;
}

function applyMove(mt, move) {
  const next = cloneMatrix(mt);
  next[move.to.r][move.to.c] = next[move.from.r][move.from.c];
  next[move.from.r][move.from.c] = null;
  return next;
}

function safeAfterMove(side, move, mt) {
  const next = applyMove(mt, move);
  return !twoKingsFacing(next) && !inCheck(side, next);
}

function collectMoves(side, mt) {
  const out = [];

  for (let r1 = 0; r1 < 10; r1++) {
    for (let c1 = 0; c1 < 9; c1++) {
      const p = mt[r1][c1];
      if (!p || p.side !== side) continue;

      for (let r2 = 0; r2 < 10; r2++) {
        for (let c2 = 0; c2 < 9; c2++) {
          const target = mt[r2][c2];
          if (target && target.side === side) continue;

          if (!canMove(p.type, side, c1, r1, c2, r2, mt)) continue;

          const move = {
            from: { c: c1, r: r1 },
            to: { c: c2, r: r2 },
            piece: p,
            target: target || null
          };

          if (!safeAfterMove(side, move, mt)) continue;

          out.push(move);
        }
      }
    }
  }

  return out;
}

function posBonus(p, c, r) {
  if (!p) return 0;

  const k = kind(p.type);
  const center = 4 - Math.abs(c - 4);
  let s = 0;

  if (k === "rook" || k === "cannon" || k === "horse") {
    s += center * 8;
  }

  if (k === "pawn") {
    if (p.side === "do") {
      if (r <= 4) s += 80;
      s += (9 - r) * 8;
    } else {
      if (r >= 5) s += 80;
      s += r * 8;
    }
  }

  if (k === "king") {
    s -= Math.abs(c - 4) * 10;
  }

  return s;
}

function evaluate(mt, aiSide) {
  const enemy = otherSide(aiSide);

  if (!findKing(aiSide, mt)) return -9999999;
  if (!findKing(enemy, mt)) return 9999999;

  let score = 0;

  for (let r = 0; r < 10; r++) {
    for (let c = 0; c < 9; c++) {
      const p = mt[r][c];
      if (!p) continue;

      let v = valueOf(p.type) + posBonus(p, c, r);

      if (p.side === aiSide) score += v;
      else score -= v;
    }
  }

  if (inCheck(enemy, mt)) score += 260;
  if (inCheck(aiSide, mt)) score -= 360;

  return score;
}

function quickScore(move, side, mt) {
  let s = 0;

  if (move.target) {
    s += valueOf(move.target.type) * 12;
    s -= valueOf(move.piece.type) * 0.25;
  }

  const next = applyMove(mt, move);
  const enemy = otherSide(side);

  if (!findKing(enemy, next)) s += 999999;
  if (inCheck(enemy, next)) s += 480;

  s += posBonus(move.piece, move.to.c, move.to.r);

  return s;
}

function orderMoves(moves, side, mt) {
  return moves
    .map(m => ({ ...m, quickScore: quickScore(m, side, mt) }))
    .sort((a, b) => b.quickScore - a.quickScore);
}

function hashBoard(mt, side) {
  let s = side + "|";

  for (let r = 0; r < 10; r++) {
    for (let c = 0; c < 9; c++) {
      const p = mt[r][c];
      s += p ? p.side[0] + p.type : ".";
    }
  }

  return s;
}

function trimCache() {
  if (AI_TT.size <= AI_CACHE_MAX) return;

  let i = 0;
  const remove = Math.floor(AI_CACHE_MAX * 0.25);

  for (const key of AI_TT.keys()) {
    AI_TT.delete(key);
    i++;
    if (i >= remove) break;
  }
}

function alphaBeta(mt, depth, sideToMove, aiSide, alpha, beta, deadline, topLimit) {
  if (performance.now() >= deadline) return evaluate(mt, aiSide);

  const enemy = otherSide(aiSide);

  if (!findKing(aiSide, mt)) return -9999999;
  if (!findKing(enemy, mt)) return 9999999;

  if (depth <= 0) return evaluate(mt, aiSide);

  const key = hashBoard(mt, sideToMove) + "|d" + depth;
  const cached = AI_TT.get(key);

  if (cached && cached.depth >= depth) {
    return cached.score;
  }

  let moves = collectMoves(sideToMove, mt);

  if (!moves.length) {
    return sideToMove === aiSide ? -8888888 : 8888888;
  }

  moves = orderMoves(moves, sideToMove, mt).slice(0, topLimit);

  const maxing = sideToMove === aiSide;
  let best = maxing ? -Infinity : Infinity;

  for (const move of moves) {
    if (performance.now() >= deadline) break;

    const next = applyMove(mt, move);
    const val = alphaBeta(
      next,
      depth - 1,
      otherSide(sideToMove),
      aiSide,
      alpha,
      beta,
      deadline,
      topLimit
    );

    if (maxing) {
      best = Math.max(best, val);
      alpha = Math.max(alpha, val);
    } else {
      best = Math.min(best, val);
      beta = Math.min(beta, val);
    }

    if (beta <= alpha) break;
  }

  AI_TT.set(key, {
    depth,
    score: best,
    at: Date.now()
  });

  trimCache();

  return best;
}

function sameMove(a, b) {
  return !!a && !!b &&
    a.from.c === b.from.c &&
    a.from.r === b.from.r &&
    a.to.c === b.to.c &&
    a.to.r === b.to.r;
}

function findBestMove(board, side, maxDepth, timeLimitMs, topLimit) {
  const start = performance.now();
  const deadline = start + Math.max(80, Number(timeLimitMs || 250));

  const mt = cloneMatrix(board || []);
  const aiSide = side || "den";

  let moves = collectMoves(aiSide, mt);

  if (!moves.length) return null;

  moves = orderMoves(moves, aiSide, mt).slice(0, topLimit || 10);

  const bestKey = hashBoard(mt, aiSide) + "|best";
  const cached = AI_TT.get(bestKey);

  if (cached && cached.move) {
    const legal = moves.find(m => sameMove(m, cached.move));
    if (legal && Date.now() - cached.at < 10 * 60 * 1000) {
      return {
        move: legal,
        depth: cached.depth || 0,
        score: cached.score || 0,
        ms: 0,
        cached: true
      };
    }
  }

  let bestMove = moves[0];
  let bestScore = -Infinity;
  let reachedDepth = 0;

  for (let d = 1; d <= maxDepth; d++) {
    if (performance.now() >= deadline) break;

    let localBest = bestMove;
    let localScore = -Infinity;

    for (const move of moves) {
      if (performance.now() >= deadline) break;

      const next = applyMove(mt, move);
      const score = alphaBeta(
        next,
        d - 1,
        otherSide(aiSide),
        aiSide,
        -Infinity,
        Infinity,
        deadline,
        topLimit || 10
      );

      move.finalScore = score;

      if (score > localScore) {
        localScore = score;
        localBest = move;
      }
    }

    if (performance.now() < deadline) {
      bestMove = localBest;
      bestScore = localScore;
      reachedDepth = d;

      moves.sort((a, b) => {
        if (sameMove(a, bestMove)) return -1;
        if (sameMove(b, bestMove)) return 1;
        return (b.finalScore || 0) - (a.finalScore || 0);
      });
    }
  }

  AI_TT.set(bestKey, {
    move: bestMove,
    score: bestScore,
    depth: reachedDepth,
    at: Date.now()
  });

  return {
    move: bestMove,
    depth: reachedDepth,
    score: bestScore,
    ms: Math.round(performance.now() - start),
    cached: false
  };
}

self.onmessage = function (e) {
  const msg = e.data || {};

  if (msg.type !== "findMove") return;

  try {
    const result = findBestMove(
      msg.board,
      msg.side || "den",
      Number(msg.maxDepth || 4),
      Number(msg.timeLimitMs || 250),
      Number(msg.topMovesLimit || 10)
    );

    self.postMessage({
      id: msg.id,
      ok: true,
      result
    });
  } catch (err) {
    self.postMessage({
      id: msg.id,
      ok: false,
      error: err?.message || String(err || "AI worker error")
    });
  }
};
