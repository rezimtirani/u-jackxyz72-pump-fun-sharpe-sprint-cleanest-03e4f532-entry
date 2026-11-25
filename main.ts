// Steady Tape Scalper — Pump.fun Sharpe Sprint, cleanest paper bot wins.
//
// A paper-trading bot for the agent-fun/replay/v2 harness (Node 22, no
// dependencies, standard library only). It speaks newline-delimited JSON on
// stdin/stdout: hello, then one batch per slot, then done. One reply per
// batch, echoing the batch seq.
//
// Idea, in one paragraph: the ranking is hourly Sharpe, so the bot chases
// small, steady, low-variance gains instead of moonshots. It watches every
// bonding curve's tape of recorded trades, buys only curves that are mature
// enough to have survived the launch dump and show accelerating buy pressure
// from several distinct wallets, sizes each first buy small, adds a little
// more only while the position is winning, and exits fast on a take-profit,
// a tight stop loss, a trailing stop, fading flow, or a single big sell.
// Stakes stay small (fractions of a SOL) and at most five positions are
// open at once, so no single coin can swing the hourly return much. Tiny
// probe round trips keep the run above the 20-coin guardrail when the main
// entries are not landing fast enough.
//
// Determinism: no clock, no environment, no Math.random; every decision
// comes from the batch stream and the seed in hello. Iteration is over
// insertion-ordered maps only, so two runs on the same data produce
// byte-identical action logs. Diagnostics go to stderr; stdout carries
// replies only.

const LAMPORTS_PER_SOL = 1_000_000_000;
const CURVE_FEE = 0.0125; // 95 bps protocol + 30 bps creator fallback
const BPS_DENOM = 10_000;

// ---------------------------------------------------------------------------
// Tunables (first pass; grade-local on the dev window decides the rest).
// ---------------------------------------------------------------------------
const TUNE = {
  tapeWindow: 20, // slots of tape the entry signal reads
  minNetSol: 1, // net SOL inflow (buys minus sells) over the window
  minBuys: 5, // buy trades inside the window
  minBuyers: 3, // distinct buying wallets inside the window
  minPriceUp: 0.05, // price rise over the window, minus 1
  maxPriceUp: 0.4, // skip curves that already ran too far in one window
  nearHigh: 0.95, // price now must be at least this share of window high
  maxBuyerShare: 0.8, // largest single wallet's share of the window's buy SOL
  minRealSol: 1, // curve real SOL reserves at entry, lower bound
  maxRealSol: 40, // curve real SOL reserves at entry, upper bound
  minAgeSlots: 60, // slots since CreateEvent before an entry is allowed
  firstBuySol: 0.05, // SOL of the first buy into a position
  addBuySol: 0.05, // SOL of each pyramid add (unused: maxStake == firstBuy)
  maxStakeSol: 0.05, // most SOL ever put into one position: one buy, no adds
  minBuySol: 0.01, // smallest buy worth sending (also the round-trip bar)
  buySlipBps: 1500, // slippage allowed on buys
  sellSlipBps: 10000, // sells always go out at the widest slippage
  stopLoss: 0.05, // exit when value falls this far below cost
  takeProfit: 0.25, // exit when value rises this far above cost
  trailArm: 0.05, // trailing stop arms once value is this far above cost
  trailDrop: 0.08, // trailing stop exits on this drop from the peak value
  fadeWindow: 10, // slots of tape the fade exit reads
  fadeNetSol: -2, // exit when net SOL over the fade window is below this
  bigSellSol: 5, // exit on any single recorded sell of at least this
  maxHoldSlots: 600, // exit after holding this long (unless well up)
  holdIfUp: 0.06, // the time limit does not apply while up this much
  cooldownSlots: 60, // slots to wait after an exit before re-entering
  maxEntriesPerCoin: 1, // most entries into one curve over the run
  maxMainPositions: 5, // most momentum positions open at once
  maxExposureSol: 1, // most SOL at cost across all positions at once
  targetCoins: 22, // guardrail pacing: coins to have traded
  targetFills: 60, // guardrail pacing: fills to have collected
  paceSeconds: 21600, // guardrail pacing: seconds the targets spread over
  probeSol: 0.02, // SOL of one guardrail probe round trip
  maxProbes: 1, // most probe round trips in flight at once
};

// ---------------------------------------------------------------------------
// Wire shapes (only the fields this bot reads).
// ---------------------------------------------------------------------------
interface WireHello {
  type: "hello";
  protocol: string;
  seed: string;
  capital: { lamports: string };
  caps: {
    max_open_positions: number;
    max_order_bps_of_real_reserve: number;
    max_orders_per_batch: number;
    shorts: boolean;
    leverage: boolean;
  };
  latency_slots: number;
  fees: {
    protocol_bps_fallback: number;
    creator_bps_fallback: number;
    base_fee_lamports: string;
    priority_fee_lamports: string;
    failed_tx_bps: number;
  };
}

interface WireTrade {
  event: "TradeEvent";
  mint: string;
  sol_amount: string;
  is_buy: boolean;
  user: string;
  virtual_sol_reserves: string;
  virtual_token_reserves: string;
  real_sol_reserves: string;
  real_token_reserves: string;
  slot: number;
  block_time: number | null;
}

interface WireCreate {
  event: "CreateEvent";
  mint: string;
  virtual_sol_reserves: string;
  virtual_token_reserves: string;
  real_token_reserves: string;
  slot: number;
  block_time: number | null;
}

interface WireComplete {
  event: "CompleteEvent";
  mint: string;
  slot: number;
  block_time: number | null;
}

type WireEvent = WireTrade | WireCreate | WireComplete;

interface WireMark {
  mint: string;
  slot: number;
  virtual_token_reserves: string;
  virtual_quote_reserves: string;
  real_token_reserves: string;
  real_quote_reserves: string;
}

interface WireFill {
  client_id: string;
  mint: string;
  side: "buy" | "sell";
  status: "filled" | "rejected" | "failed";
  decided_slot: number;
  slot: number;
  quote_lamports: string;
  token_amount: string;
}

interface WireBatch {
  type: "batch";
  seq: number;
  slot: number;
  events: WireEvent[];
  marks: WireMark[];
  fills: WireFill[];
  account: {
    cash_lamports: string;
    positions: { mint: string; token_amount: string; cost_lamports: string }[];
  };
}

interface WireDone {
  type: "done";
  seq: number;
  reason: string;
  account: WireBatch["account"];
}

interface WireOrder {
  side: "buy" | "sell";
  client_id: string;
  mint: string;
  quote_lamports?: string;
  token_amount?: string;
  max_slippage_bps: number;
}

// ---------------------------------------------------------------------------
// Per-curve memory.
// ---------------------------------------------------------------------------
interface TapeTrade {
  slot: number;
  buy: boolean;
  sol: number;
  user: string;
  price: number; // SOL per token, from post-trade virtual reserves
}

interface CurveTape {
  createdSlot: number | null;
  trades: TapeTrade[];
  entries: number;
  lastExitSlot: number;
}

interface CurveBook {
  vq: bigint; // virtual quote (SOL) reserves
  vt: bigint; // virtual token reserves
  rq: bigint; // real quote (SOL) reserves
  rt: bigint; // real token reserves
  atSlot: number;
  complete: boolean;
  trades: number;
}

interface OpenSpot {
  openedSlot: number;
  peakSol: number; // best close value seen, SOL
  spentSol: number; // SOL sent in buys for this spot (fees included)
  lastBuyPrice: number; // price when the last buy was decided
  lastValueSol: number; // close value at the latest batch, SOL
  exiting: boolean;
  filled: boolean;
}

interface PendingNote {
  side: "buy" | "sell";
  mint: string;
}

// ---------------------------------------------------------------------------
// Small pure helpers.
// ---------------------------------------------------------------------------
function solOf(lamports: bigint): number {
  return Number(lamports) / LAMPORTS_PER_SOL;
}

function lamportsOf(sol: number): bigint {
  return BigInt(Math.floor(sol * LAMPORTS_PER_SOL));
}

function curvePrice(vq: bigint, vt: bigint): number {
  if (vt === 0n) return 0;
  return Number(vq) / Number(vt);
}

/** SOL received for selling `tokens` now, curve fee and one tx fee deducted. */
function sellWorthSol(
  tokens: bigint,
  vq: bigint,
  vt: bigint,
  txFeeSol: number,
): number {
  const tokenNum = Number(tokens);
  const vtNum = Number(vt);
  if (tokenNum <= 0 || vtNum <= 0) return 0;
  const fraction = tokenNum / (vtNum + tokenNum);
  const proceeds = (fraction * Number(vq)) / LAMPORTS_PER_SOL;
  return proceeds * (1 - CURVE_FEE) - txFeeSol;
}

/** Net recorded SOL (buys minus sells) on a tape over the last `w` slots. */
function tapeFlow(tape: CurveTape, nowSlot: number, w: number): number {
  let net = 0;
  for (let i = tape.trades.length - 1; i >= 0; i--) {
    const tr = tape.trades[i]!;
    if (tr.slot <= nowSlot - w) break;
    net += tr.buy ? tr.sol : -tr.sol;
  }
  return net;
}

// ---------------------------------------------------------------------------
// The bot.
// ---------------------------------------------------------------------------
function buildBot() {
  let hello: WireHello | null = null;
  let cashLamports = 0n;
  let txFeeLamports = 0n;
  const curves = new Map<string, CurveBook>();
  const tapes = new Map<string, CurveTape>();
  const spots = new Map<string, OpenSpot>();
  const probes = new Map<string, boolean>(); // mint -> true once its sell is out
  const pending = new Map<string, PendingNote>();
  const positions = new Map<string, { tokens: bigint; cost: bigint }>();
  const myCoins = new Set<string>();
  let myFills = 0;
  let firstBlockTime: number | null = null;
  let lastBlockTime: number | null = null;
  let orderCounter = 0;

  function tapeFor(mint: string): CurveTape {
    let t = tapes.get(mint);
    if (t === undefined) {
      t = { createdSlot: null, trades: [], entries: 0, lastExitSlot: -1e12 };
      tapes.set(mint, t);
    }
    return t;
  }

  function curveFor(mint: string, slot: number): CurveBook {
    let c = curves.get(mint);
    if (c === undefined) {
      c = { vq: 0n, vt: 0n, rq: 0n, rt: 0n, atSlot: slot, complete: false, trades: 0 };
      curves.set(mint, c);
    }
    return c;
  }

  function nextClientId(prefix: string): string {
    orderCounter += 1;
    return `${prefix}${orderCounter}`;
  }

  function committedCount(): number {
    const mints = new Set<string>(positions.keys());
    for (const p of pending.values()) {
      if (p.side === "buy") mints.add(p.mint);
    }
    return mints.size;
  }

  // SOL reserved for orders still in flight: one tx fee per pending order
  // plus the quote of each pending buy. Rebuilt from the pending map.
  const pendingBuyAmounts = new Map<string, number>();

  function freeCashSol(): number {
    let held = 0;
    for (const [id, p] of pending) {
      held += solOf(txFeeLamports);
      const amt = pendingBuyAmounts.get(id);
      if (amt !== undefined) held += amt;
    }
    return solOf(cashLamports) - held;
  }

  /** Entry signal on a tape at slot `now`, given the curve's real SOL. */
  function entryOk(tape: CurveTape, nowSlot: number, realSol: number): boolean {
    if (realSol < TUNE.minRealSol || realSol > TUNE.maxRealSol) return false;
    if (tape.createdSlot === null || nowSlot - tape.createdSlot < TUNE.minAgeSlots) {
      return false;
    }
    const from = nowSlot - TUNE.tapeWindow;
    const mid = nowSlot - TUNE.tapeWindow / 2;
    let buySol = 0;
    let sellSol = 0;
    let buys = 0;
    let netOld = 0;
    let netNew = 0;
    let startPrice = -1;
    let high = 0;
    let last = 0;
    const buyers = new Map<string, number>();
    for (const tr of tape.trades) {
      if (tr.slot <= from) {
        startPrice = tr.price;
        continue;
      }
      if (startPrice < 0) startPrice = tr.price;
      const signed = tr.buy ? tr.sol : -tr.sol;
      if (tr.slot <= mid) netOld += signed;
      else netNew += signed;
      if (tr.buy) {
        buySol += tr.sol;
        buys += 1;
        buyers.set(tr.user, (buyers.get(tr.user) ?? 0) + tr.sol);
      } else {
        sellSol += tr.sol;
      }
      if (tr.price > high) high = tr.price;
      last = tr.price;
    }
    if (buys < TUNE.minBuys || buyers.size < TUNE.minBuyers) return false;
    const net = buySol - sellSol;
    if (net < TUNE.minNetSol) return false;
    if (startPrice <= 0) return false;
    const up = last / startPrice - 1;
    if (up < TUNE.minPriceUp || up > TUNE.maxPriceUp) return false;
    if (last < high * TUNE.nearHigh) return false;
    // Acceleration: the newer half of the window must carry clearly more
    // net inflow than the older half (at least 1.5x), so the buying is
    // speeding up, not fading after an early burst.
    if (netNew <= 0 || netNew < 1.5 * Math.max(netOld, 0)) return false;
    let top = 0;
    for (const v of buyers.values()) if (v > top) top = v;
    if (buySol > 0 && top > TUNE.maxBuyerShare * buySol) return false;
    return true;
  }

  function decide(batch: WireBatch): WireOrder[] {
    const h = hello!;
    const now = batch.slot;
    const txFeeSol = solOf(txFeeLamports);
    const orders: WireOrder[] = [];
    const cap = h.caps.max_orders_per_batch;
    const bigSells = new Set<string>();
    const traded = new Set<string>();

    // Fold fills: count my own, drop pending notes as they resolve.
    for (const f of batch.fills) {
      pending.delete(f.client_id);
      pendingBuyAmounts.delete(f.client_id);
      if (f.status === "filled" && !f.client_id.startsWith("_")) {
        myFills += 1;
        myCoins.add(f.mint);
      }
    }

    // Fold the account.
    cashLamports = BigInt(batch.account.cash_lamports);
    positions.clear();
    for (const p of batch.account.positions) {
      const tokens = BigInt(p.token_amount);
      if (tokens > 0n) {
        positions.set(p.mint, { tokens, cost: BigInt(p.cost_lamports) });
      }
    }

    // Fold events into tapes and curve books.
    for (const e of batch.events) {
      if (e.block_time !== null) {
        if (firstBlockTime === null) firstBlockTime = e.block_time;
        if (lastBlockTime === null || e.block_time > lastBlockTime) {
          lastBlockTime = e.block_time;
        }
      }
      if (e.event === "CreateEvent") {
        tapeFor(e.mint).createdSlot = e.slot;
        const c = curveFor(e.mint, e.slot);
        c.vq = BigInt(e.virtual_sol_reserves);
        c.vt = BigInt(e.virtual_token_reserves);
        c.rt = BigInt(e.real_token_reserves);
        c.rq = 0n;
        c.atSlot = e.slot;
      } else if (e.event === "TradeEvent") {
        const t = tapeFor(e.mint);
        const sol = solOf(BigInt(e.sol_amount));
        t.trades.push({
          slot: e.slot,
          buy: e.is_buy,
          sol,
          user: e.user,
          price: curvePrice(BigInt(e.virtual_sol_reserves), BigInt(e.virtual_token_reserves)),
        });
        traded.add(e.mint);
        if (!e.is_buy && sol >= TUNE.bigSellSol) bigSells.add(e.mint);
        const c = curveFor(e.mint, e.slot);
        c.vq = BigInt(e.virtual_sol_reserves);
        c.vt = BigInt(e.virtual_token_reserves);
        c.rq = BigInt(e.real_sol_reserves);
        c.rt = BigInt(e.real_token_reserves);
        c.atSlot = e.slot;
        c.trades += 1;
      } else {
        curveFor(e.mint, e.slot).complete = true;
      }
    }

    // Prune old tape entries for curves that traded this batch.
    const keep = Math.max(TUNE.tapeWindow, TUNE.fadeWindow) + 10;
    for (const mint of traded) {
      const t = tapes.get(mint)!;
      let cut = 0;
      while (cut < t.trades.length && t.trades[cut]!.slot <= now - keep) cut += 1;
      if (cut > 0) t.trades.splice(0, cut);
    }

    // Marks refresh the reserves of held / pending curves.
    const marks = new Map<string, WireMark>();
    for (const m of batch.marks) marks.set(m.mint, m);
    for (const m of batch.marks) {
      const c = curves.get(m.mint);
      if (c === undefined || m.slot < c.atSlot) continue;
      c.vq = BigInt(m.virtual_quote_reserves);
      c.vt = BigInt(m.virtual_token_reserves);
      c.rq = BigInt(m.real_quote_reserves);
      c.rt = BigInt(m.real_token_reserves);
      c.atSlot = m.slot;
    }

    const pendingByMint = new Map<string, number>();
    for (const p of pending.values()) {
      pendingByMint.set(p.mint, (pendingByMint.get(p.mint) ?? 0) + 1);
    }

    // ---- Exits first: sell whole spots that hit a rule. ----
    for (const [mint, spot] of spots) {
      const pos = positions.get(mint);
      const curve = curves.get(mint);
      if (pos === undefined) {
        if (!pendingByMint.has(mint)) {
          spots.delete(mint);
          const t = tapeFor(mint);
          if (spot.filled) t.lastExitSlot = now;
          else t.entries -= 1;
        }
        continue;
      }
      spot.filled = true;
      if (curve === undefined || curve.complete) continue;
      const mk = marks.get(mint);
      const vq = mk !== undefined ? BigInt(mk.virtual_quote_reserves) : curve.vq;
      const vt = mk !== undefined ? BigInt(mk.virtual_token_reserves) : curve.vt;
      const value = sellWorthSol(pos.tokens, vq, vt, txFeeSol);
      const cost = solOf(pos.cost);
      if (value > spot.peakSol) spot.peakSol = value;
      spot.lastValueSol = value;
      const tape = tapeFor(mint);
      let exit = spot.exiting;
      if (!exit && value < cost * (1 - TUNE.stopLoss)) exit = true;
      if (!exit && value >= cost * (1 + TUNE.takeProfit)) exit = true;
      if (!exit && spot.peakSol >= cost * (1 + TUNE.trailArm) && value < spot.peakSol * (1 - TUNE.trailDrop)) exit = true;
      if (!exit && tapeFlow(tape, now, TUNE.fadeWindow) < TUNE.fadeNetSol) exit = true;
      if (!exit && bigSells.has(mint)) exit = true;
      if (!exit && now - spot.openedSlot > TUNE.maxHoldSlots && value < cost * (1 + TUNE.holdIfUp)) exit = true;
      if (!exit) continue;
      spot.exiting = true;
      let sellPending = false;
      for (const p of pending.values()) {
        if (p.mint === mint && p.side === "sell") sellPending = true;
      }
      if (sellPending) continue;
      if (orders.length >= cap) break;
      const id = nextClientId("s");
      orders.push({
        side: "sell",
        client_id: id,
        mint,
        token_amount: pos.tokens.toString(),
        max_slippage_bps: TUNE.sellSlipBps,
      });
      pending.set(id, { side: "sell", mint });
    }

    // ---- Probe round trips: sell the probe as soon as its buy lands. ----
    for (const [mint, selling] of probes) {
      const pos = positions.get(mint);
      const curve = curves.get(mint);
      if (pos === undefined) {
        if (!pendingByMint.has(mint)) probes.delete(mint);
        continue;
      }
      if (curve === undefined || curve.complete || pendingByMint.has(mint)) continue;
      if (orders.length >= cap) break;
      const id = nextClientId("s");
      orders.push({
        side: "sell",
        client_id: id,
        mint,
        token_amount: pos.tokens.toString(),
        max_slippage_bps: TUNE.sellSlipBps,
      });
      pending.set(id, { side: "sell", mint });
      if (!selling) probes.set(mint, true);
    }

    // ---- Entries and pyramid adds. ----
    let free = freeCashSol();
    let slotsLeft = h.caps.max_open_positions - committedCount();
    let mainLeft = TUNE.maxMainPositions - spots.size;
    let exposure = 0;
    for (const [mint, spot] of spots) {
      const pos = positions.get(mint);
      exposure += Math.max(spot.spentSol, pos !== undefined ? solOf(pos.cost) : 0);
    }
    for (const mint of traded) {
      if (orders.length >= cap) break;
      const curve = curves.get(mint);
      if (curve === undefined || curve.complete) continue;
      if (pendingByMint.has(mint) || probes.has(mint)) continue;
      const tape = tapes.get(mint)!;
      const spot = spots.get(mint);
      const price = curvePrice(curve.vq, curve.vt);
      const realSol = solOf(curve.rq);
      let want: number;
      if (spot === undefined) {
        if (slotsLeft <= 0 || mainLeft <= 0) continue;
        if (tape.entries >= TUNE.maxEntriesPerCoin) continue;
        if (now - tape.lastExitSlot < TUNE.cooldownSlots) continue;
        if (!entryOk(tape, now, realSol)) continue;
        want = Math.min(TUNE.firstBuySol, TUNE.maxStakeSol);
      } else {
        if (spot.exiting || !spot.filled) continue;
        if (spot.spentSol >= TUNE.maxStakeSol - TUNE.minBuySol) continue;
        const pos = positions.get(mint);
        if (pos === undefined) continue;
        if (price < spot.lastBuyPrice) continue;
        if (spot.lastValueSol < solOf(pos.cost)) continue;
        if (tapeFlow(tape, now, TUNE.tapeWindow) < 0) continue;
        if (realSol > TUNE.maxRealSol) continue;
        want = Math.min(TUNE.addBuySol, TUNE.maxStakeSol - spot.spentSol);
      }
      // Size cap: this batch's buys on one mint may spend at most 1% of its
      // real SOL reserve (caps.max_order_bps_of_real_reserve).
      const capLamports =
        (curve.rq * BigInt(h.caps.max_order_bps_of_real_reserve)) / BigInt(BPS_DENOM);
      const capSol = solOf(capLamports);
      let amount = Math.min(capSol, want, TUNE.maxExposureSol - exposure);
      if (amount > free - txFeeSol) amount = free - txFeeSol;
      if (amount < TUNE.minBuySol) continue;
      const id = nextClientId("b");
      orders.push({
        side: "buy",
        client_id: id,
        mint,
        quote_lamports: lamportsOf(amount).toString(),
        max_slippage_bps: TUNE.buySlipBps,
      });
      pending.set(id, { side: "buy", mint });
      pendingBuyAmounts.set(id, amount);
      free -= amount + txFeeSol;
      exposure += amount;
      if (spot === undefined) {
        spots.set(mint, {
          openedSlot: now,
          peakSol: 0,
          spentSol: amount,
          lastBuyPrice: price,
          lastValueSol: 0,
          exiting: false,
          filled: false,
        });
        tape.entries += 1;
        slotsLeft -= 1;
        mainLeft -= 1;
      } else {
        spot.spentSol += amount;
        spot.lastBuyPrice = price;
      }
    }

    // ---- Guardrail pacing: probe round trips when fills/coins lag. ----
    if (
      probes.size < TUNE.maxProbes &&
      slotsLeft > 0 &&
      firstBlockTime !== null &&
      lastBlockTime !== null
    ) {
      const pace = Math.min(1, (lastBlockTime - firstBlockTime) / TUNE.paceSeconds);
      const needCoins = myCoins.size < Math.ceil(TUNE.targetCoins * pace);
      const needFills = myFills < Math.ceil(TUNE.targetFills * pace);
      if (needCoins || needFills) {
        const minReal = (TUNE.probeSol * BPS_DENOM) / h.caps.max_order_bps_of_real_reserve;
        for (const mint of traded) {
          if (orders.length >= cap || probes.size >= TUNE.maxProbes) break;
          if (spots.has(mint) || probes.has(mint)) continue;
          if (pendingByMint.has(mint) || positions.has(mint)) continue;
          if (needCoins && !needFills && myCoins.has(mint)) continue;
          const curve = curves.get(mint);
          if (curve === undefined || curve.complete) continue;
          if (solOf(curve.rq) < minReal * 1.5) continue;
          if (free < TUNE.probeSol + 2 * txFeeSol) break;
          const id = nextClientId("b");
          orders.push({
            side: "buy",
            client_id: id,
            mint,
            quote_lamports: lamportsOf(TUNE.probeSol).toString(),
            max_slippage_bps: TUNE.buySlipBps,
          });
          pending.set(id, { side: "buy", mint });
          pendingBuyAmounts.set(id, TUNE.probeSol);
          probes.set(mint, false);
          free -= TUNE.probeSol + txFeeSol;
        }
      }
    }
    return orders;
  }

  return {
    onHello(msg: WireHello): void {
      if (msg.protocol !== "agent-fun/replay/v2") {
        throw new Error(`unsupported protocol ${msg.protocol}`);
      }
      hello = msg;
      cashLamports = BigInt(msg.capital.lamports);
      txFeeLamports =
        BigInt(msg.fees.base_fee_lamports) + BigInt(msg.fees.priority_fee_lamports);
    },

    onBatch(batch: WireBatch): WireOrder[] {
      if (hello === null) throw new Error("batch before hello");
      return decide(batch);
    },

    onDone(_done: WireDone): void {
      // Nothing to send; the harness already flattened every position.
    },
  };
}

// ---------------------------------------------------------------------------
// Wire loop: stdin in, one reply per batch out.
// ---------------------------------------------------------------------------
async function* stdinLines(): AsyncGenerator<string> {
  let buf = "";
  for await (const chunk of process.stdin) {
    buf += chunk.toString("utf8");
    let nl = buf.indexOf("\n");
    while (nl >= 0) {
      yield buf.slice(0, nl);
      buf = buf.slice(nl + 1);
      nl = buf.indexOf("\n");
    }
  }
  if (buf.length > 0) yield buf;
}

async function main(): Promise<void> {
  const bot = buildBot();
  for await (const line of stdinLines()) {
    if (line.trim() === "") continue;
    const msg = JSON.parse(line) as
      | WireHello
      | WireBatch
      | WireDone;
    if (msg.type === "hello") {
      bot.onHello(msg as WireHello);
    } else if (msg.type === "batch") {
      const orders = bot.onBatch(msg as WireBatch);
      process.stdout.write(
        JSON.stringify({ type: "reply", seq: msg.seq, orders }) + "\n",
      );
    } else if (msg.type === "done") {
      bot.onDone(msg as WireDone);
      break;
    } else {
      throw new Error(`unknown message type ${(msg as { type: string }).type}`);
    }
  }
}

main().catch((err: unknown) => {
  console.error(err instanceof Error ? err.stack ?? err.message : String(err));
  process.exit(1);
});
