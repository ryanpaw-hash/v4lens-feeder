// Chain decoding shared by the Worker cron and the local backfill scripts.
// No Worker-only imports: everything that talks to the chain takes an `Rpc`.

export type Rpc = <T = unknown>(method: string, params: unknown[]) => Promise<T>;
export type Log = { address: string; topics: string[]; data: string; blockNumber: string; blockTimestamp?: string; transactionHash: string };

export const POOL_MANAGER = "0x8366a39cc670b4001a1121b8f6a443a643e40951";
export const PONS_FACTORY = "0x7ed598bcef8bd9edd8c97a195c6d13f40801ec7e";
export const ZERO = "0x0000000000000000000000000000000000000000";
export const DYNAMIC_FEE = 0x800000;
export const SNIPE_WINDOW_S = 5;

export const TOPIC = {
  initialize: "0xdd466e674ea557f56295e2d0218a125ea4b4f0f6f3307b95f85e6110838d6438",
  swap: "0x40e9cecb9f5f1f1c5b9c97dec2917b7ee92e57ba5563708daca94dd84ad7112f",
  ponsLaunch: "0x8d4aad4953d0ca700d468f3753aa14432d1b35b43ec6409f051fb6aa43a89607",
  // Emitted by a Pons v2 curve once per address registered as exempt from the snipe tax.
  ponsExempt: "0xe4b7e48fbd47c2f602bacadee76ad33b16542ddb4997cfc0de04c311adcfa8c7",
  // CurveBuy(buyer, recipient, quoteIn, tokensOut, fee, tax)
  ponsBuy: "0xec36bf571f136799e8dc0b0b8bea4b04d8bd3d43de838aab0d5fc21d4cbfc455",
  // CurveSell(seller, recipient, tokensIn, quoteOut, fee, tax)
  ponsSell: "0x8113d738abdcb6b38357e9d53a54a7157861a09031b453651f0fe7fe151f59df",
  // Snipe tax charged to a buyer inside the opening window (wallet, amount in the quote asset).
  ponsSnipeTax: "0x3bc39a5562b28f5fe8f36cecabfbaa12bb969acf05717994709225fc412a9934",
} as const;

/** Currencies we can price. ETH-like are valued at the ETH price, stables at $1. */
export const PRICED: Record<string, { decimals: number; kind: "eth" | "usd"; symbol: string }> = {
  [ZERO]: { decimals: 18, kind: "eth", symbol: "ETH" },
  "0x0bd7d308f8e1639fab988df18a8011f41eacad73": { decimals: 18, kind: "eth", symbol: "WETH" },
  "0x5fc5360d0400a0fd4f2af552add042d716f1d168": { decimals: 6, kind: "usd", symbol: "USDG" },
};

const word = (data: string, i: number) => data.slice(2 + 64 * i, 2 + 64 * (i + 1));
const addrTopic = (t: string) => "0x" + t.slice(26).toLowerCase();
const int128Abs = (w: string) => {
  let v = BigInt("0x" + w.slice(32));
  if (v >> 127n) v = (1n << 128n) - v;
  return v;
};
export const hex = (n: number) => "0x" + n.toString(16);
export const tsOf = (l: Log) => (l.blockTimestamp ? parseInt(l.blockTimestamp, 16) : 0);
export const hourOf = (ts: number) => Math.floor(ts / 3600) * 3600;

/**
 * Some RPCs (including the official one) omit blockTimestamp on logs. Fill the gaps by
 * interpolating between the timestamps of the first and last block in the batch.
 */
export async function fillTimestamps(rpc: Rpc, logs: Log[], from?: number, to?: number): Promise<Log[]> {
  const missing = logs.filter((l) => !l.blockTimestamp || l.blockTimestamp === "0x0");
  if (!missing.length) return logs;
  const blocks = missing.map((l) => parseInt(l.blockNumber, 16));
  const a = from ?? Math.min(...blocks), b = to ?? Math.max(...blocks);
  const get = (n: number) => rpc<{ timestamp: string }>("eth_getBlockByNumber", [hex(n), false]).then((x) => parseInt(x.timestamp, 16));
  const [ta, tb] = await Promise.all([get(a), b === a ? Promise.resolve(0) : get(b)]);
  for (const l of missing) {
    const n = parseInt(l.blockNumber, 16);
    const t = b === a ? ta : Math.round(ta + ((n - a) * (tb - ta)) / (b - a));
    l.blockTimestamp = hex(t);
  }
  return logs;
}

export type InitEvt = { id: string; c0: string; c1: string; fee: number; tickSpacing: number; hook: string; block: number; at: number; tx: string };
export type SwapEvt = { pool: string; a0: bigint; a1: bigint; sqrt: bigint; liquidity: bigint; block: number; at: number };
export type LaunchEvt = { token: string; curve: string; creator: string; block: number; at: number; tx: string };

export function decodeInit(l: Log): InitEvt {
  return {
    id: l.topics[1].toLowerCase(),
    c0: addrTopic(l.topics[2]),
    c1: addrTopic(l.topics[3]),
    fee: parseInt(word(l.data, 0).slice(-6), 16),
    tickSpacing: parseInt(word(l.data, 1).slice(-6), 16),
    hook: "0x" + word(l.data, 2).slice(24).toLowerCase(),
    block: parseInt(l.blockNumber, 16),
    at: tsOf(l),
    tx: l.transactionHash,
  };
}

export function decodeSwap(l: Log): SwapEvt {
  return {
    pool: l.topics[1].toLowerCase(),
    a0: int128Abs(word(l.data, 0)),
    a1: int128Abs(word(l.data, 1)),
    sqrt: BigInt("0x" + word(l.data, 2)),
    liquidity: BigInt("0x" + word(l.data, 3)),
    block: parseInt(l.blockNumber, 16),
    at: tsOf(l),
  };
}

export function decodeLaunch(l: Log): LaunchEvt {
  return {
    token: addrTopic(l.topics[1]),
    curve: addrTopic(l.topics[2]),
    creator: addrTopic(l.topics[3]),
    block: parseInt(l.blockNumber, 16),
    at: tsOf(l),
    tx: l.transactionHash,
  };
}

const Q96 = 2 ** 96;

/** USD per ETH implied by an ETH/WETH–USDG swap, or null. ETH-like always sorts below USDG, so it is currency0. */
export function ethUsdFrom(c0: string, c1: string, sqrt: bigint): number | null {
  const p0 = PRICED[c0], p1 = PRICED[c1];
  if (!p0 || !p1 || p0.kind !== "eth" || p1.kind !== "usd") return null;
  const s = Number(sqrt) / Q96;
  const v = s * s * 10 ** (p0.decimals - p1.decimals);
  return v > 200 && v < 50_000 ? v : null;
}

/**
 * Address-poisoning tokens copy the first and last hex digits of a real token (USDG, WETH).
 * Swaps against them are not real USD volume, so such pools are priced at zero.
 */
export function isLookalike(c0: string, c1: string): boolean {
  const look = (real: string, other: string) => PRICED[real] && real !== other && real.slice(0, 6) === other.slice(0, 6) && real.slice(-4) === other.slice(-4);
  return !!(look(c0, c1) || look(c1, c0));
}

const unitUsd = (kind: "eth" | "usd", ethUsd: number) => (kind === "usd" ? 1 : ethUsd);

/** Volume of one swap in USD, measured on the stablecoin side first, then the ETH side. */
export function swapUsd(c0: string, c1: string, a0: bigint, a1: bigint, ethUsd: number): number {
  const p0 = PRICED[c0], p1 = PRICED[c1];
  const pick = p1?.kind === "usd" ? 1 : p0?.kind === "usd" ? 0 : p0 ? 0 : p1 ? 1 : -1;
  if (pick < 0) return 0;
  const p = pick === 0 ? p0! : p1!;
  const amt = pick === 0 ? a0 : a1;
  return (Number(amt) / 10 ** p.decimals) * unitUsd(p.kind, ethUsd);
}

/**
 * Depth ±2%: value (on the priced side) that moves the price 2% up plus 2% down, using the
 * active liquidity at the last swap. Δy ≈ L·√P·0.01 per side for token1, Δx ≈ (L/√P)·0.01 for token0.
 * Assumes liquidity stays constant across ±2%, which is exact for full-range positions.
 */
export function depthUsd(c0: string, c1: string, sqrt: bigint, liquidity: bigint, ethUsd: number): number | null {
  const p0 = PRICED[c0], p1 = PRICED[c1];
  if ((!p0 && !p1) || sqrt === 0n) return null;
  const s = Number(sqrt) / Q96;
  const L = Number(liquidity);
  if (p1) return ((0.02 * L * s) / 10 ** p1.decimals) * unitUsd(p1.kind, ethUsd);
  return ((0.02 * (L / s)) / 10 ** p0!.decimals) * unitUsd(p0!.kind, ethUsd);
}

/* ---------------- permissions & categories ---------------- */

export const flagsOf = (hook: string) => Number(BigInt(hook) & 0x3fffn);
const has = (f: number, b: number) => ((f >> b) & 1) === 1;
export const canCut = (f: number) => has(f, 3) || has(f, 2) || has(f, 1) || has(f, 0);
export const gatesSwaps = (f: number) => has(f, 7);
export const gatesLiquidity = (f: number) => has(f, 11) || has(f, 9);

/** Deterministic category from permission bits and pool fee mode — never a guess. */
export function categoryOf(flags: number, dynamicShare: number): string {
  if (has(flags, 3) || has(flags, 2)) return "Swap fee-taker";
  if (has(flags, 1) || has(flags, 0)) return "LP fee-taker";
  if (dynamicShare >= 0.5 && has(flags, 7)) return "Dynamic fee";
  if (has(flags, 7)) return "Swap gate";
  if (gatesLiquidity(flags)) return "Liquidity gate";
  if (has(flags, 6) || has(flags, 10) || has(flags, 8)) return "Observer";
  if (has(flags, 13) || has(flags, 12)) return "Pool-creation only";
  if (flags === 0) return "No callbacks";
  return "Other";
}

/* ---------------- Launch X-ray (Pons v2) ---------------- */

export type XrayWallet = {
  address: string;
  boughtPct: number;
  soldPct: number;
  quoteIn: number;
  snipeTax: number;
  exempt: boolean;
  inLaunchTx: boolean;
  creator: boolean;
  fresh: boolean | null;
  firstBuyS: number;
};
export type Xray = {
  quote: string;
  exemptCount: number;
  windowBuys: number;
  taxedBuys: number;
  insiderPct: number;
  insiderSoldPct: number;
  launchTxPct: number;
  firstMinPct: number;
  freshInsiders: number;
  raisedQuote: number;
  wallets: XrayWallet[];
  seconds: { s: number; buys: number; pct: number; insiderPct: number }[];
  totalSupply: number;
  name: string | null;
  symbol: string | null;
  coveredSeconds: number;
};

// Control, zero-width and bidi characters are stripped from on-chain names.
const UNSAFE = /[\u0000-\u001f\u007f-\u009f​-‏‪-‮⁦-⁩]/g;

function abiString(h: string | null): string | null {
  if (!h || h === "0x" || h.length < 130) return null;
  try {
    const len = parseInt(h.slice(66, 130), 16);
    const bytes = h.slice(130, 130 + len * 2);
    const s = decodeURIComponent(bytes.replace(/../g, "%$&"));
    return s.replace(UNSAFE, "").trim().slice(0, 64) || null;
  } catch {
    return null;
  }
}

export async function erc20Meta(rpc: Rpc, token: string) {
  const call = (data: string) => rpc<string>("eth_call", [{ to: token, data }, "latest"]).catch(() => null);
  const [n, s, d, ts] = await Promise.all([call("0x06fdde03"), call("0x95d89b41"), call("0x313ce567"), call("0x18160ddd")]);
  return {
    name: abiString(n),
    symbol: abiString(s),
    decimals: d && d.length >= 66 ? parseInt(d.slice(-4), 16) : null,
    totalSupply: ts && ts.length >= 66 ? BigInt(ts) : null,
  };
}

/** Quote asset of a Pons v2 launch: first word of the Launched event (zero = ETH). */
export const launchQuote = (data: string) => "0x" + word(data, 0).slice(24).toLowerCase();

export const XRAY_SPAN = 3000; // blocks, ~5 minutes on Robinhood Chain

/**
 * Reads the first five minutes of a Pons v2 launch: who was registered as exempt from the
 * snipe tax, who paid it, who bought inside the creation transaction, and how much of what
 * those insiders bought they had already sold.
 */
export async function xray(rpc: Rpc, l: LaunchEvt, quote: string): Promise<Xray> {
  const logs = await fillTimestamps(
    rpc,
    await rpc<Log[]>("eth_getLogs", [
      { address: l.curve, fromBlock: hex(l.block), toBlock: hex(l.block + XRAY_SPAN), topics: [[TOPIC.ponsExempt, TOPIC.ponsBuy, TOPIC.ponsSell, TOPIC.ponsSnipeTax]] },
    ]),
    l.block,
  );
  const meta = await erc20Meta(rpc, l.token);
  const supply = meta.totalSupply ? Number(meta.totalSupply) / 1e18 : 1e9;
  const quoteDec = quote === ZERO ? 18 : ((await erc20Meta(rpc, quote)).decimals ?? 18);
  const exempt = new Set<string>();
  for (const g of logs) if (g.topics[0] === TOPIC.ponsExempt) exempt.add(addrTopic(g.topics[1]));

  const W = new Map<string, XrayWallet>();
  const wallet = (a: string, s: number) => {
    let w = W.get(a);
    if (!w) W.set(a, (w = { address: a, boughtPct: 0, soldPct: 0, quoteIn: 0, snipeTax: 0, exempt: exempt.has(a), inLaunchTx: false, creator: a === l.creator, fresh: null, firstBuyS: s }));
    return w;
  };
  const seconds = new Map<number, { s: number; buys: number; pct: number; insiderPct: number }>();
  let windowBuys = 0, taxedBuys = 0, launchTx = 0, firstMin = 0, raised = 0, lastTs = l.at;
  for (const g of logs) {
    const t = g.topics[0];
    if (t === TOPIC.ponsExempt) continue;
    const at = tsOf(g) || l.at;
    const dt = Math.max(0, at - l.at);
    lastTs = Math.max(lastTs, at);
    if (t === TOPIC.ponsSnipeTax) {
      const w = wallet(addrTopic(g.topics[1]), dt);
      w.snipeTax += Number(BigInt("0x" + word(g.data, 0))) / 10 ** quoteDec;
      if (dt <= SNIPE_WINDOW_S) taxedBuys++;
      continue;
    }
    if (t === TOPIC.ponsSell) {
      const w = wallet(addrTopic(g.topics[1]), dt);
      w.soldPct += (Number(BigInt("0x" + word(g.data, 0))) / 1e18 / supply) * 100;
      continue;
    }
    const recipient = addrTopic(g.topics[2]);
    const quoteIn = Number(BigInt("0x" + word(g.data, 0))) / 10 ** quoteDec;
    const pct = (Number(BigInt("0x" + word(g.data, 1))) / 1e18 / supply) * 100;
    const w = wallet(recipient, dt);
    w.boughtPct += pct;
    w.quoteIn += quoteIn;
    w.firstBuyS = Math.min(w.firstBuyS, dt);
    const inLaunchTx = g.transactionHash === l.tx;
    w.inLaunchTx ||= inLaunchTx;
    if (dt <= SNIPE_WINDOW_S) windowBuys++;
    if (dt <= 60) {
      firstMin += pct;
      raised += quoteIn;
      if (inLaunchTx) launchTx += pct;
      const sec = seconds.get(dt) ?? { s: dt, buys: 0, pct: 0, insiderPct: 0 };
      sec.buys++;
      sec.pct += pct;
      if (w.exempt || inLaunchTx) sec.insiderPct += pct;
      seconds.set(dt, sec);
    }
  }
  const all = [...W.values()].filter((w) => w.boughtPct > 0 || w.soldPct > 0);
  const insiders = all.filter((w) => w.exempt || w.inLaunchTx).sort((a, b) => b.boughtPct - a.boughtPct);
  const checks = insiders.filter((w) => !w.creator).slice(0, 12);
  await Promise.all(
    checks.map(async (w) => {
      const n = await rpc<string>("eth_getTransactionCount", [w.address, "latest"]).catch(() => null);
      w.fresh = n === null ? null : parseInt(n, 16) <= 2;
    }),
  );
  const insiderBought = insiders.reduce((s, w) => s + w.boughtPct, 0);
  const insiderSold = insiders.reduce((s, w) => s + Math.min(w.soldPct, w.boughtPct), 0);
  const others = all.filter((w) => !insiders.includes(w)).sort((a, b) => b.boughtPct - a.boughtPct).slice(0, 12);
  const round = (w: XrayWallet): XrayWallet => ({
    ...w,
    boughtPct: +w.boughtPct.toFixed(3),
    soldPct: +w.soldPct.toFixed(3),
    quoteIn: +w.quoteIn.toPrecision(5),
    snipeTax: +w.snipeTax.toPrecision(4),
  });
  return {
    quote,
    exemptCount: exempt.size,
    windowBuys,
    taxedBuys,
    insiderPct: +insiderBought.toFixed(3),
    insiderSoldPct: +insiderSold.toFixed(3),
    launchTxPct: +launchTx.toFixed(3),
    firstMinPct: +firstMin.toFixed(3),
    freshInsiders: checks.filter((w) => w.fresh).length,
    raisedQuote: +raised.toPrecision(6),
    wallets: [...insiders, ...others].map(round),
    seconds: [...seconds.values()].sort((a, b) => a.s - b.s).map((s) => ({ ...s, pct: +s.pct.toFixed(3), insiderPct: +s.insiderPct.toFixed(3) })),
    totalSupply: supply,
    name: meta.name,
    symbol: meta.symbol,
    coveredSeconds: Math.min(300, lastTs - l.at),
  };
}
