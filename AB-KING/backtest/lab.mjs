/* ================================================================
   AB-KING مختبر المحاكاة (Backtest Lab)
   - يغذّي المحرك الحقيقي بشموع تاريخية من KuCoin كنبضات سعر
   - يقيس: عدد الصفقات، الصافي، الرسوم، أعمق تعليق، الانحدار الأقصى
   - المحاكاة حقيقية: نفس دوال القرار، نفس الرسوم، نفس منطق الجني
   استخدام:
     node lab/run.mjs --symbol 1000000PEPEUSDTM --days 3 --gran 1
   ================================================================ */
import { build } from 'esbuild';
import { fileURLToPath } from 'node:url';
import path from 'node:path';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, '..');

// Build the engine as an importable ESM module (no server, no WS autostart)
const outfile = path.resolve(__dirname, '_engine-bundle.mjs');
await build({
  entryPoints: [path.join(ROOT, 'app/ab-king/src/engine.ts')],
  bundle: true, platform: 'node', format: 'esm',
  target: 'node18', outfile,
  define: { 'process.env.ABKING_SERVER': '"1"' },
  logLevel: 'silent',
});
// block live network inside engine (paper sim must be deterministic)
const realFetch = globalThis.fetch;
globalThis.fetch = () => Promise.reject(new Error('lab: offline'));
// node shim — engine expects browser-ish env
globalThis.localStorage = { _d: {}, getItem(k) { return k in this._d ? this._d[k] : null; },
  setItem(k, v) { this._d[k] = String(v); }, removeItem(k) { delete this._d[k]; }, clear() { this._d = {}; } };
const E = await import('file://' + outfile);
const S = E.S;

// ---------- CLI ----------
const args = process.argv.slice(2);
const arg = (n, d) => { const i = args.indexOf('--' + n); return i >= 0 ? args[i + 1] : d; };
const SYMBOL = arg('symbol', 'XBTUSDTM');
const DAYS = +arg('days', 3);
const GRAN = +arg('gran', 1);          // 1-minute candles
const BAL = +arg('balance', 200);
const MAKER_FEE = 0.0002, TAKER_FEE = 0.0006;

// ---------- historical data ----------
async function fetchKlinesAll(symbol, gran, days) {
  const granMs = { 1: 60000, 5: 300000, 15: 900000, 60: 3600000 }[gran] || 60000;
  const end = Math.floor(Date.now() / granMs) * granMs;
  const start = end - days * 24 * 3600 * 1000;
  let from = start, out = [];
  while (from < end) {
    const url = `https://api-futures.kucoin.com/api/v1/kline/query?symbol=${symbol}&granularity=${gran}&from=${from}&to=${Math.min(from + 200 * granMs, end)}`;
    const r = await realFetch(url).then(r => r.json()).catch(() => null);
    const rows = r && r.data ? r.data : [];
    if (!rows.length) break;
    let lastT = +rows[rows.length-1][0]; if(lastT<1e12) lastT*=1000;
    if (lastT <= from) break;
    for (const r2 of rows) { let t=+r2[0]; if(t<1e12) t*=1000; // ms or s
      out.push({ t, o: +r2[1], h: +r2[2], l: +r2[3], c: +r2[4], v: +r2[5] }); }
    from += 200 * granMs;
    process.stdout.write('.');
  }
  console.log('');
  return out.filter(k => k.c > 0).sort((a, b) => a.t - b.t);
}

// ---------- config the engine ----------
S.config.symbol = SYMBOL;
S.config.displaySymbol = SYMBOL.replace(/USDTM$/i, '').replace(/^XBT/i, 'BTC');
S.config.mode = 'paper';
S.config.cycleBalance = BAL;
S.config.levels = 12;
S.config.leverage = 3;
S.config.gridStepPct = 0.3;
S.config.tpNet = 0.03;
S.config.hfMode = process.argv.includes('--hf');
S.config.gridStepPct = S.config.hfMode ? 0.12 : 0.3;
S.config.huntPct = S.config.hfMode ? 0.06 : 0.15;
if (S.config.hfMode) {
  S.config.levels = 12;           // same as winning run
  S.config.leverage = 3;
  // keep full cycle balance — the winning 52-trade run used it
}
S.multiplier = 1;
S.tickSize = Math.pow(10, -Math.ceil(Math.log10(Math.max(1e-12, 1))) - 4);
S.makerFee = MAKER_FEE; S.takerFee = TAKER_FEE;
S.studyUntil = 0;
S.ignoreExchangeUntil = 0;
// fingerprint pre-seeded from history (prime 60 candles) so study is short (15s)
S.memory = { pairs: {}, cycles: 0, totalPnl: 0,
  profiles: { [SYMBOL]: { at: Date.now(), vol: 0.05, spread: 0.02, act: 30, whales: 0 } } };

// ---------- run: feed each candle as ticks (o,h,l,c path per candle) ----------
const candles = await fetchKlinesAll(SYMBOL, GRAN, DAYS);
if (candles.length < 50) { console.error('not enough data'); process.exit(1); }
console.log(`\nAB-KING Lab — ${SYMBOL} — ${candles.length} candles (${DAYS}d @ ${GRAN}m)`);

// contract multiplier & tick from exchange
try {
  const ct = await realFetch(`https://api-futures.kucoin.com/api/v1/contracts/${SYMBOL}`).then(r => r.json());
  if (ct && ct.data) {
    S.multiplier = +ct.data.multiplier || 1;
    S.tickSize = +ct.data.tickSize || S.tickSize;
  }
} catch (e) {}

const feedTick = (price, ts) => {
  S.lastPrice = price; S.markPrice = price; S.lastTickAt = ts;
  S.priceTrail = [...(S.priceTrail || []), price].slice(-120);
  S._tradeTs = [...(S._tradeTs || []), ts].slice(-300);
  S.tape = [{ side: price % 2 < 1 ? 'buy' : 'sell', size: 1, price }, ...(S.tape || [])].slice(0, 32);
};

let t0 = Date.now();
const nowOrig = Date.now;
const clock = { now: candles[0].t };
Date.now = () => clock.now;       // engine sees historical time

// prime fingerprint then start
for (const k of candles.slice(0, 60)) feedTick(k.c, k.t);
E.startBot && E.startBot();

const stats = { fills: 0, harvests: 0, fees: 0, maxDD: 0, peak: BAL, stuck: 0, lastFillPx: 0 };
const fillsBefore = () => S.journal ? S.journal.filter(j => j.status !== 'open').length : 0;
let f0 = fillsBefore();

for (let i = 60; i < candles.length; i++) {
  const k = candles[i];
  clock.now = k.t;
  // per-candle price path: open -> high -> low -> close (conservative intrabar order)
  const path = [k.o, k.h, k.l, k.c];
  for (const p of path) {
    try { E.onStreamTick({ symbol: SYMBOL, price: p }); } catch (e) {}
    clock.now = k.t + 1;
    try { await E.botTick(); } catch (e) {}
  }
  if (S.grid.length > 400) S.grid = S.grid.filter(g => ['armed','open','filled','staged'].includes(g.status) || g.reduceOnly);
  if (i % 100 === 0) console.log('  ...candle', i, 'pos', S.position ? 1 : 0, 'grid', S.grid.length, 'bal', S.config.cycleBalance.toFixed(2));
  const fNow = fillsBefore();
  if (fNow > f0) { stats.fills += fNow - f0; f0 = fNow; }
  const eq = S.config.cycleBalance + (S.position ? S.position.unrealized : 0);
  stats.peak = Math.max(stats.peak, eq);
  stats.maxDD = Math.max(stats.maxDD, stats.peak - eq);
  if (S.position) stats.stuck++; else stats.stuck = 0;
}
Date.now = nowOrig;

const eq = S.config.cycleBalance + (S.position ? S.position.unrealized : 0);
const j = S.journal || [];
const closed = j.filter(r => r.status !== 'open');
const wins = closed.filter(r => r.pnl > 0), losses = closed.filter(r => r.pnl < 0);
console.log('\n════ AB-KING LAB RESULTS ════');
console.log(`symbol:            ${S.config.displaySymbol}`);
console.log(`period:            ${DAYS} days (${candles.length} candles @${GRAN}m)`);
console.log(`start balance:     $${BAL}`);
console.log(`end equity:        $${eq.toFixed(2)}`);
console.log(`net PnL:           $${(eq - BAL).toFixed(2)} (${((eq / BAL - 1) * 100).toFixed(2)}%)`);
console.log(`closed trades:     ${closed.length} (win ${wins.length} / lose ${losses.length})`);
console.log(`win rate:          ${closed.length ? (wins.length / closed.length * 100).toFixed(1) : 0}%`);
console.log(`fees paid:         $${(S.feesPaid || 0).toFixed(2)}`);
console.log(`funding paid:       $${(S.fundingPaid || 0).toFixed(2)}`);
console.log(`max drawdown:      $${stats.maxDD.toFixed(2)} (peak $${stats.peak.toFixed(2)})`);
console.log(`still open:        ${S.position ? 'YES (stuck ' + stats.stuck + ' candles)' : 'no'}`);
if (S.position) console.log(`open unrealized:   $${S.position.unrealized.toFixed(2)}`);
console.log('══════════════════════════════');
process.exit(0);
