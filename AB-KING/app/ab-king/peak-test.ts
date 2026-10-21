import './src/server-shims';
import { S, initEngine, studiedEarlyExit } from './src/engine';
const log = (m: any) => console.log('[TEST]', m);
const p0 = 0.000047026; // متوسط صورة المالك
async function main() {
  initEngine();
  S.config.mode = 'paper'; S.keys = null; S.status = 'running'; S.multiplier = 1;
  S.takerFee = 0.0006; S.makerFee = 0.0002;
  S.journal = [{ id: 'j', status: 'open', side: 'short', qty: 260000, entry: p0, fees: 0.10, source: 'صيد' }];
  S.position = { side: 'short', size: 260000, entry: p0, unrealized: 0, openedAt: Date.now() - 3600000 };
  const net = (p: number) => { // صافٍ من سعر التنفيذ
    const gross = (p0 - p) * 260000;
    return gross - 0.10 - 260000 * p * 0.0006;
  };
  // هدف مدروس تقريبي كصورة المالك (1.03$) — نثبّت dynamicTpNet بمحاكاة بصمة
  const prof = S.memory.profiles as any;
  prof['PEPEUSDTM'] = { vol: 0.3, spread: 0.005 }; // تقلب عالٍ → هدف واسع
  S.orderBook = { asks: [], bids: [] }; S.tape = []; // بلا شهادات أخرى — قفل الذروة وحده يجب أن يعمل

  // 1) الذروة: ربح 0.60$ عائم — لا بيع (الذروة تُقفل الآن)
  const pPeak = p0 - (0.60 + 0.10 + 260000 * p0 * 0.0006) / 260000;
  S._posPeakNet = null;
  const r1 = studiedEarlyExit(pPeak);
  log('ذروة 0.60$: ' + (r1 ? '❌ باع في الذروة مبكراً' : '✅ لا بيع — الذروة تُقفل وتُراقب'));

  // 2) التراجع: الربح نزل إلى 0.35$ (58% من الذروة) — يجب البيع فوراً
  const pRetrace = p0 - (0.35 + 0.10 + 260000 * p0 * 0.0006) / 260000;
  const r2 = studiedEarlyExit(pRetrace);
  log('تراجع إلى 0.35$ (عاد 40% من الذروة): ' + (r2 && r2.tag === 'peaklock'
    ? '✅ بيع فوري — ' + r2.why : '❌ لم يبع — ' + (r2 ? JSON.stringify(r2) : 'null')));

  // 3) لا بيع بخسارة أبداً: تراجع للخسارة
  S._posPeakNet = 0.60 as any;
  const pLoss = p0 * 1.002;
  const r3 = studiedEarlyExit(pLoss);
  log('تراجع حتى الخسارة: ' + (r3 ? '❌ باع بخسارة!' : '✅ لا بيع — الصمود (لكن الذروة حُققت قبلها بفضل القفل)'));

  // 4) ذروة تافهة (0.15$) لا تُقفل — لا جني تافه
  S._posPeakNet = null;
  const pTiny = p0 - (0.15 + 0.10 + 260000 * p0 * 0.0006) / 260000;
  const r4 = studiedEarlyExit(pTiny); // يُسجل الذروة 0.15
  const pTinyRetrace = p0 - (0.09 + 0.10 + 260000 * p0 * 0.0006) / 260000;
  const r5 = studiedEarlyExit(pTinyRetrace);
  log('ذروة تافهة 0.15$ ثم تراجع: ' + (r5 ? '❌ جني تافه' : '✅ لا قفل على تافه — الهدف هو الحكم'));

  const pass = !r1 && r2 && r2.tag === 'peaklock' && !r3 && !r5;
  log(pass ? '=== قفل الذروة يعمل — سيناريو المالك مُعالج ===' : '=== فشل ===');
  process.exit(pass ? 0 : 1);
}
main().catch(e => { console.error('[TEST ERROR]', e && e.message); process.exit(3); });
