import './src/server-shims';
import { S, initEngine, studiedEarlyExit, dynamicTpNet } from './src/engine';

const log = (m: any) => console.log('[TEST]', m);

async function main() {
  initEngine();
  S.config.mode = 'paper'; S.keys = null; S.status = 'running';
  S.multiplier = 1; S.takerFee = 0.0006; S.makerFee = 0.0002;

  const p = 0.00003850;
  S.position = { side: 'short', size: 1000, entry: p * 1.004, unrealized: 0.14, openedAt: Date.now() - 600000 };
  S.journal = [{ id: 'j1', status: 'open', side: 'short', qty: 1000, entry: p * 1.004, fees: 0.01, source: 'صيد' }];

  const bids: any[] = [];
  for (let i = 1; i <= 12; i++) bids.push({ price: p * (1 - 0.0002 * i), size: 50 });
  bids.push({ price: p * 0.99955, size: 500 });
  bids.sort((a, b) => b.price - a.price); // الدفتر الحقيقي مرتب من الأقرب للأبعد
  const asks: any[] = [];
  for (let i = 1; i <= 12; i++) asks.push({ price: p * (1 + 0.0002 * i), size: 50 });
  S.orderBook = { bids, asks };

  const ex = studiedEarlyExit(p);
  log('جدار ارتداد + ربح 0.14: ' + (ex ? '✅ يبيع فوراً — ' + ex.why + ' | صافي ' + ex.netNow.toFixed(3) + '$' : '❌ لم يبع'));
  if (!ex) process.exit(1);

  S.position.unrealized = -0.50;
  const exLoss = studiedEarlyExit(p * 0.999);
  log('مركز خاسر مع نفس الجدار: ' + (exLoss ? '❌ باع بخسارة!' : '✅ لا يبيع — لا إقفال بخسارة أبداً'));

  S.position.unrealized = 0.14;
  const exAgain = studiedEarlyExit(p);
  log('قرار ثانٍ فوري (يجب ألا يُتخذ): ' + (exAgain ? '❌ قرار مزدوج' : '✅ تهدئة 90 ثانية تعمل'));

  const bigQty = Math.round(200 / p), smallQty = Math.round(20 / p);
  S.position = { side: 'short', size: bigQty, entry: p, unrealized: 0, openedAt: Date.now() };
  const tBig = dynamicTpNet();
  S.position = { side: 'short', size: smallQty, entry: p, unrealized: 0, openedAt: Date.now() };
  const tSmall = dynamicTpNet();
  log('هدف مركز 200$: ' + tBig + '$ | مركز 20$: ' + tSmall + '$ → ' + (tBig > tSmall && tSmall >= 0.05 ? '✅ الهدف يتبع حجم الصفقة (بأرضية دنيا)' : '⚠️ تحقق'));

  const pass = ex && !exLoss && !exAgain && tBig > tSmall;
  log(pass ? '=== نجاح كامل ===' : '=== فشل ===');
  process.exit(pass ? 0 : 1);
}
main().catch(e => { console.error('[TEST ERROR]', e && e.message); process.exit(3); });
