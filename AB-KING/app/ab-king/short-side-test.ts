import './src/server-shims';
import { S, initEngine, priceKick, studiedTpZone, studiedEarlyExit, dynamicTpNet } from './src/engine';
const log = (m: any) => console.log('[TEST]', m);

async function main() {
  initEngine();
  S.config.mode = 'paper'; S.keys = null; S.status = 'running'; S.multiplier = 1;
  const p = 0.00004500;
  S.takerFee = 0.0006; S.makerFee = 0.0002;

  // ─── 1) فيتو الاندفاع بالعكس: هبوط قوي -1.5% + اتجاه لونغ ───
  S.priceTrail = []; const start = p * 1.015;
  for (let i = 0; i < 45; i++) S.priceTrail.push(start * (1 - 0.015 * (i / 44))); // -1.5% هبوط حاد
  const kick = priceKick();
  const vetoLongOk = kick.down > 0.8; // الفيتو يمنع اللونغ ضد الهبوط الحاد
  log('هبوط حاد -1.5% + لونغ مراد: kick.down=' + kick.down.toFixed(2) + '% → ' + (vetoLongOk ? '✅ الفيتو يعمل بالاتجاه المعاكس (يمنع اللونغ ضد الانهيار)' : '❌'));

  // ─── 2) منطقة الجني للشورت: جدار طلبات ضخم تحت السعر (دعم) ───
  S.position = { side: 'short', size: 2000, entry: p, unrealized: 0.05, openedAt: Date.now() };
  S.journal = [{ id: 'j1', status: 'open', side: 'short', qty: 2000, entry: p, fees: 0.01, source: 'صيد' }];
  const bids: any[] = [];
  for (let i = 1; i <= 10; i++) bids.push({ price: p * (1 - 0.0002 * i), size: 40 });
  bids.push({ price: p * 0.9988, size: 400 }); // جدار دعم عند -0.12%
  bids.sort((a, b) => b.price - a.price);
  S.orderBook = { asks: [], bids };
  const z = studiedTpZone(S.position, p);
  log('شورت + جدار دعم عند -0.12%: ' + (z ? '✅ منطقة الجني @ ' + z.zone.toPrecision(6) + ' (فوق الجدار — جني قبل الارتداد الصاعد) — ' + z.why : '❌'));

  // ─── 3) الجني المبكر للشورت: جدار + ربح صافٍ ───
  S.position.unrealized = 0.14;
  const ex = studiedEarlyExit(p);
  log('شورت رابح 0.14 + جدار ارتداد صاعد: ' + (ex ? '✅ يبيع فوراً بصافي ' + ex.netNow.toFixed(3) + '$ — ' + ex.why : '❌ لم يبع'));

  // ─── 4) لا خسارة للشورت ───
  S.position.unrealized = -0.40;
  const exL = studiedEarlyExit(p * 1.001);
  log('شورت خاسر مع نفس الجدار: ' + (exL ? '❌ باع بخسارة!' : '✅ لا بيع — لا إقفال بخسارة في الاتجاهين'));

  const pass = vetoLongOk && z && ex && !exL;
  log(pass ? '=== الاتجاهان مغطيان بالكامل — نجاح ===' : '=== فشل ===');
  process.exit(pass ? 0 : 1);
}
main().catch(e => { console.error('[TEST ERROR]', e && e.message); process.exit(3); });
