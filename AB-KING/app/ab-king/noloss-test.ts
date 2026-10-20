import './src/server-shims';
import { S, initEngine, flattenAt, studiedEarlyExit } from './src/engine';
const log = (m: any) => console.log('[TEST]', m);

async function main() {
  initEngine();
  S.config.mode = 'paper'; S.keys = null; S.status = 'running'; S.multiplier = 1;
  S.takerFee = 0.0006; S.makerFee = 0.0002;
  const p = 0.00004500;

  // ─── 1) الحاجز المطلق: محاولة إغلاق خاسر من مصدر عادي = مرفوضة ───
  S.position = { side: 'long', size: 1000, entry: p * 1.003, unrealized: -0.14, openedAt: Date.now() - 300000 };
  S.journal = [{ id: 'j1', status: 'open', side: 'long', qty: 1000, entry: p * 1.003, fees: 0.01, source: 'صيد' }];
  await flattenAt(p, 'حد الصبر');
  log('إغلاق بخسارة من «حد الصبر»: ' + (S.position === null ? '❌ نُفّذت الخسارة!' : '✅ مرفوض — المركز باقٍ والصمود حتى الجني'));

  await flattenAt(p, 'عودة إندكس');
  log('إغلاق بخسارة من «عودة إندكس»: ' + (S.position === null ? '❌ نُفّذت الخسارة!' : '✅ مرفوض'));

  // ─── 2) الإيقاف اليدوي من المالك = يُنفّذ دائماً (قرارك بيدك) ───
  await flattenAt(p, 'إيقاف');
  log('إيقاف المالك اليدوي: ' + (S.position === null ? '✅ يُنفّذ — قرارك بيدك' : '❌ لم يُنفّذ'));

  // ─── 3) الجني المبكر: السعر نفسه لا قيمة مخزنة — ربح مضمون ≥ 5 سنت ───
  S.position = { side: 'long', size: 500000, entry: p * 0.99, unrealized: 0, openedAt: Date.now() - 600000 };
  S.journal = [{ id: 'j2', status: 'open', side: 'long', qty: 500000, entry: p * 0.99, fees: 0.02, source: 'صيد' }];
  // هامش رفيع جداً (0.03$) — يجب ألا يبيع (أقل من 5 سنت)
  const near = studiedEarlyExit(p * 0.99735);
  log('ربح هامشه 0.03$ (تحت أرضية الجني المبكر): ' + (near ? '❌ باع بمنخفض' : '✅ لا بيع — لا جني تافه'));
  // ربح واضح 0.20$ مع جدار — يبيع
  const asks: any[] = [];
  for (let i = 1; i <= 10; i++) asks.push({ price: p * (1 + 0.0002 * i), size: 40 });
  asks.push({ price: p * 1.0012, size: 400 });
  asks.sort((a, b) => a.price - b.price);
  S.orderBook = { asks, bids: [] };
  const ex = studiedEarlyExit(p);
  log('ربح واضح 0.20$ + جدار ارتداد: ' + (ex ? '✅ يبيع فوراً بصافي ' + ex.netNow.toFixed(3) + '$ — محسوب من سعر التنفيذ نفسه' : '❌ لم يبع'));

  const pass = S.position === null && !near && !!ex;
  log(pass ? '=== الحاجز المطلق يعمل — نجاح ===' : '=== فشل ===');
  process.exit(pass ? 0 : 1);
}
main().catch(e => { console.error('[TEST ERROR]', e && e.message); process.exit(3); });
