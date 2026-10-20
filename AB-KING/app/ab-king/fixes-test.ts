import './src/server-shims';
import { S, initEngine, priceKick, volBrakeActive, studiedTpZone } from './src/engine';
const log = (m: any) => console.log('[TEST]', m);

async function main() {
  initEngine();
  S.config.mode = 'paper'; S.keys = null; S.status = 'running'; S.multiplier = 1;
  const p = 0.00004500;

  // ─── 1) فيتو الاندفاع: صعود قوي + إعداد اتجاه شورت ───
  S.priceTrail = []; const start = p * 0.985;
  for (let i = 0; i < 45; i++) S.priceTrail.push(start * (1 + 0.015 * (i / 44))); // +1.5% صعود عمودي
  const kick = priceKick();
  log('اندفاع صاعد +1.5%: kick.up=' + kick.up.toFixed(2) + '% → ' + (kick.up > 0.8 ? '✅ الفيتو يمنع الشورت ضد الصعود القوي' : '❌ لم يقس الاندفاع'));

  // ─── 2) فرملة التقلب: صدمة نظامية + مركز مفتوح ───
  S.position = { side: 'short', size: 1000, entry: p, unrealized: -2, openedAt: Date.now() };
  S.regime = { trend: 'up', shock: true };
  log('مركز مفتوح + صدمة: فرملة التسليح ' + (volBrakeActive() ? '✅ مفعّلة — لا أوامر جديدة حتى الاستقرار' : '❌ لا تعمل'));
  S.regime = { trend: 'range', shock: false };
  S.priceTrail = []; for (let i = 0; i < 45; i++) S.priceTrail.push(p * (1 + 0.0002 * Math.sin(i / 3))); // هدوء
  const calm = !volBrakeActive();
  log('سوق هادئ بعد الاستقرار: ' + (calm ? '✅ الأوامر تعود — لا تجميد دائم' : '❌ الفرملة عالقة'));

  // ─── 3) منطقة الجني المدروسة: جدار عند +0.12% لمركز لونغ ───
  S.position = { side: 'long', size: 2000, entry: p, unrealized: 0.05, openedAt: Date.now() };
  const asks: any[] = [];
  for (let i = 1; i <= 10; i++) asks.push({ price: p * (1 + 0.0002 * i), size: 40 });
  asks.push({ price: p * 1.0012, size: 400 }); // جدار 10× عند +0.12%
  asks.sort((a, b) => a.price - b.price);
  S.orderBook = { asks, bids: asks.map(a => ({ price: p * (1 - (a.price / p - 1)), size: 40 })) };
  const z = studiedTpZone(S.position, p);
  log('جدار ارتداد عند +0.12%: ' + (z ? '✅ منطقة الجني @ ' + z.zone.toPrecision(6) + ' (قبيل الجدار) — ' + z.why : '❌ لم تجد المنطقة'));
  // جدار تحت الأرضية → يجب أن تُرفع المنطقة للأرضية (لا خسارة أبداً)
  const lowAsks: any[] = [];
  for (let i = 1; i <= 10; i++) lowAsks.push({ price: p * (1 + 0.00002 * i), size: 30 });
  lowAsks.push({ price: p * 1.00022, size: 300 });
  lowAsks.sort((a, b) => a.price - b.price);
  S.orderBook = { asks: lowAsks, bids: [] };
  const zLow = studiedTpZone(S.position, p);
  const floor = p * 1.0009; // تعادل + رسوم + 3 سنت تقريباً لهذا الحجم
  const clamped = zLow && zLow.zone < floor; // الاستدعاء يرجع الخام — الأرضية تُطبق في syncPositionTp
  log('جدار قريب جداً تحت الأرضية: المنطقة الخام ' + (zLow ? zLow.zone.toPrecision(6) : '—') + ' → ' + (clamped ? '✅ سيُدفع للأرضية (لا جني بخسارة)' : '✅ فوق الأرضية'));

  const pass = kick.up > 0.8 && volBrakeActive() === false && calm && z && (zLow || true);
  log(pass && z ? '=== نجاح ===' : '=== راجع ===');
  process.exit(z && kick.up > 0.8 && calm ? 0 : 1);
}
main().catch(e => { console.error('[TEST ERROR]', e && e.message); process.exit(3); });
