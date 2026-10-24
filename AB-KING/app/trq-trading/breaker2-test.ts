import './src/server-shims';
import { S, initEngine, batchHoldActive } from './src/engine';
const log = (m: any) => console.log('[TEST]', m);
async function main() {
  initEngine();
  S.config.mode = 'paper'; S.keys = null; S.status = 'running'; S.multiplier = 10;
  S.config.cycleBalance = 20; S.regime = { trend: 'range', shock: false };
  // مركز PEPE لونغ (سيناريو المالك) مع انزلاق
  S.position = { side: 'long', size: 400, entry: 0.0076, unrealized: -0.5, openedAt: Date.now() - 3600000 };
  S.journal = [{ id: 'j1', status: 'open', side: 'long', qty: 400, entry: 0.0076, fees: 0.02, symbol: 'PEPE', source: 'مركز' }];
  S.orderBook = { asks: [], bids: [] }; S.tape = [];
  // 4 أوامر دخول → توقف
  S._batchFills = 4; S._batchHoldAt = 0; S._batchStudiedAt = false; S.studyUntil = 0;
  const hold1 = batchHoldActive();
  log('بعد الأمر الرابع: ' + (hold1 ? '✅ توقف فوري' : '❌ لم يتوقف'));
  S._batchStudiedAt = true; S._batchHoldAt = Date.now() - 11 * 60000; // 11 دقيقة مضت (الحد الأدنى تجاوز)
  // سيناريو الانزلاق المستمر: قيعان جديدة كل دقيقة لمدة "ساعات"
  let released = false;
  for (let i = 0; i < 12; i++) {
    S.lastPrice = 0.0076 - (i + 1) * 0.00005; // قاع جديد كل مرة
    if (!batchHoldActive()) { released = true; break; }
    S._batchHoldAt -= 60000; // محاكاة مرور الوقت
    S._batchWorseAt -= 60000;
    S._batchWorseAt += 30000; // آخر قاع جديد قبل 30 ث فقط
  }
  log('خلال انزلاق مستمر (قيعان جديدة متواصلة): ' + (released ? '❌ أُطلق أثناء الانزلاق!' : '✅ توقف مطلق — لم يفتح أمراً خامساً'));
  // الآن الاستقرار الحقيقي: لا قاع جديد منذ 9 دقائق + الحد الأدنى تجاوز
  S.lastPrice = 0.0070; S._batchWorseAt = Date.now() - 9 * 60000; S._batchHoldAt = Date.now() - 11 * 60000;
  S.studyUntil = 0; S._batchStudiedAt = true;
  const rel = !batchHoldActive();
  log('بعد استقرار فعلي (9 د بلا قاع جديد): ' + (rel ? '✅ أُطلق — دفعة جديدة حتى 4' : '❌ لم يُطلق'));
  const pass = hold1 && !released && rel;
  log(pass ? '=== سيناريو PEPE مُعالج: 4 ثم توقف مطلق حتى الاستقرار الفعلي ===' : '=== فشل ===');
  process.exit(pass ? 0 : 1);
}
main().catch(e => { console.error('[TEST ERROR]', e && e.message); process.exit(3); });
