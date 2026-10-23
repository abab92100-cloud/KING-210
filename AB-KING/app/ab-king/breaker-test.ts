import './src/server-shims';
import { S, initEngine, batchHoldActive } from './src/engine';
const log = (m: any) => console.log('[TEST]', m);
async function main() {
  initEngine();
  S.config.mode = 'paper'; S.keys = null; S.status = 'running'; S.multiplier = 1;
  S.regime = { trend: 'range', shock: false };
  // محاكاة تعبئة 4 أوامر دخول متتالية
  S._batchFills = 3;
  S._batchHoldAt = 0;
  log('بعد 3 تعبئات: القاطع ' + (batchHoldActive() ? 'نشط ❌ (مبكر)' : 'غير نشط ✅ — الرابع مسموح'));
  S._batchFills = 4;
  const hold = batchHoldActive();
  log('بعد التعبئة الرابعة: ' + (hold ? '✅ القاطع نشط — توقف الفتح' : '❌'));
  // أثناء الدراسة الجارية — لا إطلاق
  S._batchHoldAt = Date.now(); S._batchStudiedAt = true; S.studyUntil = Date.now() + 10000;
  log('الدراسة جارية: ' + (batchHoldActive() ? '✅ ما زال موقوفاً حتى تكتمل' : '❌ أُطلق مبكراً'));
  // اكتملت الدراسة + مرت دقيقتان + استقرار → إطلاق
  S.studyUntil = 0; S._batchHoldAt = Date.now() - 130000;
  const released = !batchHoldActive();
  log('بعد اكتمال الدراسة والاستقرار: ' + (released ? '✅ أُطلق — دفعة جديدة حتى 4 أوامر' : '❌ لم يُطلق'));
  log('عدّاد الدفعة الجديدة: ' + S._batchFills + ' (يجب 0)');
  // وتبدأ دورة جديدة من 4
  S._batchFills = 4;
  const rehold = batchHoldActive();
  log('بعد 4 تعبئات أخرى: ' + (rehold ? '✅ توقف ثانٍ — الدورة تتكرر كما صممت' : '❌'));
  const pass = hold && released && S._batchFills === 0 && rehold;
  log(pass ? '=== القاطع المرحلي يعمل بدقة تصميمك ===' : '=== فشل ===');
  process.exit(pass ? 0 : 1);
}
main().catch(e => { console.error('[TEST ERROR]', e && e.message); process.exit(3); });
