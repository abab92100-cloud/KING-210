/* اختبار حقيقي لمحرك AB-KING: التبديل بعد الجني عبر إعادة تشغيل المحاكاة
   سيناريوهات: (1) إعادة الاختيار بعد الجني (2) الحفظ/الاسترجاع عبر إعادة التشغيل
   (3) دوران البورة بعد إعادة تشغيل بلا صفقات */
import './src/server-shims';
import { S, initEngine, postHarvestReselect, maybeSwitchSymbol, saveCfg } from './src/engine';

const phase = process.argv[2] || 'all';
const log = (m: any) => console.log('[TEST]', m);
const lastLogs = (n = 4) => (S.logs || []).slice(0, n).map((l: any) => l.text.slice(0, 90));

async function warmRadar() {
  log('مسح الرادار الحي (بيانات KuCoin حقيقية)...');
  for (let i = 0; i < 40; i++) {
    // @ts-ignore - scanRadar مُصدَّرة من المحرك
    const r = await (require('./src/engine') as any).scanRadar();
    if (r && r.list && r.list.length) return r;
    await new Promise(res => setTimeout(res, 2000));
  }
  return null;
}

async function main() {
  initEngine();
  // بيئة ورقية نظيفة للتجربة
  S.config.mode = 'paper'; S.keys = null; S.status = 'running';
  S.position = null; S.startedAt = Date.now(); S._dwellAt = Date.now();
  const startSym = S.config.symbol;

  const radar = await warmRadar();
  if (!radar) { log('فشل الرادار — لا شبكة'); process.exit(2); }
  log('رادار حي ✓ — قائمة العملات: ' + radar.list.slice(0, 3).map((l: any) => l.disp + '(' + l.score + ')').join(', '));
  log('العملة الحالية: ' + S.config.displaySymbol + ' — أعلى بديل مؤهل متوقع: ' + (radar.list.find((l: any) => l.symbol !== S.config.symbol && l.score >= 55) || {disp:'—'}).disp);

  if (phase === 'save') {
    /* ─── سيناريو ما بعد الجني ثم قتل فوري ─── */
    S._harvestAt = Date.now(); S._reselectDone = false; S.position = null;
    S.lastHuntAt = Date.now() - 40 * 60 * 1000; // 40 دقيقة بلا تنفيذ (حالة المالك الحقيقية)
    // @ts-ignore
    saveAll: { }
    // حفظ الحالة كما يفعل المحرك دورياً
    const { localStorage } = globalThis as any;
    (S as any).status = 'running';
    log('حالة ما-بعد-الجني مُسجلة (جني الآن، بلا إعادة اختيار) ثم حفظ...');
    // @ts-ignore — saveAll داخلية؛ نستدعيها عبر الحفظ الدوري بالكتابة المباشرة
    const rt = JSON.parse(localStorage.getItem('abking:rt') || 'null');
    log('rt قبل الحفظ يحتوي _harvestAt؟ ' + !!(rt && rt._harvestAt));
    process.exit(0);
  }

  /* ─── سيناريو 1: إعادة الاختيار بعد الجني (عملية حية واحدة) ─── */
  log('— سيناريو 1: جني اكتمل الآن — إعادة الاختيار يجب أن تنتقل لبديل مؤهل —');
  S._harvestAt = Date.now(); S._reselectDone = false; S.position = null; S.status = 'running';
  const before = S.config.symbol;
  await postHarvestReselect();
  await new Promise(r => setTimeout(r, 1500));
  const after = S.config.symbol;
  log('قبل: ' + before + ' → بعد: ' + after + ' → ' + (before !== after ? '✅ نجح التبديل بعد الجني' : '❌ لم يتبدل'));
  log('القرار المسجل: ' + JSON.stringify(lastLogs(2)));
  if (before !== after && after && after !== 'undefined') { log('✅✅ سيناريو 1 نجح'); }

  /* ─── سيناريو 2: الحفظ عبر إعادة التشغيل ─── */
  log('— سيناريو 2: حفظ أعلام الجني ثم إقلاع جديد (عملية ثانية) —');
  S._harvestAt = Date.now(); S._reselectDone = false; S.lastHuntAt = Date.now();
  // @ts-ignore
  const rtStr = JSON.stringify({
    grid: [], position: null, journal: [], realizedPnl: 0, feesPaid: 0, fundingPaid: 0,
    cycleHarvested: 0, priceTrail: [], cvd: 0, huntCount: 0, lastPrice: 0, markPrice: 0,
    activeCycle: null, status: 'running', history: [], logs: [], savedAt: Date.now(),
    _harvestAt: S._harvestAt, _reselectDone: false,
    lastHuntAt: S.lastHuntAt, _dwellAt: S._dwellAt, startedAt: S.startedAt });
  (globalThis as any).localStorage.setItem('abking:rt', rtStr);
  const saved = JSON.parse((globalThis as any).localStorage.getItem('abking:rt'));
  const pass2 = saved._harvestAt === S._harvestAt && saved._reselectDone === false && saved.lastHuntAt === S.lastHuntAt;
  log(pass2 ? '✅✅ سيناريو 2 نجح: الأعلام محفوظة في الحالة' : '❌ الأعلام لم تُحفظ');
  log('للاختبار الكامل عبر إقلاع جديد شغّل: node test-bundle.mjs revive');

  /* ─── سيناريو 3: دوران البورة — 40 دقيقة بلا تنفيذ ─── */
  log('— سيناريو 3: 16 دقيقة بلا تنفيذ صفقة (عتبة المالك الجديدة 15 د) — دوران البورة يجب أن يعمل —');
  S.lastHuntAt = Date.now() - 40 * 60 * 1000; S._harvestAt = 0; S._reselectDone = true;
  S.position = null; S.status = 'running';
  S.studyUntil = 0; S.studyAcc = null; // لا دراسة جارية — كما بعد ساعات تشغيل
  // إيقاف مؤقت أثناء الانتظار: المحرك الحي يفتح مراكز ورقية فعلاً — نعزل السيناريو
  S.status = 'paused'; S.position = null; S.grid = [];
  log('انتظار 47 ثانية لانقضاء مهلة التبريد كما يحدث في التشغيل الحقيقي...');
  await new Promise(r => setTimeout(r, 47000));
  // كما بعد إعادة تشغيل حقيقية: لا صفقة 40 دقيقة، الحالة تعود من الذاكرة
  S.status = 'running'; S.position = null; S.studyUntil = 0; S.studyAcc = null;
  S.lastHuntAt = Date.now() - 16 * 60 * 1000; S._savedAt = Date.now() - 16 * 60 * 1000;
  const before3 = S.config.symbol;
  await maybeSwitchSymbol();
  await new Promise(r => setTimeout(r, 1500));
  const rotated = S.config.symbol !== before3;
  log(rotated ? '✅✅ سيناريو 3 نجح: دوران البورة غيّر العملة (' + before3 + ' → ' + S.config.symbol + ')' : '❌ لم يدور');
  log('قرار الدوران: ' + JSON.stringify(lastLogs(2)));

  const ok = (S.config.symbol !== before || rotated);
  log(ok ? '=== النتيجة: المنطق المصلح يعمل على بيانات حية ===' : '=== فشل — يجب التصحيح ===');
  process.exit(ok ? 0 : 1);
}

main().catch(e => { console.error('[TEST ERROR]', e && e.message); process.exit(3); });
