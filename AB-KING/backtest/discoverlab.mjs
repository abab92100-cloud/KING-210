/* ================================================================
   KING-trading مختبر الاستكشاف العميق (Discovery Lab)
   يجرب كل نقاط النهاية العامة في KuCoin Futures API
   ويحصي كل حقل بيانات متاح ويستخرج إشارات تداول منه
   ================================================================ */
const BASE = 'https://api-futures.kucoin.com';

// كل نقاط النهاية العامة المعروفة في وثائق KuCoin Futures
const ENDPOINTS = [
  ['/api/v1/contracts/active', 'كل العقود النشطة (شامل: تمويل، OI، رسوم، رافعة، حدود)'],
  ['/api/v1/tickers', 'أسعار كل العقود لحظيًا'],
  ['/api/v1/interest/query', 'أسعار الفائدة للعملات'],
  ['/api/v1/index/query', 'قيم الإنديكس (من 5 منصات)'],
  ['/api/v1/mark/prices', 'أسعار المارك لكل العقود'],
  ['/api/v1/premium/query', 'أعلى/أدنى علاوة تمويل في المنصة'],
  ['/api/v1/timestamp', 'توقيت السيرفر (لقياس انحراف الساعة)'],
  ['/api/v1/status', 'حالة الصيانة'],
  ['/api/v1/gains', 'أرباح المنصة؟'],
  ['/api/v1/rates', 'أسعار صرف؟'],
  ['/api/v1/statistics/trading-volumes', 'أحجام التداول الإحصائية'],
  ['/api/v1/market/all/overview', 'نظرة عامة على السوق'],
  ['/api/v1/funding-rate/XBTUSDTM/current', 'معدل التمويل الحالي XBT'],
  ['/api/v1/contract/funding-rates', 'معدلات تمويل متعددة'],
  ['/api/v1/interest/index/query', 'إنديكس الفائدة'],
  ['/api/v1/kline/query?symbol=XBTUSDTM&granularity=60', 'شموع XBT ساعة'],
  ['/api/v1/level2/depth20?symbol=XBTUSDTM', 'دفتر أوامر XBT'],
  ['/api/v1/trade/history?symbol=XBTUSDTM', 'صفقات XBT الأخيرة'],
  ['/api/v1/ticker?symbol=XBTUSDTM', 'تيكر XBT'],
  ['/api/v1/statistics/funding-rate?symbol=XBTUSDTM', 'إحصاءات التمويل XBT'],
];

async function probe(path, desc) {
  const t0 = Date.now();
  try {
    const r = await fetch(BASE + path, { signal: AbortSignal.timeout(10000) });
    const ms = Date.now() - t0;
    if (!r.ok) return { path, desc, ok: false, status: r.status, ms };
    const ct = r.headers.get('content-type') || '';
    if (!ct.includes('json')) return { path, desc, ok: false, status: 'html', ms };
    const j = await r.json();
    return { path, desc, ok: j.code === '200000', status: j.code, ms, data: j.data };
  } catch (e) {
    return { path, desc, ok: false, status: String(e.message || e).slice(0, 40) };
  }
}

// حصي الحقول بشكل تكراري
function countFields(obj, prefix = '', out = new Set(), depth = 0) {
  if (depth > 4 || obj == null) return out;
  if (Array.isArray(obj)) {
    if (obj.length) countFields(obj[0], prefix + '[]', out, depth + 1);
    return out;
  }
  if (typeof obj === 'object') {
    for (const k of Object.keys(obj)) {
      const p = prefix ? prefix + '.' + k : k;
      out.add(p);
      countFields(obj[k], p, out, depth + 1);
    }
  }
  return out;
}

const results = [];
for (const [path, desc] of ENDPOINTS) {
  const r = await probe(path, desc);
  const fields = r.data !== undefined ? countFields(r.data) : new Set();
  results.push({ ...r, fields: fields.size, fieldList: [...fields].slice(0, 40) });
  console.log((r.ok ? '✓' : '✗') + ' [' + (r.ms || '-') + 'ms] ' + path + (r.ok ? ' → ' + fields.size + ' حقل' : ' (' + r.status + ')'));
}

// تقرير إجمالي
let totalFields = 0;
console.log('\n════════ نقاط النهاية الناجحة ════════');
for (const r of results) {
  if (r.ok) {
    totalFields += r.fields;
    console.log('\n■ ' + r.path + ' — ' + r.desc);
    console.log('  حقول: ' + r.fieldList.join(' · '));
  }
}
console.log('\n════════');
console.log('نقاط ناجحة: ' + results.filter(r => r.ok).length + '/' + results.length);
console.log('إجمالي حقول البيانات المكتشفة: ' + totalFields);
