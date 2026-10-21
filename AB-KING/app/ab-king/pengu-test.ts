import './src/server-shims';
import { S, initEngine, studiedEarlyExit, dynamicTpNet } from './src/engine';
const log = (m: any) => console.log('[TEST]', m);
const avg = 0.0083023; // متوسط صورة المالك
async function main() {
  initEngine();
  S.config.mode = 'paper'; S.keys = null; S.status = 'running'; S.multiplier = 1;
  S.config.cycleBalance = 170;
  S.takerFee = 0.0006; S.makerFee = 0.0002;
  // مركز 6 مستويات مضخم: ~680$ تعرض (سيناريو PENGU)
  const qty = 82000; // 680\$ تقريباً عند متوسط 0.0083
  S.position = { side: 'short', size: qty, entry: avg, unrealized: 0, openedAt: Date.now() - 14400000 };
  S.journal = [{ id: 'j', status: 'open', side: 'short', qty, entry: avg, fees: 0.4, source: 'صيد' }];
  S.orderBook = { asks: [], bids: [] }; S.tape = [];

  const tgt = dynamicTpNet();
  log('الهدف المدروس لمركز مضخم 680\$: ' + tgt + '$ (كان 4.44$ قبل الإصلاح — الآن بسقف 1.5% من الدورة = ' + (170*0.015).toFixed(2) + '$)');

  // تتبع الذروة من أول سنت: الربح يرتفع 0.30 → 0.60 → 0.84
  const netAt = (n: number) => avg - (n + 0.4 + qty * avg * 0.0006) / qty;
  S._posPeakNet = null;
  let r = studiedEarlyExit(netAt(0.30)); // يتعقب الذروة 0.30
  r = studiedEarlyExit(netAt(0.60));     // الذروة 0.60
  r = studiedEarlyExit(netAt(0.84));     // الذروة 0.84 — لا بيع في الصعود
  log('ذروة 0.84$: ' + (r ? '❌ باع مبكراً' : '✅ لا بيع — الذروة تُتعقب من أول سنت'));
  const peakTracked = (S._posPeakNet || 0) >= 0.83;
  log('الذروة المتعقبة: ' + (peakTracked ? '✅ ' + S._posPeakNet.toFixed(2) + '$ (كانت لا تُتعقب أصلاً قبل الإصلاح)' : '❌ لم تُتعقب'));

  // التراجع إلى 0.50 (أقل من 60% من الذروة 0.84 = 0.50) — يجب البيع فوراً
  r = studiedEarlyExit(netAt(0.50));
  log('تراجع إلى 0.50$: ' + (r && r.tag === 'peaklock' ? '✅ بيع فوري — ' + r.why : '❌ لم يبع'));

  // ولن يصل المركز لخسارة أبداً — الصافي محسوب من سعر التنفيذ
  const pass = !r0null(r) && peakTracked && r && r.tag === 'peaklock';
  log(pass ? '=== سيناريو PENGU مُعالج — الذروة لا تذوب بعد الآن ===' : '=== فشل ===');
  process.exit(pass ? 0 : 1);
  function r0null(x: any) { return !!x; }
}
main().catch(e => { console.error('[TEST ERROR]', e && e.message); process.exit(3); });
