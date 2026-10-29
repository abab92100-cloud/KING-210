import './src/server-shims';
import { S, initEngine, batchHoldActive } from './src/engine';
const log=(m:any)=>console.log('[TEST]',m);
const H=3600000;
async function main(){
  initEngine();
  S.config.mode='paper'; S.keys=null; S.status='running';
  S.config.cycleBalance=34; S.config.leverage=15;
  S.position={side:'long',size:400,entry:0.0076,unrealized:-0.5,openedAt:Date.now()};
  S.lastPrice=0.0074;
  S._batchFills=4; S._batchHoldAt=0; S._batchStudiedAt=false; S.studyUntil=0;
  const h1=batchHoldActive();
  log('بعد الأمر الرابع: '+(h1?'✅ توقف':'❌'));
  // مرت 6 ساعات كاملة + السوق هادئ (لا قاع جديد منذ ساعتين) + الدراسة اكتملت
  S._batchHoldAt=Date.now()-6*H; S._batchStudiedAt=true; S.studyUntil=0;
  S._batchWorseAt=Date.now()-2*H;
  const h2=batchHoldActive();
  log('بعد 6 ساعات هدوء تام مع صفقة مفتوحة: '+(h2?'✅ لا يزال موقوفاً — صفر دخول خامس':'❌ أُطلق!'));
  S._batchHoldAt=Date.now()-24*H; // حتى بعد يوم كامل
  const h3=batchHoldActive();
  log('بعد 24 ساعة صفقة مفتوحة: '+(h3?'✅ موقوف":"':'')+(h3?'✅ موقوف':'❌ أُطلق!'));
  // جني الصفقة: _harvestCleanup يصفّر العداد
  S._batchFills=0; S._batchHoldAt=0; S._batchWorst=null; S._batchWorseAt=0;
  S.position=null;
  const h4=batchHoldActive();
  log('بعد الجني الكامل (دورة جديدة): '+(h4?'❌':'✅ متاح 4 أوامر جديدة'));
  const pass=h1&&h2&&h3&&!h4;
  log(pass?'=== القاطع المطلق مُثبت: 4 فقط حتى الجني مهما طال الزمن ===':'=== فشل ===');
  process.exit(pass?0:1);
}
main().catch(e=>{console.error('[ERR]',e&&e.message);process.exit(3);});
