import './src/server-shims';
import { S, initEngine, batchHoldActive } from './src/engine';
const log=(m:any)=>console.log('[TEST]',m);
const H=3600000;
async function main(){
  initEngine();
  S.config.mode='paper'; S.keys=null; S.status='running';
  S.config.cycleBalance=34; S.config.leverage=15; S.regime={trend:'range',shock:false};
  S.position={side:'long',size:400,entry:0.0076,unrealized:-0.5,openedAt:Date.now()};
  S._batchFills=4; S._batchHoldAt=0; S._batchStudiedAt=false; S.studyUntil=0;
  const h1=batchHoldActive();
  log('بعد الأمر الرابع: '+(h1?'✅ توقف فوري':'❌'));
  // توقف 3 ساعات مضت، آخر انزلاق جديد قبل 30 دقيقة فقط → يجب الانتظار ساعة إضافية
  S._batchStudiedAt=true; S._batchHoldAt=Date.now()-3*H-5*60000; S._batchWorseAt=Date.now()-30*60000;
  const h2=batchHoldActive();
  log('عند 3 ساعات لكن الانزلاق نشط (قبل 30 د): '+(h2?'✅ لا إطلاق — +ساعة إضافية (قاعدتك)':'❌ أُطلق مبكراً!'));
  // مرت ساعة إضافية + استقرار ساعة كاملة → إطلاق
  S._batchHoldAt=Date.now()-4*H-5*60000; S._batchWorseAt=Date.now()-H-5*60000;
  const h3=batchHoldActive();
  log('بعد +ساعة واستقرار ساعة كاملة: '+(h3?'❌ لم يُطلق!':'✅ أُطلق — 4 أوامر متباعدة'));
  // جني الصفقة: تصفير
  S._batchFills=0; S._batchHoldAt=0; S._batchWorst=null; S._batchWorseAt=0;
  const h4=batchHoldActive();
  log('بعد الجني (دورة جديدة): '+(h4?'❌':'✅ متاح فتح جديد'));
  const pass=h1&&h2&&!h3&&!h4;
  log(pass?'=== قاعدة المالك تعمل حرفياً: 4 → 3 ساعات → +ساعة → استقرار ساعة → دخول ===':'=== فشل ===');
  process.exit(pass?0:1);
}
main().catch(e=>{console.error('[ERR]',e&&e.message);process.exit(3);});
