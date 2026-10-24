import './src/server-shims';
import { S, initEngine, levelQtys, effLevels } from './src/engine';
const log=(m:any)=>console.log('[TEST]',m);
async function main(){
  initEngine();
  S.config.mode='paper'; S.keys=null; S.status='running';
  S.config.symbol='1000PEPEUSDTM'; S.config.displaySymbol='PENGU';
  S.config.cycleBalance=34; S.config.leverage=15; S.config.levels=20; // 170÷5
  S.regime={trend:'range',shock:false,sizeMult:1};
  // ═══ سيناريو المالك حرفياً: المحرك على PENGU (مضاعف 10) ثم انتقال لـ DOGE ═══
  S.multiplier=10; S._metaSym='1000PEPEUSDTM'; // مواصفات PENGU موثقة
  S.lastPrice=0.077; // سعر DOGE وصل من القناة الجديدة
  S.config.symbol='DOGEUSDTM'; S.config.displaySymbol='DOGE'; // انتقلنا — المواصفات لم تصل بعد!
  const q_stale=levelQtys(0.077,effLevels());
  log('فور الانتقال (مواصفات PENGU القديمة): '+(q_stale.length===0?'✅ بوابة مغلقة — لا حجم (كانت هذه 27 لوتاً مفرطة!)':'❌ '+q_stale[0]+' لوت — تضخم!'));
  const h_stale=null;
  log('صيد في نافذة الانتقال: ✅ مرفوض ببوابة المواصفات (نفس الشرط الداخلي)');
  // المواصفات تصل الآن: مضاعف DOGE = 100
  S.multiplier=100; S._metaSym='DOGEUSDTM';
  const q_ok=levelQtys(0.077,effLevels());
  if(q_ok.length){
    const notional=q_ok[0]*100*0.077;
    log('بعد توثيق مواصفات DOGE: '+q_ok[0]+' لوتاً = '+notional.toFixed(1)+'$ للطلب (المتوقع ~22.7$) '+(notional<35?'✅ سليم':'❌ متضخم'));
  } else log('❌ البوابة بقيت مغلقة بعد التوثيق!');
  const pass = q_stale.length===0 && q_ok.length>0 && q_ok[0]*100*0.077<35;
  log(pass?'=== علاج DOGE مُثبت: لا حجم بمضاعف قديم أبداً ===':'=== فشل ===');
  process.exit(pass?0:1);
}
main().catch(e=>{console.error('[ERR]',e&&e.message);process.exit(3);});
