import './src/server-shims';
import { S, initEngine, batchHoldActive } from './src/engine';
const log=(m:any)=>console.log('[TEST]',m);
async function main(){
  initEngine();
  S.config.mode='paper'; S.keys=null; S.status='running';
  S.config.symbol='DOGEUSDTM'; S.config.displaySymbol='DOGE';
  S.config.cycleBalance=34; S.config.leverage=15; S.config.levels=20;
  S.config.direction='long'; S.config.directionMode='fixed';
  S.multiplier=100; S._metaSym='DOGEUSDTM'; S.lastPrice=0.077; S.regime={trend:'range',shock:false};
  S.position={side:'long',size:300,entry:0.077,unrealized:-0.1,openedAt:Date.now()};
  // سلم مسلح
  S.grid=[{id:'g1',lane:'add',side:'buy',price:0.077,qty:1,status:'armed',reduceOnly:false,exchangeOrderId:null,filledAt:null,origin:'grid',createdAt:Date.now()}];
  const g1=S.grid[0];
  // تفعيل القاطع: 4 تعبئات
  S._batchFills=4; S._batchHoldAt=0; S._batchStudiedAt=false; S.studyUntil=0;
  const hold=batchHoldActive();
  log('القاطع نشط: '+(hold?'✅':'❌'));
  // ═══ المحاولة 1: مستوى شبكة عادي أثناء التوقف ═══
  const before1=S.grid.filter(x=>x.status==='armed').length;
  (S as any); // fillLevel داخلي — نستدعي عبر أي مسار متاح:
  const eng=require('./src/engine');
  eng.fillLevel(g1.id, 0.077, false);
  const stayed=S.grid.find(x=>x.id===g1.id);
  log('تعبئة شبكة أثناء التوقف: '+(stayed.status==='cancelled'?'✅ مرفوضة وأُلغيت':'❌ نُفّذت! ('+stayed.status+')'));
  // ═══ المحاولة 2: مستوى تعويض (comp) أثناء التوقف ═══
  S.grid.push({id:'comp1',lane:'comp',side:'buy',price:0.077,qty:1,status:'armed',reduceOnly:false,exchangeOrderId:null,filledAt:null,origin:'comp',createdAt:Date.now()});
  eng.fillLevel('comp1', 0.077, false);
  const comp=S.grid.find(x=>x.id==='comp1');
  log('تعبئة تعويض جني أثناء التوقف: '+(comp.status==='cancelled'?'✅ مرفوضة (كانت ثقب 8×/9×!)':'❌ نُفّذت!'));
  // ═══ المحاولة 3: جني reduceOnly يجب أن يعمل طبيعياً ═══
  S.grid.push({id:'tp1',lane:'tp',side:'sell',price:0.078,qty:1,status:'armed',reduceOnly:true,exchangeOrderId:null,filledAt:null,origin:'tp',createdAt:Date.now()});
  eng.fillLevel('tp1', 0.078, true);
  const tp=S.grid.find(x=>x.id==='tp1');
  log('جني reduceOnly أثناء التوقف: '+(tp.status==='filled'?'✅ يعمل (الجني لا يُمنع)':'❌ ممنوع! ('+tp.status+')'));
  const pass=hold&&stayed.status==='cancelled'&&comp.status==='cancelled'&&tp.status==='filled';
  log(pass?'=== الثقب مسدود: دخول مرفوض من كل المسارات، جني يعمل ===':'=== فشل ===');
  process.exit(pass?0:1);
}
main().catch(e=>{console.error('[ERR]',e&&e.message);process.exit(3);});
