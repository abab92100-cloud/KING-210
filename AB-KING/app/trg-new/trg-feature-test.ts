import './src/server-shims';
import { S, initEngine, saveCfg } from './src/engine';
const log=(m:any)=>console.log('[TEST]',m);
async function main(){
  initEngine();
  S.config.mode='paper'; S.keys=null; S.status='running';
  S.config.symbol='DOGEUSDTM'; S.config.displaySymbol='DOGE';
  S.config.cycleBalance=34; S.config.leverage=15;
  S.multiplier=100; S._metaSym='DOGEUSDTM'; S.lastPrice=0.077;
  saveCfg({ symbol:'DOGEUSDTM', tpUsd:1.5 });
  const a = (S.config as any).tpUsd;
  saveCfg({ symbol:'DOGEUSDTM', tpUsd:3 });
  const b = (S.config as any).tpUsd;
  saveCfg({ symbol:'DOGEUSDTM', tpUsd:0 });
  const c = (S.config as any).tpUsd;
  log('حفظ 1.5$ → tpUsd='+a+' '+(Math.abs(a-1.5)<1e-9?'✅':'❌'));
  log('حفظ 3$ → tpUsd='+b+' '+(Math.abs(b-3)<1e-9?'✅':'❌'));
  log('صفر = مدروس → tpUsd='+c+' '+(c===0?'✅':'❌'));
  const pass = Math.abs(a-1.5)<1e-9 && Math.abs(b-3)<1e-9 && c===0;
  log(pass?'=== جني الربح الثابت بالدولار يعمل: 1$/3$/صفر=مدروس ===':'=== فشل ===');
  process.exit(pass?0:1);
}
main().catch(e=>{console.error('[ERR]',e&&e.message);process.exit(3);});
