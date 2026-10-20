import './src/server-shims';
import { S, initEngine, studiedEarlyExit } from './src/engine';
const p = 0.00004500;
async function main() {
  initEngine();
  S.config.mode='paper'; S.keys=null; S.status='running'; S.multiplier=1;
  S.takerFee=0.0006; S.makerFee=0.0002;
  S.position={side:'long',size:5000,entry:p*0.99,unrealized:0,openedAt:Date.now()-600000};
  S.journal=[{id:'j2',status:'open',side:'long',qty:5000,entry:p*0.99,fees:0.02,source:'صيد'}];
  const asks:any[]=[];
  for(let i=1;i<=10;i++) asks.push({price:p*(1+0.0002*i),size:40});
  asks.push({price:p*1.0012,size:400});
  asks.sort((a,b)=>a.price-b.price);
  S.orderBook={asks,bids:[]};
  const openFees=0.02, exitFee=5000*p*0.0006;
  const gross=(p-p*0.99)*5000;
  console.log('gross=',gross.toFixed(4),'netNow=',(gross-openFees-exitFee).toFixed(4));
  console.log('wall dist=',((p*1.0012-p)/p).toFixed(5));
  const ex=studiedEarlyExit(p);
  console.log('earlyExit=',ex?JSON.stringify(ex):'NULL');
}
main();
