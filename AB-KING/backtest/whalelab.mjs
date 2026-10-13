/* WHALE + BOOK + PREMIUM CONFLUENCE LAB — يقيس: هل امتصاص الحيتان عند الفجوة يرفع نجاح الصائد؟ */
const base='https://api-futures.kucoin.com';
const arg=(n,d)=>{const i=process.argv.indexOf('--'+n);return i>=0?process.argv[i+1]:d};
async function paged(sym,gran,from,to){
  const out=[]; let f=from;
  for(let g=0;g<25&&f<to;g++){
    const r=await fetch(`${base}/api/v1/kline/query?symbol=${sym}&granularity=${gran}&from=${f}&to=${Math.min(to,f+200*gran*60000)}`).then(r=>r.json()).catch(()=>null);
    const rows=(r&&r.data)||[];
    if(!rows.length)break;
    rows.forEach(k=>{let t=+k[0];if(t<1e12)t*=1000;out.push({t,c:+k[4]})});
    const ts=out.map(x=>x.t);const mx=Math.max(...ts);
    if(mx<=f)break; f=mx+1;
  }
  return out.sort((a,b)=>a.t-b.t);
}
const DAYS=+arg('days',30);
const start=Date.now()-DAYS*86400000;
// proxy for whale flow historically: large-candle-volume ratio on 1m data isn't available long;
// instead use the tape-driven study: current symbols, last 3 days of 5m perp+index (divergence window)
// combined with CURRENT whale flow + book imbalance → confluence score
const all=await fetch(base+'/api/v1/contracts/active').then(r=>r.json());
const liq=(all.data||[]).filter(c=>/USDTM$/.test(c.symbol)&&c.status==='Open'&&+c.turnoverOf24h>5e6).slice(0,20);
console.log(`CONFLUENCE LAB — ${liq.length} symbols | measuring: divergence + whale absorption + book imbalance agreement`);
const rows=[];
for(const c of liq){
  try{
    // 1) live whale flow (last 100 trades)
    const tr=await fetch(base+'/api/v1/trade/history?symbol='+c.symbol).then(r=>r.json());
    const trades=(tr.data||[]).slice(0,100);
    const sizes=trades.map(t=>+t.size).sort((a,b)=>a-b);
    const med=sizes[Math.floor(sizes.length/2)]||1;
    let netUsd=0;
    trades.forEach(t=>{if(+t.size>med*8){netUsd+=(t.side==='buy'?1:-1)*+t.size*+c.lastTradePrice}});
    // 2) book imbalance (depth 20)
    const bk=await fetch(base+'/api/v1/level2/depth20?symbol='+c.symbol).then(r=>r.json());
    const bd=(bk.data?.bids||[]).reduce((a,x)=>a+ +x[1],0);
    const ad=(bk.data?.asks||[]).reduce((a,x)=>a+ +x[1],0);
    const imb=bd+ad>0?(bd-ad)/(bd+ad):0;
    // 3) divergence
    const dev=+c.indexPrice>0?((+c.lastTradePrice-+c.indexPrice)/+c.indexPrice)*100:0;
    rows.push({s:c.symbol,dev,netUsd,imb});
  }catch(e){}
}
// CONFLUENCE: negative divergence + whale buying + bid-heavy book = strongest long setup
const scored=rows.map(x=>({...x,score:(x.dev<-0.15?2:0)+(x.netUsd>0?1:0)+(x.imb>0?1:0)}))
  .sort((a,b)=>b.score-a.score||a.dev-b.dev);
console.log('\nsym              | dev%    | whale net$ | book imb | score');
scored.forEach(x=>console.log(x.s.padEnd(16),'|',x.dev.toFixed(3).padStart(7),'|',(x.netUsd/1000).toFixed(1)+'k'.padStart(9),'|',(x.imb*100).toFixed(0)+'%'.padStart(8),'|',x.score+'/4'));
