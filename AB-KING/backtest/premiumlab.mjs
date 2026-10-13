/* PREMIUM LAB — سلسلة البريميوم الرسمية من المنصة (.SYMPI8H) عبر الترقيم: 90 يومًا */
const base='https://api-futures.kucoin.com';
const arg=(n,d)=>{const i=process.argv.indexOf('--'+n);return i>=0?process.argv[i+1]:d};
async function pagedPI(sym,from,to){
  const out=[]; let f=from;
  for(let guard=0;guard<30&&f<to;guard++){
    const r=await fetch(`${base}/api/v1/kline/query?symbol=.${sym}PI8H&granularity=480&from=${f}&to=${Math.min(to,f+10*86400000)}`).then(r=>r.json()).catch(()=>null);
    const rows=(r&&r.data)||[];
    if(!rows.length)break;
    rows.forEach(k=>{let t=+k[0];if(t<1e12)t*=1000;out.push({t,c:+k[4]})});
    // rows order unknown — advance from the max timestamp seen
    const ts=out.map(x=>x.t);
    const maxT=Math.max(...ts);
    if(maxT<=f)break;
    f=maxT+28800000; // advance one 8h bar
  }
  return out.sort((a,b)=>a.t-b.t);
}
const DAYS=+arg('days',90);
const syms=(arg('syms','PEPEUSDTM,XBTUSDTM,DOGEUSDTM,LONGXIAUSDTM,BATUSDTM,WUSDTM')).split(',');
for(const sym of syms){
  const prem=await pagedPI(sym,Date.now()-DAYS*86400000,Date.now());
  if(prem.length<30){console.log(sym,'→ insufficient:',prem.length);continue;}
  const vals=prem.map(x=>x.c*100);
  const sorted=[...vals].sort((a,b)=>a-b);
  const p5=sorted[Math.floor(sorted.length*0.05)];
  const med=sorted[Math.floor(sorted.length/2)];
  // signal: deep premium (<p5) → does it snap back within 24h?
  let ev=0,rec=0,snap=0;
  for(let i=0;i<prem.length-3;i++){
    if(vals[i]<p5){ ev++;
      const d3=prem[i+3].c-prem[i].c;
      if(d3>0)rec++; snap+=d3*100; }
  }
  console.log(sym.padEnd(14),prem.length+'bars | p5:',p5.toFixed(3)+'% | med:',med.toFixed(3)+'% | deep-events:',ev,'| recovered:',ev?Math.round(rec/ev*100):0,'% | avg snap-back:',ev?(snap/ev).toFixed(3):0,'%');
}
