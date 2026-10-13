/* GAP LAB 60-DAY — the pagination breakthrough enables 2-month deep verification */
const arg=(n,d)=>{const i=process.argv.indexOf('--'+n);return i>=0?process.argv[i+1]:d};
const base='https://api-futures.kucoin.com';
async function paged(sym,gran,from,to){
  const out=[]; let f=from;
  while(f<to){
    const r=await fetch(`${base}/api/v1/kline/query?symbol=${sym}&granularity=${gran}&from=${f}&to=${Math.min(to,f+200*gran*60000)}`).then(r=>r.json()).catch(()=>null);
    const rows=(r&&r.data)||[];
    if(!rows.length)break;
    rows.forEach(k=>{let t=+k[0];if(t<1e12)t*=1000;out.push({t,c:+k[4]})});
    const lastT=out[out.length-1].t;
    if(lastT<=f)break;
    f=lastT+1;
    if(out.length>2500)break;
  }
  return out.sort((a,b)=>a.t-b.t);
}
const DAYS=+arg('days',60);
const start=Date.now()-DAYS*86400000;
const all=await fetch(base+'/api/v1/contracts/active').then(r=>r.json());
const liq=(all.data||[]).filter(c=>/USDTM$/.test(c.symbol)&&c.status==='Open'&&+c.turnoverOf24h>5e6).map(c=>c.symbol).slice(0,+arg('syms',25));
console.log(`GAP LAB ${DAYS}d — ${liq.length} symbols`);
let TE=0,TW=0,TG=0,worstA=0,worstS='',per=[];
const monthStats={}; // per-month breakdown
for(const sym of liq){
  try{
    const [ix,pk]=await Promise.all([paged('.K'+sym.replace('USDTM','USDT'),60,start,Date.now()),paged(sym,60,start,Date.now())]);
    const idx={},perp={};
    ix.forEach(k=>idx[k.t]=k.c);
    pk.forEach(k=>perp[k.t]=k.c);
    const devs=[];
    for(const t in perp){ if(idx[t]>0&&perp[t]>0) devs.push({t:+t,d:(perp[t]-idx[t])/idx[t]*100,p:perp[t],idx:idx[t]}); }
    devs.sort((a,b)=>a.t-b.t);
    let events=0,wins=0,gain=0,worst=0;
    for(let i=27;i<devs.length-1;i++){
      const win=devs.slice(i-24,i);
      const mean=win.reduce((a,b)=>a+b.d,0)/win.length;
      const sd=Math.sqrt(win.reduce((a,b)=>a+(b.d-mean)**2,0)/win.length);
      if(sd>0.001){
        const z=(devs[i].d-mean)/sd;
        const stable=devs[i].p>=devs[i-3].p*0.9985;
        const idxNow=devs[i].idx;
        const idx6hAgo=devs[Math.max(0,i-6)].idx||idxNow;
        const idxFlat=Math.abs((idxNow-idx6hAgo)/idx6hAgo*100)<1.5;
        if(z<-2&&devs[i].d<-0.15&&stable&&idxFlat){
          for(let j=i+1;j<Math.min(i+13,devs.length);j++){
            if(devs[j].d>=mean){
              const ret=(devs[j].p-devs[i].p)/devs[i].p*100;
              events++;gain+=ret;if(ret>0)wins++;if(ret<worst)worst=ret;
              const mo=new Date(devs[i].t).toISOString().slice(0,7);
              if(!monthStats[mo])monthStats[mo]={e:0,w:0,g:0};
              monthStats[mo].e++;if(ret>0)monthStats[mo].w++;monthStats[mo].g+=ret;
              i=j;break;
            }
          }
        }
      }
    }
    if(events>0){per.push({sym,events,wins,gain,worst});TE+=events;TW+=wins;TG+=gain;
      if(worst<worstA){worstA=worst;worstS=sym}}
  }catch(e){}
}
console.log('\n── per-symbol (top 10 by gain) ──');
per.sort((a,b)=>b.gain-a.gain).slice(0,10).forEach(x=>
  console.log(x.sym.padEnd(16),'ev:',String(x.events).padStart(3),'| win:',Math.round(x.wins/x.events*100)+'%','| total: '+(x.gain>=0?'+':'')+x.gain.toFixed(2)+'%'));
console.log('\n── per-month consistency ──');
Object.keys(monthStats).sort().forEach(m=>{
  const s=monthStats[m];
  console.log(m,'| events:',s.e,'| win:',Math.round(s.w/s.e*100)+'%','| total: '+(s.g>=0?'+':'')+s.g.toFixed(2)+'%');
});
console.log('\n════ TOTAL ════');
console.log(`${TE} events | win: ${TE?Math.round(TW/TE*100):0}% | total: ${TG.toFixed(2)}% | avg/trade: ${TE?(TG/TE).toFixed(3):0}% | worst: ${worstA.toFixed(2)}% on ${worstS}`);
