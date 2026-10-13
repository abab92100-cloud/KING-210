/* GAP HUNTER LAB — يحاكي استراتيجية صائد الفجوات على تاريخ الإنديكس الحقيقي */
const arg=(n,d)=>{const i=process.argv.indexOf('--'+n);return i>=0?process.argv[i+1]:d};
const DAYS=+arg('days',8);
const base='https://api-futures.kucoin.com';
const realFetch=globalThis.fetch;

async function klines(sym,gran,from,to){
  const out=[];
  let f=from;
  while(f<to){
    const r=await realFetch(`${base}/api/v1/kline/query?symbol=${sym}&granularity=${gran}&from=${f}&to=${Math.min(f+200*gran*60000,to)}`).then(r=>r.json()).catch(()=>null);
    const rows=r&&r.data?r.data:[];
    if(!rows.length)break;
    rows.forEach(k=>{let t=+k[0];if(t<1e12)t*=1000;out.push({t,o:+k[1],h:+k[2],l:+k[3],c:+k[4]})});
    const lastT=+rows[rows.length-1][0]*(+rows[rows.length-1][0]<1e12?1000:1);
    if(lastT<=f)break;
    f=lastT+gran*60000;
  }
  return out.filter(k=>k.c>0).sort((a,b)=>a.t-b.t);
}

// liquid symbols list
const all=await realFetch(base+'/api/v1/contracts/active').then(r=>r.json());
const liq=(all.data||[]).filter(c=>/USDTM$/.test(c.symbol)&&c.status==='Open'&&+c.turnoverOf24h>5e6).map(c=>c.symbol).slice(0,40);
console.log(`GAP LAB — ${liq.length} liquid symbols, ${DAYS} days (5m index history ~16h limit → use 60m for depth)`);

const end=Date.now()-+arg('ago',0)*86400000, start=end-DAYS*86400000;
let TE=0,TW=0,TG=0,worstA=0,worstS='',perSym=[];
for(const sym of liq){
  try{
    const [ix,pk]=await Promise.all([
      klines('.K'+sym.replace('USDTM','USDT'),60,start,end),
      klines(sym,60,start,end),
    ]);
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
        // KILLER FILTER: index (cross-exchange fair value) must be FLAT — the flush is KuCoin-local only
        const idxNow=devs[i].idx;
        const idx6hAgo=devs[Math.max(0,i-6)].idx||idxNow;
        const idxFlat=Math.abs((idxNow-idx6hAgo)/idx6hAgo*100)<1.5; // index moved <1.5% in 6h = no market-wide crash
        if(z<-2&&devs[i].d<-0.15&&stable&&idxFlat){
          // exit when dev returns to mean (max 12 bars)
          for(let j=i+1;j<Math.min(i+13,devs.length);j++){
            if(devs[j].d>=mean){
              const ret=(devs[j].p-devs[i].p)/devs[i].p*100;
              events++;gain+=ret;if(ret>0)wins++;if(ret<worst)worst=ret;
              i=j;break;
            }
          }
        }
      }
    }
    if(events>0){perSym.push({sym,events,wins,gain,worst});TE+=events;TW+=wins;TG+=gain;
      if(worst<worstA){worstA=worst;worstS=sym}}
  }catch(e){}
}
perSym.sort((a,b)=>b.gain-a.gain).slice(0,12).forEach(x=>
  console.log(x.sym.padEnd(16),'events:',String(x.events).padStart(3),'| win:',Math.round(x.wins/x.events*100)+'%','| total: '+(x.gain>=0?'+':'')+x.gain.toFixed(2)+'%','| worst:',x.worst.toFixed(2)+'%'));
console.log('════════════════════');
console.log(`TOTAL: ${TE} events | win: ${TE?Math.round(TW/TE*100):0}% | total: ${TG.toFixed(2)}% | worst: ${worstA.toFixed(2)}% on ${worstS}`);
console.log(`avg per trade: ${TE?(TG/TE).toFixed(3):0}%`);
