declare global {
  interface Window {
    Capacitor?: any;
    webkitAudioContext?: any;
    __kingHb?: any;
    __KING_REMOTE?: string | null;
    __chartDbg?: any;
    __S?: any;
  }
}

/* ================================================================
   KuCoin Futures — عميل WebSocket عام (تدفق مستمر بلا انقطاع)
   - يجلب رمز bullet-public عبر البروكسي المحلي (/kucoin)
   - يتصل بخادم الدفع wss://ws-api-futures.kucoin.com
   - يشترك في: السعر اللحظي + دفتر الأوامر + الصفقات المنفذة
   - نبض ping/pong حسب مواصفات KuCoin + إعادة اتصال تلقائية
   ================================================================ */
const DIRECT='https://api-futures.kucoin.com';
// على الجوال (Capacitor) لا بروكسي — مباشر من البداية
let REST=(typeof window!=='undefined'&&(window.Capacitor||location.protocol==='capacitor:'))?DIRECT:'/kucoin';

export const streamState = {
  connected:false, connecting:false, lastMsgAt:0, bookAt:0, tradeAt:0,
  msgs:0, reconnects:0, error:null as any,
};

let ws:any=null, pingTimer:any=null, watchTimer:any=null, retryTimer:any=null;
let symbol:any=null, onMessage:any=null, onStatus:any=null;
let stopped=true, attempts=0, connectSeq=0;
let offline=false;

function forceReconnect(reason:any){
  if(stopped) return;
  if(retryTimer){clearTimeout(retryTimer);retryTimer=null;}
  connectSeq++;
  clearTimers();
  if(ws){ try{ws.close();}catch{ /* intentional: optional step */ } ws=null; }
  streamState.connected=false; streamState.connecting=false;
  streamState.error=String(reason||'reconnect');
  attempts=0;
  connect();
  if(onStatus) onStatus({...streamState});
}

async function getBullet(){
  async function tryFetch(base){
    const res=await fetch(base+'/api/v1/bullet-public',{method:'POST',cache:'no-store'});
    if(!res.ok) throw new Error('bullet http '+res.status);
    // خادم الجوال المحلي يرجع HTML برمز 200 — اعتبره فشل بروكسي وتحوّل للمباشر
    if(!((res.headers.get('content-type')||'').toLowerCase().includes('json'))) throw new Error('bullet not json');
    return res;
  }
  let res;
  try{ res=await tryFetch(REST); }
  catch(e){ if(REST===DIRECT) throw e; REST=DIRECT; res=await tryFetch(REST); }
  const j=await res.json();
  if(j.code!=='200000') throw new Error(j.msg||('bullet '+j.code));
  const srv=(j.data.instanceServers||[])[0]||{};
  return { token:j.data.token, endpoint:srv.endpoint,
    pingInterval:srv.pingInterval||18000, pingTimeout:srv.pingTimeout||10000 };
}

function topics(sym){
  return [
    '/contractMarket/tickerV2:'+sym,      // السعر اللحظي
    '/contractMarket/level2Depth5:'+sym,  // دفتر الأوامر (أفضل 5 مستويات)
    '/contractMarket/execution:'+sym,     // الصفقات المنفذة لحظيًا
  ];
}

function subscribe(){
  if(!ws||ws.readyState!==1||!symbol) return;
  for(const t of topics(symbol)){
    ws.send(JSON.stringify({id:Date.now()+''+Math.floor(Math.random()*1000),
      type:'subscribe', topic:t, privateChannel:false, response:true}));
  }
}

function clearTimers(){
  if(pingTimer){clearInterval(pingTimer);pingTimer=null;}
  if(watchTimer){clearInterval(watchTimer);watchTimer=null;}
}

function handleData(j){
  streamState.lastMsgAt=Date.now(); streamState.msgs++;
  if(j.type==='pong'||j.type==='ack'||j.type==='welcome') return;
  if(j.type!=='message'||!j.topic||!j.data) return;
  const d=j.data;
  const msgSym=j.topic.includes(':')?j.topic.split(':')[1]:null;
  if(msgSym&&symbol&&msgSym!==symbol) return; // تجاهل رسائل الزوج القديم بعد التبديل
  if(j.topic.startsWith('/contractMarket/ticker')){
    let price=+d.price||0;
    const bb=+d.bestBidPrice||0, ba=+d.bestAskPrice||0;
    if(!price&&bb>0&&ba>0) price=(bb+ba)/2; // منتصف السبريد عند غياب آخر صفقة
    if(price>0&&onMessage) onMessage({symbol:msgSym,price, side:d.side, size:+d.size||0});
  }else if(j.topic.startsWith('/contractMarket/level2')){
    const bids=(d.bids||[]).map(r=>({price:+r[0],size:+r[1]})).filter(r=>r.price>0);
    const asks=(d.asks||[]).map(r=>({price:+r[0],size:+r[1]})).filter(r=>r.price>0);
    if(bids.length||asks.length){ streamState.bookAt=Date.now();
      if(onMessage) onMessage({symbol:msgSym,book:{bids,asks}}); }
  }else if(j.topic.startsWith('/contractMarket/execution')){
    streamState.tradeAt=Date.now();
    if(onMessage) onMessage({symbol:msgSym,trade:{side:String(d.side||''),size:+d.size||0,price:+d.price||0}});
  }
}

async function connect(){
  if(stopped) return;
  const seq=++connectSeq;
  streamState.connecting=true; streamState.error=null;
  if(onStatus) onStatus({...streamState});
  try{
    const b=await getBullet();
    if(stopped||seq!==connectSeq) return;
    const socket=new WebSocket(b.endpoint+'?token='+b.token+'&connectId='+seq+'_'+Date.now().toString(36));
    ws=socket;
    socket.onopen=()=>{
      if(seq!==connectSeq){ try{socket.close();}catch{ /* intentional: optional step */ } return; }
      attempts=0;
      streamState.connected=true; streamState.connecting=false;
      subscribe();
      pingTimer=setInterval(()=>{
        try{ socket.send(JSON.stringify({id:String(Date.now()),type:'ping'})); }catch{ /* intentional: optional step */ }
      },Math.max(5000,b.pingInterval-2000));
      // مراقب النبض: إن صمت الخادم أكثر من المهلة نُغلق لإجبار إعادة الاتصال
      watchTimer=setInterval(()=>{
        if(Date.now()-streamState.lastMsgAt>b.pingInterval+b.pingTimeout+4000){
          try{socket.close();}catch{ /* intentional: optional step */ }
        }
      },5000);
      streamState.lastMsgAt=Date.now();
      if(onStatus) onStatus({...streamState});
    };
    socket.onmessage=(ev)=>{ try{ handleData(JSON.parse(ev.data)); }catch{ /* intentional: optional step */ } };
    socket.onerror=()=>{ streamState.error='خطأ في القناة'; };
    socket.onclose=()=>{
      if(seq!==connectSeq) return;
      clearTimers();
      streamState.connected=false; streamState.connecting=false;
      if(onStatus) onStatus({...streamState});
      scheduleRetry();
    };
  }catch(e){
    streamState.connecting=false; streamState.error=String((e as any)?.message||e);
    if(onStatus) onStatus({...streamState});
    scheduleRetry();
  }
}

function scheduleRetry(){
  if(stopped) return;
  if(offline){
    // الإنترنت مقطوع: لا تراكم محاولات — أول عودة للشبكة تعيد الاتصال فورًا
    if(retryTimer) clearTimeout(retryTimer);
    retryTimer=null;
    return;
  }
  attempts++;
  streamState.reconnects++;
  const base=Math.min(15000,1000*Math.pow(1.6,Math.min(attempts,8)));
  const wait=base+Math.floor(Math.random()*750);
  if(retryTimer) clearTimeout(retryTimer);
  retryTimer=setTimeout(connect,wait);
}

// المراقب الأصلي (Java ConnectivityManager) — يعمل حتى مع التطبيق في الخلفية
// لأنه حدث نظام لا مؤقت WebView. يبث "netChange" من KingNative.startWatchdog().
if(typeof window!=='undefined'&&window.Capacitor){
  const KN=(window.Capacitor.Plugins||{}).KingNative;
  if(KN&&KN.addListener){
    try{
      const sub=KN.addListener('netChange',(d:any)=>{
        const online=!!(d&&d.online);
        if(online){
          offline=false;
          if(stopped) return;
          forceReconnect('المراقب الأصلي: عاد الإنترنت — وصل فوري');
        }else{
          offline=true;
          if(retryTimer){clearTimeout(retryTimer);retryTimer=null;}
          streamState.connected=false;
          streamState.error='انقطع الإنترنت — المراقب الأصلي ينتظر عودة الشبكة';
          if(onStatus) onStatus({...streamState});
        }
      });
      // addListener يُرجع Promise على بعض المنصات وكائن اشتراك مباشر على أخرى — نتعامل مع الحالتين
      if(sub&&typeof (sub as any).catch==='function') (sub as any).catch(()=>{});
    }catch{ /* intentional: optional step */ }
    // الحالة الأولية من نظام التشغيل مباشرة
    try{ if(KN.isOnline) KN.isOnline().then((r:any)=>{ offline=!(r&&r.online); }).catch(()=>{}); }catch{ /* intentional: optional step */ }
  }
}

export function startStream(sym,handlers){
  stopStream();
  stopped=false; attempts=0;
  symbol=sym; onMessage=handlers.onMessage; onStatus=handlers.onStatus;
  connect();
}

export function setStreamSymbol(sym){
  if(sym===symbol) return;
  const old=symbol;
  symbol=sym;
  if(ws&&ws.readyState===1){
    // إلغاء اشتراك الزوج القديم حتى لا تختلط بياناته مع الجديد
    if(old) for(const t of topics(old)){
      try{ ws.send(JSON.stringify({id:'u'+Date.now()+t.length,type:'unsubscribe',topic:t,privateChannel:false,response:false})); }catch{ /* intentional: optional step */ }
    }
    subscribe();
  }
}

export function stopStream(){
  stopped=true;
  if(retryTimer){clearTimeout(retryTimer);retryTimer=null;}
  clearTimers();
  connectSeq++;
  if(ws){ try{ws.close();}catch{ /* intentional: optional step */ } ws=null; }
  streamState.connected=false; streamState.connecting=false;
}

// عند عودة التبويب للواجهة: تحقق فوري من حياة القناة
if(typeof document!=='undefined'){
  document.addEventListener('visibilitychange',()=>{
    if(!document.hidden&&!stopped){
      const silent=Date.now()-streamState.lastMsgAt;
      if(!ws||ws.readyState!==1||silent>30000){ forceReconnect('عودة التطبيق للواجهة والقناة صامتة'); }
    }
  });
}

// استماع لأحداث الشبكة: عودة الإنترنت تعيد الاتصال فورًا، والانقطاع يوقف المحاولات العبثية
if(typeof window!=='undefined'){
  window.addEventListener('online',()=>{
    offline=false;
    if(stopped) return;
    forceReconnect('عودة الاتصال بالإنترنت — إعادة وصل القناة فورًا');
  });
  window.addEventListener('offline',()=>{
    offline=true;
    streamState.connected=false;
    streamState.error='انقطع الاتصال بالإنترنت — في انتظار عودة الشبكة';
    if(onStatus) onStatus({...streamState});
    if(retryTimer){clearTimeout(retryTimer);retryTimer=null;}
  });
}

// في الجوال (Capacitor) لا يوجد حدث offline موثوق في WebView دائمًا — فحص دوري لكل من حالة الملاحّح وحياة القناة
if(typeof navigator!=='undefined'&&navigator.onLine!==undefined){
  setInterval(()=>{
    if(stopped) return;
    const online=typeof navigator.onLine==='boolean'?navigator.onLine:true;
    if(online&&offline){
      offline=false;
      forceReconnect('شُوهدت عودة الشبكة (فحص دوري)');
    }
    if(!online&&!offline){
      offline=true;
      if(retryTimer){clearTimeout(retryTimer);retryTimer=null;}
      streamState.connected=false;
      streamState.error='انقطع الاتصال بالإنترنت';
      if(onStatus) onStatus({...streamState});
    }
    if(online&&!offline&&ws&&ws.readyState===1){
      const silent=Date.now()-streamState.lastMsgAt;
      if(silent>45000) forceReconnect('صمت القناة أكثر من 45 ثانية أثناء العمل في الخلفية');
    }
  },10000);
}
