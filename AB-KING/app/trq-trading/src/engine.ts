/* ================================================================
   AB-KING — بوت تداول عقود KuCoin الآجلة
   النسخة المتصلة: بيانات لحظية عبر WebSocket (دفع مستمر) +
   REST عبر بروكسي محلي (/kucoin) لتجاوز CORS.
   منطق المحرك مطابق للنسخة الأصلية: شبكة + صيد + جني +
   انعكاس بشبكتين واسعتين + هروب من التصفية.
   ================================================================ */
import { startStream, setStreamSymbol, streamState } from './ws';
declare global {
  interface Window {
    Capacitor?: any;
    webkitAudioContext?: any;
    __kingHb?: any;
    __ABKING_REMOTE?: string | null;
    __chartDbg?: any;
    __S?: any;
  }
}


const TICK_MS = 1000; // القرارات كل ثانية على أسعار لحظية من القناة
const MIN_NET_USD = 0.05, MAX_HUNT_OPEN = 3, HUNT_COOLDOWN = 10000,
      ADD_COOLDOWN = 15000, SCOUT_MS = 90000, MIN_HUNT_GAP = 0.08;
// جني الربح الصافي بعد الرسوم (بالدولار) — الافتراضي 0.12 ويضبطه المالك من
// الإعدادات (S.config.tpNet)؛ يُطبَّق على المركز كاملًا بمعادلة واحدة مهما
// تعددت الأوامر المفتوحة
const TP_BASE_NET = 0.12; // لم يعد مستخدمًا كهدف ثابت — الهدف الآن مدروس ديناميكي (قاعدة المالك)
// حد الكارثة (بالدولار): الإغلاق الوحيد بخسارة = انعكاس مؤكد بضغط معاكس قوي
// مع بلوغ الخسارة هذا المبلغ — ما دونه لا إغلاق بخسارة أبدًا
const LOSS_STOP_USD = 16;
/* ─── هدف الجني المدروس — لا رقم ثابت (قاعدة المالك) ───
   الهدف يُشتق من دراسة الوضع الحالي ويتحدث باستمرار مع كل دراسة وبكل نبضة:
   · رصيد الدورة: الهدف نسبة منه — يكبر مع رأس المال ويصغر معه تلقائياً
   · تقلب العملة المقاس (بصمة الدراسة): تقلب أعلى = مساحة حركة أوسع = هدف أوسع
   · سبريد العملة المقاس: احتكاك التنفيذ مغطى بالكامل فوق الرسوم
   · التتبع الصاعد فوق الهدف (trailTarget) يكمل اقتناص الموجات القوية
   الأرضية الصلبة: الرسوم مغطاة + 5 سنت صافياً على الأقل — لا يقفل بخسارة أبداً */
export function dynamicTpNet(){
  const hf=S&&S.config&&S.config.hfMode;
  // الأساس حجم الصفقة الفعلي (قاعدة المالك) — لا رأس المال وحده: المركز
  // الكبير يُجنى بمساحته الحقيقية والصغير بمساحته، والدراسة تعدّل المسافة
  const posN=S.position?Math.max(0,(S.position.size||0)*(S.multiplier||1)*(S.position.entry||0)):0;
  const bal=Math.max(1,posN||((S.config&&S.config.cycleBalance)||100)*0.8);
  const prof=profOf()||{};
  const vol=Math.max(0.005,+prof.vol||0.05);    // تقلب % المقاس من دراسة البصمة
  const spread=Math.max(0,+prof.spread||0.02);  // سبريد % المقاس
  const volMult=clamp(vol/0.05,0.6,3);          // مرجع التقلب 0.05%
  // نسبة من حجم الصفقة تتوسع مع التقلب المقاس: 0.2% .. 1.2% (مراجعة شاملة —
  // لا أهداف تافهة: الربح المستحق مساحة حقيقية من الحركة)
  let target=bal*clamp(0.003*volMult,0.002,0.008);
  // احتكاك السبريد مغطى بالكامل مع ربح صافٍ
  target=Math.max(target, bal*spread/100*2+0.05);
  if(hf) target=Math.min(target,0.05);           // وضع الجني السريع
  // سقف المنع (درس PENGU): التعرض المضخم بمستويات المتوسط كان يضخم الهدف معه
  // (4.44$ على مركز 6 مستويات) فيصبح منالًا لا يدانيه السوق فيعلق المركز ساعات.
  // الهدف لا يتجاوز 1.5% من رصيد الدورة مهما كبر المتوسط
  target=Math.min(target,Math.max(0.30,(S.config&&S.config.cycleBalance||0)*0.015));
  // الأرضية 8 سنت (كانت 15): على الأحجام الصغيرة كانت الأرضية تضخم مسافة
  // الجني إلى ~1% — النطاق النسبي (0.2-0.8%) هو الحاكم والحجم الأكبر يجني أوسع
  return Math.max(0.08,Math.round(target*100)/100); }
function tpBase(){
  // ─── TRG-NEW (قاعدة المالك): جني الربح الثابت بالدولار من الإعدادات ───
  // المالك يضع 1$ أو 3$ أو أي مبلغ → الجني عند بلوغ صافي المركز هذا المبلغ
  // بعد الرسوم، مهما كانت الأوامر. صفر = الهدف المدروس الديناميكي (كـTRQ)
  const fix=Number(S.config.tpUsd);
  if(fix>0) return fix;
  return dynamicTpNet(); } // الهدف المرجعي: الثابت بالدولار أو المدروس

/* ---------- أدوات ---------- */
const clamp=(v,a,b)=>Math.min(b,Math.max(a,v));
const uid=p=>p+'_'+Math.random().toString(36).slice(2,10)+Date.now().toString(36).slice(-4);
const fmtPx=n=>{ n=Number(n); if(!n||!isFinite(n)||n<=0) return '—';
  if(n>=1000) return n.toLocaleString('en-US',{maximumFractionDigits:2});
  if(n>=1) return n.toFixed(2);
  // كسور صغيرة جدًا (عملات صفرية): منازل عشرية حقيقية حتى 12 خانة — لا صيغة علمية،
  // وبدقة تطابق عرض المنصة (0.0000040212 لا 0.000004021) حتى لا يُظن اختلافًا بالدخول
  const d=Math.min(12,Math.max(4,-Math.floor(Math.log10(n))+4));
  return n.toFixed(d); };
const fmtUsd=(n,d=2)=>{ const s=n<0?'-':n>0?'+':''; return s+Math.abs(n).toFixed(d); };
const fmtTime=t=>new Date(t).toLocaleTimeString('en-GB',{hour12:false});
const nextEma=(p,x,k)=>p==null?x:x*k+p*(1-k);
const volSum=l=>(l||[]).reduce((a,b)=>a+(b.size||0),0);
function roundTick(p,t){ if(!t||t<=0) return p;
  const n=Math.round(p/t)*t, d=Math.max(0,Math.ceil(-Math.log10(t)));
  return Number(n.toFixed(d)); }
function beep(tone:any){ try{ if(typeof window==='undefined') return; const c=_kaCtx||new (window.AudioContext||window.webkitAudioContext)();
  // سياق البقاء الصامت شغّال أصلًا — استخدامه يتجاوز قفل التشغيل التلقائي في الويب فيو
  if(c.state==='suspended') c.resume().catch(()=>{});
  const o=c.createOscillator(),g=c.createGain(); o.connect(g); g.connect(c.destination);
  const t=c.currentTime;
  if(tone==='bell'){ o.type='sine'; o.frequency.setValueAtTime(1319,t);
    o.frequency.setValueAtTime(1760,t+.1);
    g.gain.setValueAtTime(.16,t); g.gain.exponentialRampToValueAtTime(.001,t+.35); o.start(t); o.stop(t+.36); }
  else if(tone==='alarm'){ o.type='square'; o.frequency.value=540;
    g.gain.setValueAtTime(.1,t); g.gain.setValueAtTime(0,t+.12);
    g.gain.setValueAtTime(.1,t+.18); g.gain.setValueAtTime(0,t+.32); o.start(t); o.stop(t+.36); }
  else { o.type='sine'; o.frequency.setValueAtTime(880,t);
    o.frequency.exponentialRampToValueAtTime(660,t+.14);
    g.gain.setValueAtTime(.1,t); g.gain.exponentialRampToValueAtTime(.001,t+.22); o.start(t); o.stop(t+.24); }
  if(c!==_kaCtx) setTimeout(()=>{ try{c.close();}catch(e){ logSwallow("silent-catch", e); } },800); }catch(e){ logSwallow("silent-catch", e); } }
/* إشعارات الجوال الأصلية — تعمل والتطبيق في الخلفية */
function lnPlugin(){ try{ if(typeof window==='undefined') return null; return window.Capacitor&&window.Capacitor.Plugins&&window.Capacitor.Plugins.LocalNotifications; }catch(e){ return null; } }
let _channelsReady=false;
function ensureChannels(LN){ if(_channelsReady||!LN.createChannel) return; _channelsReady=true;
  // قناة تنبيهات عالية الأهمية: تظهر منبثقة مع صوت حتى والشاشة مقفلة
  LN.createChannel({id:'king_alerts',name:'تنبيهات KING',description:'صفقات وجني أرباح وتحذيرات',
    importance:5,visibility:1,vibration:true,lights:true}).catch(()=>{});
  // قناة حالة دائمة صامتة للإشعار المستمر
  LN.createChannel({id:'king_status',name:'حالة KING',description:'إشعار البقاء الدائم',
    importance:2,visibility:-1,vibration:false}).catch(()=>{}); }
function nativeNotify(title:any,body:any,ongoing?:any){ const LN=lnPlugin(); if(!LN) return;
  ensureChannels(LN);
  LN.requestPermissions().then(r=>{ if(r.display!=='granted') return;
    LN.schedule({notifications:[{id:ongoing?7:((Date.now()%2000000000)+10),
      title,body,ongoing:!!ongoing,autoCancel:!ongoing,
      channelId:ongoing?'king_status':'king_alerts'}]}).catch(()=>{}); }).catch(()=>{}); }
/* بقاء صوتي صامت — تشغيل تيار صوتي غير مسموع يمنع نظام الويب من خنق
   مؤقتات المحرك وقناة الأسعار عند إطفاء الشاشة (السبب الأكبر لتوقف البوت) */
let _kaCtx:any=null;
export function startSilentKeepAlive(){ if(_kaCtx||!(typeof window!=='undefined'&&window.Capacitor)) return;
  try{ const c=new (window.AudioContext||window.webkitAudioContext)(); _kaCtx=c;
    const o=c.createOscillator(),g=c.createGain(); g.gain.value=0.0001;
    o.frequency.value=18; o.connect(g); g.connect(c.destination); o.start();
    setInterval(()=>{ if(_kaCtx&&_kaCtx.state!=='running') _kaCtx.resume().catch(()=>{}); },15000);
  }catch(e){ logSwallow("silent-catch", e); } }
/* خدمة أمامية أندرويد: إشعار دائم يُبقي العملية حية ويمنع النظام من قتلها في الخلفية،
   + طلب استثناء «تحسين البطارية» مرة واحدة — السبب الثاني الأكبر لتوقف البوت أثناء النوم */
let _fsTimer:any=null;
function fsStart(){ if(!(typeof window!=='undefined'&&window.Capacitor)) return;
  try{ const FS=((window.Capacitor.Plugins||{}).ForegroundService);
    if(!FS||!FS.startForegroundService) return;
    // بلا serviceType صريح: أندرويد يستخدم النوع المعلن في المانيفست (specialUse)
    // — القيمة القديمة (1 = camera) كانت تُفشل بدء الخدمة وتُسقط التطبيق كليًا
    FS.startForegroundService({id:7,title:'KING يعمل',
      body:'متصل بالمنصة ويراقب الصفقات باستمرار',smallIcon:'ic_launcher',silent:true,
      }).catch(()=>{});
  }catch(e){ logSwallow("silent-catch", e); } }
function startForegroundSvc(){ if(!(typeof window!=='undefined'&&window.Capacitor)) return;
  try{ const P=(window.Capacitor&&window.Capacitor.Plugins)||{};
    const FS=P.ForegroundService;
    fsStart();
    // إعادة تأكيد الخدمة كل 5 دقائق وعند العودة للواجهة — لو قتلها النظام تعود فورًا
    if(!_fsTimer) _fsTimer=setInterval(()=>{ try{ fsStart(); }catch(e){ logSwallow("silent-catch", e); } },300000);
    if(typeof document!=='undefined') document.addEventListener('visibilitychange',()=>{
      if(!document.hidden){ try{ fsStart(); }catch(e){ logSwallow("silent-catch", e); } } });
    // أندرويد 13+: إذن الإشعارات مطلوب لإظهار إشعار الخدمة الأمامية
    try{ if(FS&&FS.requestPermissions&&!(localStorage.getItem('abking_notif_asked')))
      FS.requestPermissions().then(()=>{ localStorage.setItem('abking_notif_asked','1'); }).catch(()=>{}); }catch(e){ logSwallow("silent-catch", e); }
    const BO=P.BatteryOptimization;
    if(BO&&BO.isBatteryOptimizationEnabled&&!(localStorage.getItem('abking_bo_asked')))
      BO.isBatteryOptimizationEnabled().then(r=>{
        if(r&&r.enabled&&BO.requestIgnoreBatteryOptimization){
          localStorage.setItem('abking_bo_asked','1');
          BO.requestIgnoreBatteryOptimization().catch(()=>{}); } }).catch(()=>{});
  }catch(e){ logSwallow("silent-catch", e); } }
/* قفل استيقاظ المعالج + منعّاش أصلي: الإضافة المحلية KingNative تمنع نوم المعالج
   بإطفاء الشاشة، وتراقب نبض المحرك من خارج الويب فيو — إن جُمّد 90 ثانية
   أُعيد تحميل التطبيق تلقائيًا فيُستأنف الاتصال والعمل دون تدخل */
function startNativeKeepAlive(){ if(!(typeof window!=='undefined'&&window.Capacitor)) return;
  const N=(window.Capacitor.Plugins||{}).KingNative; if(!N) return;
  try{ N.acquireWakeLock&&N.acquireWakeLock().catch(()=>{}); }catch(e){ logSwallow("silent-catch", e); }
  try{ N.startWatchdog&&N.startWatchdog().catch(()=>{}); }catch(e){ logSwallow("silent-catch", e); }
  if(!window.__kingHb) window.__kingHb=setInterval(()=>{
    try{ N.heartbeat&&N.heartbeat().catch(()=>{}); }catch(e){ logSwallow("silent-catch", e); } },15000);
  pushLog('server','قفل المعالج والمنعّاش الذاتي فعّالان — المحرك لا ينام بإطفاء الشاشة'); }
// علامة الإنعاش: المنعّاش الأصلي أعاد تحميل التطبيق بعد تجميد — يُسجَّل عند الإقلاع
function logRevival(){ try{ const at=localStorage.getItem('abking_revived');
  if(at){ localStorage.removeItem('abking_revived');
    const s=Math.max(1,Math.round((Date.now()-+at)/1000));
    pushLog('server','أُنعش التطبيق تلقائيًا بعد تجميد النظام له — استؤنف العمل خلال '+s+' ث'); } }catch(e){ logSwallow("silent-catch", e); } }
function clearNativeOngoing(){ const LN=lnPlugin(); if(LN) LN.cancel({notifications:[{id:7}]}).catch(()=>{}); }
function notify(txt:any,kind?:any){ if(S.sound){
    // نغمة مميزة لكل حدث: ربح = نغمتان صاعدتان · تحذير = تنبيه قوي · عادي = النغمة المختارة
    if(kind==='win'){ beep(S.soundTone); setTimeout(()=>{ try{beep('bell');}catch(e){ logSwallow("silent-catch", e); } },200); }
    else if(kind==='warn'){ beep('alarm'); }
    else beep(S.soundTone);
    try{ if(typeof navigator!=='undefined'&&navigator.vibrate) navigator.vibrate(kind==='win'?[80,60,140]:120); }catch(e){ logSwallow("silent-catch", e); } }
  pushLog(kind==='warn'?'error':'fill',txt); nativeNotify('AB-KING',txt); }

/* ---------- مخزن الحالة + إشعارات الواجهة ---------- */
const listeners=new Set();
let emitTimer:any=null;
export function subscribe(fn){ listeners.add(fn); return ()=>listeners.delete(fn); }
export function emit(){ for(const fn of [...listeners] as any[]) fn(); }
function emitThrottled(){
  if(emitTimer) return;
  // توفير البطارية: التطبيق في الخلفية = تحديثات واجهة أبطأ، والمحرك والتنفيذ بلا تغيير
  const d=(typeof document!=='undefined'&&document.hidden)?1500:300;
  emitTimer=setTimeout(()=>{ emitTimer=null; emit(); },d);
}
function toast(m:any){ S.toastMsg={text:m,at:Date.now()}; emit(); }

/* ================================================================
   وضع سيرفر الجوال (Termux) — العقل يعمل في Node دائمًا بلا نوم،
   والتطبيق واجهة فقط: إن ضُبط window.__ABKING_REMOTE قبل initEngine
   لا يُشغَّل محرك محلي، تُزامَن S من السيرفر كل ثانيتين، وكل أمر
   (تشغيل/إيقاف/إعدادات/مفاتيح) يُرسل إليه عبر HTTP.
   ================================================================ */
// ملاحظة حاسمة: لا ثابت مُقيَّم عند التحميل — window.__ABKING_REMOTE يُضبط بعد تقييم الوحدة،
// فدالة حية تقرأه كل مرة وإلا بقي null للأبد واشتغل محرك محلي موازٍ (أوامر مزدوجة!)
const R=()=>(typeof window!=='undefined'&&window.__ABKING_REMOTE)?String(window.__ABKING_REMOTE):null;
async function remoteCmd(action:any,payload?:any){ const base=R(); if(!base) return {ok:false,error:'no server'};
  try{ const r=await fetch(base+'/cmd',{method:'POST',headers:{'Content-Type':'application/json'},
      body:JSON.stringify({action,...(payload||{})})});
    return await r.json(); }catch(e){ return {ok:false,error:String((e as any)?.message||e)}; } }
let _remoteFails=0;
let _remoteLogSeen:any=null; // أحدث ختم زمني لسجل السيرفر عولج — أول مزامنة تُؤسسه بلا إعادة تشغيل الماضي
let _relayText='', _relayAt=0; // مانع التكرار — نفس النص خلال دقيقة لا يُبلّغ مرتين
function relayServerLogs(){ // أحداث السيرفر تعيش في Termux ولا تصل الواجهة وحدها —
  // كل حدث مهم (جني/فتح/كابح/خدش) يُترجم هنا صوتًا وإشعارًا أصليًا حتى والتطبيق في الخلفية
  const logs=S.logs||[];
  if(_remoteLogSeen==null){ _remoteLogSeen=Math.max(0,...logs.map(l=>l.at||0)); return; }
  const fresh=logs.filter(l=>(l.at||0)>_remoteLogSeen);
  if(!fresh.length) return;
  _remoteLogSeen=Math.max(_remoteLogSeen,...fresh.map(l=>l.at||0));
  const imp=fresh.filter(l=>l.kind==='fill'||l.kind==='error'||
    /جني|كابح|تصفية|خدش/.test(l.text||'')).slice(0,4).reverse(); // الأحدث 4، بترتيب قديم→جديد
  let sent=0;
  for(const l of imp){
    if(sent>=2) break; // حد أقصى إشعاران في النبضة — لا رشاش إشعارات
    if(l.text===_relayText&&Date.now()-_relayAt<60000) continue; // نفس الخبر المكرر = صمت
    _relayText=l.text; _relayAt=Date.now(); sent++;
    const win=l.kind==='fill'&&/جني|ربح/.test(l.text||'');
    if(S.sound!==false){
      if(win){ beep(S.soundTone); setTimeout(()=>{ try{beep('bell');}catch(e){ logSwallow("silent-catch", e); } },200); }
      else if(l.kind==='error') beep('alarm'); else beep(S.soundTone);
      try{navigator.vibrate&&navigator.vibrate(win?[80,60,140]:120);}catch(e){ logSwallow("silent-catch", e); } }
    nativeNotify('AB-KING',l.text); } }
export async function remoteSync(){ const base=R(); if(!base) return false;
  try{ const __sel=(typeof window!=='undefined'&&(window as any).__FLEET_SEL)||0;
    const r=await fetch(base+'/state'+(__sel?('?sel='+__sel):'')); const j=await r.json();
    if(!j||!j.ok) throw new Error('bad state');
    Object.assign(S,j.state); _remoteFails=0;
    relayServerLogs();
    // مؤشر القناة في الواجهة يتبع قناة السيرفر: حيّة ما دامت المزامنة حيّة
    streamState.connected=true; streamState.connecting=false; streamState.lastMsgAt=Date.now();
    emit(); return true;
  }catch(e){ _remoteFails++; streamState.connected=false;
    if(_remoteFails===5) toast('⚠️ سيرفر Termux لا يرد — شغّله من جديد: node king-server.mjs');
    emit(); return false; } }
// جسّ نبض السيرفر قبل إقلاع المحرك — 900 مللي ثانية كحد أقصى فلا يتأخر فتح التطبيق
export async function detectRemoteServer(){ if(typeof window==='undefined') return false;
  try{ const c=new AbortController(); const t=setTimeout(()=>c.abort(),900);
    const r=await fetch('http://127.0.0.1:8987/state',{signal:c.signal}); clearTimeout(t);
    const j=await r.json();
    if(j&&j.ok){ window.__ABKING_REMOTE='http://127.0.0.1:8987'; return true; }
  }catch(e){ logSwallow("silent-catch", e); }
  return false; }

/* ---------- الحالة ---------- */
export const S: any = {
  keys:null,
  config:{symbol:'XBTUSDTM',displaySymbol:'BTC',leverage:3,gridStepPct:0.3,huntPct:0.15,
    levels:12,direction:'short',directionMode:'auto',cycleBalance:100,mode:'paper',tpNet:0.12,tpUsd:1,patienceHrs:24,patienceUsd:0,gapMode:false},
  status:'idle', heartbeat:0, lastPrice:null, markPrice:null,
  // قاعدة المالك: تجميد الدخول عند وجود مراكز منصة غير مغلقة + حد أوامر الدورة
  _exOpenCount:0, _noNewEntries:false, _cycleMaxFills:4, _nextCycleMax:0,
  exPositions:[] as any[], _flatStrikes:0, _flatConfirmed:false,
  multiplier:1, tickSize:0.1, makerFee:0.0002, takerFee:0.0006, funding:0,
  _metaSym:null, maxOrderQty:0, exAvail:null, exLeverage:null, // _metaSym: الرمز الذي وُثّقت مواصفات عقده — لا أمر حقيقي بدونها
  grid:[], position:null, journal:[], logs:[],
  history:[], linkOk:false, intel:null, exEquity:null,
  realizedPnl:0, feesPaid:0, fundingPaid:0, cycleHarvested:0, slipCost:0,
  priceTrail:[], emaFast:null, emaSlow:null, cvd:0, tape:[],
  biasScore:0, biasReasons:[], regime:null, confluence:null,
  huntAnchor:null, gridAnchor:null, lastHuntAt:0, lastAddAt:0, lastWorkAt:0, huntCount:0,
  liqPrice:null, exLiqPrice:null, mmr:0.004,
  orderBook:{bids:[],asks:[]}, startedAt:null, activeCycle:null,
  studyUntil:0, studyAcc:null, // دراسة العملة الجديدة قبل أي دخول
  ignoreExchangeUntil:0, lastTickAt:null, sound:true, soundTone:'soft', permDenied:false,
  toastMsg:null, _metaAt:0, _lastTrailAt:0,
  memory:{pairs:{},cycles:0,totalPnl:0}, // الذاكرة القوية — تبقى عبر الدورات ولا تُمسح أبدًا
};

/* ---------- مراقب الشبكة (سيرفر الجوال) ----------
   أندرويد قد يخنق إنترنت Termux في الخلفية دون قتل العملية — الطلبات تفشل بصمت
   والبوت كان يظن السوق هادئًا. الآن: أول فشل متتالٍ يعلّم «انقطاع» فورًا،
   إعادة الدخول (hunt) تتوقف حتى العودة، وعند العودة: مزامنة كاملة (مراكز + أوامر) تستعيد السيطرة. */
let _netFailStreak=0, _netDownAt=0;
export function netIsDown(){ return _netDownAt>0; }
export function netDownSince(){ return _netDownAt>0?Math.round((Date.now()-_wasDownStart)/1000):0; }
export function netAlivePing(ok){
  if(ok){
    const wasDown=_netDownAt>0; _netFailStreak=0; _netDownAt=0;
    if(wasDown){
      const secs=Math.round((Date.now()-_wasDownStart)/1000);
      pushLog('recover','عودة الاتصال بالمنصة بعد انقطاع '+secs+' ث — بدأت مراجعة الوضع وإصلاح الأوامر');
      liveSync().then(()=>pushLog('recover','🔧 اكتملت المراجعة بعد الانقطاع — المراكز والأوامر والجني متطابقة مع المنصة'))
        .catch(()=>pushLog('error','تعذرت مزامنة المنصة بعد العودة — سيتكرر المحاولة كل 5 ث'));
      scanRadar().catch(()=>{});
    }
  } else {
    // لا انقطاع إلا إذا كانت القناة اللحظية صامتة أيضًا — فشل REST العابر وحده ليس دليلًا
    const wsSilent=Date.now()-(streamState.lastMsgAt||0)>20000;
    _netFailStreak++;
    if(!wsSilent) _netFailStreak=1; // القناة حية = الإنترنت حي
    if(_netFailStreak>=4&&wsSilent&&_netDownAt===0){ _netDownAt=Date.now(); _wasDownStart=_netDownAt;
      pushLog('error','انقطاع الإنترنت فعليًا (قناة صامتة + طلبات فاشلة) — بانتظار العودة'); }
  }
}
let _wasDownStart=0;

/* ---------- التخزين ---------- */
function saveAll(){ try{
  localStorage.setItem('abking:cfg',JSON.stringify(S.config));
  localStorage.setItem('abking:keys',S.keys?JSON.stringify(S.keys):'');
  localStorage.setItem('abking:snd',S.sound?'1':'0');
  localStorage.setItem('abking:tone',S.soundTone||'soft');
  const rt={grid:S.grid,position:S.position,journal:S.journal,realizedPnl:S.realizedPnl,
    feesPaid:S.feesPaid,fundingPaid:S.fundingPaid||0,cycleHarvested:S.cycleHarvested||0,priceTrail:S.priceTrail,cvd:S.cvd,huntCount:S.huntCount,
    lastPrice:S.lastPrice,markPrice:S.markPrice,activeCycle:S.activeCycle,status:S.status,
    history:S.history,logs:S.logs,savedAt:Date.now(),
    // أعلام دورة الجني (قاعدة المالك: إعادة الاختيار بعد الجني) — تُحفظ حتى لا
    // تضيع إعادة الاختيار عند إعادة تشغيل السيرفر/الجوال بعد الجني مباشرة
    _harvestAt:S._harvestAt||0,_reselectDone:!!S._reselectDone,
    lastHuntAt:S.lastHuntAt||0,_dwellAt:S._dwellAt||0,startedAt:S.startedAt||0};
  localStorage.setItem('abking:rt',JSON.stringify(rt));
  localStorage.setItem('abking:mem',JSON.stringify(S.memory||{pairs:{},cycles:0,totalPnl:0}));
}catch(e){ logSwallow("silent-catch", e); } }
function loadAll(){ try{
  const cfg=JSON.parse(localStorage.getItem('abking:cfg')||'null'); if(cfg) S.config={...S.config,...cfg};
  // أسطول TRQ: أول إقلاع بلا إعداد — الرصيد الافتراضي نصيب هذا المحرك وحده
  if(!cfg&&FLEET_MODE()) S.config.cycleBalance=Math.max(1,Math.round(S.config.cycleBalance/5*100)/100);
  const ks=localStorage.getItem('abking:keys'); if(ks){ try{S.keys=JSON.parse(ks);}catch(e){S.keys=null;} }
  S.sound=localStorage.getItem('abking:snd')!=='0';
  S.soundTone=localStorage.getItem('abking:tone')||'soft';
  const rt=JSON.parse(localStorage.getItem('abking:rt')||'null');
  if(rt){ Object.assign(S,{grid:rt.grid||[],position:rt.position||null,journal:rt.journal||[],
    realizedPnl:rt.realizedPnl||0,feesPaid:rt.feesPaid||0,fundingPaid:rt.fundingPaid||0,cycleHarvested:rt.cycleHarvested||0,priceTrail:rt.priceTrail||[],
    cvd:rt.cvd||0,huntCount:rt.huntCount||0,lastPrice:rt.lastPrice??null,
    markPrice:rt.markPrice??null,activeCycle:rt.activeCycle||null,status:rt.status||'idle',
    history:rt.history||[],logs:rt.logs||[],
    _harvestAt:rt._harvestAt||0,_reselectDone:!!rt._reselectDone,
    lastHuntAt:rt.lastHuntAt||0,_dwellAt:rt._dwellAt||0,startedAt:rt.startedAt||0});
    S._savedAt=rt.savedAt||0;
    // تنظيف الإقلاع: صفوف بقيم شاذة من نافذات انتقال قديمة (قبل وجود درع
    // الإقفال) — تُصفَّر قيمها الوهمية حتى لا تبقى في السجل والتاريخ
    { const __bound=Math.max(50,(S.config.cycleBalance||0)*3); let __n=0;
      (S.journal||[]).forEach(j=>{ if(j.status==='closed'&&typeof j.pnl==='number'&&Math.abs(j.pnl)>__bound){ j.pnl=0; __n++; } });
      if(__n>0) pushLog('server','🧼 تصحيح '+__n+' صفقة بقيم شاذة من إصدار سابق — صُفّرت قيمها الوهمية'); }
    // ─── حماية الإقلاع (أمر المالك): أي مركز قائم عند التشغيل = دخلت أوامره —
    // القاطع مقفل عليه فوراً (لا إضافات لصفقات معلقة قديمة مثل 11× و14×) ───
    if(S.position){ const __oj=(S.journal||[]).find(j=>j.status==='open');
      S._batchFills=Math.max(4,(__oj&&__oj.mergedOrders)||4);
      if(!S._batchHoldAt){ S._batchHoldAt=Date.now(); S._batchStudiedAt=true; }
      pushLog('server','🧷 حماية الإقلاع: مركز قائم ('+S._batchFills+' أمر) — القاطع مقفل حتى الجني الكامل'); } }
  // قتل أندرويد للتطبيق في الخلفية ≠ خروج المالك: جلسة كانت تعمل وقُتلت حديثًا
  // (أقل من 30 دقيقة) تُستأنف تلقائيًا من حيث توقفت — وإلا بقيت القاعدة:
  // لا تشغيل بعد فتح التطبيق إلا بضغطة «تشغيل» من المالك (تثبيت جديد / جلسة قديمة)
  if(S.status==='running'){
    if(S._savedAt&&Date.now()-S._savedAt<30*60*1000){
      pushLog('server','⚡ أوقف النظام التطبيق لحظيًا — استُؤنف الصيد تلقائيًا من حيث توقف');
      nativeNotify('AB-KING عاد للعمل ✓','أوقف النظام التطبيق في الخلفية — استُؤنف الصيد تلقائيًا',true);
    } else { S.status='paused';
      pushLog('info','استُعيدت الجلسة متوقفة مؤقتًا — اضغط «تشغيل» للبدء'); } }
  // بصمات العملات (دراسة 60 ثانية) تُحفظ — أما سجل التعلم فأُلغي نهائيًا بطلب المالك
  const mem=JSON.parse(localStorage.getItem('abking:mem')||'null');
  S.memory={v:3,pairs:{},cycles:0,totalPnl:0,profiles:(mem&&mem.profiles)||{}};
}catch(e){ logSwallow("silent-catch", e); } }

/* ---------- السجل ---------- */
export function pushLog(kind:any,text:any){ S.logs=[{id:uid('l'),at:Date.now(),kind,text},...S.logs].slice(0,80); }
const _swallowAt={};
function logSwallow(where:any,err:any){
  try{
    const msg=String((err&&err.message)||err||'unknown');
    // 404 «Not Found» متوقع (إلغاء أمر منتهٍ/مركز غير موجود) — ليس حدثًا يُسجل
    if(/not found|404/i.test(msg)) return;
    // أخطاء الشبكة العابرة (تقطيع تغطية/مهلة زمنية مقصودة) — متوقعة على الجوال ولا تسجل أبدًا:
    // البوت مصمم ليعمل عبر قناة WebSocket، وREST للقراءة الثانوية فقط — فشله العابر ليس حدثًا
    if(/failed to fetch|networkerror|network error|load failed/i.test(msg)) return;
    if(/aborted?|abort/i.test(msg)) return; // مهلة زمنية مصممة عمدًا (AbortController)
    if(/timeout|timed? ?out/i.test(msg)) return;
    const key=where+'|'+msg;
    const now=Date.now();
    if(now-(_swallowAt[key]||0)<180000) return;
    _swallowAt[key]=now;
    pushLog('warn', where + ': ' + msg);
  }catch(e2){ /* rate-limit logger must never throw */ }
}
function pushJr(row:any){ row.symbol=row.symbol||S.config.displaySymbol; // عملة الصفقة تُسجل معها — السجل لا يفقد سياقه عند الانتقال
  S.journal=[row,...S.journal].slice(0,120); }

/* ================================================================
   عميل KuCoin — REST عبر البروكسي المحلي /kucoin
   مع تحويل تلقائي للاتصال المباشر داخل WebView/APK (بلا CORS)
   ================================================================ */
const DIRECT_BASE = 'https://api-futures.kucoin.com';
// سيرفر Termux (Node) وتطبيق الجوال (Capacitor): لا بروكسي إطلاقًا — اتصال مباشر من أول طلب
const IS_SERVER = typeof process!=='undefined' && !!(process && process.env && process.env.ABKING_SERVER);
const IS_NATIVE = IS_SERVER || (typeof window !== 'undefined' && (window.Capacitor || location.protocol === 'capacitor:'));
let API_BASE = IS_NATIVE ? DIRECT_BASE : '/kucoin';
// AbortSignal.timeout غير مدعوم في WebViews القديمة — بديل متوافق
function sig(ms){ const c=new AbortController(); setTimeout(()=>c.abort(),ms); return c.signal; }
async function kcFetch(path,opts){
  
  try{ const _r=await kcFetchInner(path,opts); netAlivePing(true); return _r; }
  catch(e){ if((e as any)?.name!=='AbortError') netAlivePing(false); throw e; } }
async function kcFetchInner(path,opts){
  if(API_BASE===DIRECT_BASE){
    if(IS_NATIVE) return await fetch(API_BASE+path,opts);
    // تحوّل سابق بسبب عثرة بروكسي — إن فشل المباشر أيضًا (CORS) نرجع للبروكسي ولا نقفل عليه
    try{ return await fetch(API_BASE+path,opts); }
    catch(e){ if((e as any)?.name==='AbortError') throw e;
      API_BASE='/kucoin'; return await fetch(API_BASE+path,freshOpts(opts)); } }
  let res;
  try{ res=await fetch(API_BASE+path,opts); }
  catch(e){
    // مهلة/إجهاض = مشكلة الطلب نفسه وليست غياب البروكسي — لا تحويل نهائي للمباشر بسببها
    if((e as any)?.name==='AbortError') throw e;
    API_BASE=DIRECT_BASE; return await fetch(API_BASE+path,freshOpts(opts)); }
  // 404 أو استجابة HTML (خادم الجوال المحلي يرجع index.html برمز 200) = لا بروكسي — تحوّل مباشر نهائي
  if(res.status===404 || !(res.headers.get('content-type')||'').toLowerCase().includes('json')){
    API_BASE=DIRECT_BASE; return await fetch(API_BASE+path,freshOpts(opts)); }
  return res;
}
// إشارة المهلة الأولى قد تكون استُنفدت — المحاولة البديلة تحتاج إشارة جديدة وإلا فشلت فورًا
function freshOpts(opts){ if(!opts||!opts.signal) return opts;
  const o={...opts}; o.signal=sig(8000); return o; }
function normFee(raw:any,fb:any){ const n=Number(raw);
  if(!Number.isFinite(n)||n<=0) return fb;
  // نطاق الرسوم السليم كسرًا: 0.00001 .. 0.003 (0.001% .. 0.3%). أي قيمة أكبر = صيغة مئوية
  // (مثل 0.03 = 3%) فتُقسم على 100. سقف نهائي 0.003 يمنع أي رسوم مجنونة من دخول الحسابات.
  const frac=n<0.05?n:n/100;
  return Math.min(0.003,Math.max(0.00001,frac)); }

async function hmacB64(secret,payload){
  const key=await crypto.subtle.importKey('raw',
    new TextEncoder().encode(secret),{name:'HMAC',hash:'SHA-256'},false,['sign']);
  const sig=await crypto.subtle.sign('HMAC',key,new TextEncoder().encode(payload));
  return btoa(String.fromCharCode(...new Uint8Array(sig)));
}
async function kcPublic(path,timeout){
  const res=await kcFetch(path,{cache:'no-store',
    signal:sig(timeout||3500)});
  if(!res.ok) throw new Error('KuCoin '+res.status);
  const j=await res.json();
  if(j.code!=='200000') throw new Error(j.msg||j.code);
  return j.data;
}
async function kcPrivate(method:any,path:any,body?:any){
  if(!S.keys) throw new Error('لا مفاتيح — اربط KuCoin من الإعدادات');
  const ts=String(Date.now());
  const raw=body?JSON.stringify(body):'';
  const signStr=ts+method+path+raw;
  const res=await kcFetch(path,{method,cache:'no-store',
    signal:sig(6000),
    headers:{'Content-Type':'application/json',
      'KC-API-KEY':S.keys.apiKey,
      'KC-API-SIGN':await hmacB64(S.keys.apiSecret,signStr),
      'KC-API-TIMESTAMP':ts,
      'KC-API-PASSPHRASE':await hmacB64(S.keys.apiSecret,S.keys.passphrase),
      'KC-API-KEY-VERSION':'2'},
    body:raw||undefined});
  const j=await res.json().catch(()=>({code:'-1',msg:'bad json'}));
  if(j.code!=='200000') throw new Error(j.msg||('KuCoin '+res.status));
  return j.data;
}

const fetchTicker  = s => kcPublic('/api/v1/ticker?symbol='+encodeURIComponent(s),2500);
const fetchContract= s => kcPublic('/api/v1/contracts/'+encodeURIComponent(s),3500);
let _bookFailsAt:any=0, _tapeFailsAt:any=0;
const fetchBook    = s => kcPublic('/api/v1/level2/depth20?symbol='+encodeURIComponent(s),2500)
  .then(d=>{ _bookFailsAt=0; return {bids:(d.bids||[]).map(r=>({price:+r[0],size:+r[1]})).filter(r=>r.price>0),
             asks:(d.asks||[]).map(r=>({price:+r[0],size:+r[1]})).filter(r=>r.price>0)}; })
  .catch(e=>{ _bookFailsAt=Date.now(); throw e; });
const fetchTape    = s => kcPublic('/api/v1/trade/history?symbol='+encodeURIComponent(s),2500)
  .then(a=>{ _tapeFailsAt=0; return (a||[]).slice(0,32).map(t=>({side:String(t.side||''),size:+t.size||0,price:+t.price||0})); })
  .catch(e=>{ _tapeFailsAt=Date.now(); throw e; });

/* ---------- شموع الأسعار + قائمة كل عقود المنصة ---------- */
export function fetchKlines(symbol,gran){
  return kcPublic('/api/v1/kline/query?symbol='+encodeURIComponent(symbol)+'&granularity='+gran,6000)
    .then(a=>(a||[]).map(r=>{
      let t=+r[0]; if(t<1e12) t*=1000; // توحيد الطابع الزمني إلى مللي ثانية
      return {t,o:+r[1],h:+r[2],l:+r[3],c:+r[4],v:+r[5]||0};
    }).filter(k=>k.t>0&&k.o>0).sort((x,y)=>x.t-y.t));
}
let contractsCache:any=null;
export function listContracts(){
  if(contractsCache&&Date.now()-contractsCache.at<300000) return Promise.resolve(contractsCache.items);
  return kcPublic('/api/v1/contracts/active',6000).then(d=>{
    const items=(d||[]).map(c=>({
      symbol:c.symbol,
      base:String(c.baseCurrency||c.symbol.replace(/USDTM?$/i,'')),
      name:String(c.baseCurrency||'')}));
    contractsCache={at:Date.now(),items};
    return items;
  });
}

/* ================================================================
   رادار الفرص — مسح كل عقود المنصة وترتيبها بدرجة ملاءمة للشبكة
   مرحلتان: (1) كل العقود بحجم 24 ساعة من طلب واحد
   (2) أعلى 18 سيولةً: شموع 15د + أفضل عرض/طلب لقياس التقلب والسبريد والاتجاه
   ================================================================ */
function radarScore(x:any){ // x: {vol,spread,chg,trendPct,turnover,price}
  let sc=50; const why:any[]=[];
  // السعر المنخفض (العملات الصفرية) أولوية قصوى: برأس مال صغير تعطي عقودًا كثيرة
  // بخطوات شبكة دقيقة وأرباحًا متكررة، والتصفية أبعد — الغالية تعطي عقدًا هشًا وتصفية قريبة
  if(x.price>0){
    if(x.price<0.001){ sc+=16; why.push('صفرية مثالية'); }
    else if(x.price<0.01){ sc+=12; why.push('صفرية'); }
    else if(x.price<0.1){ sc+=8; }
    else if(x.price<1){ sc+=3; }
    else if(x.price>100){ sc-=18; why.push('غالية — لا تناسب رأس المال'); }
    else if(x.price>10){ sc-=8; why.push('سعر مرتفع'); } }
  // التقلب المتحقق لكل شمعة 15د: الشبكة تريد ذبذبة كافية لا ميتة ولا وحشية
  if(x.vol>=0.18&&x.vol<=1.1){ sc+=18; why.push('تذبذب مثالي'); }
  else if(x.vol>1.1&&x.vol<=2){ sc+=6; why.push('تذبذب حاد'); }
  else if(x.vol<0.18){ sc-=14; why.push('سوق ميت'); }
  else { sc-=10; why.push('تذبذب وحشي'); }
  // السبريد: واسع يأكل ربح المستوى
  if(x.spread>0){ if(x.spread<=0.03) sc+=12; else if(x.spread<=0.08) sc+=6;
    else if(x.spread>0.2){ sc-=16; why.push('سبريد واسع'); } }
  // السيولة: دوران 24 ساعة بالدولار
  if(x.turnover>=5e7) sc+=10; else if(x.turnover>=1e7) sc+=6;
  else if(x.turnover<1e6){ sc-=14; why.push('سيولة ضعيفة'); }
  // نوع السوق: العرضي المتذبذب بيئة الشبكة المثالية؛ الاتجاه القوي يفيد الصيد لا الشبكة
  const a=Math.abs(x.trendPct);
  if(a<1.2){ sc+=8; x.state='عرضي متذبذب'; }
  else if(a<3.5){ sc+=4; x.state=x.trendPct>0?'صاعد':'هابط'; }
  else { sc-=4; x.state=x.trendPct>0?'صاعد بقوة':'هابط بقوة'; why.push('اتجاه حاد'); }
  // حركة 24 ساعة المتطرفة = إرهاق أو مخاطرة
  if(Math.abs(x.chg)>12){ sc-=6; why.push('حركة يوم متطرفة'); }
  return {score:clamp(Math.round(sc),0,100),why}; }

// ===================================================================
// background pipeline: the NEXT trade is prepared while the current one works.
// every 30s the full universe is ranked and the next candidate is kept warm;
// the moment the position closes (harvest), the bot opens it instantly.
// ===================================================================
S._nextSymbol=null; S._nextReady=false; let _bgPipeAt=0;
export async function backgroundPipeline(){
  if(S.status!=='running') return;
  if(Date.now()-_bgPipeAt<30000) return;
  _bgPipeAt=Date.now();
  try{
    const r=S.radar&&S.radar.list&&S.radar.list.length?S.radar:(await scanRadar());
    if(!r||!r.list||!r.list.length) return;
    const cands=r.list.filter(l=>l.symbol!==S.config.symbol&&(l.score||0)>=55);
    if(!cands.length) return;
    const nxt=cands[0];
    S._nextReady=true;
    S._nextStudiedAt=Date.now();
    if(S._nextSymbol!==nxt.symbol){
      S._nextSymbol=nxt.symbol;
      if(Date.now()-(S._bgLogAt||0)>600000){ S._bgLogAt=Date.now();
        pushLog('server','\u{1F9E0} \u0627\u0644\u062E\u0637 \u0627\u0644\u062E\u0644\u0641\u064A \u0627\u0644\u0645\u0633\u062A\u0645\u0631: \u0627\u0644\u0645\u0631\u0634\u062D \u0627\u0644\u062A\u0627\u0644\u064A '+nxt.disp+' (\u062F\u0631\u062C\u0629 '+nxt.score+') \u0645\u0631\u0634\u0651\u062D \u0628\u0639\u062F \u0627\u0644\u062C\u0646\u064A \u2014 \u064A\u064F\u062E\u062A\u0627\u0631 \u0625\u0646 \u0643\u0627\u0646\u062A \u0645\u0646\u0637\u0642\u062A\u0647 \u0623\u0642\u0648\u0649'); } }
  }catch(e){ logSwallow('silent-catch', e); }
}
export function openNextReady(){
  if(!S._nextSymbol||!S._nextReady) return false;
  if(S.position) return false;
  // ─── إصلاح الدوران المتسارع (المسار الثاني): القفز الفوري للمرشح المدروس
  // مسموح فقط خلال 10 دقائق بعد جني مكتمل — لا كل 30 ثانية بلا مركز كما كان ───
  if(!S._harvestAt||Date.now()-S._harvestAt>10*60*1000){ S._nextReady=false; S._nextSymbol=null; return false; }
  // المرشح يجب أن يبقى مدروسًا حديثًا (خلال 15 دقيقة) — لا فتح ببيانات باردة
  if(Date.now()-(S._nextStudiedAt||0)>15*60*1000){ S._nextReady=false; S._nextSymbol=null; return false; }
  if(S._nextSymbol===S.config.symbol){ S._nextReady=false; return false; }
  const sym=S._nextSymbol;
  S._nextReady=false; S._nextSymbol=null; S._reselectDone=true; // قفز ما بعد الجني — إعادة الاختيار تمت
  pushLog('server','\u26A1 \u0641\u062A\u062D \u0641\u0648\u0631\u064A \u0628\u0639\u062F \u0627\u0644\u062C\u0646\u064A: \u0627\u0644\u0639\u0645\u0644\u0629 \u0627\u0644\u0645\u062F\u0631\u0648\u0633\u0629 \u062E\u0644\u0641\u064A\u064B\u0627 '+sym.replace(/USDTM$/i,'')+' \u062A\u062F\u062E\u0644 \u0627\u0644\u0622\u0646');
  saveCfg({symbol:sym,leverage:S.config.leverage,levels:S.config.levels,
    gridStepPct:S.config.gridStepPct,huntPct:S.config.huntPct,tpNet:S.config.tpNet,tpUsd:S.config.tpUsd,
    cycleBalance:S.config.cycleBalance,directionMode:S.config.directionMode,
    mode:S.config.mode,sound:S.sound,soundTone:S.soundTone});
  return true;
}
/* ─── أسطول TRQ-trading: كل محرك يتجنب عملات محركات الأسطول الأخرى ───
   التنويع مضمون مركزياً: قائمة المحركات الأخرى تُجلب من المنسق كل 30 ث،
   والرادار يحذفها من القائمة (عملة المحرك نفسه تبقى — يديرها هو) */
export const FLEET_MODE=()=>!!(typeof process!=='undefined'&&process.env&&process.env.ABKING_SLOT);

export async function scanRadar(){
  if(S._radarBusy) return S.radar; S._radarBusy=true;
  try{
    const all=await kcPublic('/api/v1/contracts/active',6000);
    const rows0=(all||[]).filter(c=>c&&c.symbol&&/USDTM$/i.test(c.symbol)&&c.status==='Open')
      .map(c=>({symbol:c.symbol,
        turnover:+(c.turnoverOf24h??c.turnover24h??c.volumeOf24h??c.volume24h??0)||0,
        price:+(c.lastTradePrice??c.markPrice??0)||0,
        chg:+(c.priceChgPct??0)*100||0,
        funding:+(c.fundingFeeRate??0)||0,
        fundHrs:+(c.fundingRateGranularity??28800000)/3600000||8,
        nextFundMs:+(c.nextFundingRateTime??0)||0,
        oi:+(c.openInterest??0)||0,
        maxLev:+(c.maxLeverage??0)||0,
        markP:+(c.markPrice??0)||0,
        idxP:+(c.indexPrice??0)||0,
        lowP:+(c.lowPrice??0)||0,
        highP:+(c.highPrice??0)||0}))
      .filter(r=>r.turnover>0&&r.price>0);
    // قاعدة المرشحين: الصفرية أولًا — رأس مال صغير يحتاج عملات رخيصة بسيولة،
    // مع إبقاء بضع عملات كبيرة للمقارنة فقط
    const rows=rows0; // full universe - every contract is ranked and watched
    // instant pre-ranking of ALL contracts from the single aggregate response
    const preScored=rows.map(r=>{
      let sc=50;
      if(r.turnover>=5e7) sc+=10; else if(r.turnover>=1e7) sc+=6; else if(r.turnover<3e5) sc-=25; else if(r.turnover<1e6) sc-=12;
      const a=Math.abs(r.chg); if(a<1.2) sc+=8; else if(a<3.5) sc+=4; else if(a>8) sc-=8; else sc-=3;
      const range=r.lowP>0?((r.highP-r.lowP)/r.lowP*100):0; if(range>=4) sc+=10; else if(range>=2) sc+=6; else if(range>0&&range<0.6) sc-=6;
      const basis=(r.markP>0&&r.idxP>0)?((r.markP-r.idxP)/r.idxP*100):0; if(Math.abs(basis)>0.2) sc+=6;
      if(r.price>0&&r.price<0.01) sc+=6; else if(r.price>100) sc-=10;
      const oiUsd=r.oi*r.price; if(r.oi>0&&r.turnover>0){ const rto=oiUsd/r.turnover; if(rto<0.5) sc+=4; else if(rto>5) sc-=4; }
      return {...r, preScore: Math.max(0, Math.min(100, Math.round(sc))) };
    }).sort((a,b)=>b.preScore-a.preScore);
    // only the top 30 candidates get the deep check (candles + ticker) - fast AND precise
    const det=await Promise.all(preScored.slice(0,30).map(async r=>{
      const out={...r,vol:0,spread:0,trendPct:0,price:0};
      try{ const ks=await fetchKlines(r.symbol,15);
        if(ks.length>=20){ const last=ks.slice(-24);
          let v=0; for(const k of last) v+=(k.h-k.l)/k.c; out.vol=v/last.length*100;
          out.price=ks[ks.length-1].c;
          out.trendPct=(out.price-ks[ks.length-20].o)/ks[ks.length-20].o*100; }
      }catch(e){ logSwallow("silent-catch", e); }
      try{ const t=await fetchTicker(r.symbol);
        const bb=+t.bestBidPrice||0, ba=+t.bestAskPrice||0;
        if(bb>0&&ba>0) out.spread=(ba-bb)/((ba+bb)/2)*100;
        if(!out.price) out.price=+t.price||0; }catch(e){ logSwallow("silent-catch", e); }
      return out; }));
    const deepList=det.filter((d:any)=>d.price>0).map((d:any)=>{ const s=radarScore(d);
      const fundAnn=(d.funding||0)*(24/(d.fundHrs||8))*100;
      const oiUsd=(d.oi||0)*(d.price||0);
      const skew=d.chg<0&&d.funding<0?'شورت متكدس — مرشح انعكاس صاعد':d.chg>0&&d.funding>0?'لونغ متكدس — حذر من انعكاس هابط':(d.funding||0)!==0?'محايد':'—';
      const basis=(d.markP>0&&d.idxP>0)?((d.markP-d.idxP)/d.idxP)*100:0;
      const range=(d.lowP>0)?((d.highP-d.lowP)/d.lowP)*100:0;
      const oiToVol=(d.turnover>0)?oiUsd/d.turnover:0;
      return {symbol:d.symbol,disp:d.symbol.replace(/USDTM$/i,'').replace(/^XBT/i,'BTC'),
        price:d.price,score:s.score,why:s.why,state:d.state||'—',
        vol:+d.vol.toFixed(2),chg:Math.round(d.chg*10)/10,turnover:d.turnover,
        funding:+(d.funding||0).toFixed(6),fundHrs:d.fundHrs||8,fundAnnPct:+fundAnn.toFixed(3),
        nextFundMs:d.nextFundMs||0,oi:+d.oi||0,oiUsd,oiLev:d.maxLev||0,skew,
        basisPct:+basis.toFixed(3),rangePct:+range.toFixed(1),oiToVolPct:+(oiToVol*100).toFixed(0),
        grade:s.score>=65?'🟢':s.score>=45?'🟡':'🔴'}; })
      // الأفضل أولًا بلا تعادل مربك: الدرجة، ثم العرضي المتذبذب يتقدم، ثم الأعلى سيولة
      .sort((a:any,b:any)=>b.score-a.score||
        ((b.state==='عرضي متذبذب')?1:0)-((a.state==='عرضي متذبذب')?1:0)||b.turnover-a.turnover);
    // the rest of the universe (no deep fetch) - watched every 45s with live pre-scores
    const deepSyms=new Set(deepList.map((x:any)=>x.symbol));
    const rest=preScored.filter((r:any)=>!deepSyms.has(r.symbol)).map((r:any)=>({
      symbol:r.symbol,disp:r.symbol.replace(/USDTM$/i,'').replace(/^XBT/i,'BTC'),
      price:r.price,score:r.preScore,why:['full-universe watch'],state:'watch',
      vol:0,chg:Math.round(r.chg*10)/10,turnover:r.turnover,
      funding:+(r.funding||0).toFixed(6),fundHrs:r.fundHrs||8,
      fundAnnPct:+((r.funding||0)*(24/(r.fundHrs||8))*100).toFixed(3),
      nextFundMs:r.nextFundMs||0,oi:+r.oi||0,oiUsd:(r.oi||0)*(r.price||0),oiLev:r.maxLev||0,
      skew:'-',basisPct:0,rangePct:0,oiToVolPct:0,
      grade:r.preScore>=65?'\ud83d\udfe2':r.preScore>=45?'\ud83d\udfe1':'\ud83d\udd34'}));
    const list=[...deepList,...rest];
    const fundRanked=[...list].sort((a:any,b:any)=>b.fundAnnPct-a.fundAnnPct);
    const oiRanked=[...list].sort((a:any,b:any)=>b.oiUsd-a.oiUsd).slice(0,5);
    const gridRanked=[...list].filter((x:any)=>x.turnover>2e6).sort((a:any,b:any)=>b.rangePct-a.rangePct).slice(0,5);
    const basisRanked=[...list].filter((x:any)=>x.turnover>2e6&&Math.abs(x.basisPct)>0.1)
      .sort((a:any,b:any)=>Math.abs(b.basisPct)-Math.abs(a.basisPct)).slice(0,5);
    S.intel={at:Date.now(),
      hottest:fundRanked.slice(0,5),
      coldest:fundRanked.slice(-3).reverse(),
      biggestOi:oiRanked,
      bestGrid:gridRanked,
      extremeBasis:basisRanked,
      squeeze:fundRanked.filter((f:any)=>f.fundAnnPct>=0.05&&f.chg>3).slice(0,3),
      rebound:fundRanked.filter((f:any)=>f.fundAnnPct<=-0.03&&f.chg<-3).slice(0,3)};
    // أسطول TRQ: حذف عملات المحركات الأخرى — كل عملة لمحرك واحد فقط
    let flist=list;
    if(FLEET_MODE()){ const taken=S._fleetTaken||[];
      if(taken.length) flist=list.filter(l=>l.symbol===S.config.symbol||!taken.includes(l.symbol)); }
    S.radar={at:Date.now(),list:flist};
    // توصية التبديل: عملتي الحالية ضعيفة وأخرى أقوى منها بفارق واضح
    const cur=flist.find(l=>l.symbol===S.config.symbol);
    const top=flist[0];
    if(top&&cur&&top.symbol!==cur.symbol&&top.score-cur.score>=18&&cur.score<50){
      if(!S._radarTipAt||Date.now()-S._radarTipAt>1800000){ S._radarTipAt=Date.now();
        pushLog('server','📡 رادار: '+cur.disp+' ضعيفة ('+cur.score+') بينما '+top.disp+
          ' '+top.state+' بدرجة '+top.score+' — يُنصح بالتبديل بعد إغلاق أي مركز');
        toast('📡 الرادار يقترح '+top.disp+' بدل '+cur.disp);
        nativeNotify('📡 فرصة أقوى',top.disp+' '+top.state+' — درجة '+top.score+' مقابل '+cur.score+' لعملتك'); } }
    else if(top&&!cur&&top.score>=70){
      if(!S._radarTipAt||Date.now()-S._radarTipAt>1800000){ S._radarTipAt=Date.now();
        pushLog('server','📡 رادار: '+top.disp+' '+top.state+' بدرجة '+top.score+' — أقوى فرصة الآن'); } }
    emit(); saveAll();
    return S.radar;
  }catch(e){ return S.radar; }
  finally{ S._radarBusy=false; }
}

/* ================================================================ */
/* ─── إعادة الاختيار الإجباري بعد كل جني (قاعدة المالك) ───
   بعد كل صفقة مكتملة: البحث عن أفضل عملة أخرى بمنطقة أقوى؛ إن وُجدت
   انتقل إليها مباشرة (تُدرس ثم تدخل)، وإن لم توجد فالعودة للعملة الحالية
   إن كانت جيدة. القرار داخل نافذة 15 دقيقة ثم يستمر التداول عاديًا. */
export async function postHarvestReselect(){
  if(S.status!=='running'||S.position) return;
  if(!S._harvestAt||S._reselectDone) return;
  if(Date.now()-S._harvestAt>15*60*1000){ S._reselectDone=true; return; } // فاتت النافذة — استمر عاديًا
  // تعذّر إتمام المسح أكثر من 5 دقائق (شبكة متقطعة)؟ استئناف فوري بدل تجميد الدخول
  if(Date.now()-S._harvestAt>5*60*1000&&!S._reselectBusy){
    S._reselectDone=true;
    pushLog('server','⏱ بعد الجني: تعذر اكتمال مسح الرادار — استئناف الصيد على العملة الحالية');
    return; }
  try{
    const r=await scanRadar(); if(!r||!r.list||!r.list.length) return;
    const cur=r.list.find(l=>l.symbol===S.config.symbol);
    const bestOther=r.list.filter(l=>l.symbol!==S.config.symbol).find(l=>(l.score||0)>=55);
    S._reselectDone=true; S._nextReady=false; S._nextSymbol=null; // القرار اتخذ — لا قفز مزدوج
    if(!bestOther){
      pushLog('server','🔎 بعد الجني: لا عملة أقوى من '+
        (cur?cur.disp:S.config.displaySymbol)+' في الرادار — استئناف الصيد على العملة الحالية');
      return; }
    const curSc=cur?(cur.score||0):0;
    // قاعدة المالك النهائية: بعد الجني يوجد بديل مؤهل (55+)؟ انتقال مباشر إليه.
    // البقاء على العملة الحالية فقط إذا لم يوجد أي بديل مؤهل في الرادار.
    // عملة أخرى أفضل أو منافسة → انتقال مباشر ثم دراسة ثم دخول
    _lastSwitchAt=Date.now();
    const curDisp=cur?cur.disp:S.config.displaySymbol;
    pushLog('server','🔄 بعد الجني: انتقال مباشر من '+curDisp+' ('+curSc+') إلى '+
      bestOther.disp+' ('+bestOther.score+') — منطقة أقوى — تُدرس ثم تُفتح الصفقة');
    toast('🔄 بعد الجني: '+bestOther.disp+' — '+bestOther.score);
    nativeNotify('AB-KING — انتقال بعد الجني',bestOther.disp+' درجة '+bestOther.score);
    saveCfg({symbol:bestOther.symbol,leverage:S.config.leverage,levels:S.config.levels,
      gridStepPct:S.config.gridStepPct,huntPct:S.config.huntPct,tpNet:S.config.tpNet,tpUsd:S.config.tpUsd,
      cycleBalance:S.config.cycleBalance,directionMode:S.config.directionMode,
      mode:S.config.mode,sound:S.sound,soundTone:S.soundTone});
  }catch(e){ logSwallow('silent-catch',e); }
}

/* BG RADAR: permanent study every 5 min while bot runs (never miss a chance) */
let _bgScanAt=0;
export function bgRadarTick(){ if(!R()){
    if(Date.now()-_bgScanAt<5*60*1000) return;
    _bgScanAt=Date.now();
    scanRadar().catch(()=>{}); } }

/* SMART SWITCH: called only after a completed harvest (position closed).
   Switch to a stronger symbol when clearly better: gap >=15 and current <55. */
let _lastSwitchAt=0;
/* ================================================================
   صائد الفجوات (GAP HUNTER) — اختراع مبني على بيانات المنصة العميقة
   المبدأ: سعر KuCoin المحلي ينحرف عن الإنديكس (.K* من 5 منصات)
   إحصائيًا (z<-2σ) + استقرار السعر → ضجيج محلي يعود للإنديكس — ندخل مع العودة
   القياس المختبري: 63% نجاح، +66%/8 أيام على 10 عملات
   ================================================================ */
const GAP_MODE_ON=()=>!!S.config.gapMode;
let _gapScanAt=0, _gapStats:any=null;

/* فحص فجوات كل العملات السائلة: يسحب السعر+الإنديكس لكل عقد من /contracts/active وحديث
   قائمة الفجوات الحالية مع z-score متدرج (24 شمعة تاريخية 5m) */
export async function gapScan(){
  if(!GAP_MODE_ON()) return null;
  if(Date.now()-_gapScanAt<60*1000) return _gapStats; // مسح كل دقيقة
  _gapScanAt=Date.now();
  try{
    const all=await kcPublic('/api/v1/contracts/active',6000);
    const liq=(all||[]).filter((c:any)=>/USDTM$/.test(c.symbol)&&c.status==='Open'
      &&+(c.turnoverOf24h||0)>3e6&&+(c.lastTradePrice||0)>0&&+(c.indexPrice||0)>0);
    const out:any[]=[];
    for(const c of liq.slice(0,60)){ // سقف الفحص للسيولة الكافية
      const dev=((+c.lastTradePrice-+c.indexPrice)/+c.indexPrice)*100;
      if(dev<-0.15){ // فجوة سالبة ملحظة فقط
        out.push({symbol:c.symbol,dev,oi:+c.openInterest||0,turn:+c.turnoverOf24h,
          fund:+c.fundingFeeRate||0,last:+c.lastTradePrice,idx:+c.indexPrice});
      }
    }
    out.sort((a,b)=>a.dev-b.dev);
    _gapStats={at:Date.now(),list:out};
    return _gapStats;
  }catch(e){ logSwallow('silent-catch',e); return _gapStats; }
}

/* الإشارة المدروسة: فجوة عميقة (z<-2σ) + استقرار السعر محليًا
   يسحب 24 شمعة 5m منطقية للإنديكس والعقد → z والاستقرار */
export async function gapSignal(symbol:string){
  try{
    const [ixr,pkr]=await Promise.all([
      kcPublic('/api/v1/kline/query?symbol=.K'+symbol.replace('USDTM','USDT')+'&granularity=5',6000),
      kcPublic('/api/v1/kline/query?symbol='+symbol+'&granularity=5',6000),
    ]);
    const idx:any={},perp:any={};
    (ixr||[]).forEach((k:any)=>idx[k[0]]=+k[4]);
    (pkr||[]).forEach((k:any)=>perp[k[0]]=+k[4]);
    const devs:any[]=[];
    for(const t in perp){ if(idx[t]>0&&perp[t]>0) devs.push({t:+t,d:(perp[t]-idx[t])/idx[t]*100,p:perp[t],idx:idx[t]}); }
    if(devs.length<27) return null;
    devs.sort((a:any,b:any)=>a.t-b.t);
    const win=devs.slice(-25,-1);
    const mean=win.reduce((a:number,b:any)=>a+b.d,0)/win.length;
    const sd=Math.sqrt(win.reduce((a:number,b:any)=>a+(b.d-mean)**2,0)/win.length);
    if(!(sd>0.001)) return null;
    const cur=devs[devs.length-1];
    const z=(cur.d-mean)/sd;
    const stable=devs.length>3&&cur.p>=devs[devs.length-4].p*0.9985; // استقرار تقريبي (15 دقيقة)
    const deep=cur.d<-0.15&&z<-2;
    const idxNow=cur.idx||cur.p;
    const idx6h=devs.length>6?(devs[devs.length-7].idx||idxNow):idxNow;
    const idxFlat=Math.abs((idxNow-idx6h)/idx6h*100)<1.5;
    const ready=deep&&stable&&idxFlat;
    return {z,dev:cur.d,ready,price:cur.p,exitDev:mean};
  }catch(e){ return null; }
}

/* حارس الخروج: الفجوة عادت للمتوسط أو حد الصبر بلغ — جني كامل */
function gapExitCheck(p:number):boolean{
  const ctx=S._gapCtx;
  if(!ctx) return false;
  const dev=((p-ctx.idx)/ctx.idx)*100;
  if(dev>=ctx.exitDev) return true;  // عادة الإنديكس ✓
  return false;
}

/* \u0646\u0628\u0636\u0629 \u0635\u0627\u0626\u062f \u0627\u0644\u0641\u062c\u0648\u0627\u062a: \u062a\u0645\u0633\u062d \u0643\u0644 \u062f\u0642\u064a\u0642\u0629\u060c \u0648\u0625\u0646 \u0648\u062c\u062f\u062a \u0641\u062c\u0648\u0629 \u0645\u062c\u0647\u0632\u0629 (z<-2\u03c3 + \u0627\u0633\u062a\u0642\u0631\u0627\u0631) \u062a\u062d\u0648\u0644 \u0644\u0644\u0639\u0645\u0644\u0629 \u0648\u062a\u062f\u062e\u0644 \u0637\u0648\u064a\u0644 */
/* التوافق السبعي: الحيتان + الفجوة + الدفتر (0-4) */
async function confluenceScore(symbol:string,dev:number){
  let score=0;
  try{
    // 1) جريان الحيتان: آخر 100 صفقات
    const tr=await kcPublic('/api/v1/trade/history?symbol='+encodeURIComponent(symbol),4000);
    const trades=(Array.isArray(tr)?tr:[]).slice(0,100);
    let whaleNet=0;
    if(trades.length>=20){
      const sizes=trades.map((t:any)=>+t.size).sort((a:number,b:number)=>a-b);
      const med=sizes[Math.floor(sizes.length/2)]||1;
      let lastPx=0;
      for(const t of trades){ if(+t.price>0) lastPx=+t.price; }
      if(lastPx>0){
        for(const t of trades){
          if(+t.size>med*8) whaleNet+=(t.side==='buy'?1:-1)*+t.size*lastPx;
        }
      }
    }
    if(whaleNet>0) score++;
    // 2) الدفتر (أعمق 20)
    const bk=await kcPublic('/api/v1/level2/depth20?symbol='+encodeURIComponent(symbol),3000);
    let imb=0;
    if(bk&&bk.bids&&bk.asks){
      const bd=(bk.bids||[]).reduce((a:number,x:any)=>a+ +x[1],0);
      const ad=(bk.asks||[]).reduce((a:number,x:any)=>a+ +x[1],0);
      if(bd+ad>0) imb=(bd-ad)/(bd+ad);
    }
    if(imb>0.05) score++;
  }catch(e){ logSwallow('silent-catch',e); }
  if(dev<-0.15) score+=2;
  return score;
}

/* PEAK-HOUR WINDOW: gaps spawn/revert around US open (13-19 UTC, measured 15:00 peak) */
function inPeakWindow():boolean{
  const h=new Date().getUTCHours();
  return h>=13&&h<=19;
}

/* BTC LEAD: BTC moves 1min ahead of alts (correlation 0.117 measured) */
let _btcLead:{t:number,ret:number}|null=null;
async function btcLead():Promise<number>{
  try{
    const now=Date.now();
    if(_btcLead&&now-_btcLead.t<60000) return _btcLead.ret;
    const k=await kcPublic('/api/v1/kline/query?symbol=XBTUSDTM&granularity=1',3000);
    const rows=(Array.isArray(k)?k:[]).slice(-3);
    if(rows.length<2) return 0;
    const last=+rows[rows.length-1][4], prev=+rows[0][4];
    const ret=prev>0?(last-prev)/prev*100:0;
    _btcLead={t:now,ret};
    return ret;
  }catch(e){ return 0; }
}

/* WALL PERSISTENCE: real support wall survives two snapshots 15s apart */
async function wallReal(symbol:string):Promise<boolean>{
  try{
    const snap=async()=>{
      const b=await kcPublic('/api/v1/level2/depth20?symbol='+encodeURIComponent(symbol),3000);
      if(!b||!b.bids||!b.bids.length) return null;
      let mx=0,mxP=0;
      for(const x of b.bids.slice(0,15)){
        if(+x[1]>mx){mx=+x[1];mxP=+x[0];}
      }
      return {wallP:mxP,wallS:mx};
    };
    const s1=await snap();
    if(!s1||!(s1.wallS>0)) return true;
    await new Promise(r=>setTimeout(r,15000));
    const s2=await snap();
    if(!s2) return true;
    const sameZone=Math.abs(s2.wallP-s1.wallP)/Math.max(1e-12,s1.wallP)<0.004;
    const stillSized=s2.wallS>=s1.wallS*0.5;
    return sameZone&&stillSized;
  }catch(e){ return true; }
}

export async function gapHunterTick(){
  if(S._gapHuntBusy) return; S._gapHuntBusy=true;
  try{
    const st=await gapScan();
    if(!st||!st.list||!st.list.length) return;
    // أ\u0641\u0636\u0644 3 \u0645\u0631\u0634\u062d\u064a\u0646 \u0641\u0642\u0637 (أ\u0639\u0645\u0642 ف\u062c\u0648\u0629) — ت\u0641\u0627\u062d\u064a ا\u0644\u0625\u0634\u0627\u0631\u0629 ا\u0644\u0645\u062f\u0631\u0648\u0633\u0629
    for(const cand of st.list.slice(0,3)){
      const sig=await gapSignal(cand.symbol);
      if(sig&&sig.ready){
        const cs=await confluenceScore(cand.symbol,sig.dev);
        if(cs<3){ continue; }
        if(!inPeakWindow()){ continue; }
        const lead=await btcLead();
        if(lead<-0.08){ continue; }
        if(!(await wallReal(cand.symbol))){ continue; } // التوافق أقل من 3/4 — انتظر فرصة أفضل
        // ت\u062d\u0648\u064a\u0644 ا\u0644\u0631\u0645\u0632 إ\u0646 ل\u0632\u0645 + د\u062e\u0648\u0644 ط\u0648\u064a\u0644 ب\u0627\u0644\u0633\u0639\u0631 ا\u0644\u062d\u0627\u0644\u064a
        if(cand.symbol!==S.config.symbol){
          pushLog('server','\ud83d\udd0d \u0635\u0627\u0626\u062f \u0627\u0644\u0641\u062c\u0648\u0627\u062a: \u062a\u062d\u0648\u064a\u0644 \u0625\u0644\u0649 '+cand.symbol.replace(/USDTM$/,'')+' \u2014 \u0641\u062c\u0648\u0629 '+sig.dev.toFixed(2)+'% (z='+sig.z.toFixed(1)+')');
          S.config.symbol=cand.symbol;
          S.config.displaySymbol=cand.symbol.replace(/USDTM$/i,'').replace(/^XBT$/i,'BTC');
          S.multiplier=1; S.tickSize=0.1; S._metaSym=null; S._metaAt=0;
          setStreamSymbol(cand.symbol);
          S.grid=[]; S.tape=[]; S.priceTrail=[]; S.emaFast=S.emaSlow=null; // السجل يبقى — الانتقال لا يمسح التاريخ
        }
        S._gapCtx={idx:cand.idx,exitDev:sig.exitDev,entryAt:Date.now()};
        // الحجم = نف معايير تطبيق KING النهائي (contractsForLevel):
        // 80% من رصيد الدورة ÷ المستويات × الرافعة — ليس كمية مخترعة صغيرة
        S.lastPrice=sig.price;
        S._metaSym=null; S._metaAt=0; // يجب قراءة مواصفات العقد الجديد قبل الحجم
        try{ const ct=await kcPublic('/api/v1/contracts/'+encodeURIComponent(cand.symbol),4000);
          if(ct&&ct.multiplier){ S.multiplier=+ct.multiplier; S.tickSize=+ct.tickSize||S.tickSize; S._metaSym=cand.symbol; S._metaAt=Date.now(); }
        }catch(e){ logSwallow('silent-catch',e); }
        maybeHunt(sig.price);
        pushLog('server','\u2696\ufe0f \u0641\u062c\u0648\u0629 \u0645\u062c\u0647\u0632\u0629: \u062f\u062e\u0648\u0644 \u0637\u0648\u064a\u0644 @ '+fmtPx(sig.price)+' \u2014 \u062e\u0631\u0648\u062c \u0639\u0646\u062f \u0639\u0648\u062f\u0629 \u0627\u0644\u0641\u062c\u0648\u0629');
        break;
      }
    }
  }catch(e){ logSwallow('silent-catch',e); }
  finally{ S._gapHuntBusy=false; }
}
export async function maybeSwitchSymbol(){
  if(S.status!=='running') return;
  // مع مركز مفتوح: الصفقة تُدار حتى الجني — لا تبديل في منتصف صفقة أبدًا
  if(S.position){
    try{ await scanRadar(); }catch(e){ logSwallow('silent-catch', e); }
    return; }
  // ─── دورة المالك: أفضل عملة بكل الشروط قبل كل صفقة، وبعد الجني تُختار الأفضل من جديد ───
  // مهلة تبريد 45 ثانية + لا تبديل وسط دراسة — يكتمل تقييم العملة الحالية أولًا
  if(Date.now()-_lastSwitchAt<45000) return;
  if(studying()) return;
  try{
    const r=await scanRadar(); if(!r||!r.list||!r.list.length) return;
    const cur=r.list.find(l=>l.symbol===S.config.symbol);
    // أفضل عملة تستوفي كل الشروط (درجة قوية ≥ 55 من رادار الفرص)
    const best=r.list.filter(l=>l.symbol!==S.config.symbol).find(l=>(l.score||0)>=55);
    if(!best) return; // لا فرصة أقوى — البقاء على الحالية والدخول حين تكتمل شروطها
    // نحن الأفضل بالفعل؟ اكتملت الدراسة هنا وادخل الصفقة
    // بعد جني مكتمل (خلال 10 دقائق): إعادة اختيار حاسمة — الأفضل مطلقاً بلا هامش
    const afterHarvest=!!(S._harvestAt&&Date.now()-S._harvestAt<10*60*1000);
    // ─── دوران البورة (قاعدة المالك: 15 دقيقة بلا تنفيذ) ───
    // 15 دقيقة بلا تنفيذ صفقة رغم الدراسة الكاملة = العملة بارة الآن:
    // انتقال لأفضل بديل مؤهل حتى لو كانت الحالية متصدرة — لا جلوس أبدي أوامر مسلحة لا تُنفذ
    // ─── إصلاح تجويع الصيد: عدّاد البورة يقيس «مدة الجلوس على العملة الحالية» ───
    // من _dwellAt (لحظة الوصول للعملة) — لا من lastHuntAt (آخر صفقة في التاريخ).
    // القياس من lastHuntAt كان يجعل كل عملة تُدار بعد دقيقة (الصفقة السابقة قديمة
    // دائماً) فلا يكمل أي صيد أبداً — صفر صفقات لساعات
    const idleMs=Date.now()-(S._dwellAt||S.startedAt||S._savedAt||Date.now());
    const barren=idleMs>15*60*1000;
    if(barren&&best){ _lastSwitchAt=Date.now();
      const curDisp=cur?cur.disp:S.config.displaySymbol, curSc=cur?cur.score:0;
      pushLog('server','🔄 دوران البورة ('+Math.round(idleMs/60000)+' د بلا تنفيذ): '+curDisp+' ('+curSc+') → '+best.disp+' ('+best.score+') — لا جلوس على عملة خامدة');
      toast('🔄 دوران: '+best.disp+' — '+best.score);
      nativeNotify('AB-KING — دوران البورة',best.disp+' درجة '+best.score+' بدل '+curDisp+' الخامدة');
      saveCfg({symbol:best.symbol,leverage:S.config.leverage,levels:S.config.levels,
        gridStepPct:S.config.gridStepPct,huntPct:S.config.huntPct,tpNet:S.config.tpNet,tpUsd:S.config.tpUsd,
        cycleBalance:S.config.cycleBalance,directionMode:S.config.directionMode,
        mode:S.config.mode,sound:S.sound,soundTone:S.soundTone});
      return; }
    // نحن الأفضل بالفعل وننتج؟ اكتملت الدراسة هنا وادخل الصفقة
    if(cur&&(cur.score||0)>=(best.score||0)) return;
    // هامش وضوح 12 درجة يمنع التأرجح بين عملتين متقاربتين (خارج نافذة ما بعد الجني)
    if(!afterHarvest&&cur&&best.score-cur.score<12) return;
    _lastSwitchAt=Date.now();
    const curDisp=cur?cur.disp:S.config.displaySymbol, curSc=cur?cur.score:0;
    pushLog('server','🎯 انتقال للأفضل قبل الصفقة: '+curDisp+' ('+curSc+') → '+best.disp+' ('+best.score+') — كل الشروط متوفرة — تُدرس 60 ثانية ثم تُفتح الصفقة');
    toast('🎯 الأفضل الآن: '+best.disp+' — '+best.score);
    nativeNotify('AB-KING — انتقال للأفضل',best.disp+' درجة '+best.score+' قبل الصفقة');
    // الجني التالي: يُعاد الاختيار من جديد عبر openNextReady وهذه الدالة
    saveCfg({symbol:best.symbol,leverage:S.config.leverage,levels:S.config.levels,
      gridStepPct:S.config.gridStepPct,huntPct:S.config.huntPct,tpNet:S.config.tpNet,tpUsd:S.config.tpUsd,
      cycleBalance:S.config.cycleBalance,directionMode:S.config.directionMode,
      mode:S.config.mode,sound:S.sound,soundTone:S.soundTone});
    startBot();
  }catch(e){ logSwallow('silent-catch', e); } }

async function exPosition(symbol:any){
  const raw=await kcPrivate('GET','/api/v1/position?symbol='+encodeURIComponent(symbol));
  const p=Array.isArray(raw)?raw[0]:raw;
  const qty=p?Number(p.currentQty)||0:0;
  if(qty===0) return null;
  return {side:qty<0?'short':'long', size:Math.abs(qty),
    entry:Number(p.avgEntryPrice)||S.lastPrice||0,
    // الرافعة الحقيقية المطبّقة على المنصة — قد تخالف المضبوط (CROSS يتجاهل رافعة الأمر)
    leverage:Number(p.realLeverage)||Number(p.leverage)||S.config.leverage,
    unrealized:Number(p.unrealisedPnl)||0,
    liquidation:Number(p.liquidationPrice)||null,
    openedAt:Date.now()};
}
const exOrders = symbol => kcPrivate('GET','/api/v1/orders?status=active&symbol='+encodeURIComponent(symbol))
  .then(d=>{const a=Array.isArray(d)?d:(d.items||[]);
    return a.map(o=>({orderId:o.id,clientOid:o.clientOid||null,side:o.side==='sell'?'sell':'buy',
      price:+o.price,size:+o.size,filledSize:+(o.filledSize??o.dealSize??0)||0,reduceOnly:!!o.reduceOnly}));})
  .catch(()=>null); // null = فشل القراءة — جولة مزامنة تُتخطى كاملة، لا «لا أوامر» زائفة تفكك الشبكة
// قائمة أوامر الوقف — قناة منفصلة عن الأوامر العادية لا يطالها كنس اليتيمة
// العادي، ولهذا تراكمت 9 ستوبات على مركز المالك. null = فشل قراءة = لا كنس أعمى
const exFills = symbol => kcPrivate('GET','/api/v1/fills?symbol='+encodeURIComponent(symbol)+'&pageSize=100')
  .then(d=>{const a=Array.isArray(d)?d:(d.items||[]);
    return a.map(f=>({orderId:f.orderId||'',clientOid:f.clientOid||null,
      side:f.side==='sell'?'sell':'buy',price:+f.price||0,size:+f.size||0,
      fee:Math.abs(+f.fee||0),feeCurrency:f.feeCurrency||'',at:+f.createdAt||0,ts:f.tradeId||''}));})
  .catch(()=>null);
const exStops = symbol => kcPrivate('GET','/api/v1/stopOrders?symbol='+encodeURIComponent(symbol))
  .then(d=>{const a=Array.isArray(d)?d:(d.items||[]);
    return a.map(o=>({id:o.id||o.orderId||'',stopPrice:+(o.stopPrice??o.price??0)||0}));})
  .catch(()=>null);
/* رفض «وضع هامش الأمر لا يتطابق» (KuCoin 330005) — القاعدة الرسمية: وضع الهامش في الأمر
   يجب أن يطابق وضع الرمز الحالي في الحساب حرفيًا، وحقله في المركز اسمه marginMode (نص)،
   وإن حُذف من الأمر يُفترض ISOLATED فيفشل على رموز CROSS. الحل: كشف الوضع الصحيح مرة،
   ثم عند أي رفض جرّب كل الأوضاع واحفظ الفائز طويلًا — فتختفي العاصفة من أول نجاح */
const mmTried={};
const mmCache={}; // وضع هامش كل زوج — يُكشف من المركز أو من أول نجاح ويُحفظ 10 دقائق
function _mmFromPos(pos){ if(!pos) return null;
  if(typeof pos.marginMode==='string'&&pos.marginMode) return pos.marginMode.toUpperCase();
  if(typeof pos.crossMode==='boolean') return pos.crossMode?'CROSS':'ISOLATED';
  return null; }
async function placeOrderSmart(body){
  const sym=body.symbol;
  try{ const cm=mmCache[sym];
    if(cm&&cm.mode&&Date.now()-cm.at<600000){ body={...body,marginMode:cm.mode}; }
    else { const pos=await kcPrivate('GET','/api/v1/position?symbol='+encodeURIComponent(sym));
      const mm=_mmFromPos(pos);
      if(mm){ mmCache[sym]={at:Date.now(),mode:mm}; body={...body,marginMode:mm}; } }
  }catch(_){ logSwallow("silent-catch", _); }
  try{ return await kcPrivate('POST','/api/v1/orders',body); }
  catch(e){ let m=(e as any)?.message||String(e);
    // تصنيف دقيق: «insufficient available margin» خطأ كمية/رصيد وليس وضع هامش —
    // الخلط بينهما كان يدخل سلسلة الوضع ويُنتج «margin mode does not match» وهميًا
    const isFunds=x=>/insufficient|too high|available margin|available balance/i.test(x);
    const isMode =x=>/margin mode|330005/i.test(x);
    // رفض postOnly (السعر قاطع الدفتر): أعد المحاولة كأمر حدّي عادي — مستوى بعيد عابر لا يُفوَّت
    if(body.postOnly&&!isFunds(m)&&!isMode(m)){ const b3={...body}; delete b3.postOnly;
      try{ return await kcPrivate('POST','/api/v1/orders',b3); }
      catch(e2){ m=(e2 as any)?.message||String(e2); body=b3; } }
    // رصيد/كمية: أبطِل توثيق المضاعف ليُعاد جلبه فورًا وارمِ — لا علاقة لوضع الهامش هنا
    if(isFunds(m)){ S._metaAt=0; if(S._metaSym===sym) S._metaSym=null; throw e; }
    if(!isMode(m)) throw e;
    // سوِّ وضع الرمز إلى CROSS عبر نقطة KuCoin الرسمية v2 — تنجح فقط بلا مركز ولا أوامر
    // (بأمر المالك: الهامش متبادل دائمًا ليحمي الرصيد الكلي الصفقة — لا معزول أبدًا)
    if(!mmTried[sym]){ mmTried[sym]=1;
      try{ await kcPrivate('POST','/api/v2/position/batchChangeMarginMode',
        {marginMode:'CROSS',symbols:[sym]}); }catch(_){ logSwallow("silent-catch", _); } }
    // جرّب كل الأوضاع بالتناوب واحفظ الفائز — أول نجاح يُسكت العاصفة نهائيًا
    const errs=[m];
    for(const alt of ['CROSS','ISOLATED',null]){
      if(alt===body.marginMode) continue;
      const b2={...body}; if(alt) b2.marginMode=alt; else delete b2.marginMode;
      try{ const r=await kcPrivate('POST','/api/v1/orders',b2);
        mmCache[sym]={at:Date.now(),mode:alt}; return r; }
      catch(e3){ const m3=(e3 as any)?.message||String(e3); if(!errs.includes(m3)) errs.push(m3); } }
    // فشل كل شيء: اعرض كل الأسباب الحقيقية لا آخرها فقط — الخطأ الخفي كان يُبتلع
    throw new Error(errs.join(' | ')); } }
// بوابة التوثيق: لا أمر دخول حقيقي بكمية محسوبة محليًا قبل توثيق مضاعف العقد
// من المنصة لهذا الرمز بالذات — كمية بمضاعف خاطئ تُرفض أو تفتح مركزًا كارثي الحجم
async function ensureLiveMeta(symbol){ if(S._metaSym===symbol&&S.multiplier>0) return;
  try{ const ct=await fetchContract(symbol); const mul=+ct.multiplier;
    if(!(mul>0)) throw new Error('no multiplier');
    S.multiplier=mul; S.tickSize=+ct.tickSize||S.tickSize; S._metaSym=symbol;
    if(+ct.maxOrderQty>0) S.maxOrderQty=+ct.maxOrderQty; S._metaAt=Date.now();
  }catch(_){ throw new Error('بيانات العقد غير جاهزة — أُجّل الأمر لحين توثيق المضاعف'); } }
/* ─── سرب إنقاذ المراكز اليتيمة (كارثة 8 مراكز): الانتقال بين العملات مع
   فشل إغلاق المركز القديم (429/هامش) ترك مراكز حقيقية بلا إدارة. هذا الفحص
   يلتقط كل مركز في الحساب ليس للعملة الحالية ويُثبت له أمر جني رابح (tpUsd)
   — لا إغلاق بخسارة أبداً، ولا ترك مركز بلا إدارة ─── */
// قراءة قائمة مراكز الحساب — شكل kcPrivate يعيد data مباشرة (مصفوفة)،
// والقراءة القديمة resp.data كانت ترجع فارغاً دائماً (لهذا repair رأى 0 مركز)
async function exAllPositions(){
  const raw=await kcPrivate('GET','/api/v1/positions');
  const arr=Array.isArray(raw)?raw:((raw&&(raw as any).data)||[]);
  return (arr||[]).filter((x:any)=>Math.abs(Number(x.currentQty)||0)>1e-12);
}
/* ─── إصلاح الحساب الشامل (أمر المالك الصريح): يمسح كل مراكز الحساب، يلغي
   كل الأوامر والستوبات، يثبت لكل مركز هدف جني رابحاً واحداً، ويعيد تقريراً
   كاملاً يُعرض في التطبيق — المنصة هي المرجع الوحيد ─── */
export async function repairAccount(){
  if(S.config.mode!=='live'||!S.keys) return { ok:false, error:'الوضع الحقيقي فقط' };
  pushLog('server','🔧 بدء إصلاح الحساب الشامل — المنصة هي المرجع...');
  const report:any[]=[]; let cancelled=0, stops=0;
  try{
    const list=await exAllPositions();
    // إلغاء كل الأوامر والستوبات على كل رموز المراكز
    for(const x of list){ const sym=String(x.symbol);
      try{ await exCancelStops(sym); stops++; }catch{}
      try{ const os=await exOrders(sym);
        for(const o of (os||[])){ try{ await exCancelOne((o as any).orderId); cancelled++; }catch{} } }catch{} }
    // لكل مركز: هدف جني رابح واحد
    for(const x of list){ const sym=String(x.symbol);
      const qty=Number(x.currentQty)||0, side=qty>0?'long':'short', absQ=Math.abs(qty);
      const entry=Number(x.avgEntryPrice)||0, mult=Number(x.multiplier)||1;
      const pnl=Number((x as any).unrealisedPnl)||0;
      let tp=0;
      try{
        if(entry>0&&absQ>0){ const tpU=Math.max(0.5,Number(S.config.tpUsd)||1);
          const dPx=tpU/absQ/mult*1.25;
          const price=roundTick(side==='long'?entry+dPx:entry-dPx,S.tickSize||1e-8);
          await exPlaceLimit({symbol:sym,side:side==='long'?'sell':'buy',price,qty:absQ,reduceOnly:true,clientOid:uid('rep')});
          tp=price; } }catch(e:any){ pushLog('warn','هدف جني '+sym+' فشل: '+String(e?.message||e)); }
      report.push({symbol:sym,side,size:absQ,entry:Number(entry.toFixed(8)),pnl:Number(pnl.toFixed(2)),tp:Number(tp)||null});
    }
    const msg='🔧 إصلاح الحساب: '+list.length+' مركزاً حياً'+(list.length?(' ('+list.map((x:any)=>String(x.symbol).replace('USDTM','')).join(' · ')+')'):'')+' — أُلغيت '+cancelled+' أمراً و'+stops+' ستوب — لكل مركز هدف جني رابح'+(list.length?' — لا خسارة ولا دخول جديد حتى تُجنى كلها':'');
    pushLog('server',msg);
    return { ok:true, positions:report, cancelledOrders:cancelled, cancelledStops:stops, message:msg };
  }catch(e:any){ pushLog('error','إصلاح الحساب فشل: '+String(e?.message||e)); return { ok:false, error:String(e?.message||e) }; } }

async function orphanSweep(){
  if(S.config.mode!=='live'||!S.keys) return;
  try{
    const list=await exAllPositions();
    const cur=S.config.symbol;
    for(const x of list){
      const qty=Number(x.currentQty)||0; if(Math.abs(qty)<1e-12) continue;
      const sym=String(x.symbol); if(sym===cur) continue; // مركز العملة الحالية يديره المحرك
      const side=qty>0?'long':'short', absQ=qty>0?qty:-qty;
      const entry=Number(x.avgEntryPrice)||0; if(!(entry>0)) continue;
      // 1) إلغاء كل ستوبات الزوج (قاعدة المالك: لا وقف خسارة على المنصة أبداً)
      try{ await exCancelStops(sym); }catch{}
      // 2) إلغاء أي أوامر دخول/زائدة غير reduceOnly (لا مزيد من الأوامر على اليتيمة)
      try{ const os0=await exOrders(sym);
        for(const o of (os0||[])){ if(!(o as any).reduceOnly) { try{ await exCancelOne((o as any).orderId); }catch{} } } }catch{}
      // 3) هل يوجد أمر جني حي؟ لا تكرر الإرسال
      try{ const os=await exOrders(sym); if(os&&os.some((o:any)=>o.reduceOnly&&(o.size-o.filledSize)>1e-12)) continue; }catch{}
      // هدف جني رابح: tpUsd المحدد + رسوم، وتحويله لمسافة سعرية بحسب الحجم
      const mult=Number(x.multiplier)||1;
      const qMax=Number(x.maxOrderQty)||0; void qMax;
      const notional=Math.max(1e-9,absQ*mult*entry);
      const tpU=Math.max(0.5,Number(S.config.tpUsd)||1);
      const dPx=tpU/absQ/mult*1.25; // صافي الهدف + هامش رسوم 25%
      const price=roundTick(side==='long'?entry+dPx:entry-dPx,S.tickSize||1e-8);
      if(!(price>0)) continue;
      try{
        await exPlaceLimit({symbol:sym,side:side==='long'?'sell':'buy',price,qty:absQ,reduceOnly:true,clientOid:uid('orph')});
        pushLog('server','🔒 صفقة معلقة سابقة ('+sym+' '+side+' '+absQ+') — ثُبّت لها هدف جني رابح @ '+price+' — لا إغلاق بخسارة');
      }catch(e:any){ pushLog('warn','تعذر تثبيت هدف جني للمركز المعلق '+sym+': '+String(e?.message||e)); }
    }
  }catch(e){ logSwallow('orphanSweep',e); } }

async function exPlaceLimit(intent){
  await ensureLiveMeta(intent.symbol);
  return placeOrderSmart({clientOid:intent.clientOid,symbol:intent.symbol,
    side:intent.side,type:'limit',price:String(intent.price),size:intent.qty,
    leverage:Number(intent.leverage)||Number(S.config.leverage)||1,timeInForce:'GTC',reduceOnly:!!intent.reduceOnly,
    postOnly:true, // كل الأوامر الحدّية صانعة سوق: رسوم 0.02% بدل 0.06% — برأس مال صغير الفرق صافٍ حقيقي
    marginMode:'CROSS'}); // متبادل دائمًا بأمر المالك — الرصيد الكلي يحمي الصفقة
}
async function exPlaceMarket(symbol,side,qty){
  await ensureLiveMeta(symbol);
  return placeOrderSmart({clientOid:'hunt_'+Date.now().toString(36),
    symbol,side,type:'market',size:qty,leverage:Number(S.config.leverage)||1,marginMode:'CROSS'});
}
const exCancelAll = symbol => kcPrivate('DELETE','/api/v1/orders?symbol='+encodeURIComponent(symbol)).catch(()=>{});
const exCancelOne = id => kcPrivate('DELETE','/api/v1/orders/'+id).catch(()=>{});
// أوامر الوقف قناة منفصلة في KuCoin — «إلغاء الكل» العادي لا يشملها أبدًا
const exCancelStops = symbol => kcPrivate('DELETE','/api/v1/stopOrders?symbol='+encodeURIComponent(symbol)).catch(()=>{});
const exCancelStopOne = id => kcPrivate('DELETE','/api/v1/stopOrders/'+id).catch(()=>{});
// إغلاق حقيقي بكمية محددة — جني ربح فعلي على المنصة (سعر السوق، تخفيض فقط)
// عبر placeOrderSmart: وضع الهامش يُطابَق مع وضع المركز الفعلي تلقائيًا —
// الرفض «margin mode does not match» كان يترك المراكز مفتوحة بلا رقيب
function exCloseQty(symbol,side,qty){
  return placeOrderSmart({clientOid:'cls_'+Date.now().toString(36)+Math.floor(Math.random()*1000),
    symbol,type:'market',side:side==='short'?'buy':'sell',size:qty,reduceOnly:true,marginMode:'CROSS'}); }
function exClose(symbol,side){
  return placeOrderSmart({clientOid:'cls_'+Date.now().toString(36),
    symbol,type:'market',side:side==='short'?'buy':'sell',closeOrder:true,reduceOnly:true,marginMode:'CROSS'});
}
async function exPing(){
  const a=await kcPrivate('GET','/api/v1/account-overview?currency=USDT');
  return a?Number(a.accountEquity??a.availableBalance??0):null;
}
/* حارس خادمي: أمر إيقاف طوارئ يعيش على خوادم KuCoin نفسها — يقفل المركز قبل التصفية
   حتى لو انقطع التطبيق أو نام الجوال (آخر خط دفاع عند غياب البوت) */
async function exPlaceStopGuard(){ if(S.config.mode!=='live'||!S.keys||!S.position) return;
  const pos=S.position, sh=pos.side==='short';
  const mark=S.markPrice||S.lastPrice||pos.entry||0; if(!(mark>0)) return;
  // مركز CROSS لا تُرجع المنصة له سعر تصفية — فكان الحارس يُبنى على قيمة منحطة (~1e-10).
  // المرساة الآن: الأسبق بين «قبل التصفية مباشرة» و«مسافة خسارة الكارثة 16$ من الدخول»
  let liq=S.exLiqPrice||pos.liquidation||S.liqPrice||0;
  if(!(liq>0)||(sh?liq<=mark*1.005:liq>=mark*0.995)) liq=0; // تصفية غير معقولة = لا تصفية
  // ─── قاعدة المالك المطلقة (حادثة SHIB 2026-10-30): لا وقف خسارة على المنصة
  // إطلاقاً. الحارس السابق عند -16$ نفّذ البيع على المنصة بخسارة والمحرك لم
  // يعلم. الحارس الآن = حارس تصفية فقط (قبل التصفية مباشرة) — وCROSS بلا سعر
  // تصفية معلن = لا حارس. الحماية الحقيقية: سقف التعرض 4× + الصمود + الجني ───
  const cands=(liq>0)?[sh?liq*0.99:liq*1.01]:[];
  const gp=roundTick(sh?Math.min(...cands):Math.max(...cands),S.tickSize||1e-10);
  // سلامة: سعر منحط أو ملاصق للسوق لا يحمي شيئًا — لا تُرسله
  if(!(gp>0)||(sh?gp<=mark*1.003:gp>=mark*0.997)){ S._guardId=null; return; }
  // حارس قائم قريب من المطلوب = لا شيء — إعادة الإرسال كل دقيقة كانت تراكم أوامر وقف مكدسة
  if(S._guardId&&S._guardPx&&Math.abs(gp-S._guardPx)/S._guardPx<0.005) return;
  try{ // حارس واحد أبدي: قبل وضع الجديد احذف كل ستوبات الزوج — الاعتماد على
    // المعرّف المحلي _guardId كان يفقده عند إعادة تشغيل التطبيق فتبقى القديمة
    // على المنصة للأبد وتتراكم (كارثة الـ9 ستوبات المكدسة على مركز المالك)
    await exCancelStops(S.config.symbol); S._guardId=null;
    // عبر placeOrderSmart: الحارس كان يُرسَل بلا marginMode فيُرفض تمامًا —
    // آخر خط دفاع كان معطّلًا عمليًا عند أي مركز CROSS
    const r=await placeOrderSmart({clientOid:'grd_'+Date.now().toString(36),
      symbol:S.config.symbol,type:'market',side:sh?'buy':'sell',
      stop:sh?'up':'down',stopPrice:String(gp),stopPriceType:'MP',
      reduceOnly:true,closeOrder:true,marginMode:'CROSS'});
    S._guardId=(r&&(r.orderId||r.id))||null; S._guardPx=gp; S._guardAt=Date.now();
    if(!S._guardLogged){ S._guardLogged=true;
      pushLog('server','حارس خادمي مفعّل — إيقاف طوارئ على المنصة عند '+fmtPx(gp)+' (يعمل حتى لو نام التطبيق)'); }
  }catch(e){ if(!S._guardErrAt||Date.now()-S._guardErrAt>300000){ S._guardErrAt=Date.now();
      pushLog('error','الحارس الخادمي: '+(String((e as any)?.message||e))); } } }

/* ---------- تدفق الشريط والحيتان — من صفقات المنصة المنفذة لحظيًا ---------- */
function tapeFlow(){ const t=S.tape; if(!t||t.length<6) return {bias:0,whale:0,whaleVol:0};
  let b=0,s=0; const sizes=t.map(x=>x.size).sort((x,y)=>x-y);
  const med=sizes[Math.floor(sizes.length/2)]||1;
  let wb=0,ws=0;
  for(const x of t){ if(x.side==='buy')b+=x.size; else s+=x.size;
    if(x.size>med*5){ if(x.side==='buy')wb+=x.size; else ws+=x.size; } } // طبعة حوت: 5× الوسيط
  const tot=b+s||1, wtot=wb+ws||1;
  return {bias:(b-s)/tot, whale:(wb-ws)/wtot, whaleVol:(wb+ws)/tot}; }

/* ================================================================
   محرك القرار — شبكة + صيد + جني + انعكاس (مطابق للأصل)
   ================================================================ */
function wavg(f){ let n=0,q=0; for(const x of f){ if(x.qty>0&&x.price>0){n+=x.price*x.qty;q+=x.qty;} }
  return q>0?n/q:0; }
/* قفل التباعد المركزي: لا دخول جديد (صيد/تعويض/شبكة) إلا إن بعد عن آخر تنفيذ حقيقي
   مسافة لا تقل عن 0.75×خطوة الشبكة المضبوطة — يمنع تكدس 6 أمراً داخل 0.57% */
function lastRealFillPx(){ const f=S.grid.filter(g=>g.status==='filled'&&g.filledPrice>0)
    .sort((a,b)=>b.filledAt-a.filledAt);
  if(f.length&&Date.now()-(f[0].filledAt||0)<30*60*1000) return f[0].filledPrice;
  return 0; }
function spacingGate(p){ if(!(p>0)) return true;
  const last=lastRealFillPx(); if(!last) return true;
  const hfMin=S.config.hfMode?0.0004:0.0012;
  const minGap=Math.max(S.config.gridStepPct/100*(S.config.hfMode?0.25:0.75),hfMin);
  return Math.abs(p-last)/last>=minGap; }
function filledAdds(){ const o=S.journal.filter(j=>j.status==='open'&&(j.symbol===undefined||j.symbol===S.config.displaySymbol));
  const n=o.reduce((a,j)=>a+Math.max(1,j.mergedOrders||1),0); return n>0?n:(S.position?1:0); }
function addsBlocked(){ if(S.status!=='running') return false;
  if(filledAdds()>=effLevels()) return true;
  return !!(S.position&&S.regime&&S.regime.shock); }
function adversePct(p){ const pos=S.position; if(!pos||!p||!pos.entry) return 0;
  return pos.side==='short'?((p-pos.entry)/pos.entry)*100:((pos.entry-p)/pos.entry)*100; }
function lastJumpPct(){ const t=S.priceTrail; if(t.length<2) return 0;
  const a=t[t.length-2],b=t[t.length-1]; return a>0?Math.abs(b-a)/a*100:0; }
// حجم الموجة المعاكسة المتدرجة خلال 60 ثانية: ارتفاع السعر عن قاع النافذة (للشورت)
// أو هبوطه عن قمتها (للونغ) — يكشف الموجات الثابتة التي لا تراها قفزة النبضة
// الواحدة (كارثة كنس 9 مستويات دفعة واحدة في موجة صاعدة بطيئة)
function waveAdversePct(){ const pos=S.position,t=S.priceTrail;
  if(!pos||t.length<10) return 0;
  const w=t.slice(-60), p=t[t.length-1]; if(!(p>0)) return 0;
  if(pos.side==='short'){ const lo=Math.min(...w); return lo>0?(p-lo)/lo*100:0; }
  const hi=Math.max(...w); return hi>0?(hi-p)/hi*100:0; }
function bookQuality(){ const b=S.orderBook,p=S.lastPrice||0;
  if(!b||!b.bids.length||!b.asks.length||!p) return true;
  const bb=b.bids[0].price,ba=b.asks[0].price;
  if(bb<=0||ba<=0) return true;
  const mid=(bb+ba)/2, sp=(ba-bb)/mid*100;
  // سقف السبريد 0.5% — كان 0.35% يحظر معظم العملات الصفرية الصغيرة التي يعمل عليها المالك
  if(sp>0.5) return false;
  if(lastJumpPct()>0.45) return false;
  const short=S.config.direction==='short';
  const wall=short?b.bids:b.asks, depth=volSum(wall.slice(0,8));
  const need=contractsForLevel(p)*2.5;
  return !(depth>0&&need>0&&depth<need); }
// وسيط أحجام مستويات الدفتر — مرجع لكشف الجدران الضخمة وتطبيع تدفق الأوامر
function medLevelSz(ob){ const s=[...ob.bids.slice(0,10),...ob.asks.slice(0,10)].map(x=>x.size).sort((a,b)=>a-b);
  return s.length?s[Math.floor(s.length/2)]:0; }
/* ─── قياس الاندفاع (قاعدة المالك): تحرك السعر خلال آخر ~45 نبضة ───
   صعود قوي جارٍ؟ لا دخول شورت ضده حتى يستقر — وعكسه صحيح */
export function priceKick(){ const t=S.priceTrail||[]; if(t.length<12) return {up:0,down:0};
  const n=Math.min(45,t.length), p0=t[t.length-n], p1=t[t.length-1];
  if(!(p0>0)) return {up:0,down:0};
  const chg=(p1-p0)/p0*100;
  return {up:chg>0?chg:0, down:chg<0?-chg:0}; }
/* ─── فرملة التقلب (قاعدة المالك): انزلاق/موجة قوية = تجميد تسليح الدخول الجديد
   حتى يستقر السوق ثم تعود الأوامر — لا 13 أمراً في ربع ساعة بعد الآن ─── */
/* ─── سقف التعرّض (بديل وقف الخسارة — قرار المالك: لا إقفال بخسارة أبداً) ───
   الكارثة: التعرض بلغ ~12× رصيد الدورة فكانت التصفية عند ~-7% فقط.
   الحماية الرياضية: التعرض الكلي ≤ 4× رصيد الدورة → التصفية تحتاج حركة
   -25%+ بلا ارتداد — وبهامش كهذا تكفي الشبكة والصمود والجني المدروس */
/* ─── القاطع المرحلي (تصميم المالك): دفعات من 4 أوامر بدراسة بينها ───
   نشطه: بعد 4 تعبئات دخول. إطلاقه: هدوء التقلب (لا فرملة) + مرور دقيقتين
   + اكتمال دراسة جديدة بدأت بعد التوقف. عند الإطلاق: دفعة جديدة 4 أوامر */
export function batchHoldActive(){
  // ─── تتبع أقصى انزلاق أثناء التوقف (درس PEPE 18 أمراً): الإطلاق لا يكون
  // بالوقت وحده — بل بعد توقف السقوط فعلياً: لا قاع/قمة جديدة ضد المركز
  // منذ 8 دقائق كاملة. انزلاق مستمر = توقف مطلق حتى يستقر ───
  if(S.position&&S._batchHoldAt&&S.lastPrice>0){
    const p=S.lastPrice, long=S.position.side==='long';
    const worst=(S._batchWorst==null)?p:(long?Math.min(S._batchWorst,p):Math.max(S._batchWorst,p));
    if(worst!==S._batchWorst){ S._batchWorst=worst; S._batchWorseAt=Date.now(); } }
  if((S._batchFills||0)<(S._cycleMaxFills||4)) return false;
  // ─── قاعدة المالك الزمنية (أمر 2026-10-24): انزلاق بعد 4 أوامر =
  // توقف كامل 3 ساعات، ثم دراسة: استمرار الانزلاق = +ساعة إضافية ودراسة،
  // ولا عودة إلا باستقرار فعلي (لا انزلاق جديد منذ ساعة كاملة) + إشارة
  // جيدة — فحينها 4 أوامر متباعدة. جني الصفقة يُصفّر الدورة كلها ───
  const H=3600000;
  if(S._batchHoldAt&&(!S._batchHoldLogAt||Date.now()-S._batchHoldLogAt>1800000)){
    S._batchHoldLogAt=Date.now();
    const el=Math.floor((Date.now()-S._batchHoldAt)/H), min=Math.floor(((Date.now()-S._batchHoldAt)%H)/60000);
    const calm=S._batchWorseAt?Math.floor((Date.now()-S._batchWorseAt)/H):0;
    pushLog('server','🧷 القاطع ماضٍ: '+S._batchFills+' أوامر دخول — مضى '+el+'س'+min+'د من وجوب الـ3 ساعات'+
      (S._batchWorseAt?(Date.now()-S._batchWorseAt>H?' — الانزلاق متوقف منذ '+calm+' ساعة':' — الانزلاق ما زال نشطاً'):' — بانتظار القراءة')); }
  // ─── القاطع المطلق (أمر المالك 2026-10-30، بعد تكرار 11× و14×): لا إطلاق
  // أبداً ما دامت الصفقة مفتوحة. جهاز الـ3+1 ساعات كان يفتح دفعة جديدة عند
  // كل هدوء مؤقت في انزلاق بسيط متدرج فيتراكم 4+4+4... القاعدة النهائية:
  // 4 أوامر دخول للصفقة الواحدة ثم توقف مطلق حتى تُجنى كاملة — والعودة
  // للدخول بدورة جديدة نظيفة فقط بعد اكتمال الجني (يصفّر العداد تلقائياً) ───
  if(S._batchHoldAt&&!S._batchStudiedAt){ S._batchStudiedAt=true;
    startStudy(false); // دراسة جيدة جديدة قبل أي عودة — بصمة محدثة للوضع الحالي
    pushLog('server','🔍 بدأت دراسة ما بعد القاطع — لا فتح أوامر حتى يكتمل تقييم الوضع'); }
  return true; }

export function exposureCapped(){ const pos=S.position; if(!pos) return false;
  const notional=pos.size*(S.multiplier||1)*(pos.entry||0);
  return notional>Math.max(20,(S.config.cycleBalance||0)*4); }

export function volBrakeActive(){
  if(!S.position) return false;
  const vol=retStdev()||0;
  const shocked=!!(S.regime&&S.regime.shock);
  const oppWave=(S._waveN||0)>=3;
  return !!(shocked||vol>0.40||oppWave); } // الانهيار الحقيقي فقط — لا فرملة على تقلب عادي

function retStdev(){ const t=S.priceTrail; if(t.length<4) return 0;
  const r:any[]=[]; for(let i=1;i<t.length;i++) if(t[i-1]>0) r.push((t[i]-t[i-1])/t[i-1]*100);
  if(r.length<3) return 0; const m=r.reduce((a,b)=>a+b,0)/r.length;
  return Math.sqrt(r.reduce((s,x)=>s+(x-m)**2,0)/r.length); }
function readRegime(){ const mom=S.confluence?S.confluence.momentum:0;
  const vol=retStdev(), jump=lastJumpPct();
  const shock=jump>0.9||Math.abs(mom)>0.9;
  const up=S.emaFast!=null&&S.emaSlow!=null&&S.emaFast>S.emaSlow;
  const dn=S.emaFast!=null&&S.emaSlow!=null&&S.emaFast<S.emaSlow;
  let trend='range';
  if((up&&mom>0.04&&S.biasScore>3)||mom>0.12) trend='up';
  else if((dn&&mom<-0.04&&S.biasScore<-3)||mom<-0.12) trend='down';
  if(Math.abs(mom)<0.04&&vol<0.08) trend='range';
  let vk='normal';
  if(vol>=0.22||shock) vk='wild'; else if(vol<=0.045&&Math.abs(mom)<0.045) vk='calm';
  let label='سوق عرضي';
  if(shock) label='تغير مفاجئ';
  else if(trend==='up') label=vk==='calm'?'صاعد هادئ':'اتجاه صاعد';
  else if(trend==='down') label=vk==='calm'?'هابط هادئ':'اتجاه هابط';
  else if(vk==='wild') label='عرضي متذبذب';
  else if(vk==='calm') label='عرضي هادئ';
  const young=!!(S.startedAt&&Date.now()-S.startedAt<SCOUT_MS);
  return {trend,vol:vk,shock,label,
    sizeMult:shock?0.65:vk==='wild'?0.75:trend==='range'?0.9:1,
    stepMult:vk==='wild'||shock?1.35:vk==='calm'?0.9:1,
    huntMult:shock?1.5:vk==='wild'?1.25:1,
    maxAdds:shock||vk==='wild'?1:3, scoutOnly:shock||vk==='wild'||young};
}
function scoreBias(){ let sc=0; const rs:any[]=[];
  const bv=volSum(S.orderBook.bids),av=volSum(S.orderBook.asks),bt=bv+av;
  if(bt>0){ const im=(bv-av)/bt; sc+=im*40;
    if(Math.abs(im)>0.08) rs.push(im>0?'دفتر مشترين':'دفتر بائعين'); }
  let buyT=0,sellT=0; for(const t of S.tape){ if(t.side==='buy')buyT+=t.size; else sellT+=t.size; }
  const tt=buyT+sellT;
  if(tt>0){ const im=(buyT-sellT)/tt; sc+=im*25;
    if(Math.abs(im)>0.1) rs.push(im>0?'شريط شراء':'شريط بيع'); }
  if(S.funding){ sc+=clamp(-S.funding*80000,-20,20);
    rs.push(S.funding>0?'تمويل إيجابي ← شورت':'تمويل سلبي ← لونغ'); }
  if(S.lastPrice&&S.emaFast!=null&&S.emaSlow!=null){
    if(S.lastPrice>S.emaFast&&S.emaFast>S.emaSlow){sc+=15;rs.push('متوسط سريع فوق البطيء');}
    else if(S.lastPrice<S.emaFast&&S.emaFast<S.emaSlow){sc-=15;rs.push('متوسط سريع تحت البطيء');} }
  if(S.cvd){ sc+=Math.sign(S.cvd)*Math.min(20,Math.log10(Math.abs(S.cvd)+1)*5); }
  return {direction:sc>=0?'long':'short',score:Math.round(sc*10)/10,reasons:rs.slice(0,3)};
}
function readConfluence(){ const t=S.priceTrail,w=t.slice(-10);
  const bids=volSum(S.orderBook.bids),asks=volSum(S.orderBook.asks),bd=bids+asks;
  const imb=bd>0?(bids-asks)/bd:0;
  let mom=0; if(w.length>=2&&w[0]>0) mom=(w[w.length-1]-w[0])/w[0]*100;
  let tn=0,tp=0; for(const r of S.tape){tn+=r.size;tp+=r.price*r.size;}
  const vwap=tn>0?tp/tn:null; let sc=0; const rs:any[]=[];
  if(Math.abs(imb)>0.08){sc+=imb*40;rs.push(imb>0?'دفتر شراء غالب':'دفتر بيع غالب');}
  if(Math.abs(mom)>=0.04){sc+=mom*8;rs.push(mom>0?'زخم صاعد':'زخم هابط');}
  if(vwap&&S.lastPrice){ if(S.lastPrice>vwap){sc+=8;rs.push('فوق VWAP');} else {sc-=8;rs.push('تحت VWAP');} }
  if(S.emaFast!=null&&S.emaSlow!=null){ if(S.emaFast>S.emaSlow){sc+=10;rs.push('ميل صاعد');}
    else {sc-=10;rs.push('ميل هابط');} }
  const idle=w.length>=8&&S.tape.length>=8&&Math.abs(mom)<0.04&&Math.abs(imb)<0.08;
  if(idle) rs.unshift('السوق خامل');
  return {score:Math.round(sc*10)/10,idle,momentum:Math.round(mom*1000)/1000,
    bookImb:Math.round(imb*1000)/1000,vwap,reasons:rs.slice(0,3)};
}
function resolveDirection(now){ const mode=S.config.directionMode;
  if(mode==='long'||mode==='short'){ S.config.direction=mode; return mode; }
  if(S.status==='paused'||S.status==='stopped') return S.config.direction;
  const bias=scoreBias(); S.biasScore=bias.score; S.biasReasons=bias.reasons;
  const mom=S.confluence?S.confluence.momentum:0;
  if(S.position){ const flipped=S._flipUntil&&now<S._flipUntil&&S.config.direction!==S.position.side;
    if(!flipped) S.config.direction=S.position.side;
    return S.config.direction; }
  const prev=S.config.direction;
  if(S.regime&&S.regime.shock) return prev;
  // لا تبديل اتجاه أكثر من مرة كل 60 ثانية — التقليب السريع كان يقطع الخاسر ويطارد الضجيج
  const flipOk=!S._dirFlipAt||now-S._dirFlipAt>60000;
  if(S.regime&&S.regime.trend==='up'&&bias.score>-6&&mom>-0.04&&!toxicDir('long')){
    if(prev!=='long'){ if(!flipOk) return prev; S._dirFlipAt=now; }
    S.config.direction='long'; return 'long'; }
  if(S.regime&&S.regime.trend==='down'&&bias.score<6&&mom<0.04&&!toxicDir('short')){
    if(prev!=='short'){ if(!flipOk) return prev; S._dirFlipAt=now; }
    S.config.direction='short'; return 'short'; }
  if(Math.abs(bias.score)<4) return prev;
  if(bias.direction!==prev&&flipOk&&Math.abs(bias.score)>=14&&!toxicDir(bias.direction)){
    S.config.direction=bias.direction; S._dirFlipAt=now; }
  else if(bias.direction===prev) S.config.direction=bias.direction;
  return S.config.direction;
}
// حجم صفقة الصيد = الدرجة الأولى من سلّم التدرج الحسابي (أصغر أمر)
export function contractsForLevel(p){ const q=levelQtys(p,effLevels()); return q.length?q[0]:1; }
// المستويات الفعّالة: رأس المال الصغير يُركَّز لا يُفتَّت —
// عدد يضمن أن يغطي جني كل مستوى هدف الصافي $0.12 بخطوة شبكة واحدة بعد الرسوم
export function effLevels(){ const c=S.config;
  // العدد = إعداد المالك حرفيًا (جدوله: 12 / 16 / 25 مستوى حسب ضبطه) —
  // التقسيم التدريجي يوزّع 80% من الرصيد عليها كلها بنسب 1:1.5:2...
  // لا تركيز تلقائي ولا أرضية وحدة تفسد النسبة — القسمة طاعة للجدول فقط
  let m=clamp(Math.round(c.levels)||12,1,64);
  const p=S.lastPrice||0, cv=Math.max(1e-12,(S.multiplier||1)*p), lev=Math.max(1,c.leverage);
  const total=Math.max(0,c.cycleBalance)*0.80, denom=mm=>mm+0.25*mm*(mm-1);
  // تقليص وحيد مقبول: أصغر مستوى عجز عن شراء عقد واحد حقيقي
  if(p>0) while(m>1&&Math.floor((total/denom(m))*lev/cv)<1) m--;
  return m; }
// مضاعف خطوة الشبكة المستمد من الذاكرة القوية:
// زوج/اتجاه رابح تاريخيًا → خطوة أضيق (التقاط أكثر) · خاسر → خطوة أوسع (حذر أكبر)
export function memStepMult(){ const lv=sanctionLevel(S.config.direction);
  if(lv>=2) return 1.4; // مقيّد/محظور: خطوة أوسع — حذر أكبر
  const r=memRec(S.config.direction); if(!r||r.n<6) return 1;
  const wr=smWR(r);
  if(wr>=0.6&&r.pnl>0) return 1; // لا تقليص الخطوة تحت المضبوط — التكدس كان يلتقط 6 أمراً بحركة واحدة
  if(lv===1) return 1.25;
  return 1; }
function addStepPct(){ return Math.max(0.12,S.config.gridStepPct)*memStepMult()*(S.regime?S.regime.stepMult:1)*profStepMult(); }
function tooClose(a,b,st){ return a>0&&b>0&&Math.abs(a-b)/Math.max(a,b)*100<st*0.55; }
// أحجام متدرجة بنسبة المالك: 1 : 1.5 : 2 : 2.5 : 3 ... لكل مستوى —
// لكن «الوحدة» تُشتق من رأس المال نفسه، لا تُنسخ من المثال حرفيًا:
// 20% من رصيد الدورة احتياطي هامش لا يُتداول أبدًا، والباقي (80%) يُقسم على
// مجموع النسب، فيخرج أول أمر = وحدة واحدة وكل لاحق يزيد نصف وحدة بالنسبة.
// رصيد 100 → وحدة صغيرة · رصيد 186 → وحدة أكبر · ويتدرج تلقائيًا مع أي مبلغ
export function levelQtys(center,n){ const c=S.config;
  // ─── بوابة المواصفات (درس DOGE: 81 لوت حُسبت بمضاعف PENGU القديم =
  // تضخم 9× = 621$ على محرك رصيده 34$): لا حساب حجم أبداً قبل توثيق
  // مضاعف عقد العملة الحالية نفسها (_metaSym) ───
  if(S._metaSym!==c.symbol||!(S.multiplier>0)) return [];
  const cv=Math.max(1e-12,S.multiplier*center); // قيمة العقد الواحد بالدولار
  // الرافعة ثابتة = ما ضبطه المالك حرفيًا (بأمره: لا تكيّف ولا مرونة) —
  // وتُفرض القيمة نفسها على المنصة عبر changeCrossUserLeverage في liveSync
  const lev=Math.max(1,c.leverage);
  const total=Math.max(0,c.cycleBalance)*0.80; // 20% احتياطي هامش — قاعدة المالك
  const used=S.journal.filter(j=>j.status==='open'&&(j.symbol===undefined||j.symbol===S.config.displaySymbol))
    .reduce((a,j)=>a+j.qty*(S.multiplier||1)*j.entry,0)/lev;
  let budget=Math.max(0,total-used) // هامش حرّ بالدولار
    *(S.regime?S.regime.sizeMult:1)*memSizeMult();
  // الحقيقي يُحكَم بالمتاح الفعلي على المنصة لا برصيد الدورة المخطط —
  // الهامش المحجوز بأوامر قائمة لا يظهر في «used» فيرفض الكميات الزائدة
  // الميزانية الحية تُحكَم برصيد الحساب الكلي (equity) لا بـ«المتاح» اللحظي:
  // المتاح ينهار لحظة حجز الأوامر المركونة فيتقلص السلم ثم يتمدد — حلقة هدم
  // وإعادة بناء ورفض «insufficient available margin» عند كل أمر جديد
  // المتاح الفعلي لدى المنصة أدق من رصيد الحساب الكلي: الهامش المحجوز لا يُنفق منه
  if(c.mode==='live'&&S.exAvail>0) budget=Math.min(budget,Math.max(0,S.exAvail*0.95));
  else if(c.mode==='live'&&S.exEquity>0) budget=Math.min(budget,Math.max(0,S.exEquity*0.95-used));
  // مجموع نسب السلم = m + 0.25·m(m−1) — الوحدة = الميزانية ÷ المجموع
  const denom=mm=>mm+0.08*mm*(mm-1); // تسطيح التدرج (طلب المالك): أوائل المستويات أكبر بكثير — لا فتات
  let m=Math.max(1,n);
  // قلّص العدد فقط إن عجز أصغر مستوى عن شراء عقد واحد — لا أرضية $1 تُفسد النسبة
  while(m>1&&Math.floor((budget/denom(m))*lev/cv)<1) m--;
  const unit=budget/denom(m);
  const out:any[]=[]; let acc=0;
  for(let i=0;i<m;i++){ let q=Math.max(1,Math.floor(unit*(1+0.5*i)*lev/cv));
    if(S.maxOrderQty>0) q=Math.min(q,S.maxOrderQty); // سقف المنصة للأمر الواحد
    if(acc+q*cv/lev>budget&&out.length) break; // لا تتجاوز السيولة أبدًا
    out.push(q); acc+=q*cv/lev; }
  return out; }
function buildGrid(center:any,wide?:any,initialCount?:any){ const c=S.config,tick=S.tickSize||1e-10;
  // خطوة متكيفة مع التقلب اللحظي: الحركات المفاجئة توسّع المناطق تلقائيًا
  const vol=retStdev()||0.05;
  const volF=wide?1:Math.max(1,Math.min(3,vol*10));
  // التباعد المنطقي (أمر المالك 31 أكتوبر): في الوضع الحقيقي الخطوة ×1.8
  // (0.3% → 0.54% بين كل أمر) — كانت 0.3% ملاصقة فيفتح 4 أوامر شبه متطابقة
  const liveWide=(S.config.mode==='live'&&S.keys)?1.8:1;
  const step=c.gridStepPct/100*(wide?4:memStepMult()*(S.regime?S.regime.stepMult:1)*volF)*liveWide;
  const stepPct=wide?Math.max(0.12,c.gridStepPct)*4:addStepPct();
  const n=effLevels(); // تركيز يناسب رأس المال — لا تفتيت على 25 مستوى تافهًا
  const qtys=levelQtys(center,n);
  const filledN=filledAdds(); // عدد الأوامر الفعلية المملوءة (وليس صفوف الدفتر المدمجة) — حجم المستوى التالي يتبع ترتيبه الحقيقي
  const entry=S.position?S.position.entry:center;
  // السلم يمتد للخارج فقط: لا تسليح أبدًا داخل منطقة سبق الدخول فيها —
  // آخر منطقة دخول + خطوة كاملة هو الحد الأدنى للمستوى الجديد (يمنع تراكم الصفقات)
  // آخر منطقة دخول فعلية + خطوة كاملة هو الحد الأدنى للمستوى الجديد —
  // أسعار التنفيذ الحقيقية من الشبكة، لا متوسط الدفتر المدمج الذي يتأخر خلف السعر
  const fills=S.grid.filter(g=>!g.reduceOnly&&g.status==='filled')
    .map(g=>g.filledPrice||g.price).filter(v=>v>0);
  let from=c.direction==='short'?Math.max(center,entry):Math.min(center,entry);
  if(fills.length&&!wide){
    if(c.direction==='short') from=Math.max(from,Math.max(...fills)*(1+step*0.8));
    else from=Math.min(from,Math.min(...fills)*(1-step*0.8)); }
  const grid:any[]=[];
  const liq=S.liqPrice||(S.position&&S.position.liquidation)||0;
  // سلم التشبع المتسارع: يمتد من آخر منطقة دخول حتى حارس التصفية بتوزيع
  // ناعم متسارع (i/n)^1.5 — المستويات القريبة تلتقط الارتدادات السريعة،
  // والبعيدة أكبر حجمًا عند مناطق التشبع الحقيقية. كل المستويات مسلّحة
  // ومواقعها معلومة — الصيغة الهندسية القديمة بسقفها كانت تنهار بعد
  // المستوى الرابع على سعر واحد فيرفضها فلتر التقارب فيختفي السلم
  const spanMin=step*n*0.9;
  let span=Math.max(spanMin,0.02);
  if(liq>0&&!wide){ const room=c.direction==='short'?liq*0.985/from-1:1-liq*1.015/from;
    if(room>step*0.5) span=Math.max(spanMin,room); }
  // سقف الامتداد: السلم للمتوسطات ضمن تصحيح حقيقي، لا للوصول قرب التصفية —
  // liq المنفجر (×4 الدخول لحظة التسليح لأن pend يحسب open فقط) مدّ السلم إلى
  // +260% فوُلدت مستويات زومبي ضخمة ترفضها المنصة (insufficient margin كل دقيقة)
  const reach=Math.max(spanMin,Math.min(step*n*2,0.12));
  if(span>reach) span=reach;
  let prevDist=0;
  for(let i=1;i<=n;i++){
    // الشبكة الواسعة أيضًا لها سقف (5%) — الامتداد المفتوح كان يسلّح مستويات على بعد 30%
    // تباعد ذكي: لا يقرب أي مستوى من سابقه أقل من 0.8×خطوة الشبكة المضبوطة
    // — الصيغة التدريجية كانت تكتظ أول 4 مستويات داخل 0.6% فيلتقطها انزلاق واحد
    let dist=wide?Math.min(step*i,0.05):span*Math.pow(i/n,1.5);
    if(!wide){ const minGap=Math.max(step*(S.config.hfMode?0.3:1),(stepPct/100)*(S.config.hfMode?0.3:1));
      if(i===1) dist=Math.max(dist,minGap);
      else dist=Math.max(dist,prevDist+minGap); }
    prevDist=dist;
    const raw=c.direction==='short'?from*(1+dist):from*(1-dist);
    const price=roundTick(raw,tick); if(!(price>0)) continue;
    // لا تسليح في آخر 1.5% قبل التصفية (متوافق مع حد الخطر) —
    // مستوى لن يعيش المركز ليراه عبء أعمى، لكن بقية المسافة صالحة للتسليح
    if(liq>0){ if(c.direction==='short'&&price>=liq*0.985) continue;
      if(c.direction==='long'&&price<=liq*1.015) continue; }
    if(grid.some(g=>tooClose(g.price,price,stepPct))) continue;
    if(c.direction==='short'&&price<=center*(1+step*0.4)) continue;
    if(c.direction==='long'&&price>=center*(1-step*0.4)) continue;
    // حجم المستوى يتبع ترتيبه الحقيقي في السلم (بعد المناطق المملوءة)
    const qi=Math.min(filledN+i-1,qtys.length-1);
    const qty=qtys.length?qtys[Math.max(0,qi)]:contractsForLevel(center);
    const staged=initialCount!=null&&i>initialCount;
    grid.push({id:uid('lvl'),clientOid:uid('oid'),
      side:c.direction==='short'?'sell':'buy',price,qty,status:staged?'staged':'armed',
      reduceOnly:false,exchangeOrderId:null,filledAt:null,origin:'grid',createdAt:Date.now()});
  }
  return grid.sort((a,b)=>b.price-a.price); }
/* PROGRESSIVE GRID: open with 4 orders; when only 1 armed remains, arm 2 more */
function armProgressive(){ if(S.status!=='running') return;
  // القاطع المرحلي: لا تسليح جديد أثناءه — الدفعة القادمة بعد الدراسة
  if(batchHoldActive()) return;
  if(S._noNewEntries) return; // تجميد الدخول حتى تُغلق المراكز الزائدة بربح
  // سقف التعرّض: لا مزيد من المستويات عند بلوغه — حماية من التصفية بلا وقف خسارة
  if(exposureCapped()){
    if(!S._expCapAt||Date.now()-S._expCapAt>300000){ S._expCapAt=Date.now();
      pushLog('server','🧮 سقف التعرّض (4× رصيد الدورة): السلم مكتمل — لا مزيد من المستويات — الصمود والجني المدروس يتكفلان'); }
    return; }
  // بعد الجني: لا تسليح دخول جديد حتى تكتمل إعادة اختيار العملة — لا تعبئة مبكرة تكسر الدورة
  if(!S.position&&S._harvestAt&&!S._reselectDone&&Date.now()-S._harvestAt<15*60*1000) return;
  // ─── فرملة التقلب (قاعدة المالك): انزلاق/صدمة/موجة معاكسة = تجميد التسليح حتى الاستقرار ───
  if(volBrakeActive()){
    if(!S._volBrakeAt||Date.now()-S._volBrakeAt>120000){ S._volBrakeAt=Date.now();
      const vol=retStdev()||0;
      pushLog('server','🌊 فرملة التقلب: '+(S.regime&&S.regime.shock?'صدمة نظامية':'تقلب '+(vol*100).toFixed(0)+'%')+
        ' — تجميد تسليح دخول جديد حتى يستقر السوق ثم تعود الأوامر'); }
    return; }
  const armed=S.grid.filter(g=>g.status==='armed'||g.status==='open');
  const staged=S.grid.filter(g=>g.status==='staged');
  if(!staged.length) return;
  if(armed.length>1) return;
  let toArm=armed.length===0?3:2;
  const side=S.config.direction==='short'?'sell':'buy';
  for(const g of staged){
    if(g.side!==side){ g.status='cancelled'; continue; } // انقلاب الاتجاه: المتراكم المخالف يُلغى
    if(toArm<=0) continue;
    g.status='armed'; g.createdAt=Date.now();
    toArm--; }
}
function sanitizeAdds(){ const cap=effLevels();
  if(filledAdds()>=cap){ const side=S.position&&S.position.side;
    for(const g of S.grid){ if(g.reduceOnly||g.filledAt) continue;
      if(g.status!=='armed'&&g.status!=='open') continue;
      if(g.lane==='trend'||g.lane==='comp') continue;
      const gs=g.side==='sell'?'short':'long';
      if(!side||gs===side){ g.status='cancelled'; g.exchangeOrderId=null; } }
    return; }
  const st=addStepPct(),px=S.lastPrice||0;
  // كنّاس الزومبي: مستوى أبعد من مدى السلم المسموح (×1.25 هيستيريسيس) لا يمكن
  // أن يُملأ قبل كابح الـ$16 أصلًا — وجوده يحجز هامشًا ويغرق السجل برفض المنصة.
  // يمسح مخلّفات الامتداد المنفجر من المخزن عند أول تشغيل لهذه النسخة
  if(px>0){ const nz=effLevels(), stf=st/100;
    const reach=Math.max(stf*nz*0.9,Math.min(stf*nz*2,0.12))*1.25;
    for(const g of S.grid){ if(g.reduceOnly||g.filledAt||g.lane) continue;
      if(g.status!=='armed'&&g.status!=='open') continue;
      if(Math.abs((g.price||0)-px)/px>reach){ g.status='cancelled';
        if(S.config.mode==='live'&&S.keys&&g.exchangeOrderId){ const id=g.exchangeOrderId; g.exchangeOrderId=null; exCancelOne(id); }
        else g.exchangeOrderId=null; } } }
  for(const side of ['buy','sell']){
    const live=S.grid.filter(g=>!g.reduceOnly&&g.side===side&&(g.status==='armed'||g.status==='open'))
      .sort((a,b)=>Math.abs((b.price||0)-px)-Math.abs((a.price||0)-px));
    const kept:any[]=[];
    for(const g of live){ if(g.lane==='comp'){ kept.push(g); continue; } // التعويض لا يُقلم ولا يُحسب ضد السقف
      const gap=g.lane?Math.max(st,S.config.gridStepPct*4):st;
      if(kept.length>=cap||kept.some(k=>tooClose(k.price,g.price,gap))){
        g.status='cancelled'; g.exchangeOrderId=null; continue; }
      kept.push(g); } } }
function shouldFill(l,p){ if(l.status!=='open'&&l.status!=='armed') return false;
  return l.side==='sell'?p>=l.price:p<=l.price; }
function feeFor(notional,taker){ return notional*(taker?S.takerFee:S.makerFee); }
function lotNet(side,entry,exit,qty,fees){
  return (side==='short'?1:-1)*(entry-exit)*(S.multiplier||1)*qty-fees; }
function accrueFunding(){ const pos=S.position;
  if(!pos||!(pos.size>0)||!S.funding) return 0;
  const now=Date.now(); const last=S._fundAt||now;
  const hours=Math.min(8,(now-last)/3600000);
  if(hours<0.25) return 0;
  const notional=pos.size*(S.multiplier||1)*(pos.entry||S.lastPrice||0);
  const rate=S.funding; // موجب: اللونغ يدفع للشورت؛ سالب: الشورت يدفع للونغ
  const payDir=(pos.side==='long'?1:-1)*(rate>=0?1:-1);
  const signed=notional*Math.abs(rate)*(hours/8)*payDir; // موجب = ندفع، سالب = نتلقى
  S._fundAt=now;
  S.fundingPaid=Math.round(((S.fundingPaid||0)+signed)*10000)/10000;
  return signed; }
function applyDelta(side,qty,price){ const dir=side==='sell'?'short':'long';
  if(!S.position){ S.position={side:dir,size:qty,entry:price,leverage:S.config.leverage,
    unrealized:0,openedAt:Date.now(),liquidation:null};
    S._exc={mae:0,mfe:0}; // انحرافات تُقاس من ولادة المركز
    S._tpBanked=0; S._trailFloor=0; // بنك الجني الجزئي يخص المركز الوليد فقط — لا يُرث من سابق
    return {realized:0,closedQty:0,addedQty:qty}; }
  const pos=S.position;
  const same=(pos.side==='short'&&side==='sell')||(pos.side==='long'&&side==='buy');
  if(same){ const t=pos.size+qty; pos.entry=(pos.entry*pos.size+price*qty)/t; pos.size=t;
    return {realized:0,closedQty:0,addedQty:qty}; }
  const closeQty=Math.min(qty,pos.size);
  const sgn=pos.side==='short'?1:-1;
  const pnl=sgn*(pos.entry-price)*(S.multiplier||1)*closeQty;
  S.realizedPnl+=pnl; pos.size-=closeQty;
  if(pos.size<=1e-9) S.position=null;
  const leftover=qty-closeQty;
  if(leftover>1e-9){ S.position={side:dir,size:leftover,entry:price,
    leverage:S.config.leverage,unrealized:0,openedAt:Date.now(),liquidation:null};
    S._exc={mae:0,mfe:0}; S._tpBanked=0; S._trailFloor=0;
    return {realized:pnl,closedQty:closeQty,addedQty:leftover}; }
  return {realized:pnl,closedQty:closeQty,addedQty:0}; }
function noteEntry(qty:any,price:any,fee:any,source?:any,noCount?:any){ const side=S.position?S.position.side:(source==='hunt'?S.config.direction:'short');
  // لقطة ظروف الدخول لحظتها — تُختم على الصفقة عند إغلاقها في الذاكرة
  S._entryCtx={rg:S.regime?(S.regime.shock?'صدمة':S.regime.trend==='up'?'صاعد':S.regime.trend==='down'?'هابط':'عرضي'):'عرضي',
    ses:sessionOf(Date.now()),origin:source==='hunt'?'صيد':'شبكة',at:Date.now()};
  const label=source==='hunt'?'صفقة':'شبكة';
  const ex=S.journal.find(j=>j.status==='open'&&j.side===side&&j.symbol===S.config.displaySymbol);
  if(ex){ const t=ex.qty+qty; ex.entry=(ex.entry*ex.qty+price*qty)/t; ex.qty=t;
    // الاعتمادات الجزئية المتتالية لنفس المستوى لا ترفع عدد الأوامر —
    // تضخم العدّاد كان يوهم addsBlocked بامتلاء الشبكة فيلغي تسليحها كاملة
    if(!noCount) ex.mergedOrders=(ex.mergedOrders||1)+1; ex.fees+=fee;
    if(S.position) ex.entry=S.position.entry;
    if(ex.source!==label) ex.source='مركز';
    if(S.activeCycle){S.activeCycle.fills++;S.activeCycle.entry=S.position?S.position.entry:ex.entry;}
    return ex.id; }
  const id=uid('jr');
  pushJr({id,source:label,side,qty,entry:S.position?S.position.entry:price,exit:0,pnl:0,
    fees:fee,openedAt:Date.now(),closedAt:0,mergedOrders:1,status:'open'});
  if(!S.activeCycle){ S.activeCycle={side,entry:price,openedAt:Date.now(),fills:1,levels:S.config.levels,pnl:0}; }
  else { S.activeCycle.fills++; if(S.position)S.activeCycle.entry=S.position.entry; }
  return id; }
function noteClose(qty:any,exit:any,fee:any,source?:any,pnl?:any,lotId?:any){ const lotId_=lotId;
  const row=(lotId_?S.journal.find(j=>j.id===lotId_&&j.status==='open'):null)
    ||S.journal.find(j=>j.status==='open'&&(source==='إيقاف'||j.source===source)
      &&j.symbol===S.config.displaySymbol)
    ||S.journal.find(j=>j.status==='open'&&j.symbol===S.config.displaySymbol);
  if(row){ const share=row.qty>0?row.fees*(qty/row.qty):row.fees;
    let net=lotNet(row.side,row.entry,exit,qty,share+fee);
    // ─── درع الإقفال: سباق بيانات الانتقال بين عملتين (سعر/مضاعف العملة
    // السابقة يصل متأخراً فيكتب سياقه فوق الجديدة) ينتج نتائج شاذة مثل
    // -302$ على محرك رصيده 20$. أي نتيجة خارج الحد المادي للمحرك مرفوضة —
    // الصف يُقفل بصفر لا بقيمة وهمية تسمم الرصيد والتاريخ ───
    const __bound=Math.max(50,(S.config.cycleBalance||0)*3);
    if(Number.isFinite(net)&&Math.abs(net)>__bound){
      pushLog('warn','🛡️ درع الإقفال: رفض نتيجة شاذة '+net.toFixed(2)+'$ (خارج الحد المادي '+
        __bound.toFixed(0)+'$) — سياق مختلط من انتقال عملة — الصف يُقفل بصفر');
      net=0; }
    row.status='closed'; row.exit=exit; row.fees+=fee; row.closedAt=Date.now();
    // أرباح الأجزاء المُجناة سابقًا تُضاف لصافي الصفقة المعروض — لا تضيع عند الإقفال
    row.pnl=Math.round((net+(row.partPnl||0))*10000)/10000;
    if(source==='إيقاف') row.source='إيقاف'; pnl=net; }
  else { const __bound2=Math.max(50,(S.config.cycleBalance||0)*3);
    if(Number.isFinite(pnl)&&Math.abs(pnl)>__bound2){
      pushLog('warn','🛡️ درع الإقفال: رفض نتيجة شاذة '+pnl.toFixed(2)+'$ — صُفّرت');
      pnl=0; }
    pushJr({id:uid('jr'),source,side:S.position?S.position.side:S.config.direction,
    qty,entry:exit,exit,pnl,fees:fee,openedAt:Date.now(),closedAt:Date.now(),
    mergedOrders:1,status:'closed'}); }
  if(S.activeCycle){ S.activeCycle.pnl+=pnl;
    if(!S.position){ S.activeCycle=null; } }
  // الربح المجني يُضاف فورًا لرصيد الدورة — التراكم يكبّر أحجام الصفقات التالية تلقائيًا
  if(Number.isFinite(pnl)&&pnl!==0){
    // درع الرصيد: نتيجة أكبر من أي شيء مادي لهذا المحرك (3× الرصيد) لا تدخل
    // الرصيد — حماية من أي مسار خلل مستقبلي يسمم دورة كاملة برقم وهمي
    const __bound=Math.max(50,(S.config.cycleBalance||0)*3);
    if(Math.abs(pnl)>__bound){
      pushLog('warn','🛡️ درع الرصيد: تجاهل نتيجة شاذة '+pnl.toFixed(2)+'$ (خارج الحد المادي '+__bound.toFixed(0)+'$) — لم تلمس رصيد الدورة'); }
    else {
      S.cycleHarvested=Math.round(((S.cycleHarvested||0)+pnl)*10000)/10000;
      S.config.cycleBalance=Math.max(1,Math.round((S.config.cycleBalance+pnl)*100)/100); } }
  // تعلّم من نتيجة الصفقة — يُخزَّن في الذاكرة القوية التي لا تُمسح مع الدورات
  { const mem=S.memory; if(mem&&mem.pairs){ const pk=S.config.symbol;
      const pr=mem.pairs[pk]||(mem.pairs[pk]={n:0,w:0,l:0,pnl:0,grossWin:0,grossLoss:0,maxLoseStreak:0,loseStreak:0});
      pr.n++; pr.pnl=Math.round(((pr.pnl||0)+pnl)*10000)/10000;
      if(pnl>=0){ pr.w++; pr.grossWin=Math.round(((pr.grossWin||0)+pnl)*10000)/10000; pr.loseStreak=0; }
      else { pr.grossLoss=Math.round(((pr.grossLoss||0)-pnl)*10000)/10000;
        pr.loseStreak=(pr.loseStreak||0)+1; pr.maxLoseStreak=Math.max(pr.maxLoseStreak||0,pr.loseStreak); }
      saveAll(); } }
  return pnl; }
// إقفال جزئي لكمية من صفقة مفتوحة (جني مجزّأ على المنصة) —
// يُبقي سجل الصفقة مفتوحًا بالكمية المتبقية ويراكم صافي الجزء في partPnl،
// ويضيف الربح فورًا لرصيد الدورة كأي جني — لا ينتظر الإقفال الكامل
function notePartialClose(qty,exit,fee,lotId){
  const row=(lotId?S.journal.find(j=>j.id===lotId&&j.status==='open'):null)
    ||S.journal.find(j=>j.status==='open'&&(j.symbol===undefined||j.symbol===S.config.displaySymbol));
  if(!row||!(qty>0)) return 0;
  const share=row.qty>0?row.fees*Math.min(1,qty/row.qty):0;
  const net=lotNet(row.side,row.entry,exit,qty,share+fee);
  row.qty=Math.max(0,row.qty-qty); row.fees=Math.max(0,row.fees-share);
  row.partPnl=Math.round(((row.partPnl||0)+net)*10000)/10000;
  if(Number.isFinite(net)&&net!==0){
    S.cycleHarvested=Math.round(((S.cycleHarvested||0)+net)*10000)/10000;
    S.config.cycleBalance=Math.max(1,Math.round((S.config.cycleBalance+net)*100)/100); }
  if(S.activeCycle){ S.activeCycle.pnl+=net; if(!S.position) S.activeCycle=null; }
  if(row.qty<=1e-9){ row.status='closed'; row.exit=exit; row.closedAt=Date.now();
    row.pnl=row.partPnl; }
  return net; }
// تعويض الكمية المُجناة جزئيًا بمنطقة دخول جديدة عند متوسط المركز —
// إن عاد السعر لمنطقة الخسارة أعاد شراء ما بِيع بنفس الكمية (قاعدة المالك)
function armCompEntry(qty){ const pos=S.position; if(!pos||!(qty>0)) return;
  if(!spacingGate(pos.entry)) return; // تعويض قريب جدًا من آخر تنفيذ = تكدس بلا فائدة
  const side=pos.side==='short'?'sell':'buy';
  const px=roundTick(pos.entry,S.tickSize||1e-10); if(!(px>0)) return;
  const q=Math.max(1,Math.round(qty));
  const tol=Math.max((S.tickSize||1e-10)*2, px*1e-7);
  // لا تكديس: مستوى تعويض قائم قرب النقطة نفسها يكفي
  if(S.grid.some(g=>!g.reduceOnly&&(g.status==='armed'||g.status==='open')&&
    g.side===side&&Math.abs(g.price-px)<=tol)) return;
  S.grid.push({id:uid('cmp'),clientOid:uid('oid'),side,price:px,qty:q,
    status:'armed',reduceOnly:false,exchangeOrderId:null,filledAt:null,
    origin:'comp',lane:'comp',createdAt:Date.now()});
  S.grid.sort((a,b)=>b.price-a.price);
  pushLog('server','تعويض: أُسلّح دخول '+side+' @ '+fmtPx(px)+' بكمية '+q+
    ' — إن عاد السعر للمتوسط أعاد شراء المُجناة'); }
// فترة اليوم (UTC): آسيا 0-8 · أوروبا 8-16 · أمريكا 16-24
function sessionOf(ts:any):any { const h=new Date(ts).getUTCHours(); return h<8?'آسيا':h<16?'أوروبا':'أمريكا'; }
// التعلّم من نتائج الصفقات معطّل نهائيًا بطلب المالك — لا كتابة ولا قيود

// قياس الانزلاق معطّل مع التعلم — لا كتابة في الذاكرة

// إحصاءات مجمعة من الذاكرة القوية — للوحة الأداء والتقارير
export function memStats(){ const mem=S.memory; const o={n:0,w:0,l:0,pnl:0,grossWin:0,grossLoss:0,maxLoseStreak:0,pf:0,winRate:0,avgWin:0,avgLoss:0};
  if(!mem||!mem.pairs) return o;
  for(const k of Object.keys(mem.pairs)){ const r=mem.pairs[k];
    o.n+=r.n||0; o.w+=r.w||0; o.l+=r.l||0; o.pnl+=r.pnl||0;
    o.maxLoseStreak=Math.max(o.maxLoseStreak,r.maxLoseStreak||0);
    if(r.w&&r.pnl>0){ /* التقريب على مستوى الزوج */ } }
  // الإجماليات الدقيقة تحتاج تفصيل كل صفقة — نقدّرها من متوسطات الأزواج
  o.winRate=o.n?o.w/o.n:0; o.pnl=Math.round(o.pnl*100)/100;
  let gw=0,gl=0,aw=0,al=0;
  for(const k of Object.keys(mem.pairs)){ const r=mem.pairs[k];
    if(r.pnl>0){ gw+=r.pnl; aw+=r.w?r.pnl/r.w:0; } else { gl+=-r.pnl; al+=r.l?-r.pnl/r.l:0; } }
  o.grossWin=Math.round(gw*100)/100; o.grossLoss=Math.round(gl*100)/100;
  o.pf=gl>0?Math.round(gw/gl*100)/100:(gw>0?99:0);
  o.avgWin=Math.round(aw*100)/100; o.avgLoss=Math.round(al*100)/100;
  return o; }
// حجم الصفقة التكيفي (Kelly مبسّط) — سلسلة نجاح تكبّر الحجم وسلسلة خسارة تصغّره
function memSizeMult(){ let m=1;
  const r=memRec(S.config.direction);
  if(r){ const st=r.streak||0;
    if(st>=3) m=1.25; else if(st<=-3) m=0.5; else if(st<=-2) m=0.7; }
  if(sanctionLevel(S.config.direction)>=2) m=Math.min(m,0.5); // مقيّد = نصف حجم
  if(S._probation) m=Math.min(m,0.5); // صفقة إعادة التأهيل = نصف حجم
  return m; }
// انحراف الحيتان: سعر يتحرك عكس تدفق الصفقات الكبيرة = انعكاس محتمل
// يرجع 1 (انحراف صعودي) / -1 (هبوطي) / 0 (لا انحراف)
function cvdDivergence(){ const t=S.cvdTrail; if(!t||t.length<15) return 0;
  let mn=Infinity,mx=-Infinity; for(const p of t){ if(p.cvd<mn)mn=p.cvd; if(p.cvd>mx)mx=p.cvd; }
  const range=mx-mn; if(!(range>0)) return 0;
  const a=t[Math.max(0,t.length-25)], b=t[t.length-1];
  const flow=(b.cvd-a.cvd)/range;
  const mom=S.confluence?S.confluence.momentum:0;
  if(mom>0.05&&flow<-0.4) return -1;
  if(mom<-0.05&&flow>0.4) return 1;
  return 0; }
// تقرير أداء دوري بإشعار — كل 6 ساعات أثناء العمل
function maybeReport(){ const now=Date.now();
  if(S.status!=='running'&&S.status!=='paused') return;
  if(S._lastReportAt&&now-S._lastReportAt<6*3600*1000) return;
  const st=memStats(); if(!st.n) return;
  S._lastReportAt=now;
  const msg='صفقات متعلَّمة: '+st.n+' · نجاح '+(st.winRate*100).toFixed(0)+'% · عامل الربح '+st.pf+' · صافي الذاكرة '+fmtUsd(st.pnl);
  pushLog('info','تقرير دوري — '+msg); nativeNotify('AB-KING — تقرير الأداء',msg); }
// ——— أمر الجني الموحّد الوحيد (نظام جديد كليًا — حُذفت tpTargetPrice/placeTpOpposite) ———
// يُوضع فور فتح الصفقة ويُعاد حسابه مع كل إضافة: متوسط الدخول الفعلي + رسوم
// الدخول المدفوعة فعلًا من السجل + رسوم الخروج taker المقدّرة + صافي TP_BASE_NET
// ($0.12) — تنفيذه يحجز $0.12 صافية للمركز كاملًا مهما بلغ عدد أوامره (1 أو 25).
/* جني تتبعي تصاعدي سريع: الهدف يبدأ من ثبابي البوت (مثال 0.03)؛ كلما وصل الربح العائم إلى
   الضعين من هدفين للبوت (مثال: هدف 0.03 → عند 0.06 يرتفع الهدف إلى 0.04؛ عند 0.09 → 0.05)
   — يُحدّث كل نبضة (ثانية) فيرفع التبع مع الربح العائم ولا ينخط أبدًا */
function trailTarget(base,floating){
  if(!(floating>0)) return base;
  let t=base;
  // مثال المالك: base 0.03 — عند 0.06 → 0.04؛ عند 0.09 → 0.05؛ عند 0.12 → 0.06
  // الصيغة: كلما بلغ الربح 2×الهدف يُرفع الهدف بالنصف
  while(floating>=t*2){ t=t+Math.max(0.01,t*0.25); }
  return t; }
function syncPositionTp(){ if(S.status!=='running'||!S.position) return;
  const pos=S.position, mult=S.multiplier||1;
  if(!(pos.size>0)) return;
  const entryFees=S.journal.filter(j=>j.status==='open').reduce((a,j)=>a+(j.fees||0),0)
    ||pos.size*mult*pos.entry*(S.makerFee||0.0002);
  const exitFee=pos.size*mult*pos.entry*(S.takerFee||0.0006);
  const notional=Math.max(1e-9,pos.size*mult*pos.entry);
  // ما جُني جزئيًا يُخصم من الهدف: المتبقي يكمل الصافي الكلي للمجموع لا هدفًا جديدًا
  // التبع التصاعدي: الهدف يتبع الربح العائم فوق الأساس
  const base=tpBase();
  const floatNet=(pos.unrealized||0)
    -S.journal.filter(j=>j.status==='open').reduce((a,j)=>a+(j.fees||0),0)
    -pos.size*mult*(pos.entry||0)*(S.takerFee||0.0006);
  S._trailFloor=Math.max(S._trailFloor||0,trailTarget(base,floatNet));
  const need=Math.max(S._trailFloor,tpBase())-(S._tpBanked||0);
  if(need<0){ /* الهدف مجني بالك\u0627\u0645\u0644 */ }
  const needPos=Math.max(0,need);
  const cov=(entryFees+exitFee+needPos)/notional; // النسبة فوق المتوسط التي تضمن الصافي
  let want=roundTick(pos.side==='long'?pos.entry*(1+cov):pos.entry*(1-cov),S.tickSize);
  if(!(want>0)) return;
  /* ─── مراجعة شاملة (قاعدة المالك): الجني يهدف صافياً مدروساً واسعاً ───
     الهدف = نسبة من حجم الصفقة تتوسع مع التقلب المقاس (لا تافه ولا ضيقة).
     جدار الارتداد القريب يرفع الجني نحوه فقط (قبيل انكساره — ربح أوسع)
     ولا ينزله تحت الهدف أبداً — البيع القريب قبل الارتداد شأن «الجني
     المبكر» بشهادة حيتان/زخم، لا شأن أمر الجني الموقف. تحديث مستمر كل 4 ثوان
     للأمر فقط عند تحرك ملموس — لا قصف أوامر */
  const nowT=Date.now();
  if(!S._zoneAt||nowT-S._zoneAt>4000){
    S._zoneAt=nowT;
    const z=studiedTpZone(pos,S.lastPrice||0);
    if(z&&z.zone>0){
      const cap=want+(pos.side==='long'?1:-1)*Math.abs(want-pos.entry)*0.25; // سقف: ربع المسافة — جني أسرع
      S._zone=pos.side==='long'?Math.min(z.zone,cap):Math.max(z.zone,cap); }
    else S._zone=0; }
  if(S._zone>0){ const z=roundTick(S._zone,S.tickSize);
    // رفع فقط — الجني لا يُنزل تحت هدفه الصافي المدروس أبداً
    if(pos.side==='long'?z>want:z<want){
      if(Math.abs(z-want)>Math.max(S.tickSize*2,want*0.0004)) want=z; } }
  const tickTol=S.tickSize>0?S.tickSize*0.5:Math.abs(want)*1e-9;
  const liveTp=S.grid.filter(g=>g.reduceOnly&&(g.status==='armed'||g.status==='open'));
  // كمية الأمر الحيّة = أصله ناقص ما نُفّذ منه جزئيًا على المنصة
  const remQty=g=>Math.max(0,g.qty-(g.exFilled||0));
  const match=liveTp.find(g=>{
    // بعد جني جزئي: نفس النقطة محفوظة للمتبقي حرفيًا (قاعدة المالك) ما لم تتغير الكمية بإضافة
    if((S._tpBanked||0)>0&&Math.abs(remQty(g)-pos.size)<1e-6) return true;
    return Math.abs(g.price-want)<=tickTol&&Math.abs(remQty(g)-pos.size)<1e-6; });
  // لا هدم وإعادة بناء كل نبضة: الأمر المطابق يبقى، ويُلغى غيره فقط —
  // الهدم الدائم كان يراكم آلاف الأوامر الملغاة ويرسل إلغاءات للمنصة بلا توقف
  if(match){ for(const g of liveTp){ if(g!==match){ g.status='cancelled'; g.exchangeOrderId=null; } }
    return; }
  for(const g of liveTp){ g.status='cancelled'; g.exchangeOrderId=null; }
  S.grid.push({id:uid('tp'),clientOid:uid('oid'),side:pos.side==='short'?'buy':'sell',
    price:want,qty:pos.size,status:'armed',reduceOnly:true,exchangeOrderId:null,
    filledAt:null,origin:'grid',createdAt:Date.now(),
    lotId:S.journal.find(j=>j.status==='open')?.id});
  S.grid.sort((a,b)=>b.price-a.price); }
function coveringLoser(l,p){ const pos=S.position; if(!pos||l.reduceOnly) return false;
  const cover=(pos.side==='short'&&l.side==='buy')||(pos.side==='long'&&l.side==='sell');
  return cover&&adversePct(p)>0.08; }
// تنظيف ما بعد اكتمال الجني: إلغاء كل الأوامر (منصة وبوت) وتصفير بنك الجني
// الجزئي ومراسي الشبكة — دورة جديدة تبدأ نظيفة بلا أوامر يتيمة
function _harvestCleanup(fp){
  // طلب تأكيد الصفرية من المنصة بعد كل تنقية — الجني لا يُقبل إلا بتنفيذ منصي
  S._needsFlatCheck=Date.now();
  if(S.config.mode==='live'&&S.keys){
    exCancelAll(S.config.symbol).catch(()=>{});
    exCancelStops(S.config.symbol).catch(()=>{}); }
  S._harvestAt=Date.now(); // لحظة اكتمال الدورة — القفز للعملة التالية مسموح بعدها فقط
  S._reselectDone=false; // إعادة الاختيار الإجباري بعد كل جني (قاعدة المالك)
  S._posPeakNet=null; // ذروة جديدة لكل دورة — قفل الذروة لا يتسرب بين الصفقات
  S._batchFills=0; S._batchHoldAt=0; S._batchStudiedAt=false; // القاطع المرحلي: دفعة جديدة لكل دورة
  S._cycleMaxFills=S._nextCycleMax||4; S._nextCycleMax=0; // بعد انزلاق: التالية 3 فقط
  S._batchWorst=null; S._batchWorseAt=0; S._batchHoldLogAt=0;
  S._guardId=null; S._tpBanked=0; S._trailFloor=0;
  for(const g of S.grid){ if(g.status==='armed'||g.status==='open'||g.status==='staged'){
    g.status='cancelled'; g.exchangeOrderId=null; } }
  S.gridAnchor=null; if(fp>0) S.huntAnchor=fp; }
// اعتماد تنفيذ جزئي/كامل ورد من المنصة — الحقيقة عند المنصة لا عند تقاطع
// السعر المحلي. دخول: يُضاف للمركز والسجل فورًا. جني: يُقفل جزئيًا ويُبنك صافيه،
// فإن بلغ البنك الهدف بِيع المتبقي بسعر ربحي فورًا، وإلا بقي عند نفس النقطة
// وعُوّض المُباع بمنطقة دخول جديدة (قواعد المالك حرفيًا)
function creditExFill(l,delta,fp){ return creditExFillReal(l,delta,fp,null); }
function creditExFillReal(l,delta,fp,realFee){ if(!(delta>0)||!(fp>0)) return false;
  l.exFilled=(l.exFilled||0)+delta;
  const mult=S.multiplier||1;
  if(!l.reduceOnly){
    const estFee=feeFor(delta*mult*fp,false);
    if(realFee!=null&&S._realFeesAt&&Math.abs(realFee-estFee)>1e-9){
      S.feesPaid=Math.max(0,S.feesPaid-estFee); S.feesPaid+=realFee; }
    else S.feesPaid+=estFee;
    const fee=realFee!=null&&realFee>0?realFee:estFee;
    const dd=applyDelta(l.side,delta,fp);
    if(dd.addedQty>0){ const lotId=noteEntry(dd.addedQty,fp,fee,l.origin==='hunt'?'hunt':'grid',
      !!l._counted); l._counted=true; l.lotId=l.lotId||lotId; }
    S.lastAddAt=Date.now();
    if(l.qty-(l.exFilled||0)>1e-9) pushLog('server','تنفيذ جزئي: دُخل '+delta+' من '+l.qty+
      ' @ '+fmtPx(fp)+' — الباقي معلّق يُكمل أو يُلغى عند الجني');
    if(S.position&&S.status==='running'){ syncPositionTp();
      backgroundPipeline().catch(()=>{}); }
    return false; }
  const estFee2=feeFor(delta*mult*fp,false);
  if(realFee!=null&&S._realFeesAt&&Math.abs(realFee-estFee2)>1e-9){
    S.feesPaid=Math.max(0,S.feesPaid-estFee2); S.feesPaid+=realFee; }
  else S.feesPaid+=estFee2;
  const fee=realFee!=null&&realFee>0?realFee:estFee2;
  const dd=applyDelta(l.side,delta,fp);
  const net=notePartialClose(delta,fp,fee,l.lotId);
  S._tpBanked=Math.round(((S._tpBanked||0)+net)*10000)/10000;
  if(!S.position){ // هذا الجزء أتمّ المركز — جني مكتمل
    l.status='filled'; l.filledAt=Date.now(); l.filledPrice=fp;
    const tb=S._tpBanked; _harvestCleanup(fp);
    pushLog('server','جني مكتمل ✓ — صافي الدورة '+fmtUsd(tb)+' — أُلغيت الأوامر وتُدرس صفقة جديدة');
    if(S.status==='running') maybeSwitchSymbol().catch(()=>{});
    return true; }
  const rem=Math.max(0,l.qty-(l.exFilled||0));
  if(S._tpBanked>=tpBase()-1e-9&&rem>0){
    // الهدف تحقق من الجزء المُجناة → بِع المتبقي بسعر ربحي فورًا (السعر تجاوز
    // نقطة الجني = ربح مؤكد — لا بيع بخسارة أبدًا) وألغِ باقي الأمر المعلّق
    l.status='filled'; l.filledAt=Date.now(); l.filledPrice=fp;
    if(l.exchangeOrderId) exCancelOne(l.exchangeOrderId);
    const side=S.position.side, q=Math.min(rem,S.position.size);
    if(q>0){ const xp=S.lastPrice||fp;
      const xfee=feeFor(q*mult*xp,true); S.feesPaid+=xfee;
      applyDelta(l.side,q,xp);
      noteClose(q,xp,xfee,'شبكة',0,l.lotId);
      exCloseQty(S.config.symbol,side,q)
        .catch(e=>pushLog('error','إغلاق المتبقي الربحي فشل: '+(e.message||e))); }
    if(!S.position){ const tb=S._tpBanked; _harvestCleanup(fp);
      pushLog('server','جني مكتمل ✓ — بنك '+fmtUsd(tb)+' والمتبقي بِيع بسعر ربحي');
      if(S.status==='running') maybeSwitchSymbol().catch(()=>{});
      return true; }
    return false; }
  if(rem>0){ // الهدف لم يكتمل: المتبقي محفوظ عند نفس النقطة + تعويض المُجناة بدخول جديد
    armCompEntry(delta);
    pushLog('server','جني جزئي '+fmtUsd(net)+' — المتبقي '+rem+' محفوظ عند نفس النقطة '+
      fmtPx(l.price)+' وعُوّض المُجناة بمنطقة دخول عند المتوسط');
    if(S.status==='running') syncPositionTp(); }
  return false; }
function fillLevel(id,fp,taker){ const l=S.grid.find(g=>g.id===id);
  if(!l||l.status==='filled') return;
  // ─── البوابة الحديدية (قاعدة المالك، أمر 2026-10-25): أثناء توقف القاطع
  // لا تنفَّذ أي تعبئة دخول مهما كان مسارها (شبكة/صيد/تعويض جني جزئي —
  // ثقوب 8× و9× كلها من هنا). الجني reduceOnly وحده يعمل ───
  if(!l.reduceOnly&&(batchHoldActive()||S._noNewEntries)){
    l.status='cancelled'; l.exchangeOrderId=null; return; }
  if(!l.reduceOnly&&coveringLoser(l,fp)) return;
  l.status='filled'; l.filledAt=Date.now(); l.filledPrice=fp;
  // كمية الجني الفعّالة = المتبقي فعلًا في المركز — تنفيذ جزئي سابق على المنصة
  // يجعل pos.size أصغر من l.qty، وبدون هذا القصّ يُفتح مركز عكسي بالفرق (كارثة)
  const effQty=l.reduceOnly&&S.position?Math.min(l.qty,S.position.size):l.qty;
  const fee=feeFor(effQty*(S.multiplier||1)*fp,taker); S.feesPaid+=fee;
  const d=applyDelta(l.side,effQty,fp);
  if(d.addedQty>0){
    // ─── القاطع المرحلي (درس PEPE -136$): عدّ دفعة الأوامر — كل 4 أوامر
    // دخول متتالية يوقف الفتح كلياً حتى يستقر السوق وتُعاد الدراسة الجيدة
    if(!l.reduceOnly){ S._batchFills=(S._batchFills||0)+1;
      if(S._batchFills>=(S._cycleMaxFills||4)&&!S._batchHoldAt){
        S._batchHoldAt=Date.now(); S._batchStudiedAt=false;
        cancelPendingAdds();
        pushLog('server','🧷 القاطع المرحلي: اكتملت 4 أوامر دخول — توقف الفتح حتى الاستقرار + دراسة جديدة ثم 4 أخرى'); } }
    const lotId=noteEntry(d.addedQty,fp,fee,l.origin==='hunt'?'hunt':'grid');
    l.lotId=l.lotId||lotId;
    // قياس انزلاق التنفيذ: فرق سعر التنفيذ الفعلي عن سعر المستوى المطلوب
    if(l.price>0){ const slipUsd=Math.abs(fp-l.price)*(S.multiplier||1)*effQty;
    if(slipUsd>0){ S.slipCost=(S.slipCost||0)+slipUsd;
      if(slipUsd>1&&(!S._slipLogAt||Date.now()-S._slipLogAt>60000)){ S._slipLogAt=Date.now();
        pushLog('warn','انزلاق تنفيذ '+slipUsd.toFixed(2)+'$ عند '+fmtPx(fp)+' (المطلوب '+fmtPx(l.price)+')'); } } } }
  if(d.closedQty>0){ const src=l.origin==='hunt'?'صفقة':'شبكة';
    const row=l.lotId?S.journal.find(j=>j.id===l.lotId):null;
    const net=row?lotNet(row.side,row.entry,fp,row.qty,row.fees+fee)
               :d.realized-fee;
    noteClose(d.closedQty,fp,fee,src,net,l.lotId);
    if(l.origin==='hunt'&&l.reduceOnly) S.huntOpen=Math.max(0,(S.huntOpen||0)-1);
    const parent=S.grid.find(g=>!g.reduceOnly&&g.status==='filled'&&
      (l.lotId?g.lotId===l.lotId:g.origin===l.origin));
    if(parent) parent.status='cancelled';
    S.huntAnchor=fp; S.lastHuntAt=Date.now()-400;
    // ——— الجني الحقيقي على المنصة ———
    // الإغلاق المحلي وحده لا يحرّك المركز الفعلي — أغلق الكمية حقيقةً وألغِ أمر الجني المحدد
    if(l.reduceOnly&&S.config.mode==='live'&&S.keys){
      const row2=l.lotId?S.journal.find(j=>j.id===l.lotId):null;
      const cSide=row2?row2.side:(S.position?S.position.side:S.config.direction);
      if(l.exchangeOrderId) exCancelOne(l.exchangeOrderId);
      // الكمية الحقيقية هي الحاكمة: تنفيذ جزئي سابق على المنصة قد يكون قلّص
      // المركز — إغلاق بكمية محلية أكبر من المركز يُرفض أو يرتد عكسيًا
      (async()=>{ let q=d.closedQty;
        try{ const lp=await exPosition(S.config.symbol); q=lp?Math.min(q,lp.size):0; }catch(_){ logSwallow("silent-catch", _); }
        if(q>0) await exCloseQty(S.config.symbol,cSide,q)
          .catch(e=>pushLog('error','إغلاق الجني الحقيقي فشل: '+(e.message||e))); })();
      // اكتمل المركز بالكامل؟ — ألغِ كل الأوامر المعلقة (منصة وبوت) وابدأ دراسة دخول جديدة نظيفة
      if(!S.position){ _harvestCleanup(fp);
        pushLog('server','جني مكتمل ✓ — أُغلقت الصفقة حقيقيًا وأُلغيت أوامرها، تُدرس صفقة جديدة');
        if(S.status==='running') maybeSwitchSymbol().catch(()=>{}); } } }
  if(S.position&&S.status==='running') syncPositionTp();
  S.lastWorkAt=Date.now();
  notify((l.side==='sell'?'بيع ':'شراء ')+(l.origin==='hunt'?'صفقة':l.reduceOnly?'جني':'شبكة')+' @ '+fmtPx(fp),
    (l.reduceOnly&&d.closedQty>0)?'win':null); }
function selectDueAdds(due,p){ if(addsBlocked()) return [];
  if(studying()) return [];
  if(toxicBlocked(S.config.direction)) return [];
  // تعبئة المستوى عند بلوغ السعر موقعه مقدسة — فيتو الزخم/الاتجاه/الحيتان
  // القديم كان يمنع التعبئة فيقتل فكرة السلم كاملة: المتوسط لا ينكسر أبدًا
  // والمركز ينزف نحو الكابح. الحارس الوحيد المتبقي هنا: لا اصطياد سكينٍ
  // في لحظة طبعة عنيفة + دفتر سليم — والحماية الحقيقية: كابح الـ$16
  // وحارس التصفية وميزانية الأحجام المتدرجة
  if(S.position&&(lastJumpPct()>0.4||!bookQuality())) return [];
  const adds=due.filter(l=>!l.reduceOnly&&!coveringLoser(l,p))
    .sort((a,b)=>Math.abs(a.price-p)-Math.abs(b.price-p));
  const now=Date.now();
  return adds.filter(()=>!(S.startedAt&&now-S.startedAt<SCOUT_MS&&filledAdds()>=1))
    .filter(()=>!(S.lastAddAt&&now-S.lastAddAt<ADD_COOLDOWN)).slice(0,1); }
function inAddZone(p){ if(S.status!=='running'||!p) return false;
  if(addsBlocked()) return false;
  if(lastJumpPct()>0.4) return false;
  if(!bookQuality()) return false;
  const pos=S.position; if(!pos||!pos.entry) return true;
  const step=Math.max(0.12,S.config.gridStepPct*memStepMult());
  const improve=pos.side==='short'?(p-pos.entry)/pos.entry*100:(pos.entry-p)/pos.entry*100;
  if(improve<step*0.7) return false;
  const mom=S.confluence?S.confluence.momentum:0;
  const r=S.regime||{trend:'range'};
  const against=(pos.side==='long'&&r.trend==='down')||(pos.side==='short'&&r.trend==='up');
  // صدمة معاكسة (اندفاع قوي ضد المركز): لا متوسطات أبدًا حتى يتوقف الاندفاع — حماية الرصيد أولًا
  if(r.shock&&against) return false;
  if(against&&((pos.side==='long'&&mom<-0.06)||(pos.side==='short'&&mom>0.06))) return false;
  if(pos.side==='long'&&mom<-0.12) return false;
  if(pos.side==='short'&&mom>0.12) return false;
  // فلتر الحيتان/التدفق: لا تزيد المركز عكس تدفق السيولة الكبيرة
  const f=tapeFlow();
  if(pos.side==='short'&&(f.whale>0.6||f.bias>0.45)) return false;
  if(pos.side==='long'&&(f.whale<-0.6||f.bias<-0.45)) return false;
  return true; }
// ——— جني الربح: أمر واحد ثابت للمركز كاملًا (نظام جديد كليًا) ———
// حُذفت كل دوال الجني القديمة (تتبع/قفل/فتيلات/معادلات متفرعة) نهائيًا بأمر
// المالك. النظام الوحيد الآن: أمر جني واحد يُوضع فور فتح الصفقة ويُعاد حسابه
// مع كل إضافة (متوسط + رسوم فعلية) ليضمن صافي TP_BASE_NET ($0.12) بعد كل
// الرسوم للمجموع كاملًا — أمر واحد أو 25 أمرًا بنفس الربح الواحد. هذه الدالة
// تنفّذ أمر الجني عند بلوغ السعر خطّه فقط.
function harvestRipe(p){ if(!p||S.status==='idle') return;
  if(S._gapCtx&&S.position&&gapExitCheck(p)){
    pushLog('server','\ud83c\udfaf \u0635\u0627\u0626\u062f \u0627\u0644\u0641\u062c\u0648\u0627\u062a: \u0639\u0627\u062f\u0629 \u0627\u0644\u0625\u0646\u062f\u064a\u0643\u0633 \u062a\u0645\u062a \u2014 \u062c\u0646\u064a \u0627\u0644\u0645\u0631\u0643\u0632 \u0643\u0627\u0645\u0644\u0627');
    flattenAt(p,'\u0639\u0648\u062f\u0629 \u0625\u0646\u062f\u064a\u0643\u0633').catch(()=>{});
    S._gapCtx=null;
    return;
  }
  // ─── الكارثة الكبرى (أمر 30 أكتوبر): كان أمر الجني «يُملّأ محلياً» عند
  // ملامسة السعر حتى في الوضع الحقيقي — فيُعلن جني تمّ والمنصة لم تنفذ شيئاً
  // وتتراكم مراكز حقيقية (8 صفقات!). الآن: التعبئة المحلية للجني في الورقي فقط.
  // الحقيقي: تنفيذ أمر الجني يأتي حصراً من مزامنة صفقات المنصة (exFills) ───
  if(S.config.mode!=='live'||!S.keys){
    for(const tp of S.grid.filter(g=>g.reduceOnly&&(g.status==='open'||g.status==='armed'))){
      const tol=Math.min(S.tickSize>0?S.tickSize*0.5:1e-12, Math.abs(tp.price)*0.0005);
      const crossed=tp.side==='sell'?p>=tp.price-tol:p<=tp.price+tol;
      if(crossed) fillLevel(tp.id,p,true); } }

  // ——— درع الانزلاق العكسي ———
  // حركة حادة ضد المركز خلال ثوانٍ: ألغِ تسليح المستويات فورًا — إضافة في شلال = متوسط كارثي
  const pv=S._harvPrev;
  if(S.position&&pv&&pv.px>0){ const dt=Date.now()-pv.at;
    if(dt>0&&dt<6000){ const adv=(S.position.side==='short'?(p-pv.px):(pv.px-p))/pv.px*100;
      if(adv>=0.35){ cancelPendingAdds(); S._waveHoldUntil=Date.now()+60000;
        if(!S._advSpkAt||Date.now()-S._advSpkAt>60000){ S._advSpkAt=Date.now();
          pushLog('server','⚡ انزلاق عكسي '+adv.toFixed(2)+'% خلال '+Math.round(dt/1000)+'ث — سُحبت المستويات من المنصة 60ث حتى يهدأ السعر'); } } } }
  // ——— درع الموجة المتدرجة ———
  // موجة ثابتة ≥0.8% ضد المركز خلال 60ث لا تراها قفزة النبضة الواحدة: هكذا
  // كُنست 9 مستويات دفعة واحدة في الحقيقي لأنها مركونة على المنصة. الرد:
  // سحب فوري من المنصة (لا تعطيل محلي فقط) + تجميد إعادة التسليح 60ث تتمدد
  // ما دامت الموجة مستمرة — «الاتباعد وقت الخطر» بشكله الصحيح للحقيقي
  if(S.position&&S.status==='running'){ const wv=waveAdversePct();
    if(wv>=0.8){ cancelPendingAdds(); S._waveHoldUntil=Date.now()+60000;
      if(!S._wavePullAt||Date.now()-S._wavePullAt>60000){ S._wavePullAt=Date.now();
        pushLog('server','🌊 موجة معاكسة '+wv.toFixed(2)+'% خلال 60ث — سُحبت المستويات من المنصة وتجميد التسليح حتى تستقر'); } } }
  S._harvPrev={px:p,at:Date.now()}; }
function trailHuntAnchor(p){ if(!p||S.status!=='running') return;
  if(!S.huntAnchor){S.huntAnchor=p;return;}
  if(S.config.direction==='short'){ if(p>S.huntAnchor)S.huntAnchor=p; }
  else if(p<S.huntAnchor) S.huntAnchor=p; }
function huntAligned(){ const conf=S.confluence||{momentum:0,idle:false,score:0};
  const short=S.config.direction==='short';
  if(!bookQuality()) return false;
  const r=S.regime||{trend:'range',shock:false};
  const withTrend=!S.position&&((short&&r.trend==='down'&&conf.momentum<=-0.06)||
    (!short&&r.trend==='up'&&conf.momentum>=0.06));
  if(r.shock&&!withTrend) return false;
  if(short&&r.trend==='up') return false;
  if(!short&&r.trend==='down') return false;
  if(conf.idle&&S.tape.length>=8) return false;
  if(short&&conf.momentum>0.06) return false;
  if(!short&&conf.momentum<-0.06) return false;
  if(short&&S.biasScore>=10) return false;
  // ═══ TRG-NEW: دراسة اتجاه أعمق — الدخول مع الاتجاه فقط ═══
  // كل المؤشرات يجب أن تتفق مع اتجاه الصفقة (لا صفقات عكس الترند):
  // متوسطات + دفتر أوامر + زخم. تتفق → دخول؛ أي تعارض → لا صفقة الآن
  if(!S.position){
    const ef=S.emaFast, es=S.emaSlow;
    if(ef&&es){
      const emaUp=ef>es;
      if(short&&emaUp) return false;       // بيع والمتوسطات صاعدة = عكس الاتجاه
      if(!short&&!emaUp) return false;     // شراء والمتوسطات هابطة = عكس الاتجاه
    }
    if(short&&S.biasScore>0) return false;  // بيع ودفتر المشترين أقوى
    if(!short&&S.biasScore<0) return false; // شراء ودفتر البائعين أقوى
  }
  if(!short&&S.biasScore<=-10) return false;
  if(short&&conf.score>=12) return false;
  if(!short&&conf.score<=-12) return false;
  if(S.position&&S.position.side===S.config.direction){
    const against=(S.position.side==='short'&&S.biasScore>=12)||
      (S.position.side==='long'&&S.biasScore<=-12);
    if(against) return false; }
  // فلتر الحيتان/التدفق: لا تدخل صفقة جديدة عكس تدفق السيولة الكبيرة
  const f=tapeFlow();
  if(f.whaleVol>0.25){ if(short&&f.whale>0.5) return false; if(!short&&f.whale<-0.5) return false; }
  if(short&&f.bias>0.35) return false; if(!short&&f.bias<-0.35) return false;
  // فلتر التمويل: تمويل مرتفع ضد اتجاهك يأكل الربح بصمت — لا دخول
  const fund=S.funding||0;
  if(!short&&fund>0.0004) return false;   // لونغ يدفع تمويلًا مرتفعًا
  if(short&&fund<-0.0004) return false;   // شورت يدفع تمويلًا مرتفعًا
  // انحراف الحيتان: سعر يتحرك عكس تدفقهم = انعكاس وشيك — لا تدخل ضده
  const dv=cvdDivergence();
  if(short&&dv===1) return false;   // تدفق شرائي قوي تحت سعر هابط
  if(!short&&dv===-1) return false; // تدفق بيعي قوي فوق سعر صاعد
  // بوابة الذاكرة القوية (بتجانس بايزي): اتجاه خاسر بعينة حقيقية يُردع —
  // الحظر الكامل للسام فقط في toxicBlocked، هنا تشدد الظرف لا إعدامه
  const mp=memRec(short?'short':'long');
  if(mp&&mp.n>=15&&mp.pnl<0&&smWR(mp)<0.30){
    if(!S._memBlockAt||Date.now()-S._memBlockAt>600000){ S._memBlockAt=Date.now();
      pushLog('info','الذاكرة: اتجاه '+(short?'الشورت':'اللونغ')+' على هذا الزوج خاسر بعينة كافية — ممنوع مؤقتًا'); }
    return false; }
  // بوابة التوقيت والظرف: فترة اليوم أو نوع السوق خاسر تاريخيًا لهذا الزوج/الاتجاه
  if(mp){
    const so=mp.sessions&&mp.sessions[sessionOf(Date.now())];
    if(so&&so.n>=8&&so.pnl<0&&smWR(so)<0.32) return false;
    const rg=S.regime?(S.regime.shock?'صدمة':S.regime.trend==='up'?'صاعد':S.regime.trend==='down'?'هابط':'عرضي'):'عرضي';
    const ro=mp.regimes&&mp.regimes[rg];
    if(ro&&ro.n>=8&&ro.pnl<0&&smWR(ro)<0.32) return false; }
  // تأكيد النشاط: معدل صفقات آخر دقيقة أدنى من نصف وسيطه التاريخي = سوق ميت — ارفض
  const am=S._actMed||0;
  if(am>=10){ const cut=Date.now()-60000, rate=(S._tradeTs||[]).filter(t=>t>=cut).length;
    if(rate<am*0.5) return false; }
  // فلتر قرب الجدار + تطبيع OFI على وسيط أحجام مستويات الدفتر
  const ob=S.orderBook,lp=S.lastPrice||0;
  if(ob&&ob.bids.length&&ob.asks.length&&lp>0){ const medL=medLevelSz(ob);
    if(medL>0){
      // جدار ضخم (5× الوسيط) على مسافة <0.15% بوجه اتجاه الصيد = مصيدة — ارفض
      if(!short){ for(const a of ob.asks){ if(a.price>lp&&(a.price-lp)/lp*100<0.15&&a.size>medL*5) return false; } }
      else { for(const b of ob.bids){ if(b.price<lp&&(lp-b.price)/lp*100<0.15&&b.size>medL*5) return false; } }
      // بوابة تدفق الأوامر (OFI): ضغط أوامر حدّية قوي عكس اتجاه الصيد = لا تدخل
      const nofi=(S._ofi||0)/(medL*20);
      if(short&&nofi>0.6) return false; if(!short&&nofi<-0.6) return false; } }
  // قائد BTC: نبضة قوية في المؤشر عكس اتجاه الصيد على العملات التابعة = ارفض
  if(S._btcImp!=null&&Math.abs(S._btcImp)>=0.10){
    if(short&&S._btcImp>0) return false; if(!short&&S._btcImp<0) return false; }
  return true; }
/* ================================================================
   فلسفة الصيد الجديدة: الإشارات أبواب مستقلة — أي باب يفتح = صفقة.
   لا «لابد أن تنطبق جميعها». المنع فقط بفيتو خطر قوي مثبت.
   ================================================================ */
// فيتو الخطر: أسباب قوية وحدها تمنع فتح صفقة — ما دونها لا يوقف الصيد
function dangerVeto(_p:any){ const short=S.config.direction==='short';
  // دفتر أقدم من 8 ثوانٍ لا يُوثق به لرفض دخول — منع الإنذار الكاذب الأعمى
  if(Date.now()-(streamState.bookAt||0)>8000) return null;
  if(!bookQuality()) return 'دفتر أو سبريد رديء';
  if(toxicBlocked(S.config.direction)) return 'اتجاه سام في الذاكرة (يُعاد اختباره كل ساعة)';
  const r=S.regime||{trend:'range',shock:false}, conf=S.confluence||{momentum:0};
  // صدمة سعرية تقود عكس اتجاه الصيد بقوة
  if(r.shock){ if(short&&conf.momentum>0.3) return 'صدمة صاعدة عنيفة ضد الشورت';
    if(!short&&conf.momentum<-0.3) return 'صدمة هابطة عنيفة ضد اللونغ'; }
  // جدار لاصق مباشرة (<0.07%) بحجم 8 أضعاف الوسيط — مصيدة محققة لا مجرد مقاومة.
  // (كان 0.10%/6× يخنق الصيد على العملات الصفريّة كثيفة الدفاتر — جدار عادي
  //  يُقرأ خطرًا دائمًا فلا صفقات؛ الجدار الحقيقي اللاصق أقرب وأضخم)
  const ob=S.orderBook, lp=S.lastPrice||0;
  if(ob&&ob.bids.length&&ob.asks.length&&lp>0){ const medL=medLevelSz(ob);
    if(medL>0){ if(!short){ for(const a of ob.asks.slice(0,5)){ if(a.price>lp&&(a.price-lp)/lp*100<0.07&&a.size>medL*8) return 'جدار بيع لاصق فوق السعر'; } }
      else { for(const b of ob.bids.slice(0,5)){ if(b.price<lp&&(lp-b.price)/lp*100<0.07&&b.size>medL*8) return 'جدار شراء لاصق تحت السعر'; } } } }
  // سيل حيتان هائل عكس الاتجاه (تدفق مهيمن + حجم استثنائي)
  const f=tapeFlow();
  if(f.whaleVol>0.4){ if(short&&f.whale>0.7) return 'سيل شراء حيتان مهيمن'; if(!short&&f.whale<-0.7) return 'سيل بيع حيتان مهيمن'; }
  // تمويل متطرف يأكل الصفقة
  const fund=S.funding||0;
  if(!short&&fund>0.001) return 'تمويل مرتفع جدًا ضد اللونغ';
  if(short&&fund<-0.001) return 'تمويل مرتفع جدًا ضد الشورت';
  return null; }
// كاشف الإشارات: الحيتان/الفجوات/الارتداد/الزخم/الضغط — أي واحدة تكفي لفتح صفقة
function huntSignals(p:any){ const short=S.config.direction==='short'; const sig:any[]=[];
  const conf=S.confluence||{momentum:0,score:0}, r=S.regime||{trend:'range'};
  // 1) المرساة الكلاسيكية: تحرك ≥ عتبة الصيد من القمة/القاع
  if(S.huntAnchor){ const pct=(p-S.huntAnchor)/S.huntAnchor*100, need=effectiveHuntPct();
    if(short?pct<=-need:pct>=need) sig.push('مرساة '+Math.abs(pct).toFixed(2)+'%'); }
  // 2) فجوة السعر عن المارك — صيد ارتداد
  const mk=S.markPrice||0;
  if(mk>0){ const dev=(p-mk)/mk;
    if(Math.abs(dev)>=0.0015&&((dev>0&&short)||(dev<0&&!short))) sig.push('فجوة مارك'); }
  // 3) فخ الاختراق (صيد السلاحف)
  if(S._brk&&Date.now()-S._brk.at<10000){
    if((S._brk.side==='up'&&p<S._brk.level&&short)||(S._brk.side==='down'&&p>S._brk.level&&!short)){
      sig.push('فخ اختراق'); S._brk=null; } }
  // 4) زخم أو اتجاه مساند
  if(short?(conf.momentum<=-0.05):(conf.momentum>=0.05)) sig.push('زخم');
  if((short&&r.trend==='down')||(!short&&r.trend==='up')) sig.push('اتجاه');
  // 5) حيتان مع الاتجاه
  const f=tapeFlow();
  if(f.whaleVol>0.2&&((short&&f.whale<-0.3)||(!short&&f.whale>0.3))) sig.push('حيتان');
  // 6) انحراف سيولة لصالحنا
  const dv=cvdDivergence();
  if((short&&dv===-1)||(!short&&dv===1)) sig.push('انحراف سيولة');
  // 7) ضغط أوامر حدّية (OFI) لصالحنا
  const ob=S.orderBook; if(ob&&ob.bids.length){ const medL=medLevelSz(ob)||1, n=(S._ofi||0)/(medL*20);
    if((short&&n<-0.4)||(!short&&n>0.4)) sig.push('ضغط أوامر'); }
  // 8) قائد BTC يساند الاتجاه
  if(S._btcImp!=null&&Math.abs(S._btcImp)>=0.10&&((short&&S._btcImp<0)||(!short&&S._btcImp>0))) sig.push('قائد BTC');
  return sig; }
function effectiveHuntPct(){ const hf=S.config.hfMode;
  const base=Math.max(hf?0.05:0.14,S.config.huntPct*(hf?0.4:1),
    (profOf()&&profOf().spread?profOf().spread*(hf?1.5:2.5):0)); // أرضية سبريد: لا صيد بعائد يأكله الفرق السعري
  const mag=Math.abs(S.biasScore);
  const stalled=S.lastWorkAt&&Date.now()-S.lastWorkAt>60000;
  // سقف 1.8 لحاصل المضاعفات — لا تراكب صدمة×ذاكرة×تقلب يرفع العتبة فوق المتناول
  const mult=Math.min(1.8,(S.regime?S.regime.huntMult:1)*memHuntMult()*profStepMult());
  let scaled=base*mult;
  // اتجاه قوي نظيف متوافق مع اتجاه البوت — سهّل الاقتناص لاستغلال الموجة بدل تفويتها
  const rg=S.regime;
  if(rg){ const sh=S.config.direction==='short';
    const aligned=(sh&&rg.trend==='down')||(!sh&&rg.trend==='up');
    if(aligned&&!rg.shock) scaled*=0.8; }
  // قائد BTC يساند اتجاه الصيد على العملات التابعة — سهّل الاقتناص
  if(S._btcImp!=null&&Math.abs(S._btcImp)>=0.10){ const sh2=S.config.direction==='short';
    if((sh2&&S._btcImp<0)||(!sh2&&S._btcImp>0)) scaled*=0.85; }
  // إعادة دخول ذكية: ربح حديث على نفس الاتجاه والموجة مستمرة — اركب الموجة التالية أسرع
  if(S._lastWinClose&&Date.now()-S._lastWinClose.at<180000&&S._lastWinClose.side===S.config.direction) scaled*=0.7;
  if(S.position&&S.position.side!==S.config.direction) return Math.max(0.07,scaled*0.4);
  if(stalled&&huntAligned()) return Math.max(0.10,scaled*0.5);
  if(mag>=28&&huntAligned()) return Math.max(0.12,scaled*0.7);
  return scaled; }
function huntTooClose(p){ 
  // حد أدنى صارم بين أي دخولين: لا تكدّس الأوامر في منطقة واحدة — انزلاق واحد لا يجب أن يلتقط السلم كله
  const need=Math.max(MIN_HUNT_GAP*1.5,addStepPct(),effectiveHuntPct()*0.9,0.12);
  // فحص كل دخول صيد فعلي على حدة — متوسط الدفتر المدمج كان يتأخر خلف السعر
  // فتمر دخولات متلاصقة (0.02%!) بدعوى بُعدها عن المتوسط
  for(const g of S.grid){ if(g.origin!=='hunt'||g.reduceOnly||g.status!=='filled') continue;
    const fp=g.filledPrice||g.price;
    if(fp>0&&Math.abs(p-fp)/fp*100<need) return true; }
  return false; }
function openHuntLots(){ // عدّ تنفيذات الصيد الفعلية — الصف المدمج واحد مهما ضمّ أوامر
  return S.grid.filter(g=>g.origin==='hunt'&&!g.reduceOnly&&g.status==='filled').length; }
// إحماء إلزامي: لا قرار دخول قبل 15 ثانية ووصول دفتر أوامر حقيقي وشريط صفقات —
// الدفتر الفارغ كان يجتاز فحص الجودة تلقائيًا فيدخل البوت بعد 3 ثوانٍ من التشغيل أعمى
function warmedUp(){ if(!S.startedAt||Date.now()-S.startedAt<15000) return false;
  const b=S.orderBook||{}; if((b.bids||[]).length<5||(b.asks||[]).length<5) return false;
  if((S.tape||[]).length<12) return false;
  return true; }
// قراءة سجل مع نسيان تدريجي: عمر نصف 10 أيام — ذنب قديم يبهت أثره ولا يطارد للأبد
// ⚠️ ذاكرة التعلم معطّلة بطلب المالك: كانت قيودها المتراكمة تخنق الصفقات —
// تعيد null دائمًا فتسقط كل البوابات (حجم/خطوة/صيد/حظر) إلى وضعها الحر الطبيعي
function memRec(_dir:any):any { return null; }
// معدل نجاح مُصحّح بايزيًا — العينة الصغيرة تُشدّ نحو الحياد بدل حكم متسرع
function smWR(r:any){ return (r.w+1)/(r.n+2); }
// سُلّم العقوبات معطّل مع ذاكرة التعلم — لا قيود متراكمة من الماضي، القرار من السوق الحي فقط
function sanctionLevel(_dir:any){ return 0; }
function toxicDir(dir){ return sanctionLevel(dir)>=2; }
function toxicBlocked(dir){ const lv=sanctionLevel(dir);
  S._probation=false;
  if(lv<3) return false;
  // إعادة تأهيل: كل ساعة صفقة استكشافية بنصف حجم — الحظر بلا إعادة اختبار عمى دائم
  const r=S.memory.pairs[S.config.symbol+':'+dir];
  if(!r.probAt||Date.now()-r.probAt>3600*1000){ r.probAt=Date.now(); S._probation=true;
    pushLog('server','إعادة تأهيل — صفقة استكشافية بنصف حجم تختبر '+(dir==='short'?'الشورت':'اللونغ')+' المحظور');
    saveAll(); return false; }
  if(!S._toxicLogAt||Date.now()-S._toxicLogAt>300000){ S._toxicLogAt=Date.now();
    pushLog('server','اتجاه سام في الذاكرة — '+(dir==='short'?'الشورت':'اللونغ')+' محظور (يُعاد اختباره كل 3 ساعات)'); }
  return true; }

/* ================================================================
   بصمة العملة — كل عملة تُدرس على حدة وتُكيَّف عليها المعاملات:
   تقلبها المقاس يضبط خطوة الشبكة وعتبة الصيد، وتعلّمها يخصها وحدها
   ================================================================ */
function profOf(){ const mem=S.memory; return mem&&mem.profiles?mem.profiles[S.config.symbol]:null; }
function profFresh(){ const p=profOf(); return p&&Date.now()-p.at<24*3600*1000?p:null; }
function startStudy(short){ S.studyUntil=Date.now()+(short?12000:28000);
  S.studyAcc={vols:[],sps:[],act:0,whales:0}; }
function studying(){ return !!(S.studyUntil&&Date.now()<S.studyUntil); }
function studyTick(){ if(!S.studyUntil) return;
  if(Date.now()>=S.studyUntil){
    // بعد كابح الكارثة: لا عودة لسوق متذبذب بعنف — مدّد الانتظار 60ث حتى يستقر
    // (صدمة نظامية أو تقلب شريط حاد أو سيولة حيتان مضطربة) — بلا سقف زمني:
    // ما دام التذبذب غير واضح الاتجاه وخطيرًا يبقى الانتظار ويُعاد الفحص كل 60ث
    if(S._postDisaster){
      const vol=retStdev()||0, shocked=!!(S.regime&&S.regime.shock), whales=tapeFlow().whaleVol||0;
      if(shocked||vol>0.20||whales>0.6){ S.studyUntil=Date.now()+60000;
        if(!S._waitLogAt||Date.now()-S._waitLogAt>60000){ S._waitLogAt=Date.now();
          pushLog('server','⏳ السوق متذبذب بعنف (تقلب/سيولة) — إطالة الانتظار 60ث حتى يستقر قبل أي دخول'); }
        return; } }
    S._postDisaster=0; finishStudy(); return; }
  const a=S.studyAcc; if(!a) return;
  const v=retStdev(); if(v>0) a.vols.push(v);
  const b=S.orderBook||{};
  if((b.bids||[]).length&&(b.asks||[]).length){ const bb=b.bids[0].price,aa=b.asks[0].price,mid=(bb+aa)/2;
    if(mid>0&&aa>bb) a.sps.push((aa-bb)/mid*100); }
  a.act=(S.tape||[]).length;
  if(tapeFlow().whaleVol>0.28) a.whales++; }
function finishStudy(){ S.studyUntil=0; const a=S.studyAcc; S.studyAcc=null; if(!a) return;
  const vols=a.vols.slice().sort((x,y)=>x-y);
  const vol=vols.length?vols[Math.floor(vols.length/2)]:0.05;
  const sp=a.sps.length?a.sps.reduce((x,y)=>x+y,0)/a.sps.length:0.02;
  const mem=S.memory=S.memory||{pairs:{},cycles:0,totalPnl:0};
  mem.profiles=mem.profiles||{};
  mem.profiles[S.config.symbol]={at:Date.now(),vol:Math.round(vol*10000)/10000,
    spread:Math.round(sp*10000)/10000,act:a.act,whales:a.whales};
  saveAll();
  pushLog('server','اكتملت دراسة '+S.config.displaySymbol+' — تقلب '+vol.toFixed(3)+'% · سبريد '+
    sp.toFixed(3)+'% · نشاط شريط '+a.act+' — كُيّفت الخطوة والصيد على خصائصها'); }
// تكييف الخطوة مع تقلب العملة المقاس: المتقلبة خطوة أوسع، الهادئة أضيق — بحدود آمنة
function profStepMult(){ const p=profOf(); if(!p||!p.vol) return 1;
  return clamp(p.vol/0.05,0.7,2.2); }
// التعلم يحسّن الدخول ولا يقيّده فقط: اتجاه ناجح لهذه العملة = اقتناص أسهل · خاسر = تشدد
function memHuntMult(){ const lv=sanctionLevel(S.config.direction);
  if(lv>=2) return 1.15; // تشديد طفيف لا خنق — العقوبة القصوى (المنع) للسام مستوى 3 فقط
  const r=memRec(S.config.direction); if(!r||r.n<6) return 1;
  const wr=smWR(r);
  if(wr>=0.6&&r.pnl>0) return 0.85;
  if(lv===1) return 1.1;
  return 1; }
function packHunt(p:any){
  if(!(S.multiplier>0)){ pushLog('warn','بانتظار مواصفات العقد — لا دخول بحجم غير معروف'); return null; }
  return {side:S.config.direction==='short'?'sell':'buy',qty:contractsForLevel(p)}; }
function huntTrigger(p){ if(S.status!=='running'||!p) return null;
  // بوابة المواصفات: لا صيد قبل توثيق مضاعف هذه العملة — سباق الانتقال يضخم الحجم
  if(S._metaSym!==S.config.symbol||!(S.multiplier>0)) return null;
  if(!warmedUp()) return null;
  // ─── فيتو الاندفاع (قاعدة المالك): حركة قوية جارية ضد الاتجاه = لا دخول حتى تستقر ───
  const kick=priceKick();
  const againstPump=(S.config.direction==='short'&&kick.up>0.8)||(S.config.direction==='long'&&kick.down>0.8);
  if(againstPump){
    if(!S._kickVetoAt||Date.now()-S._kickVetoAt>120000){ S._kickVetoAt=Date.now();
      pushLog('server','⛔ فيتو اندفاع: تحرك '+(S.config.direction==='short'?'صاعد':'هابط')+' قوي '+
        (S.config.direction==='short'?kick.up:kick.down).toFixed(2)+'% خلال دقيقة — لا دخول ضده حتى يستقر'); }
    return null; }
  // بعد الجني: إعادة اختيار إلزامية قبل أي دخول — لا إعادة دخول فورية على نفس العملة
  if(!S.position&&S._harvestAt&&!S._reselectDone&&Date.now()-S._harvestAt<15*60*1000) return null;
  if(studying()) return null; // العملة قيد الدراسة — لا دخول قبل اكتمال بصمتها
  // هدنة الخسارة المخففة: خسارتان متتاليتان بنفس الاتجاه خلال دقيقتين فقط — لا شلل 5 دقائق لخسارة واحدة
  if(!S.position&&S._lossStreak&&S._lossStreak.side===S.config.direction&&
    S._lossStreak.n>=2&&Date.now()-S._lossStreak.at<120000) return null;
  const flipping=!!(S.position&&S.config.direction!==S.position.side);
  if(addsBlocked()&&!flipping) return null;
  if(flipping){ if(S.lastHuntAt&&Date.now()-S.lastHuntAt<120000) return null;
    // لا انقلاب فوري بعد دخول حديث — انتظر تأكيدًا (عمر المركز أو ضرر حقيقي)
    const ageOk=S.position&&Date.now()-(S.position.openedAt||0)>120000;
    if(!ageOk&&adversePct(p)<0.3) return null;
    if(toxicBlocked(S.config.direction)) return null;
    return packHunt(p); }
  if(toxicBlocked(S.config.direction)) return null;
  // القاطع المرحلي: لا إضافات صيد أثناءه
  if(batchHoldActive()) return null;
  if(S._noNewEntries) return null; // تجميد كامل: مراكز منصة زائدة لم تُغلق بربح بعد
  // سقف التعرّض: لا إضافات جديدة عنده — الحجم الكلي محكوم رياضياً
  if(S.position&&exposureCapped()) return null;
  // ─── إصلاح الكارثة: لا إضافات صيد مادامت فرملة التقلب نشطة — لا تغذية للانهيار ───
  if(S.position&&volBrakeActive()){
    if(!S._brkVetoAt||Date.now()-S._brkVetoAt>180000){ S._brkVetoAt=Date.now();
      pushLog('server','🔒 لا إضافات أثناء فرملة التقلب — السلم قائم بلا تغذية حتى يستقر'); }
    return null; }
  if(S.position){ if(!inAddZone(p)) return null; }
  if(S.startedAt&&Date.now()-S.startedAt<SCOUT_MS&&filledAdds()>=1) return null;
  if(S.lastAddAt&&Date.now()-S.lastAddAt<ADD_COOLDOWN) return null;
  trailHuntAnchor(p);
  if(S.lastHuntAt&&Date.now()-S.lastHuntAt<(S.config.hfMode?3000:HUNT_COOLDOWN)) return null;
  // سقف صفقات الصيد يتكيف: سوق هادئ متذبذب = 4، عادي = 3، تذبذب عالي/صدمة = 2 فقط
  const volNow=retStdev()||0.05;
  const capHunt=S.regime&&S.regime.shock?2:(volNow>1.2?2:(volNow<0.35?4:MAX_HUNT_OPEN));
  if(openHuntLots()>=capHunt) return null;
  if(huntTooClose(p)) return null;
  if(S.position){ const pk=packHunt(p);
    if(!pk) return null;
    const cover=(S.position.side==='short'&&pk.side==='buy')||
      (S.position.side==='long'&&pk.side==='sell');
    return cover?null:pk; }
  // فلسفة الصيد: لا دخول إلا بإشارة، ولا منع إلا بفيتو خطر قوي مثبت
  const veto=dangerVeto(p);
  if(veto){ if(!S._vetoAt||Date.now()-S._vetoAt>120000){ S._vetoAt=Date.now();
      pushLog('info','فيتو خطر منع صفقة: '+veto); } return null; }
  const sig=huntSignals(p); if(!sig.length) return null;
  // فلسفة المالك الأصلية (عودة): الباب القوي وحده كافٍ — والفيتوهات تحمي
  if(sig.length<2){
    // فلسفة المالك الأصلية: الإشارات أبواب مستقلة — أي باب يفتح = صفقة.
    // الحماية في الفيتوهات (خطر/اندفاع/سموم) لا في خنق الباب
    const need=effectiveHuntPct();
    const markGap=sig.some(x=>x.indexOf('فجوة مارك')===0);
    const decentRetrace=sig.some(x=>x.indexOf('مرساة')===0&&parseFloat(x.replace(/[^0-9.]/g,''))>=need*0.8);
    const strongSingle=markGap||decentRetrace||(sig.length===1&&(
      sig[0].indexOf('زخم')===0||sig[0].indexOf('اتجاه')===0||
      sig[0].indexOf('حيتان')===0||sig[0].indexOf('انحراف')===0||
      sig[0].indexOf('فخ اختراق')===0||sig[0].indexOf('ضغط أوامر')===0));
    if(!strongSingle) return null; }
  S._lastSig=sig.slice(0,3).join(' · ');
  return packHunt(p); }
function maybeHunt(p){ const hit=huntTrigger(p); if(!hit) return false;
  // ─── لا إغلاق بخسارة من الصيد المعاكس (كان يتجاوز حاجز flattenAt ويقفل
  // بـ-0.02 و-0.22): صيد يخالف اتجاه المركز والمركز خاسر → يتخطى ويصمد ───
  if(S.position&&hit.side!==S.position.side){
    const __mult=S.multiplier||1, __g=(S.position.side==='long'?p-S.position.entry:S.position.entry-p)*__mult*(S.position.size||0);
    if(__g<0.01){ if(!S._noLossHuntAt||Date.now()-S._noLossHuntAt>180000){ S._noLossHuntAt=Date.now();
        pushLog('server','🛡️ رفض صيد معاكس يغلق بخسارة ('+fmtUsd(__g)+') — الصمود حتى الجني'); }
      return false; } }
  // ─── نافذة الانزلاق المستدام (ساعتان): تتبع قمم/قيعان النافذة ───
  { const W=2*3600000, now=Date.now();
    if(p>0){
      if(!S._slideHi||now-(S._slideHiAt||0)>W){ S._slideHi=p; S._slideHiAt=now; }
      else if(p>S._slideHi){ S._slideHi=p; S._slideHiAt=now; }
      if(!S._slideLo||now-(S._slideLoAt||0)>W){ S._slideLo=p; S._slideLoAt=now; }
      else if(p<S._slideLo){ S._slideLo=p; S._slideLoAt=now; } } }
  // ─── فيتو مناطق الدخول (قاعدة المالك 2026-10-25): لا شراء في هبوط مستدام
  // ولا بيع في صعود مستدام — لا العملة نفسها (تحت قمة ساعتينها بـ2%+) ولا
  // سلة الميمز كلها إذا كان قائدها BTC تحت قمته بساعتين بـ1.5%+ ───
  { const W=2*3600000, now=Date.now(), long=hit.side!=='short', px=S.lastPrice||p;
    let veto='', why='';
    if(long&&S._slideHiAt&&now-S._slideHiAt<W&&px>0&&px<S._slideHi*0.98)
      { veto='coin'; why='العملة تحت قمة الساعتين بـ2%+ — هبوط مستدام'; }
    else if(!long&&S._slideLoAt&&now-S._slideLoAt<W&&px>0&&px>S._slideLo*1.02)
      { veto='coin'; why='العملة فوق قاع الساعتين بـ2%+ — صعود مستدام'; }
    else if(long&&S._btcHiAt&&now-S._btcHiAt<W&&S._btcPx>0&&S._btcPx<S._btcHi*0.985)
      { veto='btc'; why='BTC تحت قمته بساعتين بـ1.5%+ — سلة الميمز تنزل معاً'; }
    else if(!long&&S._btcLoAt&&now-S._btcLoAt<W&&S._btcPx>0&&S._btcPx>S._btcLo*1.015)
      { veto='btc'; why='BTC فوق قاعه بساعتين بـ1.5%+ — السلة تصعد معاً'; }
    if(veto){ if(Date.now()-(S._slideVetoAt||0)>300000){ S._slideVetoAt=Date.now();
        pushLog('server','⛔ فيتو منطقة الدخول ('+(veto==='btc'?'السوق':'العملة')+'): '+why+' — لا دخول الآن'); }
      return false; } }
  if(!spacingGate(p)){ if(Date.now()-(S._gateLogAt||0)>120000){ S._gateLogAt=Date.now();
    pushLog('server','قفل التباعد: تُخّر دخول الصيد — للسعة قريبة جدًا من آخر تنفيذ'); } return false; }
  S.lastHuntAt=Date.now(); S.huntAnchor=p;
  const fee=feeFor(hit.qty*(S.multiplier||1)*p,true); S.feesPaid+=fee;
  const d=applyDelta(hit.side,hit.qty,p);
  // التنفيذ الحقيقي ملك دورة المحرك وحدها (ترسل الأمر قبل الاستدعاء) —
  // إرسال ثانٍ هنا كان يضاعف حجم كل صفقة صيد على المنصة
  if(d.addedQty>0){ // القاطع المرحلي: تعبئات الصيد تُحسب في دفعة الـ4 مثل الشبكة
    S._batchFills=(S._batchFills||0)+1;
    if(S._batchFills>=(S._cycleMaxFills||4)&&!S._batchHoldAt){
      S._batchHoldAt=Date.now(); S._batchStudiedAt=false; S._batchWorst=null; S._batchWorseAt=0;
      S._nextCycleMax=3; // بعد الانزلاق: الدورة القادمة 3 أوامر متباعدة فقط (قاعدة المالك)
      cancelPendingAdds();
      pushLog('server','🧷 القاطع المرحلي (صيد): اكتملت 4 أوامر دخول — توقف الفتح حتى يتوقف الانزلاق + دراسة ثم 4 أخرى'); } }
  if(d.closedQty>0){ const of=S.journal.find(j=>j.status==='open')?.fees||0;
    noteClose(d.closedQty,p,fee,'صفقة',d.realized-of-fee); S.huntOpen=Math.max(0,(S.huntOpen||0)-1); }
  if(d.addedQty>0){ S.huntCount++; S.huntOpen=(S.huntOpen||0)+1;
    const lotId=noteEntry(d.addedQty,p,fee,'hunt');
    S.grid.push({id:uid('hunt'),clientOid:uid('oid'),side:hit.side,price:p,qty:d.addedQty,
      status:'filled',reduceOnly:false,exchangeOrderId:null,filledAt:Date.now(),
      origin:'hunt',createdAt:Date.now(),lotId});
    notify('صفقة فردية #'+S.huntCount+' — '+(hit.side==='sell'?'بيع':'شراء')+' @ '+fmtPx(p)+
      (S._lastSig?' — '+S._lastSig:''));
    S._lastSig=null; }
  if(S.position&&S.status==='running') syncPositionTp();
  S.lastWorkAt=Date.now(); if(d.addedQty>0) S.lastAddAt=Date.now();
  return true; }
function computeLiq(){ const dir=S.position?S.position.side:S.config.direction;
  const pend=S.grid.filter(g=>!g.reduceOnly&&g.status==='open'&&
    (dir==='short'?g.side==='sell':g.side==='buy'));
  const posQ=S.position?S.position.size:0, pendQ=pend.reduce((a,g)=>a+g.qty,0);
  const q=posQ+pendQ; if(q<=0) return null;
  const pendPx=pend.reduce((a,g)=>a+g.price*g.qty,0);
  const entry=posQ>0?(S.position.entry*posQ+pendPx)/q:(pendQ>0?pendPx/pendQ:0);
  if(!entry) return null;
  const mult=S.multiplier||1, lev=Math.max(1,S.position?S.position.leverage:S.config.leverage);
  const mmr=lev>=75?0.025:lev>=50?0.012:lev>=25?0.008:lev>=10?0.005:0.004;
  const liqFee=Math.max(S.takerFee,0.0005);
  const notional=entry*q*mult;
  // هامش CROSS الحقيقي = هامش المركز + الرصيد المتاح كله يدعمه (قاعدة المالك:
  // متبادل دائمًا). الحساب المعزول القديم (notional/lev فقط) كان يقرّب سعر
  // التصفية وهميًا فيُطلق «خطر تصفية» كاذب ويُغلق المركز بخسارة لا وجود لها
  const curNotional=(S.position?S.position.size*mult*(S.position.entry||entry):0)||notional;
  // الدعم الحقيقي لمركز CROSS = رصيد الحساب الكلي (equity) لا «المتاح» اللحظي —
  // الأوامر المركونة تحجز المتاح فيُقرأ ≈0 ويخرج سعر تصفية وهمي ملاصق للسعر
  // فيُطلق «قرب التصفية» كاذب أقفل صفقات المالك بخسارة. equity ثابت لا يتأثر بالحجز
  const eq=Math.max(0,S.exEquity||0);
  const backing=S.config.mode==='live'
    ? Math.max(curNotional/lev+Math.max(0,(S.exAvail||0)*0.95), eq*0.95)
    : Math.max(0,S.config.cycleBalance||0);
  const margin=backing+Math.max(0,S.position?S.position.unrealized||0:0);
  // سلامة: سعر تصفية أقرب من نصف مدى الرافعة أو على الجهة الخاطئة من الدخول
  // منحط لا شك فيه — يُرفض ولا يُبنى عليه «قرب تصفية». الحماية تبقى للكابح
  // الـ$16 والحارس الخادمي على المنصة
  const minD=0.5/lev;
  if(dir==='long'){ const den=q*mult*(1-mmr-liqFee); if(den<=0) return null;
    const lp=(notional-margin)/den;
    return (lp>0&&lp<=entry*(1-minD))?lp:null; }
  const den=q*mult*(1+mmr+liqFee); if(den<=0) return null;
  const lp=(notional+margin)/den;
  return lp>=entry*(1+minD)?lp:null; }
function refreshLiq(){ const local=computeLiq();
  // تصفية المنصة قد ترجع منحطة (1e-10 لمراكز CROSS) — قيمة كهذه لا تُعرض ولا
  // يُبنى عليها حارس أو إنذار: المحسوب محليًا من الهامش والرافعة الحقيقية أصدق
  let ex=S.exLiqPrice||0; const e=S.position?S.position.entry:0;
  if(ex>0&&e>0&&S.position){ const sh=S.position.side==='short';
    // منحط لا شك فيه — تجاهله: بعيد عبثًا، أو على الجهة الخاطئة من الدخول أصلًا
    // (تصفية الشورت فوق الدخول حتمًا واللونغ تحته — العكس قيمة مستحيلة تُكذّب الحارس)
    if(sh?(ex>e*20||ex<=e*1.002):(ex<e*0.05||ex>=e*0.998)) ex=0; }
  if(ex>0&&!S.grid.some(g=>!g.reduceOnly&&g.status==='armed'&&!g.exchangeOrderId)){
    S.liqPrice=ex; }
  else S.liqPrice=local;
  if(S.position) S.position.liquidation=S.liqPrice; }
function markUnrealized(p){ if(S.position){
  S.position.unrealized=(S.position.side==='short'?1:-1)*
    (S.position.entry-p)*(S.multiplier||1)*S.position.size;
  // MAE/MFE: أقصى انحراف ضد المركز وأقصى تقدم لصالحه (%) طوال عمره
  const fav=(S.position.side==='short'?(S.position.entry-p):(p-S.position.entry))/S.position.entry*100;
  S._exc=S._exc||{mae:0,mfe:0};
  if(fav>=0) S._exc.mfe=Math.max(S._exc.mfe,fav); else S._exc.mae=Math.max(S._exc.mae,-fav); }
  refreshLiq(); }
function liqDanger(p){ const pos=S.position, liq=pos?(pos.liquidation||S.liqPrice):S.liqPrice;
  if(!pos||!liq||!p) return false;
  return pos.side==='long'?p<=liq*1.015:p>=liq*0.985; }
function reversalAgainst(p){ if(!S.position) return false;
  const r=S.regime||{trend:'range'}, mom=S.confluence?S.confluence.momentum:0;
  const adv=adversePct(p), long=S.position.side==='long';
  const tA=long?r.trend==='down':r.trend==='up';
  const mA=long?mom<=-0.1:mom>=0.1;
  const bA=long?S.biasScore<=-8:S.biasScore>=8;
  if(tA&&mA&&bA&&adv>=0.65) return true;
  if(tA&&mA&&adv>=1.4) return true;
  return false; }
function cancelPendingAdds(){ const live=S.config.mode==='live'&&S.keys;
  for(const g of S.grid){
  // مستويات تعويض الجني الجزئي مقدسة — إلا أثناء توقف القاطع: لا تعويض يزيد تعرضاً
  if(g.lane==='comp'&&!batchHoldActive()) continue;
  if(!g.reduceOnly&&(g.status==='armed'||g.status==='open')&&!g.filledAt){
    g.status='cancelled';
    // الحقيقي: الإلغاء على المنصة فورًا — الانتظار لدورة المزامنة (حتى 5ث) كان
    // يترك الأمر حيًا يُكنس في الموجة نفسها التي سحبناه من أجلها
    if(live&&g.exchangeOrderId){ const id=g.exchangeOrderId; g.exchangeOrderId=null; exCancelOne(id); }
    else g.exchangeOrderId=null; } } }
/* حد الصبر: إغلاق المركز العالق بعد مدة أو انحدار محددين — يمنع العلوق الطويل
   الذي يأكل أرباح الأسابيع الفاشل (مطلوب المالك بعد قياس المختبر) */
async function patienceLimit(p:any){
  if(!p||S.status!=='running'||!S.position) return;
  const hrs=Number(S.config.patienceHrs);
  const usd=Number(S.config.patienceUsd);
  if(!(hrs>0)&&!(usd>0)) return; // معطّل
  const ageMs=Date.now()-(S.position.openedAt||Date.now());
  const adv=adversePct(p);
  const floatUsd=-(S.position.unrealized||0);
  const aged=hrs>0&&ageMs>hrs*3600*1000;
  const sunk=usd>0&&floatUsd>=usd;
  if(!aged&&!sunk) return;
  // صبر ذكي: لا إغلاق إن كان الاتجاه معنا — السوق العرضي يعود أغلب الوقت
  // إنما الإغلاق عند اتجاه قوي مضاد للمركز (قياس المختبر: يقلب الأسبوع الفاشل إلى ربح ويحفظ الرابح)
  if(S.regime&&!S.regime.shock){
    const against=(S.position.side==='short'&&S.regime.trend==='down'&&adv>0.6)
      ||(S.position.side==='long'&&S.regime.trend==='up'&&adv>0.6);
    if(!against){
      // اتجاه معنا أو عرضي — الصمود للجني أفضل
      return; } }
  const why=aged?('\u0639\u0644\u0642 '+Math.round(ageMs/3600000)+' \u0633'):( '\u0627\u0646\u062d\u062f\u0627\u0631 '+fmtUsd(floatUsd));
  pushLog('server','\u23f0 \u062d\u062f \u0627\u0644\u0635\u0628\u0631: \u0625\u063a\u0644\u0627\u0642 \u0627\u0644\u0645\u0631\u0643\u0632 \u0627\u0644\u0639\u0627\u0644\u0642 ('+why+') \u2014 \u062a\u0639\u0648\u062f \u0627\u0644\u062f\u0648\u0631\u0629 \u0645\u0646 \u062c\u062f\u064a\u062f \u0645\u0639 \u062f\u0631\u0627\u0633\u0629');
  await flattenAt(p,'\u062d\u062f \u0627\u0644\u0635\u0628\u0631');
  S._postDisaster=Date.now();
  startStudy(false);
}
async function circuitBreak(p:any){ if(!p||S.status!=='running') return;
  // حد الكارثة على مستوى الدورة كلها — قاعدة المالك نفسها: لا كابح قبل $16 (أو 8% من الرصيد)
  const limit=Math.max(LOSS_STOP_USD,(S.config.cycleBalance||0)*0.08);
  // الانزلاق المقيس (فرق سعر التنفيذ عن المستوى) يُخصم أيضًا — الكابح يرى الخسارة الحقيقية
  const cyc=(S.realizedPnl||0)+(S.position?S.position.unrealized:0)-(S.feesPaid||0)-(S.slipCost||0)-(S.fundingPaid||0);
  if(cyc<=-limit){
    // قاعدة المالك: لا إقفال بخسارة أبدًا — الصمود حتى تعود وتُجنى
    if(!S._holdLoggedAt||Date.now()-S._holdLoggedAt>10*60*1000){ S._holdLoggedAt=Date.now();
      pushLog('server','🛡️ صمود: الدورة على '+fmtUsd(cyc)+' — لا إقفال بخسارة — الشبكة تعوض عند المناطق المدروسة'); }
    return; } }
async function escapeAdverse(p:any){ if(S.status!=='running'||!S.position||!p) return;
  const pos=S.position;
  
  let danger=liqDanger(p); reversalAgainst(p);
  // تأكيد المنصة قبل الإقفال الطارئ: سعر تصفية معلن من KuCoin سليم الجهة
  // وبعيد عن السعر = الإنذار المحلي كاذب (قراءة وهمية أقفلت صفقات بخسارة) — لا إقفال
  if(danger){ const exl=S.exLiqPrice||0, e0=pos.entry||0;
    if(exl>0&&e0>0){ const sane=pos.side==='short'?exl>e0*1.005:exl<e0*0.995;
      const nearEx=pos.side==='short'?p>=exl*0.985:p<=exl*1.015;
      if(sane&&!nearEx){ danger=false;
        if(!S._liqFalseAt||Date.now()-S._liqFalseAt>120000){ S._liqFalseAt=Date.now();
          pushLog('server','إنذار تصفية محلي كاذب — المنصة تؤكد التصفية بعيدة عند '+fmtPx(exl)+' — لا إقفال'); } } } }
  const f=tapeFlow(), mom=S.confluence?S.confluence.momentum:0;
  const whaleRaw=f.whaleVol>0.28&&(
    (pos.side==='short'&&f.whale>0.5&&mom>0.08)||
    (pos.side==='long'&&f.whale<-0.5&&mom<-0.08));
  // طبعة واحدة لا تصنع موجة: اشترط استمرار الإشارة 5 نبضات متتالية —
  // وإلا انقلب البوت على ضجيج عابر وباع القاع واشترى القمة
  S._waveN=whaleRaw?(S._waveN||0)+1:0;
  const posAge=Date.now()-(pos.openedAt||0);
  const adv=adversePct(p);
  const whaleWave=S._waveN>=5&&(posAge>120000||adv>=0.35||(S.regime&&S.regime.shock));
  if(whaleRaw&&!whaleWave&&S._waveN===5&&(!S._waveLogAt||Date.now()-S._waveLogAt>60000)){
    S._waveLogAt=Date.now(); pushLog('server','موجة حيتان مبكرة — تُراقب ولا انقلاب (مركز حديث بلا ضرر حقيقي)'); }
  // بوابة الإغلاق بخسارة الوحيدة (قاعدة المالك): انعكاس مؤكد بالاتجاه أو موجة حيتان
  // + خسارة صافية تبلغ حد الكارثة — ما دون ذلك لا إغلاق بخسارة أبدًا، المركز
  // يُدار بالجني الموحّد فقط. خطر التصفية الداهم وحده يعلو على كل قاعدة
  const mult=S.multiplier||1, qty=pos.size||0;
  const entryFees=S.journal.filter(j=>j.status==='open').reduce((a,j)=>a+(j.fees||0),0)
    ||qty*mult*pos.entry*(S.makerFee||0.0002);
  const netAll=(pos.side==='short'?1:-1)*(pos.entry-p)*mult*qty
    -entryFees-qty*mult*p*(S.takerFee||0.0006);
  // قاعدة المالك: كابح الكارثة معطّل — الإقفال الوحيد: خطر تصفية حقيقي من المنصة فقط
  if(!danger) return;
  const was=pos.side, nextSide=was==='long'?'short':'long';
  // 'تصفية داهمة' = العفو الوحيد للخطر الحقيقي (والمصدر القديم 'مركز' لم يعد معفى)
  await flattenAt(p,danger?'تصفية داهمة':'كابح الكارثة');
  if(S.config.directionMode==='auto') S.config.direction=nextSide;
  S.huntAnchor=p; S._flipAt=Date.now(); S.gridAnchor=p;
  // دراسة الوضع قبل أي دخول جديد مع بوابة استقرار: تذبذب عنيف/سيولة مضطربة =
  // انتظار ممدد حتى يهدأ السوق — الدخول الفوري بعد كارثة مطاردة خاسرة
  S._postDisaster=Date.now(); startStudy(false);
  if(danger){ pushLog('server','⚠️ قرب التصفية — أُغلق '+(was==='long'?'اللونغ':'الشورت')+
      ' فورًا — دراسة الوضع قبل أي دخول');
    notify('وقف طارئ: قرب التصفية — أُغلق المركز ويدرس السوق قبل العودة','warn'); }
  else { pushLog('server','⛔ كابح الكارثة: انعكاس مؤكد بضغط معاكس وخسارة '+fmtUsd(-netAll)+
      ' — أُغلق المركز — دراسة واستقرار ثم تقييم الدخول من جديد');
    notify('وقف الخسارة: انعكاس مؤكد — أُغلق المركز عند حد الكارثة ('+fmtUsd(-netAll)+')','warn'); } }
function ensureGrid(){ if(S.status!=='running') return;
  // تنظيف: آلاف الصفوف الملغاة القديمة تُفرز وتُفحص كل نبضة — احتفظ بآخر 100 فقط
  if(S.grid.length>400){ let dead=0;
    S.grid=S.grid.filter(g=>{ if(g.status==='armed'||g.status==='open'||g.status==='filled'||g.status==='staged') return true;
      dead++; return dead<=100; }); }
  const center=S.lastPrice||S.gridAnchor||(S.position?S.position.entry:0);
  if(!center) return;
  sanitizeAdds();
  // تجميد إعادة التسليح أثناء موجة معاكسة: بلا هذا القفل كان السلم يُعاد بناؤه
  // في النبضة التالية لسحب الدرع فيُلغى مفعول السحب وتُكنس المستويات من جديد.
  // أمر الجني (reduceOnly) لا يُمس أبدًا — يبقى حيًا على المنصة طوال التجميد
  // ─── إصلاح الكارثة: الفرملة تسود على ثغرة الـ 60 ثانية — مادام الخطر قائماً
  // (تقلب قوي/صدمة/موجة معاكسة) لا إعادة تسليح إطلاقاً. كانت الثغرة تُعيد
  // السلم في كل هدنة قصيرة فتُلتهم المستويات خلال انهيار من دقائق ───
  if(S.position&&((S._waveHoldUntil&&Date.now()<S._waveHoldUntil)||volBrakeActive())){
    syncPositionTp(); return; }
  const dual=S.grid.some(g=>g.lane==='hold'&&(g.status==='armed'||g.status==='open'))&&
    S.grid.some(g=>g.lane==='trend'&&(g.status==='armed'||g.status==='open'));
  if(dual){ if(S.position) syncPositionTp(); return; }
  if(filledAdds()>=effLevels()){ if(S.position) syncPositionTp(); return; }
  const sameSide=!!(S.position&&S.position.side===S.config.direction);
  const short=S.position&&S.position.side==='short';
  const chasing=!!(sameSide&&S.gridAnchor)&&
    ((short&&center>S.gridAnchor)||(!short&&center<S.gridAnchor));
  const jump=lastJumpPct();
  const live=S.grid.filter(g=>!g.reduceOnly&&(g.status==='armed'||g.status==='open'));
  const stagedN=S.grid.filter(g=>g.status==='staged').length;
  const drift=S.gridAnchor?Math.abs(center-S.gridAnchor)/S.gridAnchor:1;
  const empty=live.length===0&&stagedN===0, away=drift>S.config.gridStepPct/100*2.2;
  // قاعدة المالك: السلم يبقى مسلّحًا دائمًا — لا نزع للتسليح بسبب صدمة
  // أو اتجاه معاكس. استمرار الحركة ضد المركز يُصاد بمستويات أعمق وأكبر
  // حجمًا تكسر المتوسط من مناطق التشبع. الحماية من الشلال: درع الانزلاق
  // اللحظي (إلغاء لحظي ثم إعادة تسليح تلقائية هنا) + كابح الـ$16 + حارس التصفية
  if(chasing&&jump>0.18) return; // لحظة الطبعة فقط — انتظر نبضة أو نبضتين
  if(!empty&&!away&&jump<0.12) return;
  // بوابة المطاردة تضبط إعادة تمركز سلم قائم فقط؛ السلم الفارغ يُبنى فورًا
  if(chasing&&!empty){ const paused=jump>0.02&&jump<0.16;
    if(!(paused&&inAddZone(center))) return; }
  if(S.position&&!inAddZone(center)&&!empty){ if(S.position) syncPositionTp(); return; }
  cancelPendingAdds();
  const keep=S.grid.filter(g=>g.status==='filled'||g.status==='cancelled'||g.status==='staged'||
    (g.reduceOnly&&(g.status==='open'||g.status==='armed')));
  S.grid=[...keep,...buildGrid(center,false,4)].filter(g=>
    keep.includes(g)||g.reduceOnly||g.status!=='armed'||!shouldFill(g,center));
  S.gridAnchor=center; S.grid.sort((a,b)=>b.price-a.price); }
/* ─── الجني المبكر المدروس (قاعدة المالك) ───
   لا تعصّب بنقطة جني رقمية: الدراسة الحية (دفتر الأوامر + حيتان الشريط +
   زخم السوق) تحدد منطقة الارتداد — فإذا اجتمعت شهادة ارتداد مع ربح صافٍ
   موجب: بيع فوري قبل ذوبان الأرباح. أحياناً ربح بسيط وأحياناً كبير —
   ولا إقفال بخسارة أبداً (الصافي بعد كل الرسوم موجب حتماً). */
/* ─── منطقة الجني المدروسة (قاعدة المالك): تُقرأ من دفتر المنصة —
   أقرب جدار سيولة بارز في اتجاه الربح = نقطة الارتداد المحتملة — الجني
   قبيلها مباشرة. تتحدث كل 4 ثوان، وتظل فوق أرضية اللاخسارة دائماً ─── */
export function studiedTpZone(pos:any,p:any){
  try{
    const dir=pos.side==='long'?1:-1; // اتجاه الربح
    const ob=S.orderBook||{};
    const lv=pos.side==='long'?(ob.asks||[]):(ob.bids||[]);
    if(lv.length>=8){
      const sizes=lv.map(l=>+l.size||0).sort((a,b)=>a-b);
      const med=sizes[Math.floor(sizes.length/2)]||1;
      for(const l of lv){ const lp=+l.price||0, lsz=+l.size||0;
        const dist=(lp-p)/p*dir;
        if(dist<=0) continue; if(dist>0.003) break; // مدى دراسة 0.30%
        if(lsz>med*3) return {zone:lp*(dir>0?0.9995:1.0005),dist:dist*100,
          why:'جدار سيولة بارز على بعد '+((dist*100).toFixed(2))+'% — الارتداد منه مرجح'}; } }
  }catch(e){ logSwallow('silent-catch',e); }
  return null; }

export function studiedEarlyExit(p:any){
  const pos=S.position;
  if(!pos||!p||S.status!=='running') return null;
  if(Date.now()-(S._earlyExitAt||0)<90000) return null; // تهدئة: قرار كل 90 ثانية كحد أقصى
  const mult=S.multiplier||1, qty=pos.size||0;
  const openFees=S.journal.filter(j=>j.status==='open').reduce((a,j)=>a+(j.fees||0),0)
    ||qty*mult*pos.entry*(S.makerFee||0.0002);
  const exitFee=qty*mult*p*(S.takerFee||0.0006);
  // الصافي يُحسب من سعر التنفيذ نفسه (p) — لا من قيمة عائمة مخزنة قابلة
  // للانزلاق بين القرار والتنفيذ: شريحة واحدة كانت تقلب +0.02 إلى -0.02
  const grossNow=(pos.side==='long'?p-pos.entry:pos.entry-p)*mult*qty;
  const netNow=grossNow-openFees-exitFee;
  // ─── تتبع ذروة الربح من أول سنت رابح (درس PENGU: ذروة 0.84\$ لم تُقفل
  // لأن عتبة التتبع كانت أعلى منها) — لا تعيد الربح المكتشف للسوق ───
  if(netNow>0) S._posPeakNet=Math.max(S._posPeakNet||0,netNow);
  if(!(netNow>=Math.max(0.08,dynamicTpNet()*0.2))) return null; // بوابة أدنى: الجني المبكر ببربح يستحق
  const dir=pos.side==='long'?1:-1; // اتجاه الربح
  let hit:any=null; // القرار يُجمع أولاً — التهدئة لا تُستهلك إلا بقرار فعلي
  // ─── الشهادة 1: جدار سيولة في اتجاه الربح قريب — الارتداد منه مرجح ───
  const ob=S.orderBook||{};
  const lv=pos.side==='long'?(ob.asks||[]):(ob.bids||[]);
  if(lv.length>=8){
    const sizes=lv.map(l=>+l.size||0).sort((a,b)=>a-b);
    const med=sizes[Math.floor(sizes.length/2)]||1;
    for(const l of lv){ const lp=+l.price||0, lsz=+l.size||0;
      const dist=(lp-p)/p*dir; if(dist<=0) continue; if(dist>0.0018) break;
      if(dist>0&&dist<=0.0015&&lsz>med*3)
        { hit={netNow,why:'جدار سيولة بارز قبله بـ'+(dist*100).toFixed(2)+'% — ارتداد مرجح',tag:'wall'}; break; } } }
  // ─── الشهادة 2: حيتان تعصر عكس الاتجاه والربح معقول — بيع قبل الضغط ───
  if(!hit){ const f=tapeFlow();
    const whaleAgainst=pos.side==='long'?(f.whale<-0.45):(f.whale>0.45);
    if(f.whaleVol>0.25&&whaleAgainst&&netNow>=Math.max(0.05,dynamicTpNet()*0.35))
      hit={netNow,why:'حيتان '+((pos.side==='long')?'شراء':'بيع')+' مضادة ('+(f.whale>0?'+':'')+f.whale.toFixed(2)+') تضغط عكس الصفقة',tag:'whales'}; }
  // ─── الشهادة 3: زخم انقلب ضد الصفقة بعد بلوغ نصف الهدف المدروس — اقطف ───
  if(!hit){ const mom=S.confluence?S.confluence.momentum:0;
    const momAgainst=(pos.side==='long'&&mom<-0.05)||(pos.side==='short'&&mom>0.05);
    if(momAgainst&&netNow>=dynamicTpNet()*0.5)
      hit={netNow,why:'زخم انقلب ضد الصفقة ('+(mom>0?'+':'')+mom.toFixed(2)+') بعد ربح نصف الهدف',tag:'momentum'}; }
  /* ─── الشهادة 4: قفل ذروة الربح ───
     بلغ الربح العائم 40%+ من الهدف ثم تراجع عن 60% من ذروته؟ السوق رفض
     الذروة وارتد — البيع فوراً بمنطقة مرتدة مدروسة بدل مشاهدته يذوب
     كاملاً ثم الانتظار الطويل. (مثال المالك: ذروة 0.60$ والهدف 1.03$ —
     كان يجب البيع عند ~0.36$ بدل ذوبانها وعودة المركز للخسارة) */
  if(!hit){ const peak=S._posPeakNet||0, tgt=dynamicTpNet();
    const engaged=peak>=Math.max(0.12,tgt*0.3);         // ذروة 30%+ من الهدف تُقفل (كانت 40% تُقصّر الذروات)
    const retraced=netNow<=peak*0.6;                     // تراجع 40% من الذروة
    const worthwhile=netNow>=Math.max(0.08,tgt*0.08);    // والمتبقي يستحق (8%+ من الهدف)
    if(engaged&&retraced&&worthwhile)
      hit={netNow,why:'ذروة الربح كانت '+peak.toFixed(2)+'$ وتراجعت إلى '+netNow.toFixed(2)+
        '$ — قفل الربح قبل ذوبانه الكامل',tag:'peaklock'}; }
  if(hit){ S._earlyExitAt=Date.now(); return hit; } // تهدئة تُستهلك بالقرار الفعلي فقط
  return null; }

export async function flattenAt(p:any,source:any){ const pos=S.position;
  /* ─── الحاجز المطلق (قاعدة المالك): لا إغلاق بخسارة أبداً ───
     مهما كان المصدر (جني مبكر، حد صبر، عودة إندكس...) — الإغلاق ينفّذ
     فقط بصافٍ موجب. الاستثناءان الوحيدان: خطر التصفية الداهم (وإلا ضاع
     كل شيء) وإيقاف المالك بيده. رفض الإغلاق = صمود حتى الجني */
  if(pos&&p>0){
    const mult=S.multiplier||1, q=pos.size||0;
    const openFees=S.journal.filter(j=>j.status==='open').reduce((a,j)=>a+(j.fees||0),0)
      ||q*mult*pos.entry*(S.makerFee||0.0002);
    const exitFee=q*mult*p*(S.takerFee||0.0006);
    const gross=(pos.side==='long'?p-pos.entry:pos.entry-p)*mult*q;
    const net=gross-openFees-exitFee;
    // ─── تشديد الحاجز (حادثة 30 أكتوبر: إغلاقات -6.73 و-0.22): كان يعفي
    // 'مركز' و'كابح الكارثة' — وتلك بالضبط مصادر escapeAdverse فيمرّ منه
    // الإقفال بخسارة. الإعفاء الآن: يد المالك فقط + التصفية الداهمة الحقيقية
    // (حفاظاً على الهامش من مصادرة كاملة — ولو لم تُغلق سيأخذ الليكويديشن أكثر)
    const sanctioned=source==='إيقاف'||source==='تصفية داهمة';
    if(net<0.01&&!sanctioned){
      if(!S._refuseAt||Date.now()-S._refuseAt>120000){ S._refuseAt=Date.now();
        pushLog('server','🛡️ رفض إغلاق بخسارة ('+source+'): الصافي الآن '+fmtUsd(net)+
          ' — الصمود حتى الجني — لا إقفال بخسارة أبداً'); }
      return; } }
  // إغلاق حقيقي على المنصة أولًا — لا إغلاق محلي صوري في الوضع الحقيقي
  if(pos&&S.config.mode==='live'&&S.keys){
    exCancelAll(S.config.symbol).catch(()=>{});
    exCancelStops(S.config.symbol).catch(()=>{}); S._guardId=null;
    // إلزام التأكيد: حتى 5 محاولات فورية متتالية — الكارثة لا تنتظر النبضة التالية
    let closed=false;
    for(let att=0;att<5&&!closed;att++){
      try{ await exCloseQty(S.config.symbol,pos.side,pos.size); }
      catch(e:any){ pushLog('error','محاولة إغلاق '+(att+1)+' فشلت: '+String(e?.message||e)); }
      // ─── لا نثق بنجاح النداء وحده: الصفرية تُؤكد بقراءة المنصة (كان النجاح
      // الشكلي يكفي للإعلان الوهمي — أمر مقبول لكن غير منفَّذ) ───
      await new Promise(r=>setTimeout(r,700));
      try{ const live=await exPosition(S.config.symbol);
        if(live&&live.size>0) pos.size=live.size; else closed=true; }
      catch(e2){ if(att===4) pushLog('error','تعذر تأكيد الصفرية — المركز يبقى مُداراً'); } }
    if(!closed){
      // ─── لا إغلاق محلي بلا تأكيد المنصة (كان يُعلن جنيّاً وهمياً ويترك
      // المركز حياً على المنصة) — المركز يبقى في المحرك ويُعاد المحاولة لاحقاً
      pushLog('error','⚠️ الإغلاق لم تُؤكده المنصة بعد 5 محاولات — المركز يبقى مُداراً (لا جني وهمي) — يُعاد تلقائياً');
      return; } }
  if(pos&&p>0){ const cs=pos.side==='short'?'buy':'sell';
    const fee=feeFor(pos.size*(S.multiplier||1)*p,true); S.feesPaid+=fee;
    const d=applyDelta(cs,pos.size,p);
    if(d.closedQty>0){ const of=S.journal.find(j=>j.status==='open')?.fees||0;
      noteClose(d.closedQty,p,fee,source,d.realized-of-fee); } }
  for(const row of S.journal){ if(row.status==='open'){ row.status='closed';
    row.exit=p; row.closedAt=Date.now(); if(source==='إيقاف') row.source='إيقاف';
    if(!row.pnl) row.pnl=lotNet(row.side,row.entry,p,row.qty,row.fees); } }
  S.position=null; S.huntOpen=0; S._tpBanked=0; S._trailFloor=0;
  for(const l of S.grid) if(l.status==='open'||l.status==='armed'){
    l.status='cancelled'; l.exchangeOrderId=null; } }
function pruneGhosts(){ if(S.position) return;
  S.huntOpen=0;
  for(const g of S.grid){ if(g.reduceOnly&&(g.status==='open'||g.status==='armed')){
      g.status='cancelled'; g.exchangeOrderId=null; }
    if(g.status==='filled'&&!g.reduceOnly) g.status='cancelled'; }
  // ─── الوضع الحقيقي: صفوف السجل تُغلق فقط بتأكيد المنصة (تنفيذ فعلي) —
  // الإغلاق هنا بسعر السوق كان يعلن «جني ربح» وهمياً كل نبضة (3 صفقات وهمية!) ───
  if(S.config.mode==='live'&&S.keys) return;
  for(const row of S.journal){ if(row.status!=='open') continue;
    row.status='closed'; row.exit=S.lastPrice||row.entry; row.closedAt=Date.now();
    if(!row.pnl&&row.exit) row.pnl=lotNet(row.side,row.entry,row.exit,row.qty,row.fees); } }

/* ================================================================
   تدفق السوق — WebSocket مستمر + REST احتياطي
   ================================================================ */
function applyMeta(m:any){ S.lastPrice=m.price; S.markPrice=m.markPrice||m.price;
  if(m.makerFee)S.makerFee=m.makerFee; if(m.takerFee)S.takerFee=m.takerFee;
  if(m.funding!=null)S.funding=m.funding; if(m.multiplier)S.multiplier=m.multiplier;
  if(m.tickSize)S.tickSize=m.tickSize; S.lastTickAt=Date.now();
  if(m.orderBook)S.orderBook=m.orderBook;
  if(m.tape){ S.tape=m.tape;
    for(const t of m.tape) S.cvd+=t.side==='buy'?t.size:-t.size; }
  if(m.price>0){ S.priceTrail=[...S.priceTrail,m.price].slice(-120);
    S.emaFast=nextEma(S.emaFast,m.price,2/10);
    S.emaSlow=nextEma(S.emaSlow,m.price,2/22); }
  S.confluence=readConfluence(); S.regime=readRegime(); }

// رسائل القناة اللحظية — تصل دفعًا فور وقوعها (أجزاء من الثانية)
export function onStreamTick(d){
  netAlivePing(true); // رسالة لحظية من القناة = الإنترنت يعمل
  if(d.symbol&&d.symbol!==S.config.symbol) return;
  const now=Date.now();
  if(d.price>0){
    S.lastPrice=d.price;
    S.lastTickAt=now;
    if(!S._lastTrailAt||now-S._lastTrailAt>400){
      S.priceTrail=[...S.priceTrail,d.price].slice(-120);
      S.emaFast=nextEma(S.emaFast,d.price,2/10);
      S.emaSlow=nextEma(S.emaSlow,d.price,2/22);
      S._lastTrailAt=now;
      // قمة/قاع نافذة المسار — تسجيل لحظة الاختراق لمحفّز صيد السلاحف (الاختراق الكاذب)
      const tt=S.priceTrail;
      if(tt.length>=20){ let hi=-Infinity,lo=Infinity;
        for(let i=0;i<tt.length-1;i++){ if(tt[i]>hi)hi=tt[i]; if(tt[i]<lo)lo=tt[i]; }
        if(hi>-Infinity&&d.price>hi) S._brk={side:'up',at:now,level:hi};
        else if(lo<Infinity&&d.price<lo) S._brk={side:'down',at:now,level:lo}; } }
    // عينة نشاط السوق كل دقيقة: معدل الصفقات الفعلي في آخر 60 ثانية (طول الشريط مخزّن مشبع لا يقيس شيئًا)
    if(!S._actAt||now-S._actAt>60000){ S._actAt=now;
      const cut=now-60000, rate=(S._tradeTs||[]).filter(t=>t>=cut).length;
      S._actHist=[...(S._actHist||[]),rate].slice(-30);
      const h=[...S._actHist].sort((a,b)=>a-b); S._actMed=h[Math.floor(h.length/2)]||0; }
    // تنفيذ فوري لجني الربح مع كل نبضة سعر — لا انتظار لدورة المحرك
    if(S.status==='running'){ try{ harvestRipe(d.price); }catch(e){ logSwallow("silent-catch", e); } }
  }
  if(d.book){
    // تدفق الأوامر (OFI): ميزان ضغط أفضل عرض/طلب بين اللقطات — راكم 20 عينة أخيرة
    const b0=d.book.bids&&d.book.bids[0], a0=d.book.asks&&d.book.asks[0];
    const pb=S._prevBook;
    if(pb&&pb.b&&pb.a&&b0&&a0){ let ofi=0;
      if(b0.price>pb.b.price) ofi+=b0.size;
      else if(b0.price<pb.b.price) ofi-=pb.b.size;
      else ofi+=b0.size-pb.b.size;
      if(a0.price<pb.a.price) ofi+=a0.size;
      else if(a0.price>pb.a.price) ofi-=pb.a.size;
      else ofi-=a0.size-pb.a.size;
      S._ofiWin=[...(S._ofiWin||[]),ofi].slice(-20);
      S._ofi=S._ofiWin.reduce((x,y)=>x+y,0); }
    S._prevBook={b:b0?{price:b0.price,size:b0.size}:null,a:a0?{price:a0.price,size:a0.size}:null};
    S.orderBook=d.book; }
  if(d.trade&&d.trade.price>0){
    S.lastPrice=d.trade.price; S.lastTickAt=now;
    S.tape=[d.trade,...S.tape].slice(0,32);
    S._tradeTs=[...(S._tradeTs||[]),now].slice(-300); // طوابع زمنية — لقياس معدل الصفقات الحقيقي
    S.cvd+=d.trade.side==='buy'?d.trade.size:-d.trade.size;
    // مسار CVD — لكشف انحراف الحيتان (السعر عكس التدفق)
    if(!S._cvdAt||now-S._cvdAt>1500){ S.cvdTrail=[...(S.cvdTrail||[]),{t:now,cvd:S.cvd}].slice(-90); S._cvdAt=now; } }
  S.confluence=readConfluence(); S.regime=readRegime();
  if(S.position) accrueFunding();
  emitThrottled();
}

async function loadMarket(full){
  const c=S.config.symbol;
  const wsOk=Date.now()-(streamState.lastMsgAt||0)<6000;
  let price=S.lastPrice||0, mark=S.markPrice||price;
  if(!wsOk){ const t=await fetchTicker(c); price=+t.price||price; }
  if(!S._metaAt||Date.now()-S._metaAt>60000){
    const ct=await fetchContract(c); S._metaAt=Date.now();
    mark=+ct.markPrice||mark;
    S.makerFee=normFee(ct.makerFeeRate??ct.makerFeeCoefficient,S.makerFee);
    S.takerFee=normFee(ct.takerFeeRate??ct.takerFeeCoefficient,S.takerFee);
    S.funding=+ct.fundingFeeRate||0;
    // المضاعف لا يُلمس إلا بقيمة حقيقية من المنصة — multiplier=1 الافتراضي على
    // عملة مثل PEPE (العقد=520,000 وحدّة) يضخّم الكمية 520 ألف ضعف فترفض
    // المنصة كل أمر: «Order quantity is too high, insufficient available margin»
    const _mul=+ct.multiplier, _tk=+ct.tickSize;
    if(_mul>0){ S.multiplier=_mul; S._metaSym=c; }
    if(_tk>0) S.tickSize=_tk;
    if(+ct.maxOrderQty>0) S.maxOrderQty=+ct.maxOrderQty;
    if(!price) price=mark;
  }
  const m={price,markPrice:mark};
  const bookStale=Date.now()-(streamState.bookAt||0)>8000;
  const tapeStale=Date.now()-(streamState.tradeAt||0)>8000;
  if(full&&(bookStale||tapeStale||!S.tape.length)){
    const [ob,tp]=await Promise.all([fetchBook(c).catch(()=>null),fetchTape(c).catch(()=>null)]);
    if(ob&&(ob.bids.length||ob.asks.length)) (m as any).orderBook=ob;
    else if(!ob) (m as any).dataStale=Date.now();
    if(tp&&tp.length&&tapeStale) (m as any).tape=tp;
    else if(!tp) (m as any).dataStale=Date.now();
  }
  // قائد BTC: نبضة المؤشر توجّه صيد العملات التابعة — قراءة REST خفيفة كل دورة كاملة
  if(full&&c!=='XBTUSDTM'){ try{ const bt=await fetchTicker('XBTUSDTM'); const bp=+bt.price||0;
    if(bp>0){ const pv=S._btcPrev;
      if(pv&&pv.px>0){ const dtMin=Math.max(0.05,(Date.now()-pv.at)/60000);
        S._btcImp=Math.round(((bp-pv.px)/pv.px*100)/dtMin*10000)/10000; } // تغير % لكل دقيقة
      S._btcPrev={px:bp,at:Date.now()};
      // نافذة ساعتين لقائد السوق — أساس فيتو السلة
      { const W=2*3600000, now=Date.now(); S._btcPx=bp;
        if(!S._btcHi||now-(S._btcHiAt||0)>W){ S._btcHi=bp; S._btcHiAt=now; }
        else if(bp>S._btcHi){ S._btcHi=bp; S._btcHiAt=now; }
        if(!S._btcLo||now-(S._btcLoAt||0)>W){ S._btcLo=bp; S._btcLoAt=now; }
        else if(bp<S._btcLo){ S._btcLo=bp; S._btcLoAt=now; } } } }catch(e){ logSwallow("silent-catch", e); } }
  return m;
}

// كبح أخطاء المزامنة على السيرفر — لا window في Node: متغيرات وحدات عادية
let _ordErrAt=0, _syncErrAt=0, _syncErrMsg='';
let _huntErrAt=0; // خنق فشل الصيد — كل ثانية كان يغرق السجل بلا فائدة
export async function liveSync(){ if(R()) return; // وضع المرآة: الحالة ملك السيرفر — لا مزامنة منصّة محلية تزاحمه
  if(S.config.mode!=='live'||!S.keys) return;
  // حصة القراءة (درس عاصفة 429 على «قراءة المركز»): 4ث كحد أدنى بين الدورات،
  // وعند رفض الحصة (429) تبريد 45ث كاملة — لا نطرق الباب أثناء العاصفة
  if(Date.now()-(S._syncAt||0)<4000) return;
  S._syncAt=Date.now();
  // سرب إنقاذ المراكز اليتيمة كل 5 دقائق — قبل أي منطق آخر
  if(!S._orphanAt||Date.now()-S._orphanAt>300000){ S._orphanAt=Date.now(); orphanSweep().catch(()=>{}); }
  const sym=S.config.symbol;
  try{
    const pos=await exPosition(sym);
    // ─── قراءة ناجحة عادت «لا مركز»: لا تصدّقها من أول مرة (كانت قراءة واحدة
    // وسط 429 تمسح المركز وتتركه حياً على المنصة ثم تبدّل العملة — مصدر 6 و8
    // و9 مراكز). التسجيل: 3 قراءات فارغة متتالية (التباعد 4 ثوانٍ بينها) ───
    if(pos){ S._flatStrikes=0; S._flatConfirmed=false; }
    else { S._flatStrikes=(S._flatStrikes||0)+1;
      if(S._flatStrikes>=3) S._flatConfirmed=true; }
    const inGrace=S.ignoreExchangeUntil&&Date.now()<S.ignoreExchangeUntil;
    if(pos){
      if(inGrace&&!S.position){ /* ignore stale center during grace window */ }
      else if(!S.position){ S.position=pos; pushLog('recover','استُعيد مركز '+pos.side+' من المنصة'); }
      else { const prevEntry=S.position.entry, prevSize=S.position.size;
        S.position.size=pos.size; S.position.entry=pos.entry;
        S.position.unrealized=pos.unrealized; S.position.side=pos.side;
        if(pos.liquidation) S.position.liquidation=pos.liquidation;
        if(pos.entry>0&&Math.abs(pos.entry-prevEntry)/Math.max(1e-9,prevEntry)>1e-4){
          const openRow=S.journal.find(j=>j.status==='open');
          if(openRow){ const oldFees=openRow.fees||0; openRow.entry=pos.entry;
            if(openRow.qty>0&&S.position.size!==prevSize){ openRow.qty=S.position.size; openRow.fees=oldFees; } }
          if(S.activeCycle) S.activeCycle.entry=pos.entry; } } }
    else if(S.position&&Date.now()>S.ignoreExchangeUntil&&S._flatConfirmed){
      // ─── درس SHIB: المركز وُجد على المنصة سابقاً والآن لا مركز — أُغلق بلا
      // علم المحرك (وقف منصّي/يدوي/تصفية) وكان يُمسح بصمت فلا يظهر في السجل.
      // الآن: يُغلق صف السجل بالقيمة الفعلية التقريبية ويُسجل بتنبيه واضح ───
      const __row=S.journal.find(j=>j.status==='open');
      const __ex=Number(pos&&(pos as any).exitPrice)||S.markPrice||S.lastPrice||(__row?__row.entry:0);
      if(__row&&__ex>0){
        try{ noteClose(__row.qty,__ex,0,'إغلاق منصة',undefined,__row.id); }catch{}
        S._harvestAt=Date.now(); S._reselectDone=false;
        try{ _harvestCleanup(__ex); }catch{}
        pushLog('warn','⚠️ المركز أُغلق على المنصة بلا أمر من البوت (وقف/يدوي/تصفية) — سُجل في سجل الصفقات بالقيمة الفعلية وأُعيدت الدراسة لعملة جديدة');
      } else pushLog('warn','⚠️ لا مركز على المنصة — صُحّحت حالة المحرك محلياً');
      S.position=null; S._guardPx=0; S._guardId=null; S._tpBanked=0; S._trailFloor=0; }
    if(pos&&pos.liquidation) S.exLiqPrice=pos.liquidation;
    // الرصيد المتاح الحقيقي كل 30 ثانية — حدّ الميزانية به يمنع رفض
    // «insufficient available margin» عندما يكون الهامش محجوزًا كله بالأوامر
    if(!S._accAt||Date.now()-S._accAt>30000){ S._accAt=Date.now();
      kcPrivate('GET','/api/v1/account-overview?currency=USDT').then(a=>{
        const av=Number(a&&a.availableBalance); if(av>=0&&isFinite(av)) S.exAvail=av;
        // رصيد الحساب الكلي (equity) — المرجع الثابت لحساب التصفية وميزانية
        // الأحجام، لا ينهار لحظيًا بحجز الأوامر المركونة كما يفعل «المتاح»
        const eq=Number(a&&a.accountEquity); if(eq>0&&isFinite(eq)) S.exEquity=eq; }).catch(()=>{}); }
    // الرافعة ثابتة كما ضبطها المالك تمامًا — لا تكيّف ولا تحذير:
    // في وضع CROSS تُتجاهل رافعة الأمر وتُطبَّق رافعة الرمز، لذا تُفرض على المنصة
    // عبر نقطة KuCoin الرسمية (مخنوقة 5 دقائق، صامتة إن رُفضت أثناء مركز مفتوح)
    if(pos&&pos.leverage>0){ S.exLeverage=pos.leverage;
      const cfgL=S.config.leverage||1;
      if(Math.abs(pos.leverage-cfgL)/cfgL>0.05&&(!S._levFixAt||Date.now()-S._levFixAt>300000)){
        S._levFixAt=Date.now();
        kcPrivate('POST','/api/v2/changeCrossUserLeverage',
          {symbol:sym,leverage:String(cfgL)}).then(()=>{ pushLog('server',
            'فُرضت رافعة CROSS '+cfgL+'× على المنصة كما ضبطتها'); }).catch(()=>{}); } }
    // لا مركز الآن: صفّر الرافعة المقروءة وفرّض CROSS (الرصيد الكلي يحمي الصفقة —
    // قاعدة المالك) + اضبط رافعة CROSS على قيمة الإعدادات حرفيًا —
    // التبديل مخنوق كل 5 دقائق وينجح فقط بلا مراكز ولا أوامر معلقة
    if(!pos){ S.exLeverage=null;
      if(!S._mmFixAt||Date.now()-S._mmFixAt>300000){ S._mmFixAt=Date.now();
        kcPrivate('POST','/api/v2/position/batchChangeMarginMode',{marginMode:'CROSS',symbols:[sym]})
          .then(()=>{ delete mmCache[sym]; }).catch(()=>{});
        kcPrivate('POST','/api/v2/changeCrossUserLeverage',
          {symbol:sym,leverage:String(S.config.leverage)}).catch(()=>{}); } }
    // ═══ قاعدة المالك الشاملة (أمر 30 أكتوبر): لا فتح جديد حتى تُغلق كل
    // المراكز الحية بربح — عدّ المراكز الحية في الحساب كله كل 10 ثوانٍ.
    // المسموح الطبيعي: 5 في الأسطول (مركز لكل محرك) أو 1 في الأحادي —
    // أي زيادة = بقايا جلسات سابقة → تجميد فوري للدخول والتبديل ═══
    if(!S._posCntAt||Date.now()-S._posCntAt>10000){ S._posCntAt=Date.now();
      exAllPositions().then(list=>{
        const n=list.length;
        const allowed=(typeof FLEET_MODE==='function'&&FLEET_MODE())?5:1;
        const was=!!S._noNewEntries;
        S._exOpenCount=n; S._noNewEntries=n>allowed;
        // قائمة المراكز الحقيقية كما تراها المنصة — تظهر في التطبيق كمرجع حقيقي
        S.exPositions=list.filter((x:any)=>Math.abs(Number(x.currentQty)||0)>1e-12).map((x:any)=>({
          symbol:String(x.symbol), side:(Number(x.currentQty)||0)>0?'long':'short',
          size:Math.abs(Number(x.currentQty)||0), entry:Number(x.avgEntryPrice)||0,
          pnl:Math.round((Number((x as any).unrealisedPnl)||0)*100)/100 }));
        if(!S._exListLogAt||Date.now()-S._exListLogAt>300000){ S._exListLogAt=Date.now();
          if(n>0) pushLog('server','📋 مراكز المنصة الحقيقية ('+n+'): '+S.exPositions.map((p:any)=>p.symbol.replace('USDTM','')).join(' · ')); }
        if(S._noNewEntries&&!was){ pushLog('server','⛔ تجميد الدخول: '+n+' مركزاً حياً في المنصة (المسموح '+allowed+') — لا صفقات جديدة ولا تبديل حتى تُغلق الزائدة بربح'); }
        if(!S._noNewEntries&&was){ pushLog('server','✅ رُفع التجميد: المراكز عادت للحد الطبيعي — العودة للعمل'); }
        // مطابقة الدفاتر: حقوق المنصة هي المرجع الحقيقي
        try{ if(S.exEquity>0&&(!S._recAt||Date.now()-S._recAt>600000)){ S._recAt=Date.now();
          const books=(S.config.cycleBalance||0)+(S.realizedPnl||0)-(S.feesPaid||0);
          const gap=S.exEquity-books;
          if(Math.abs(gap)>Math.max(3,Math.abs(books)*0.05))
            pushLog('warn','⚠️ مطابقة الحساب: حقوق المنصة '+fmtUsd(S.exEquity)+' مقابل دفاتر التطبيق '+fmtUsd(books)+' (فارق '+fmtUsd(gap)+') — المنصة هي المرجع والجني يُحتسب تنفيذاً منصياً فقط'); } }catch{}
      }).catch(()=>{}); }
    if(S.position) exPlaceStopGuard(); // حارس تصفية فقط (لا وقف خسارة — قاعدة المالك)
  }catch(e){ const m=String((e as any)?.message||e);
    // 429 = تجاوز الحصة: تبريد كامل قبل المحاولة التالية (لا نُعمي المحرك بعاصفة طرق)
    if(/429|Too many requests/i.test(m)){ S._syncAt=Date.now()+45000; }
    if(m!==_syncErrMsg||Date.now()-_syncErrAt>60000){ _syncErrAt=Date.now(); _syncErrMsg=m;
      pushLog('error','قراءة المركز: '+m); } }
  try{
    const exs=await exOrders(sym);
    // فشل قراءة الأوامر (شبكة/حصة)؟ تخطَّ الجولة كاملة بلا أي تغيير حالة —
    // اعتبار الفشل «قائمة فارغة» كان يعيد تسليح كل مستوى فيضاعف الأوامر على
    // المنصة ويحجز الهامش حتى تنهار الأحجام (كارثة الـ17 أمرًا اليتيمة)
    if(!exs) return;
    const fills=await exFills(sym);
    S._fillsOk=!!fills;
    if(fills){
      const byOidF=new Map(); const byIdF=new Map();
      for(const f of fills){ if(!(f.size>0)||!(f.price>0)) continue;
        if(f.clientOid){ const e1=byOidF.get(f.clientOid)||{size:0,cost:0,fee:0,n:0};
          e1.size+=f.size; e1.cost+=f.size*f.price; e1.fee+=f.fee; e1.n++; byOidF.set(f.clientOid,e1); }
        if(f.orderId){ const e2=byIdF.get(f.orderId)||{size:0,cost:0,fee:0,n:0};
          e2.size+=f.size; e2.cost+=f.size*f.price; e2.fee+=f.fee; e2.n++; byIdF.set(f.orderId,e2); } }
      const agg=(key,map)=>{ if(!key) return null; const e=map.get(key);
        if(!e||!(e.size>0)) return null; return {avgPx:e.cost/e.size,size:e.size,fee:e.fee}; };
      for(const l of S.grid){ if(l.status==='cancelled'||l.status==='filled') continue;
        const e=agg(l.clientOid,byOidF)||agg(l.exchangeOrderId,byIdF);
        if(!e) continue;
        const before=l.exFilled||0;
        if(e.size>before){ const delta=e.size-before;
          const actualFee=e.fee*(delta/e.size);
          l._avgFillPx=e.avgPx; l._realFillFee=(l._realFillFee||0)+actualFee;
          const done=creditExFillReal(l,delta,e.avgPx,actualFee);
          if(done) continue; }
        else l._avgFillPx=e.avgPx; }
      S._realFeesAt=Date.now(); }
    const byOid=new Map(exs.map(o=>[o.clientOid||'',o]));
    const byId=new Map(exs.map(o=>[o.orderId,o]));
    for(const l of S.grid){ if(l.status==='cancelled'||l.status==='filled') continue;
      const hit=(l.clientOid&&byOid.get(l.clientOid))||
      (l.exchangeOrderId&&byId.get(l.exchangeOrderId));
      if(hit){ l.status='open'; l.exchangeOrderId=hit.orderId;
        const fs=hit.filledSize||0; let done=false;
        if(fs>(l.exFilled||0)){ const actualPrice=l._avgFillPx>0?l._avgFillPx:(hit.price>0?hit.price:l.price);
          done=creditExFill(l,fs-(l.exFilled||0),actualPrice); }
        if(!done&&l.status!=='filled'&&fs>0&&fs>=l.qty-1e-9){ l.status='filled';
          l.filledAt=Date.now(); l.filledPrice=l.price;
          if(l.reduceOnly&&!S.position){ const tb=S._tpBanked; _harvestCleanup(l.price);
            pushLog('server','جني مكتمل ✓ — نُفّذ على المنصة بالكامل، صافي الدورة '+fmtUsd(tb));
            if(S.status==='running') maybeSwitchSymbol().catch(()=>{}); } } }
      else if(l.status==='open'&&!shouldFill(l,S.lastPrice||0)){
        l.status='armed'; l.exchangeOrderId=null; l.exFilled=0; }
      else if(l.status==='open'&&l.exchangeOrderId&&shouldFill(l,S.lastPrice||0)&&S._fillsOk===false){
        // اختفى من النشطة وسجل التنفيذ غير متاح (شبكة) — لا تحجز ربحًا بغير دليل
        const fs2=(l.exFilled||0);
        if(!(fs2>0)){ l.status='cancelled'; l.exchangeOrderId=null; l.exFilled=0;
          pushLog('warn','\u0623\u0645\u0631 \u0627\u062E\u062A\u0641\u0649 \u0645\u0646 \u0627\u0644\u0645\u0646\u0635\u0629 \u0648\u0644\u0645 \u064A\u064F\u0646\u0641\u0651\u0638 \u0645\u0646\u0647 \u0634\u064A\u0621 \u2014 \u0644\u0627 \u064A\u064F\u062D\u062C\u0632 \u0623\u064A \u0631\u0628\u062D (\u0645\u0633\u062A\u0648\u0649 @ '+fmtPx(l.price)+')'); }
      }
      else if(l.status==='open'&&l.exchangeOrderId&&shouldFill(l,S.lastPrice||0)){
        const rem=l.qty-(l.exFilled||0); let done=false;
        if(rem>0) done=creditExFill(l,rem,l._avgFillPx>0?l._avgFillPx:(l.price>0?l.price:S.lastPrice||0));
        if(l.status!=='filled'){ l.status='filled'; l.filledAt=Date.now(); l.filledPrice=l._avgFillPx>0?l._avgFillPx:l.price; }
        if(!done&&l.reduceOnly&&!S.position){ const tb=S._tpBanked; _harvestCleanup(l.price);
          pushLog('server','جني مكتمل ✓ — نُفّذ على المنصة بالكامل، صافي الدورة '+fmtUsd(tb)); } } }
    // ─── تأكيد الجني: أي تنقية جرت الآن تُتحقق من المنصة — إن كان المركز ما
    // زال حياً (جني وهمي) يُعاد فوراً للإدارة ويُقفل عداده على 4 (درس تكرار
    // «جني 3 صفقات» والصفقات المتراكمة) ───
    if(S._needsFlatCheck&&Date.now()-S._needsFlatCheck<30000){
      try{ const lp=await exPosition(sym);
        if(lp&&lp.size>0){ S._needsFlatCheck=0; S.position=lp;
          S._batchFills=Math.max(4,S._batchFills||0); if(!S._batchHoldAt) S._batchHoldAt=Date.now();
          pushLog('warn','⚠️ الجني لم تُؤكده المنصة — المركز عاد للإدارة فوراً (القاطع مقفل، لا صفقة جديدة)'); }
        else S._needsFlatCheck=0; }catch{} }
    // أي مستوى أُلغي أو نُفّذ محليًا وله أمر حي على المنصة — ألغِه هناك فورًا
    for(const l of S.grid){ if((l.status==='cancelled'||l.status==='filled')&&l.exchangeOrderId){
      exCancelOne(l.exchangeOrderId); l.exchangeOrderId=null; } }
    // أوامر المنصة غير المعروفة محليًا: تُستورد فقط عند إقلاع بلا شبكة (استعادة بعد إعادة تشغيل)
    // — وإلا فهي أوامر يتيمة قديمة يحذفها خط التنظيف أدناه بدل إحيائها
    const hasLocalLive=S.grid.some(l=>(l.status==='armed'||l.status==='open'));
    for(const o of exs){ if(S.grid.some(l=>l.exchangeOrderId===o.orderId||
      (o.clientOid&&l.clientOid===o.clientOid))) continue;
      if(hasLocalLive) continue;
      S.grid.push({id:uid('ex'),clientOid:o.clientOid||uid('oid'),side:o.side,
        price:o.price,qty:o.size,status:'open',reduceOnly:o.reduceOnly,
        exchangeOrderId:o.orderId,filledAt:null,origin:'grid'}); }
    const want=S.grid.filter(l=>(l.status==='armed'));
    if(!S.permDenied) for(const l of want){ if(l.exchangeOrderId) continue;
      try{ const r=await exPlaceLimit({clientOid:l.clientOid,symbol:sym,side:l.side,
        price:l.price,qty:l.qty,reduceOnly:l.reduceOnly,leverage:S.config.leverage});
        l.status='open'; l.exchangeOrderId=r.orderId;
      }catch(e){ const m=String((e as any)?.message||e);
        if(/access denied|permission/i.test(m)){ if(!S.permDenied){ S.permDenied=true;
          pushLog('error','⛔ المفتاح يقرأ الرصيد لكن بلا صلاحية تداول — من KuCoin: إدارة API ← تعديل المفتاح ← فعّل «التداول» للعقود الآجلة ثم احفظ المفاتيح مجددًا');
          toast('⚠️ فعّل صلاحية التداول في مفتاح KuCoin'); } break; }
        // كبح تكرار نفس الخطأ — مرة كل دقيقة كافية (لا window هنا — السيرفر Node)
        if(Date.now()-_ordErrAt>60000){ _ordErrAt=Date.now();
          pushLog('error','أمر '+l.side+' @ '+fmtPx(l.price)+': '+m); } } }
    const liveIds=new Set(S.grid.filter(l=>l.exchangeOrderId).map(l=>l.exchangeOrderId));
    for(const o of exs){ if(!liveIds.has(o.orderId)){
      try{ await kcPrivate('DELETE','/api/v1/orders/'+o.orderId); }catch(e){ logSwallow("silent-catch", e); } } }
    // كنس الستوبات اليتيمة (مخنوق 30ث): أوامر الوقف تعيش في قائمة منفصلة لا يطالها
    // كنس الأوامر العادية أعلاه — احتفظ بالحارس الحالي فقط واحذف ما عداه. فشل
    // القراءة (null) يعني تخطي الجولة بلا حذف أعمى
    if(!S._stopSweepAt||Date.now()-S._stopSweepAt>30000){ S._stopSweepAt=Date.now();
      const stops=await exStops(sym);
      if(stops) for(const st of stops){ if(st.id&&st.id!==S._guardId){
        try{ await kcPrivate('DELETE','/api/v1/stopOrders/'+st.id); }catch(e){ logSwallow("silent-catch", e); } } } }
  }catch(e){ const m=String((e as any)?.message||e);
    // نفس الخطأ مرة كل دقيقة — السطر الواحد كل 5 ثوانٍ كان يغرق السجل ويطلق تنبيهات مجنونة
    if(m!==_syncErrMsg||Date.now()-_syncErrAt>60000){ _syncErrAt=Date.now(); _syncErrMsg=m;
      pushLog('error','مزامنة الأوامر: '+m); } } }
async function liveFlattenIfNeeded(prevSide){ if(S.config.mode!=='live'||!S.keys||!prevSide) return;
  try{ const pos=await exPosition(S.config.symbol);
    if(pos){ await exCancelAll(S.config.symbol); await exClose(S.config.symbol,pos.side);
      pushLog('server','أُغلق المركز الحقيقي على المنصة'); } }catch(e){ logSwallow("silent-catch", e); } }

// نبضة حياة: البوت يعمل لكنه صامت — كل 4 دقائق بلا أي سطر يكتب لماذا لا صيد الآن،
// فلا يبدو متجمدًا وهو يترقب (الصمت الطويل كان يُقرأ «تأخير غير مفهوم»)
function heartbeatBeat(p){ if(S.status!=='running'||!p) return;
  const now=Date.now(); if(S._lastBeatAt&&now-S._lastBeatAt<240000) return;
  if(S.logs.length&&now-S.logs[0].at<240000) return; // السجل حي أصلًا — لا ضجيج
  S._lastBeatAt=now;
  const mom=S.confluence?S.confluence.momentum:0, fl=tapeFlow();
  let why;
  if(studying()) why='أُنهي دراسة البصمة ('+Math.max(1,Math.ceil((S.studyUntil-now)/1000))+'ث متبقية)';
  else if(S.position){ const pos=S.position, sgn=pos.side==='short'?1:-1;
    const net=sgn*(pos.entry-p)*(S.multiplier||1)*pos.size-(pos.size*(S.multiplier||1)*(pos.entry*S.makerFee+p*S.takerFee));
    const tp=S.grid.find(g=>g.reduceOnly&&(g.status==='open'||g.status==='armed'));
    why='أدير مركزًا — الصافي '+fmtUsd(net)+' — جني الربح '+
      (tp?('@ '+fmtPx(tp.price)+' (صافي '+fmtUsd(tpBase())+' للمجموع)'):'قيد الوضع'); }
  else { const v=dangerVeto(p);
    if(v) why='فيتو خطر: '+v;
    else { const sig=huntSignals(p);
      why=sig.length?('إشارات تتراكم: '+sig.slice(0,2).join(' · '))
        :'لا إشارة دخول بعد — أنتظر التقاء الزخم والتدفق'; } }
  pushLog('info','💓 أراقب '+(S.config.displaySymbol||S.config.symbol)+' @ '+fmtPx(p)+
    ' — زخم '+(mom>=0?'+':'')+mom.toFixed(2)+' · تدفق '+(fl.bias>=0?'+':'')+fl.bias.toFixed(2)+' — '+why); }

// خدش الصفقات الميتة حُذف نهائيًا بأمر المالك: لا إغلاق لأي صفقة إلا بجني
// الربح الموحّد (صافي $0.12 للمجموع) أو كارثة $16 بانعكاس مؤكد أو خطر تصفية
// داهم أو إيقاف يدوي — الإغلاق عند التعادل كان يقتل صفقات كان الجني سيقطفها

export async function botTick(){
  const running=S.status==='running', paused=S.status==='paused';
  if(!running&&!paused&&S.status!=='idle') return;
  try{
    const full=S.heartbeat%5===0||!S.tape.length;
    // القرارات تعتمد سعر القناة اللحظي؛ REST للإثراء فقط — فشله لا يوقف التنفيذ أبدًا
    let m:any={price:S.lastPrice||S.markPrice||0,markPrice:S.markPrice||S.lastPrice||0};
    try{ m=await loadMarket(full); }catch(e){ logSwallow("silent-catch", e); }
    const price=S.lastPrice||m.price||0;
    if(S.status==='idle'){ applyMeta(m); emit(); saveAll(); return; }
    S.heartbeat++;
    applyMeta(m);
    studyTick(); // جمع عينات بصمة العملة أثناء الدراسة (ويُنهيها عند اكتمالها)
    armProgressive(); bgRadarTick();
    if(S.config.gapMode&&!S.position&&S.status==='running'){
      gapHunterTick().catch(()=>{}); }
    if(S.position&&S.status==='running'){ syncPositionTp();
      backgroundPipeline().catch(()=>{}); // تبع سريع: يتحدّث كل ثانية مع الربح العائم
      // ─── الجني المبكر المدروس: شهادة ارتداد مع ربح صافٍ = بيع قبل ذوبانه ───
      const ex=studiedEarlyExit(S.lastPrice||0);
      if(ex){ S._earlyExitAt=Date.now();
        pushLog('server','🎯 جني مبكر مدروس ('+ex.tag+'): '+ex.why+' — البيع الآن بصافي '+fmtUsd(ex.netNow)+' قبل الارتداد');
        toast('🎯 جني مبكر: +'+ex.netNow.toFixed(2)+'$ قبل الارتداد');
        nativeNotify('AB-KING — جني مبكر مدروس','بيع قبل الارتداد بصافي +'+ex.netNow.toFixed(2)+'$');
        await flattenAt(S.lastPrice||0,'جني مبكر مدروس');
        _harvestCleanup(S.lastPrice||0); // دورة مكتملة — إعادة اختيار الأفضل تبدأ
        return; } }
    if(!S.position&&S.status==='running'){
      // إعادة الاختيار الإجباري بعد الجني — قبل أي دخول جديد (قاعدة المالك)
      postHarvestReselect().catch(()=>{});
      if(openNextReady()) return;
      backgroundPipeline().catch(()=>{});
      // مهلة 45 ثانية + حاجز الدراسة يكملان حماية العملة الحالية من التبديل المتسارع
      maybeSwitchSymbol().catch(()=>{}); }
    // انضباط الاتجاه: أي انقلاب يلغي فورًا سلم الجهة القديمة المسلّح —
    // بقاؤه بعد الانقلاب كان يملأ مستويات معاكسة فوق مركز جديد فيغلقه
    // بخسارة عابرة ويزرع شورتًا ولونغًا في الوقت نفسه
    const _prevDir=S.config.direction; resolveDirection(Date.now());
    if(S.config.direction!==_prevDir){ S.gridAnchor=null;
      for(const g of S.grid){ if(g.reduceOnly||g.filledAt) continue;
        if(g.status!=='armed'&&g.status!=='open') continue;
        const gs=g.side==='sell'?'short':'long';
        if(gs!==S.config.direction){ g.status='cancelled'; g.exchangeOrderId=null; } } }
    if(running&&price>0){
      await patienceLimit(price);
      await circuitBreak(price);
      await escapeAdverse(price);
      harvestRipe(price);
      // الورقي: التعبئة عند تقاطع السعر محليًا. الحقيقي: التعبئة تأتي من المنصة
      // عبر creditExFill في المزامنة — التعبئة المحلية المزدوجة كانت تُنتج
      // كميات وهمية تخالف المركز الفعلي (سبب اختلاف الكمية/الهامش عن المنصة)
      const due=S.grid.filter(l=>shouldFill(l,price));
      if(S.config.mode!=='live') for(const l of selectDueAdds(due,price)){
        const before=filledAdds(); fillLevel(l.id,l.price,false);
        if(filledAdds()>before) S.lastAddAt=Date.now(); }
      const hunt=huntTrigger(price);
      if(hunt&&!S.permDenied&&price>0){ let done=false;
        if(S.config.mode==='live'&&S.keys){
          try{ await exPlaceMarket(S.config.symbol,hunt.side,hunt.qty); done=true; }
          catch(e){ const m=String((e as any)?.message||e);
            if(/access denied|permission/i.test(m)){ S.permDenied=true;
              pushLog('error','⛔ المفتاح بلا صلاحية تداول — فعّل «التداول» للعقود الآجلة في KuCoin ثم احفظ المفاتيح مجددًا');
              toast('⚠️ فعّل صلاحية التداول في مفتاح KuCoin'); }
            else { if(Date.now()-_huntErrAt>60000){ _huntErrAt=Date.now();
              pushLog('error','فشل صيد السوق: '+m); } } } }
        else done=true;
        if(done) maybeHunt(price); }
      ensureGrid(); harvestRipe(price);
    }
    pruneGhosts(); markUnrealized(price||m.price);
    // مزامنة المنصة كل 5 ثوانٍ تكفي — كل ثانية كانت تستنزف حصة الطلبات والبطارية
    if(!S._liveSyncAt||Date.now()-S._liveSyncAt>5000){ S._liveSyncAt=Date.now();
      try{ await liveSync(); }catch(e){ logSwallow("silent-catch", e); } }
    try{ maybeReport(); }catch(e){ logSwallow("silent-catch", e); }
    try{ heartbeatBeat(price||m.price); }catch(e){ logSwallow("silent-catch", e); }
    emit(); saveAll();
  }catch(e){ S.lastTickAt=Date.now();
    if(!/abort|timeout|429|50[0-4]|fetch/i.test(String((e as any)?.message||''))) pushLog('error',String((e as any)?.message||e)); }
}

/* ---------- أوامر التشغيل ---------- */
export function startBot(){
  try{ pushLog('server','⚙️ الإعدادات الفعّالة: '+(S.config.mode==='live'?'حقيقي':'ورقي')+' · '+S.config.displaySymbol+
    ' · رافعة '+S.config.leverage+'× · مستويات '+S.config.levels+' · خطوة '+S.config.gridStepPct+'%'+
    ' · صيد '+S.config.huntPct+'% · جني '+(Number(S.config.tpUsd)>0?('ثابت '+S.config.tpUsd+'$'):('مدروس '+S.config.tpNet+'$'))+
    ' · رصيد الدورة '+S.config.cycleBalance+'$ · حد الأوامر '+(S._cycleMaxFills||4)); }catch{} if(R()){ remoteCmd('start').then(()=>remoteSync()); return; }
  if(S.status==='running') return;
  if(S.config.mode==='live'&&!S.keys){ toast('اربط مفاتيح KuCoin أولاً من الإعدادات'); return; }
  const p=S.lastPrice||S.markPrice;
  if(!p){ toast('تعذر قراءة السعر — تحقق من الاتصال والزوج'); return; }
  if(S.status==='paused'&&S.position){ S.status='running'; pushLog('info','استئناف البوت'); emit(); saveAll(); return; }
  S.status='running'; S.startedAt=Date.now(); S.heartbeat=0;
  // عملة معروفة البصمة = إحماء قصير · جديدة = دراسة 60 ثانية قبل أي دخول
  const pf=profFresh(); startStudy(!!pf);
  if(!pf) pushLog('server','دراسة '+S.config.displaySymbol+' 60 ثانية — قياس التقلب والسبريد والنشاط قبل أي دخول');
  S.huntAnchor=p; S.gridAnchor=S.position?S.position.entry:p; S._flipAt=null;
  resolveDirection(Date.now());
  if(!S.grid.filter(g=>!g.reduceOnly&&(g.status==='armed'||g.status==='open')).length){
    S.grid=[...S.grid.filter(g=>g.reduceOnly&&(g.status==='open'||g.status==='armed')),
      ...buildGrid(S.position?S.position.entry:p,false,4)]; }
  pushLog('server','دورة جديدة '+(S.config.direction==='short'?'شورت':'لونغ')+
    (S.regime?' · '+S.regime.label:'')+' — شبكة فعّالة '+effLevels()+' من '+S.config.levels+
    ' (تركيز يناسب رأس المال) والصيد يعمل');
  nativeNotify('AB-KING يعمل ✓','البوت متصل بـ KuCoin ويتداول '+(S.config.displaySymbol||S.config.symbol)+' — يستمر حتى في الخلفية',true);
  toast('البوت يعمل الآن'); emit(); saveAll(); }
export function pauseBot(){ if(R()){ remoteCmd('pause').then(()=>remoteSync()); return; }
  if(S.status!=='running') return;
  S.status='paused';
  for(const l of S.grid){ if(!l.reduceOnly&&(l.status==='open'||l.status==='armed')){
    l.status='cancelled'; l.exchangeOrderId=null; }
    else if(l.reduceOnly&&l.status==='open'&&S.config.mode==='live'){
      l.status='armed'; l.exchangeOrderId=null; } }
  if(S.config.mode==='live'&&S.keys) exCancelAll(S.config.symbol).catch(()=>{});
  pushLog('info','إيقاف مؤقت — لا صفقات جديدة، الجني مستمر');
  emit(); saveAll(); }
export async function stopBot(){ if(R()){ await remoteCmd('stop'); await remoteSync(); return; }
  if(S.status==='idle') return;
  const p=S.lastPrice||S.markPrice||0, prevSide=S.position?S.position.side:null;
  if(S.config.mode==='live'&&S.keys&&prevSide){
    try{ await exCancelAll(S.config.symbol); await exCancelStops(S.config.symbol); S._guardId=null; }catch(e){ logSwallow("silent-catch", e); }
    // الإغلاق يجب أن يتأكد لا أن يُرسل ويُنسى: أعد المحاولة حتى يتسطح المركز
    // فعلًا على المنصة — الإرسال الأعمى القديم ترك مركزه مفتوحًا وأغلق محليًا فقط
    let flat=false;
    for(let i=0;i<4&&!flat;i++){
      try{ await exClose(S.config.symbol,prevSide); }catch(e){ logSwallow("silent-catch", e); }
      try{ await new Promise(r=>setTimeout(r,700));
        const pos=await exPosition(S.config.symbol); flat=!pos; }catch(e){ logSwallow("silent-catch", e); }
    }
    S.ignoreExchangeUntil=Date.now()+15000; // لا تستعد مركزًا يُسوّى الآن كشبح
    if(!flat){ pushLog('error','⚠️ إغلاق المنصة لم يتأكد بعد 4 محاولات — راجع المركز يدويًا في KuCoin فورًا');
      notify('تحذير: إغلاق المركز على المنصة لم يتأكد — تحقق يدويًا فورًا','warn'); } }
  await flattenAt(p,'إيقاف'); S.status='stopped';
  clearNativeOngoing();
  pushLog('info','إيقاف — أُغلقت كل الصفقات عند السعر الحالي');
  toast('تم إيقاف البوت'); emit(); saveAll(); }
export function newCycle(){ if(R()){ remoteCmd('newCycle').then(()=>remoteSync()); return; } // الرصيد يحمل الأرباح المجناة أصلًا (تُضاف لحظة الجني) — لا جمع مزدوج
  const rolled=Math.max(0.01,Math.round(S.config.cycleBalance*100)/100);
  const keep=S.status==='running'||S.status==='paused';
  if(S.config.mode==='live'&&S.keys){ exCancelAll(S.config.symbol).catch(()=>{});
    exCancelStops(S.config.symbol).catch(()=>{}); S._guardId=null;
    // لا تيتيم لمراكز حقيقية: دورة جديدة بمركز مفتوح تُغلقه على المنصة أولًا
    if(S.position) exCloseQty(S.config.symbol,S.position.side,S.position.size)
      .catch(e=>pushLog('error','إغلاق مركز الدورة السابقة فشل: '+(e.message||e))); }
  // نافذة تجاهل 12 ثانية: لا تستعد المركز المغلق للتو كـ«شبح» قبل أن تسوّيه المنصة
  S.ignoreExchangeUntil=Date.now()+12000;
  // قبل المسح: تُحسب حصيلة الدورة الجديدة
  const cfg={...S.config,cycleBalance:rolled};
  const keys=S.keys, snd=S.sound, tone=S.soundTone;
  // مسح كامل: سجل الصفقات + الأرشيف + سجل السيرفر — بداية نظيفة كل دورة
  Object.assign(S,{grid:[],position:null,journal:[],history:[],logs:[],realizedPnl:0,feesPaid:0,
    activeCycle:null,huntCount:0,huntOpen:0,priceTrail:[],emaFast:null,emaSlow:null,
    cvd:0,tape:[],orderBook:{bids:[],asks:[]},status:'idle',startedAt:null,
    huntAnchor:null,gridAnchor:null,liqPrice:null,exLiqPrice:null,_memBlockAt:0,
    cycleHarvested:0,fundingPaid:0,slipCost:0,_fundAt:0,_tpBanked:0,_posPeakNet:null,_spikeDone:false,_posNetPrev:null,_spikePosId:null});
  S.config=cfg; S.keys=keys; S.sound=snd; S.soundTone=tone;
  pushLog('info','دورة جديدة برصيد $'+rolled.toFixed(2)+' — مُسحت السجلات وبدأت صفحة نظيفة');
  toast('دورة جديدة برصيد $'+rolled.toFixed(2));
  emit(); saveAll(); if(keep) startBot(); }

/* ---------- إجراءات الإعدادات ---------- */
export async function saveKeys(k){
  if(R()){ const r=await remoteCmd('saveKeys',{keys:k}); await remoteSync();
    if(r&&r.ok) toast('أُرسلت المفاتيح للسيرفر ✓'); else toast('السيرفر رفض المفاتيح: '+((r&&r.error)||'تحقق منها'));
    return !!(r&&r.ok); }
  if(!k.apiKey||!k.apiSecret||!k.passphrase){ toast('أدخل المفتاح والسر والعبارة'); return false; }
  S.keys=k; S.linkOk=false; saveAll(); emit();
  try{ const eq=await exPing();
    S.linkOk=true; S.exEquity=eq; S.permDenied=false;
    toast('تم الربط ✓ الرصيد $'+(eq!=null?eq.toFixed(2):'—'));
  }catch(e){ S.linkOk=false; S.exEquity=null;
    toast('حُفظت المفاتيح لكن فشل الاتصال: '+String((e as any)?.message||e)); }
  emit(); return S.linkOk;
}
export function clearKeys(){ if(R()){ remoteCmd('clearKeys').then(()=>remoteSync()); return; }
  S.keys=null; S.linkOk=false; S.exEquity=null; saveAll(); emit(); toast('حُذفت المفاتيح'); }
export function saveCfg(v){
  if(R()){ remoteCmd('saveCfg',{cfg:v}).then(()=>remoteSync()); return; }
  const old=S.config.symbol;
  const oldLv=S.config.leverage, oldN=S.config.levels,
    oldStep=S.config.gridStepPct, oldBal=S.config.cycleBalance;
  const sym=(v.symbol||old).trim().toUpperCase();
  S.config.symbol=sym;
  S.config.displaySymbol=sym.replace(/USDTM$/i,'').replace(/^XBT$/i,'BTC');
  S.config.leverage=clamp(Math.round(+v.leverage||3),1,100);
  S.config.levels=clamp(Math.round(+v.levels||12),1,64);
  S.config.gridStepPct=clamp(+v.gridStepPct||0.3,0.05,5);
  S.config.huntPct=clamp(+v.huntPct||0.15,0.03,5);
  // هدف جني الربح الصافي ($) — يُحفظ فقط إن أُرسل صراحة حتى لا يصفّره حفظ عارض
  if(v.tpNet!=null&&v.tpNet!=='') S.config.tpNet=clamp(+v.tpNet||TP_BASE_NET,0.03,50);
  // TRG-NEW: هدف الجني الثابت بالدولار — يُحفظ فقط إن أُرسل صراحة (0 = مدروس)
  if(v.tpUsd!=null&&v.tpUsd!=='') S.config.tpUsd=clamp(+v.tpUsd||0,0,1000);
  S.config.cycleBalance=Math.max(1,+v.cycleBalance||100);
  S.config.directionMode=v.directionMode;
  S.config.mode=v.mode;
  S.sound=!!v.sound;
  if(v.soundTone) S.soundTone=v.soundTone;
  if(sym!==old){
    // ─── قاعدة المالك (مصدر المراكز الثمانية): لا انتقال بين العملات والمركز
    // مفتوح. كان الانتقال يطلق إغلاقاً بلا انتظار تأكيد → فشل الإغلاق (429/هامش)
    // يترك مركزاً حقيقياً يتيماً على المنصة. الآن: التبديل مرفوض حتى تُجنى ───
    if(S.config.mode==='live'&&S.keys&&(S.position||S._exOpenCount>0||!S._flatConfirmed)){
      S.config.symbol=old;
      S.config.displaySymbol=old.replace(/USDTM$/i,'').replace(/^XBT$/i,'BTC');
      pushLog('server','⛔ لا تبديل إلى '+sym+' — '+(S.position?'والمركز الحالي مفتوح':(!S._flatConfirmed?'بلا تأكيد صفرية من المنصة بعد':'مراكز أخرى حية'))+' — الصمود حتى تتأكد المنصة ثم يُسمح بالانتقال');
      return; }
    // الوضع الحقيقي: ألغِ أوامر الزوج السابق وأوقفه وأغلق مركزه قبل الانتقال — لا أوامر يتيمة بلا رقيب
    if(S.config.mode==='live'&&S.keys){ exCancelAll(old).catch(()=>{});
      exCancelStops(old).catch(()=>{});
      if(S.position) exCloseQty(old,S.position.side,S.position.size)
        .catch(e=>pushLog('error','إغلاق مركز الزوج السابق فشل: '+(e.message||e)));
      pushLog('server','أُلغيت أوامر ووقف '+old+' وأُغلق مركزه قبل الانتقال'); }
    S.grid=[]; S.position=null; S._tpBanked=0; S._trailFloor=0; // سجل الصفقات يبقى — التاريخ لا يُمسح عند الانتقال
    S.realizedPnl=0; S.feesPaid=0; S.fundingPaid=0; S.slipCost=0; S._fundAt=0; S.ignoreExchangeUntil=Date.now()+12000;
    S.tape=[]; S.priceTrail=[]; S.emaFast=S.emaSlow=null; S._metaAt=0;
    // مضاعف العملة السابقة سمّ زاعف للجديدة (عقد PEPE = 520 ألف وحدة!) —
    // يُصفَّر ويُحجب التداول الحقيقي حتى تُوثَّق مواصفات العقد الجديد
    S.multiplier=1; S.tickSize=0.1; S._metaSym=null; S.maxOrderQty=0;
    S.lastPrice=null; S.markPrice=null; S.lastTickAt=null; S.cvd=0;
    S.orderBook={bids:[],asks:[]}; S.biasScore=0; S.biasReasons=[];
    S.regime=null; S.confluence=null; S.heartbeat=0;
    S.liqPrice=null; S.exLiqPrice=null; S.huntAnchor=null; S.gridAnchor=null;
    // تصفير حالة الإشارات الجديدة — كل عملة تُقرأ من صفر بمعطياتها وحدها
    S._brk=null; S._ofi=0; S._ofiWin=[]; S._prevBook=null;
    S._btcImp=null; S._btcPrev=null; S._actHist=[]; S._actMed=0; S._actAt=0; S._tradeTs=[];
    S._lossStreak=null; S._lastSig=null; S._vetoAt=0; S._guardPx=0; S._guardLogged=false; S._guardId=null;
    setStreamSymbol(sym);
    // مواصفات العقد الجديد فوراً — لا تُترك للمزامنة البطيئة (429 يطيلها):
    // وطالما البوابة مغلقة بلا مواصفات، النافذة بلا حجم بأي حال (صمام مزدوج)
    fetchContract(sym).then((ct:any)=>{ if(ct&&+ct.multiplier>0&&S.config.symbol===sym){
      S.multiplier=+ct.multiplier; S.tickSize=+ct.tickSize||S.tickSize;
      S._metaSym=sym; S._metaAt=Date.now(); if(+ct.maxOrderQty>0) S.maxOrderQty=+ct.maxOrderQty;
      pushLog('server','📐 مواصفات '+S.config.displaySymbol+' موثقة: مضاعف العقد '+S.multiplier+' — فُتح باب الحجم'); } }).catch(()=>{});
    // ─── درس الأرقام الخيالية: صفوف الزوج السابق المفتوحة كانت تُقفل بعملة
    // جديدة بمضاعفاتها (صف DOGE بمضاعف PEPE = ±40K وهمية تسمم الرصيد!) —
    // تبديل العملة يعني موت كل صف مفتوح: تُزال ولا تُطابق أبداً ───
    const __staleOpen=S.journal.filter(j=>j.status==='open');
    if(__staleOpen.length){ S.journal=S.journal.filter(j=>j.status!=='open');
      pushLog('server','🧹 أُلغيت '+__staleOpen.length+' صفقات الزوج السابق المفتوحة — سياق قديم لا يُقفل بعملة جديدة'); }
    S._dwellAt=Date.now(); // بداية إقامة جديدة: لا تبديل جديد قبل 10 دقائق عمل فعلي
    S._slideHi=null; S._slideHiAt=0; S._slideLo=null; S._slideLoAt=0; S._slideVetoAt=0;
    // بصمة العملة الجديدة: معروفة وحديثة = إحماء قصير · جديدة = دراسة كاملة قبل التداول
    const pf=S.memory&&S.memory.profiles&&S.memory.profiles[sym];
    startStudy(!!(pf&&Date.now()-pf.at<24*3600*1000));
    pushLog('server','انتقال إلى '+S.config.displaySymbol+' — أُلغيت حالة الزوج السابق'+
      (pf&&Date.now()-pf.at<24*3600*1000?' · بصمتها معروفة — إحماء قصير':' · تُدرس 60 ثانية قبل أي دخول')); }
  else if(S.status==='running'&&
    (oldLv!==S.config.leverage||oldN!==S.config.levels||
     oldStep!==S.config.gridStepPct||oldBal!==S.config.cycleBalance)){
    // تغيّر رأس المال/الرافعة/المستويات/الخطوة أثناء التشغيل: أعد بناء الشبكة فورًا
    // بالأحجام والعدد الفعّال المحسوبين من القيم الجديدة — لا تبقَ شبكة بمعاملات قديمة
    cancelPendingAdds();
    if(S.config.mode==='live'&&S.keys) exCancelAll(sym).catch(()=>{});
    const c=S.lastPrice||S.gridAnchor||(S.position?S.position.entry:0);
    if(c>0){ const keep=S.grid.filter(g=>g.status==='filled'||g.status==='cancelled'||
        (g.reduceOnly&&(g.status==='open'||g.status==='armed')));
      S.grid=[...keep,...buildGrid(c,false,4)]; S.gridAnchor=c;
      S.grid.sort((a,b)=>b.price-a.price); }
    pushLog('server','أُعيد بناء الشبكة بالمعاملات الجديدة — '+effLevels()+' مستوى فعّال'); }
  saveAll(); emit(); toast('تم حفظ المعاملات');
}

/* ---------- مساعدات العرض ---------- */
// الربح المجني يُضاف لحظيًا لرصيد الدورة — الحقوق = الرصيد الحالي + غير المحقق فقط (لا ازدواج)
export function equity(){ return S.config.cycleBalance+
  (S.position?S.position.unrealized:0); }
export function desk(){ const j=S.journal;
  const closed=j.filter(r=>r.status!=='open'), open=j.filter(r=>r.status==='open');
  return {win:closed.filter(r=>r.pnl>0), lose:closed.filter(r=>r.pnl<0),
    open, winPnl:closed.filter(r=>r.pnl>0).reduce((a,r)=>a+r.pnl,0),
    losePnl:closed.filter(r=>r.pnl<0).reduce((a,r)=>a+r.pnl,0),
    fees:S.feesPaid}; }
export function pulseAge(){ if(!S.lastTickAt) return 'لا نبض';
  const s=Math.max(0,Math.round((Date.now()-S.lastTickAt)/1000));
  return s<5?'الآن':s<60?s+' ث':'قبل '+Math.floor(s/60)+' د'; }
export { fmtPx, fmtUsd, fmtTime };

/* ---------- الإقلاع ---------- */
let booted=false;
export function initEngine(){
  if(booted) return; booted=true;
  if(typeof window!=='undefined') window.__S=S; // للفحص والتشخيص
  // إذن الإشعارات يُطلب عند الإقلاع في الوضعين — بلا إذن لا يصل أي تنبيه خارج التطبيق
  try{ const LN=lnPlugin(); if(LN){ ensureChannels(LN); LN.requestPermissions().catch(()=>{}); } }catch(e){ logSwallow("silent-catch", e); }
  // وضع السيرفر: المحرك يعيش في Termux — هنا مرآة حية فقط، لا محرك محلي إطلاقًا
  // (تشغيل محركين معًا = أوامر مزدوجة على المنصة، لذلك لا سقوط تلقائي للوضع المحلي أبدًا)
  if(R()){ pushLog('info','وضع سيرفر الجوال — المحرك يعمل في Termux بلا نوم');
    remoteSync(); setInterval(remoteSync,1200); setInterval(emit,500); return; } // نبض مرآة أسرع: فجوة الاكتشاف ≤1.2 ث بدل 2 ث
  // لم يُعثر على السيرفر الآن (مثلاً HTTP محجوب أو Termux لم يبدأ بعد) — جسّ نبض دوريًا:
  // أول اكتشاف ناجح يوقف المحرك المحلي فورًا ويتحول لوضع المرآة (لا أوامر مزدوجة أبدًا)
  const _detect=setInterval(async()=>{
    if(R()){ clearInterval(_detect); return; }
    const found=await detectRemoteServer().catch(()=>false);
    if(found){ clearInterval(_detect);
      pushLog('info','تم اكتشاف سيرفر الجوال — التحول لوضع المرآة، المحرك المحلي توقف');
      try{ S.status='idle'; if(S.config.mode!=='live'){ S.grid=[]; S.position=null; } saveAll(); }catch(e){ logSwallow("local-stop", e); }
      remoteSync(); setInterval(remoteSync,(typeof window!=='undefined'&&(window as any).__FLEET_POLL_SLOW)?2500:1200); }
  },5000);
  loadAll();
  if(S.config.symbol==='BTCUSDTM') S.config.symbol='XBTUSDTM'; // الرمز الصحيح في عقود KuCoin
  S.config.displaySymbol=S.config.symbol.replace(/USDTM$/i,'').replace(/^XBT$/i,'BTC');
  pushLog('info','AB-KING جاهز — '+(S.config.mode==='live'?'وضع LIVE':'وضع ورقي'));
  // قناة الدفع المستمرة — تتصل فورًا وتعيد الاتصال تلقائيًا عند أي انقطاع
  startStream(S.config.symbol,{ onMessage:onStreamTick, onStatus:()=>emitThrottled() });
  setInterval(botTick,TICK_MS);
  setInterval(emit,1000);
  setInterval(()=>{ if(S.status==='idle') botTick().catch(()=>{}); },6000);
  // تحديث رصيد المنصة باستمرار عند وجود مفاتيح — يظهر في الإعدادات فورًا
  const refreshBal=()=>{ if(!S.keys) return;
    exPing().then(eq=>{ if(eq!=null){ S.exEquity=eq; S.linkOk=true; } })
      .catch(()=>{ S.linkOk=false; }); };
  refreshBal(); setInterval(refreshBal,20000);
  // رادار الفرص: مسح شامل كل 3 دقائق — أول قراءة بعد استقرار القناة
  setTimeout(()=>{ scanRadar().catch(()=>{}); },15000);
  // continuous full-universe scan every 45s - every contract on the platform is watched nonstop
  setInterval(()=>{ scanRadar().catch(()=>{}); },45000);
  // البقاء الصوتي الصامت: يعمل دائمًا على الجوال حتى لا يُخنق المحرك بإطفاء الشاشة
  startSilentKeepAlive();
  // خدمة أمامية + استثناء البطارية — بقاء حقيقي في الخلفية لا يعتمد على الحيل الصوتية وحدها
  startForegroundSvc();
  // قفل المعالج + منعّاش أصلي: لا نوم للمؤقتات، وإنعاش تلقائي إن جمّد النظام الويب فيو
  startNativeKeepAlive();
  logRevival();
  // عند العودة من الخلفية: دورة محرك فورية لتعويض أي فترة خنق + إعادة فحص الأرباح الناضجة
  if(typeof document!=='undefined') document.addEventListener('visibilitychange',()=>{
    if(!document.hidden){ botTick().catch(()=>{});
      if(S.lastPrice) try{ harvestRipe(S.lastPrice); }catch(e){ logSwallow("silent-catch", e); } } });
  emit();
}
