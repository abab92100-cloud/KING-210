/* ================================================================
   TRQ-TRADING — سيرفر الجوال (Termux) بوضع الأسطول
   · المنسّق (بلا ABKING_SLOT): يشغّل 5 محركات مستقلة، كل عملة لمحرك
     واحد، رأس المال الكلي ÷ 5 تلقائياً، كل محرك يجني ربحه مستقلاً
   · المحرك (ABKING_SLOT=1..5): نفس المحرك المثبّت على منفذ خاص به
   التشغيل:  node king-server.mjs   (المنسق يحضن المحركات الخمسة)
   ================================================================ */
import './server-shims'; // أولًا دائمًا: يثبّت البيئة قبل تقييم المحرك
import {
  S, initEngine, startBot, pauseBot, stopBot, newCycle,
  saveCfg, saveKeys, clearKeys, scanRadar, pushLog, liveSync, repairAccount,
} from './engine';

const SRV_VER = 'KING-1.8.8';
const FLEET_N = 1;
const BASE_PORT = 9010; // منفذ أسطول TRG-NEW — مستقل عن AB-KING (8787) وعن TRQ (8987)
const SLOT = parseInt(String((process.env as any).ABKING_SLOT || ''), 10) || 0;

import { createServer } from 'node:http';
import { spawn } from 'node:child_process';

function send(res, code, obj) {
  res.writeHead(code, {
    'Content-Type': 'application/json; charset=utf-8',
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'GET,POST,OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type',
    'Cache-Control': 'no-store',
  });
  res.end(JSON.stringify(obj));
}
async function jfetch(url, ms = 2500, payload?) {
  const c = new AbortController(); const t = setTimeout(() => c.abort(), ms);
  try {
    const r = await fetch(url, payload ? { method: 'POST', signal: c.signal, headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(payload) } : { signal: c.signal });
    clearTimeout(t); return await r.json();
  } catch { clearTimeout(t); return null; }
}

const PORT = BASE_PORT + SLOT;

/* ─── وضع المحرك (قطاع من الأسطول) ─── */
if (SLOT > 0) {
  pushLog('server', '✅ ' + SRV_VER + ' — محرك الأسطول رقم ' + SLOT + '/' + FLEET_N + ' — نصيبه من رأس المال: ÷' + FLEET_N + ' تلقائياً — تبديله وجنيه مستقلان');
  // مزامنة عملات المحركات الأخرى من المنسق كل 30 ث — التنويع المركزي
  let bootPickDone = false;
  const fleetSync = () => jfetch('http://127.0.0.1:' + BASE_PORT + '/fleet?i=' + SLOT).then(f => {
    if (f && Array.isArray(f.taken)) {
      S._fleetTaken = f.taken.map((t: any) => t.sym); // راداري يستبعد عملات المحركات الأخرى
      // تعيين الإقلاع الحتمي: المحرك N يختار العملة رقم N من راداره —
      // يمنع تقارب المحركات على نفس العملة قبل اكتمال مزامنة التنويع
      // ─── حسم التكرار (مصدر الصفقات المتراكمة): محركان على نفس العملة =
      // مركز منصّة واحد يتضخم، ومحرك «يجني» بينما شريكه يضخ بأوامر.
      // الحسم حتمي: صاحب الرقم الأدنى يحتفظ، والآخر ينتقل لأفضل عملة حرة ───
      if (!S.position && S.config.symbol) {
        const duel = f.taken.filter((t: any) => t.sym === S.config.symbol);
        if (duel.length) {
          const winner = Math.min(...duel.map((t: any) => Number(t.holder) || 99), SLOT);
          if (SLOT > winner) {
            const takenSet = new Set(f.taken.map((t: any) => t.sym));
            const cand = (S.radar && Array.isArray(S.radar.list) ? S.radar.list : [])
              .find((x: any) => x && x.symbol && !takenSet.has(x.symbol) && x.symbol !== S.config.symbol);
            if (cand) {
              pushLog('server', '⚖️ تكرار على ' + S.config.displaySymbol + ' مع المحرك ' + winner + ' — ينتقل هذا المحرك إلى ' + (cand.disp || cand.symbol));
              try { saveCfg({ symbol: cand.symbol, leverage: S.config.leverage, levels: S.config.levels,
                gridStepPct: S.config.gridStepPct, huntPct: S.config.huntPct, tpUsd: S.config.tpUsd,
                cycleBalance: S.config.cycleBalance, directionMode: S.config.directionMode, mode: S.config.mode }); } catch { /* optional */ }
              return;
            }
          }
        }
      }
      if (!bootPickDone && !S.position && S.config.symbol === 'XBTUSDTM' &&
          S.radar && Array.isArray(S.radar.list) && S.radar.list.length) {
        // ─── دراسة الإقلاع (أمر المالك: لا اختيار عشوائي): لا دخول إلا لعملة
        // درجتها 75+ وسيولتها اليومية قوية، وإقلاع متدرج 15ث لكل محرك ───
        const bootStarted=(S as any)._bootAt||((S as any)._bootAt=Date.now());
        if(Date.now()-bootStarted < (SLOT-1)*15000) return;
        const qual=S.radar.list.filter((x:any)=>x&&x.symbol&&x.symbol!=='XBTUSDTM'&&
          (Number(x.score)||0)>=75&&(Number(x.vol24h)||Number(x.turnover)||Number(x.volume)||1e18)>=3e6);
        const pick = qual[Math.min(SLOT - 1, Math.max(0, qual.length - 1))] ||
          S.radar.list[Math.min(SLOT - 1, S.radar.list.length - 1)];
        if (pick && pick.symbol && pick.symbol !== 'XBTUSDTM') {
          bootPickDone = true;
          pushLog('server', '🎯 تعيين الإقلاع: المحرك ' + SLOT + ' يبدأ على ' + pick.disp + ' (المرتبة ' + SLOT + ' في الرادار)');
          try { (require('./engine') as any).saveCfg({
            symbol: pick.symbol, leverage: S.config.leverage, levels: S.config.levels,
            gridStepPct: S.config.gridStepPct, huntPct: S.config.huntPct,
            cycleBalance: S.config.cycleBalance, directionMode: S.config.directionMode,
            mode: S.config.mode }); } catch{ /* intentional: optional step */ }
          return;
        }
      }
      const clash = f.taken.find((t: any) => t.sym === S.config.symbol);
      // القاعدة: الأدنى رقمًا يبقى والأعلى ينتقل — تقارب مستقر بلا تراش
      if (clash && clash.holder < SLOT && !S.position) {
        S._harvestAt = Date.now(); S._reselectDone = false;
        pushLog('server', '🚦 تنويع الأسطول: عملتي يسكنها المحرك ' + clash.holder + ' — انتقالي لأفضل بديل متاح');
      }
    }
  }).catch(() => {});
  setTimeout(fleetSync, 2500);
  setInterval(fleetSync, 10000).unref?.();

  setTimeout(() => {
    liveSync().then(() => pushLog('server', '🔧 اكتملت مراجعة الوضع عند إقلاع المحرك ' + SLOT + ' — متطابق مع المنصة')).catch(() => {});
    scanRadar().catch(() => {});
  }, 1500 + SLOT * 300);
}

/* ─── وضع المنسّق (الأصل) ─── */
const slotStates: any[] = []; // آخر حالة لكل محرك
async function pollSlots() {
  for (let i = 1; i <= FLEET_N; i++) {
    const j = await jfetch('http://127.0.0.1:' + (BASE_PORT + i) + '/state', 2000);
    if (j && j.ok && j.state) slotStates[i] = j.state;
  }
}
setInterval(pollSlots, 10000).unref?.();

function mergedState(selIn = 0) {
  const live: any[] = [];
  for (let i = 1; i <= FLEET_N; i++) if (slotStates[i]) live.push(slotStates[i]);
  if (!live.length) return null;
  // الأساسي: المحرك المختار من الواجهة (الضغط على عملة في الشريط)، وإلا
  // أول محرك بمركز مفتوح، وإلا المحرك الأول
  const idx = selIn >= 1 && selIn <= FLEET_N && slotStates[selIn] ? selIn : 0;
  const primary = idx ? slotStates[idx] : (live.find(x => x.position) || live[0]);
  const st: any = { ...primary };
  // سجل الصفقات لكل الأسطول (مع رمز العملة في كل صف) — والعدادات تُحسب منه أسطولياً
  st.journal = live.flatMap((x, li) => (x.journal || []).map((r: any) => ({ ...r, _slot: li + 1 })))
    .sort((a, b) => (b.at || 0) - (a.at || 0)).slice(0, 150);
  // سجل السيرفر: تفاصيل المحرك المعروض فقط (العملة المختارة)
  st.logs = (primary.logs || []).slice(-150);
  st.realizedPnl = live.reduce((a, x) => a + (x.realizedPnl || 0), 0);
  st.feesPaid = live.reduce((a, x) => a + (x.feesPaid || 0), 0);
  st.cycleHarvested = live.reduce((a, x) => a + (x.cycleHarvested || 0), 0);
  st.slipCost = live.reduce((a, x) => a + (x.slipCost || 0), 0);
  st.fundingPaid = live.reduce((a, x) => a + (x.fundingPaid || 0), 0);
  // الرصيد الكلي = مجموع أنصبة المحركات (لعرضه في الواجهة كرصيد واحد)
  st.config = { ...(primary.config || {}), cycleBalance: live.reduce((a, x) => a + (x.config?.cycleBalance || 0), 0) };
  st.status = live.some(x => x.status === 'running') ? 'running' : (live[0]?.status || 'idle');
  st.fleet = live.map((x, idx) => ({
    slot: idx + 1, symbol: x.config?.symbol, disp: x.config?.displaySymbol,
    position: !!x.position, net: x.position ? x.position.unrealized : 0,
    balance: x.config?.cycleBalance, running: x.status === 'running',
  }));
  return st;
}

const srv = createServer(async (req, res) => {
  try {
    if (req.method === 'OPTIONS') {
      res.writeHead(204, {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Methods': 'GET,POST,OPTIONS',
        'Access-Control-Allow-Headers': 'Content-Type',
      });
      return res.end();
    }
    const u = new URL(req.url || '/', 'http://127.0.0.1');

    if (req.method === 'GET' && (u.pathname === '/' || u.pathname === '/health'))
      return send(res, 200, { ok: true, name: 'TRG-NEW Fleet Server', version: SRV_VER, fleet: FLEET_N, at: Date.now() });

    if (SLOT > 0) {
      // ─── محرك قطاع: نفس واجهة المحرك الأصيلة ───
      if (req.method === 'GET' && u.pathname === '/state') {
        // الصفوف المفتوحة تحمل ربحها العائم محسوباً بمضاعف وسعر عملتها
        // نفسها (لا عملة العرض في الواجهة المدمجة) — إصلاح الأرقام الخيالية
        const lp=S.lastPrice||0, mult=S.multiplier||1, taker=S.takerFee||0.0006;
        // حمولة أنحف: آخر 30 صفقة و80 سجل سيرفر — يخفف حمل الجوال ويحمي نبض القنوات
        const st: any={...S, journal:(S.journal||[]).slice(-30).map((r:any)=>{
          if(r.status!=='open') return r;
          const feeEst=lp? r.qty*mult*lp*taker:0;
          const f=lp? ((r.side==='short'?1:-1)*(r.entry-lp)*mult*r.qty-(r.fees||0)-feeEst):0;
          return {...r, floatNet: Math.round((f+Number.EPSILON)*100)/100}; }),
          logs:(S.logs||[]).slice(-80)};
        return send(res, 200, { ok: true, at: Date.now(), state: st }); }
      if (req.method === 'POST' && u.pathname === '/cmd') {
        let body = '';
        for await (const chunk of req) body += chunk;
        let j: any = {};
        try { j = JSON.parse(body || '{}'); } catch{ /* intentional: optional step */ }
        const a = String(j.action || '');
        switch (a) {
          case 'start': startBot(); return send(res, 200, { ok: true });
          case 'pause': pauseBot(); return send(res, 200, { ok: true });
          case 'stop': await stopBot(); return send(res, 200, { ok: true });
          case 'newCycle': newCycle(); return send(res, 200, { ok: true });
          case 'saveCfg': saveCfg(j.cfg || {}); return send(res, 200, { ok: true });
          case 'saveKeys': {
            const ok = await saveKeys(j.keys || {});
            return send(res, 200, { ok: !!ok, error: ok ? null : 'فشل الاتصال بالمنصة — تحقق من المفاتيح' });
          }
          case 'clearKeys': clearKeys(); return send(res, 200, { ok: true });
          case 'scanRadar': await scanRadar(); return send(res, 200, { ok: true, radar: S.radar || null });
          case 'repair': { const rr = await repairAccount(); return send(res, 200, rr); }
          default: return send(res, 400, { ok: false, error: 'إجراء غير معروف: ' + a });
        }
      }
      send(res, 404, { ok: false, error: 'not found' });
      return;
    }

    /* ─── المنسّق: الحاضنة والواجهة الموحدة للتطبيق ─── */
    if (req.method === 'GET' && u.pathname === '/fleet') {
      const i = parseInt(u.searchParams.get('i') || '0', 10) || 0;
      const taken: any[] = [];
      for (let k = 1; k <= FLEET_N; k++) {
        if (k === i) continue;
        const st = slotStates[k];
        if (st && st.config && st.config.symbol) taken.push({ sym: st.config.symbol, holder: k });
      }
      return send(res, 200, { ok: true, taken });
    }

    if (req.method === 'GET' && u.pathname === '/state') {
      const sel = parseInt(u.searchParams.get('sel') || '0', 10) || 0;
      const st = mergedState(sel);
      if (!st) return send(res, 200, { ok: true, at: Date.now(), state: { status: 'idle', heartbeat: Date.now(), logs: [], journal: [], grid: [], config: { cycleBalance: 0 }, fleet: [] } });
      return send(res, 200, { ok: true, at: Date.now(), state: st });
    }

    if (req.method === 'POST' && u.pathname === '/cmd') {
      let body = '';
      for await (const chunk of req) body += chunk;
      let j: any = {};
      try { j = JSON.parse(body || '{}'); } catch{ /* intentional: optional step */ }
      const a = String(j.action || '');
      // saveCfg يُبث بلا رمز عملة — كل محرك يحتفظ بعملته (التنويع) — والرصيد
      // الكلي يقسمه المنسق مرة واحدة: كل محرك يستلم نصيبه (÷5) فوراً
      // repair: محرك واحد فقط (نفس المفاتيح والحساب — التكرار يكرر الأوامر)
      if (a === 'repair') {
        const r1 = await jfetch('http://127.0.0.1:' + (BASE_PORT + 1) + '/cmd', 45000, j);
        return send(res, 200, r1 || { ok: false, error: 'لم يرد المحرك' });
      }
      let payload = j;
      if (a === 'saveCfg' && j.cfg) {
        const { symbol, displaySymbol, ...rest } = j.cfg;
        if (rest.cycleBalance != null) rest.cycleBalance = Math.max(1, Math.round((+rest.cycleBalance / FLEET_N) * 100) / 100);
        payload = { ...j, cfg: rest };
      }
      const results = await Promise.all(
        Array.from({ length: FLEET_N }, (_, i) =>
          jfetch('http://127.0.0.1:' + (BASE_PORT + 1 + i) + '/cmd', 5000, payload)));
      const ok = results.some(r => r && r.ok);
      if (a === 'scanRadar') {
        const st = mergedState();
        return send(res, 200, { ok: true, radar: st ? (st.radar || null) : null });
      }
      return send(res, 200, { ok, fleet: results.map(r => !!(r && r.ok)) });
    }

    send(res, 404, { ok: false, error: 'not found' });
  } catch (e) {
    send(res, 500, { ok: false, error: String((e && (e as any).message) || e) });
  }
});

srv.listen(PORT, '127.0.0.1', () => {
  if (SLOT > 0) {
    console.log('[TRQ] محرك الأسطول ' + SLOT + ' يعمل على http://127.0.0.1:' + PORT);
  } else {
    console.log('[TRG] منسق أسطول TRG-NEW يعمل على http://127.0.0.1:' + PORT + ' — يُطلق ' + FLEET_N + ' محركات');
    pushLog('server', '🚀 أسطول TRQ-TRADING انطلق — ' + FLEET_N + ' محركات مستقلة، رأس المال ÷ ' + FLEET_N + ' تلقائياً، كل عملة بمحرك وجني مستقلين');
    // إطلاق المحركات الخمسة — نفس الملف بمعرف قطاع ومنفذ خاص
    const self = process.argv[1];
    for (let i = 1; i <= FLEET_N; i++) {
      const child = spawn(process.execPath, [self], {
        env: { ...process.env, ABKING_SLOT: String(i) },
        stdio: ['ignore', 'inherit', 'inherit'],
        detached: false,
      });
      child.on('exit', (code) => {
        console.log('[TRQ] محرك ' + i + ' خرج (' + code + ') — إعادة تشغيله خلال 5 ث');
        setTimeout(() => { try { spawn(process.execPath, [self], { env: { ...process.env, ABKING_SLOT: String(i) }, stdio: ['ignore', 'inherit', 'inherit'], detached: false }); } catch{ /* intentional: optional step */ } }, 5000);
      });
    }
    setTimeout(pollSlots, 4000);
    setInterval(() => {
      try {
        const st = mergedState();
        const fl = st ? (st.fleet || []) : [];
        pushLog('server', '💡 نبضة الأسطول ' + SRV_VER + ' — ' + fl.length + '/' + FLEET_N + ' محركات حية: ' +
          (fl.map(f => (f.disp || '?') + (f.position ? ' (مركز ' + (f.net >= 0 ? '+' : '') + f.net.toFixed(2) + ')' : '')).join(' · ') || 'بانتظار الإقلاع'));
      } catch { /* نبضة تشخيصية فقط */ }
    }, 30 * 60 * 1000).unref?.();
  }
});

initEngine();
if (SLOT === 0) console.log('[TRQ] المنسق جاهز — أبقِ Termux حيًا (الحارس الذاتي يحمي الأسطول كله)');
