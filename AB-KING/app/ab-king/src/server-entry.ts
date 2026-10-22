/* ================================================================
   AB-KING — سيرفر الجوال (Termux)
   العقل الدائم للبوت: نفس محرك التطبيق يعمل هنا في Node بلا نوم
   ولا خنق خلفية ولا قتل بطارية. التطبيق يكتشفه تلقائيًا ويتحول واجهة.
   التشغيل:  node king-server.mjs
   ================================================================ */
import './server-shims'; // أولًا دائمًا: يثبّت البيئة قبل تقييم المحرك
import {
  S, initEngine, startBot, pauseBot, stopBot, newCycle,
  saveCfg, saveKeys, clearKeys, scanRadar, pushLog, liveSync,
} from './engine';

const SRV_VER = 'AB-KING-1.5.7';
import { createServer } from 'node:http';

const PORT = 8787;

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

// ─── لافتة الإصدار + نبضة الحالة (إثبات مرئي داخل سجل التطبيق) ───
pushLog('server','✅ '+SRV_VER+' يعمل الآن — التبديل التلقائي مفعّل: بعد الجني فورًا + دوران البورة عند 15 دقيقة بلا تنفيذ');
// مراجعة الوضع عند الإقلاع (قاعدة المالك): أي إعادة تشغيل — مجمدة كانت أو تعطلًا —
// تبدأ بمراجعة كاملة: المركز الحقيقي على المنصة، الأوامر اليتيمة، ومنطقة الجني
setTimeout(() => {
  liveSync().then(() => pushLog('server', '🔧 اكتملت مراجعة الوضع عند الإقلاع — الأوامر والمراكز متطابقة مع المنصة'))
    .catch(() => pushLog('server', '⏳ بانتظار المنصة — تُعاد المراجعة تلقائيًا فور توفر الشبكة'));
  scanRadar().catch(() => {});
}, 1500);
setInterval(() => {
  try {
    const idleMs = Date.now() - (S.lastHuntAt || S._dwellAt || S.startedAt || S._savedAt || Date.now());
    const idleMin = Math.max(0, Math.round(idleMs / 60000));
    pushLog('server', '💡 نبضة '+SRV_VER+' — العملة: ' + (S.config.displaySymbol || '?') +
      ' — بلا تنفيذ منذ ' + idleMin + ' د' + (S.position ? ' — مركز مفتوح يُدار حتى الجني' : ' — الدوران التلقائي عند 15 د') +
      ' — الحالة: ' + (S.status || '؟'));
  } catch { /* نبضة تشخيصية فقط */ }
}, 30 * 60 * 1000).unref?.();

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
      return send(res, 200, { ok: true, name: 'KING Termux Server', version: SRV_VER, at: Date.now() });

    if (req.method === 'GET' && u.pathname === '/state')
      return send(res, 200, { ok: true, at: Date.now(), state: S });

    if (req.method === 'POST' && u.pathname === '/cmd') {
      let body = '';
      for await (const chunk of req) body += chunk;
      let j:any = {};
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
        default: return send(res, 400, { ok: false, error: 'إجراء غير معروف: ' + a });
      }
    }

    send(res, 404, { ok: false, error: 'not found' });
  } catch (e) {
    send(res, 500, { ok: false, error: String((e && (e as any).message) || e) });
  }
});

srv.listen(PORT, '127.0.0.1', () => {
  console.log('[AB-KING] سيرفر الجوال يعمل على http://127.0.0.1:' + PORT);
  console.log('[AB-KING] افتح تطبيق AB-KING — سيكتشف السيرفر تلقائيًا ويتحول لوضع الواجهة');
});

initEngine();
console.log('[AB-KING] المحرك بدأ — أبقِ Termux حيًا (termux-wake-lock مفعّل)');
