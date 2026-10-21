import './src/server-shims';
import { S, initEngine, dynamicTpNet } from './src/engine';
const log = (m: any) => console.log('[TEST]', m);
const p = 0.081;
async function main() {
  initEngine();
  S.config.mode = 'paper'; S.keys = null; S.status = 'running'; S.multiplier = 1;
  // مركز نموذجي بحجم صورة المالك: عقدان DOGE بمضاعف 1000 ≈ 162$
  const notional = 2 * 1000 * p; // ≈ 162$
  S.position = { side: 'short', size: 2000, entry: p, unrealized: 0, openedAt: Date.now() };
  S.journal = [{ id: 'j', status: 'open', side: 'short', qty: 2000, entry: p, fees: 0.05, source: 'صيد' }];
  // بصمة هادئة (تقلب 0.03) وعادية (0.08) ومرتفعة (0.2)
  const prof = S.memory.profiles as any;
  prof['DOGEUSDTM'] = { vol: 0.03, spread: 0.014 };
  const calm = dynamicTpNet();
  prof['DOGEUSDTM'] = { vol: 0.08, spread: 0.014 };
  const normal = dynamicTpNet();
  prof['DOGEUSDTM'] = { vol: 0.2, spread: 0.014 };
  const hot = dynamicTpNet();
  log('مركز ' + notional.toFixed(0) + '$: هادئ → ' + calm + '$ | عادي → ' + normal + '$ | متقلب → ' + hot + '$');
  const ok = calm >= 0.15 && normal >= calm && hot >= normal && normal >= 0.4;
  log(ok ? '✅ أهداف تستحق (قاعدة المالك: لا ربح تافه)' : '⚠️ راجع النطاق');
  process.exit(ok ? 0 : 1);
}
main();
