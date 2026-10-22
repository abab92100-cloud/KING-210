import './src/server-shims';
import { S, initEngine, dynamicTpNet, levelQtys, effLevels } from './src/engine';
const log = (m: any) => console.log('[TEST]', m);
async function main() {
  initEngine();
  S.config.mode = 'paper'; S.keys = null; S.status = 'running';
  S.config.cycleBalance = 170; S.config.leverage = 15; S.config.levels = 18;
  S.multiplier = 100; // مواصفات DOGE الحقيقية من المنصة
  S.lastPrice = 0.079;
  const m = effLevels();
  const q = levelQtys(0.079, m);
  const first = q[0] || 0;
  const notional = first * 100 * 0.079;
  log('أول مستوى الآن: ' + first + ' لوت = ' + notional.toFixed(0) + '$ (كان ~2 لوت = 16$)');
  // الهدف والمسافة للأحجام الجديدة
  S.position = { side: 'long', size: first * 2, entry: 0.079, unrealized: 0, openedAt: Date.now() };
  S.journal = [{ id: 'j', status: 'open', side: 'long', qty: first * 2, entry: 0.079, fees: 0.01, source: 'صيد' }];
  const tgt = dynamicTpNet();
  const notional2 = first * 2 * 100 * 0.079;
  const dist = ((tgt + 0.05) / notional2 * 100);
  log('مركز أول دخولين (' + notional2.toFixed(0) + '$): هدف ' + tgt + '$ — مسافة جني ~' + dist.toFixed(2) + '% (كانت 1.07%)');
  const pass = first >= 4 && dist < 0.8;
  log(pass ? '=== نجاح ===' : '=== فشل ===');
  process.exit(pass ? 0 : 1);
}
main();
