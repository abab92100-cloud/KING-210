// live test: full-universe scan through the REAL engine
import { build } from 'esbuild';
import { fileURLToPath } from 'node:url';
import path from 'node:path';
const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, '..');
await build({ entryPoints: [path.join(ROOT,'app/ab-king/src/engine.ts')], bundle: true,
  platform: 'node', format: 'esm', target: 'node18',
  outfile: path.resolve(__dirname,'_engine-bundle.mjs'),
  define: { 'process.env.ABKING_SERVER': '"1"' }, logLevel: 'silent' });
globalThis.localStorage = { _d: {}, getItem(k){return k in this._d?this._d[k]:null;},
  setItem(k,v){this._d[k]=String(v);}, removeItem(k){delete this._d[k];}, clear(){this._d={};} };
const E = await import('file://' + path.resolve(__dirname,'_engine-bundle.mjs'));
const S = E.S;
const t0 = Date.now();
const r = await E.scanRadar();
const ms = Date.now() - t0;
console.log('scan time:', ms + 'ms');
console.log('total contracts in radar list:', r.list.length);
const deep = r.list.filter(x => x.state !== 'watch').length;
const watching = r.list.filter(x => x.state === 'watch').length;
console.log('deep-checked:', deep, '| universe-watch:', watching);
console.log('top 5 now:');
for (const l of r.list.slice(0,5)) console.log(' ', l.disp, 'score', l.score, '|', l.state);
const ok = r.list.length > 100 && ms < 60000;
console.log(ok ? 'PASS: full-universe scan live (' + r.list.length + ' contracts in ' + ms + 'ms)' : 'FAIL');
process.exit(ok ? 0 : 1);
