/* ================================================================
   محاكيات بيئة Node لسيرفر Termux — تُقيَّم قبل المحرك (ترتيب الواردات)
   · localStorage على ملف JSON في ~/.ab-king — الحالة تنجو من إعادة التشغيل
   · ABKING_SERVER=1 يحوّل المحرك للاتصال المباشر بـ KuCoin (لا بروكسي)
   ================================================================ */
import { readFileSync, writeFileSync, existsSync, mkdirSync } from 'node:fs';
import { homedir } from 'node:os';
import { join } from 'node:path';

process.env.ABKING_SERVER = '1';

const dir = join(homedir(), '.ab-king');
try { mkdirSync(dir, { recursive: true }); } catch{ /* intentional: optional step */ }
const file = join(dir, 'ab-king-server-store.json');

let data = {};
try { if (existsSync(file)) data = JSON.parse(readFileSync(file, 'utf8') || '{}'); } catch (e) { data = {}; }

let timer:any = null;
function persist() {
  if (timer) return;
  timer = setTimeout(() => {
    timer = null;
    try { writeFileSync(file, JSON.stringify(data)); } catch{ /* intentional: optional step */ }
  }, 300);
}

;(globalThis as any).localStorage = {
  getItem: k => (k in data ? data[k] : null),
  setItem: (k, v) => { data[k] = String(v); persist(); },
  removeItem: k => { delete data[k]; persist(); },
  clear: () => { data = {}; persist(); },
};

console.log('[AB-KING] مخزن الحالة: ' + file);
