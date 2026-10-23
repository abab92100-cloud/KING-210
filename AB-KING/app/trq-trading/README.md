# KING Trading — بوت تداول عقود KuCoin الآجلة

تطبيق جوال (Capacitor + React 19) مع محرك Grid Bot يعمل في الواجهة أو كعقلة دائمة على سيرفر Node.

## التشغيل السريع (ويب/تطوير)
```bash
npm install
npm run dev
```

## البناء
```bash
npm run build          # تطبيق الويب → dist/
npm run build:server   # سيرفر الجوال → server-dist/king-server.mjs
```

## سيرفر الجوال (Termux)
نفس المحرك يعمل في Node بدون نوم — التطبيق يكتشفه تلقائيًا ويحوّل لواجهة عرض فقط:
```bash
node server-dist/king-server.mjs
```
أو التثبيت بأمر واحد على Termux عبر `king-setup.sh`. الحالة تُخزن في `~/.king/`.

## ملاحظات أمان
- مفاتيح KuCoin تُحفظ على جهازك فقط (localStorage / `~/.king`) — أنشئ مفتاحًا بصلاحية «تداول فقط» بدون سحب.
- الرافعة المالية تُحدد أنت من شاشة الإعدادات (×1–×100) وتُطبق على كل أمر.
