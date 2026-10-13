# AB-KING — بوت تداول عقود KuCoin الآجلة

تطبيق جوال (Capacitor/React) + سيرفر Termux + مختبر محاكاة حقيقي.

## المكوّنات
- `app/ab-king/` — تطبيق الواجهة (Capacitor + React) — `com.abking.trading`
- `releases/ab-king-server.mjs` — سيرفر التداول (Node.js على Termux)
- `releases/ab-king-setup.sh` — مثبّت Termux بأمر واحد
- `backtest/lab.mjs` — **مختبر المحاكاة**: يغلّف المحرك الحقيقي ويغذّيه بشموع KuCoin التاريخية

## تشغيل السيرفر على Termux
```bash
curl -sL https://raw.githubusercontent.com/<owner>/AB-KING/main/releases/ab-king-setup.sh | bash
```

## مختبر المحاكاة (Backtest Lab)
محاكاة حقيقية: نفس دول القرار، نفس الرسوم (maker 0.02%/taker 0.06%)، نفس منطق الجني والتتبع.
```bash
cd backtest && npm install
node lab.mjs --symbol XBTUSDTM --days 3 --gran 5
node lab.mjs --symbol PEPEUSDTM --days 3 --gran 5 --balance 200
```
الخيارات: `--symbol` `--days` `--gran (1|5|15|60 دقيقة)` `--balance`

النتائج المقيسة: الصافي، عدد الصفقات، نسبة النجاح، الرسوم، أعمق انحدار، الصفقة العالقة.

## منطق Market Making (من Hummingbot / Avellaneda-Stoikov)
المعادلات المستخرجة من `hummingbot/strategy/avellaneda_market_making`:
```
reservation = price − q·γ·vol
optimal_spread = γ·vol + 2·ln(1+γ/κ)/γ
bid = min(reservation − spread/2, price − min_spread/2)
ask = max(reservation + spread/2, price + min_spread/2)
```
جاهزة للدمج في المحرك كطبقة تسعير عند الحاجة.

## بناء APK
```bash
cd app/ab-king
npm install && npm run build
npx cap sync android
cd android && ./gradlew assembleRelease
```
أو عبر CI: workflow البناء موجود في `.github/workflows/`.

## إخلاء مسؤولية
تداول العقود الآجلة عالي المخاطر. النتائج السابقة (backtest) لا تضمن المستقبل.
