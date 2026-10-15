#!/data/data/com.termux/files/usr/bin/bash
# ══════════════════════════════════════════════════════════════════
#  KING-trading — تثبيت وتشغيل سيرفر الجوال بأمر واحد
#  العقل الدائم: Node.js يعمل في Termux بلا نوم بلا انقطاع
#  التطبيق يكتشفه تلقائيًا ويتحول لشاشة مرآة
#
#  الاستخدام (انسخ السطر كاملًا والصقه في Termux):
#    curl -sL https://github.com/abab92100-cloud/King/raw/main/releases/king-one.sh | bash
# ══════════════════════════════════════════════════════════════════

echo "══════════ KING — سيرفر الجوال (العقل الدائم) ══════════"

# 0) فحص الإنترنت
echo "• فحص الاتصال..."
if ! curl -sI --max-time 12 https://github.com >/dev/null 2>&1; then
  echo "✗ لا يوجد إنترنت — تحقق من الشبكة ثم أعد الأمر"; exit 1
fi
echo "  الاتصال سليم ✓"

# 1) تثبيت Node.js + الأدوات (أول مرة فقط)
if ! command -v node >/dev/null 2>&1; then
  echo "• تثبيت Node.js (أول مرة — دقيقة واحدة)..."
  pkg update -y </dev/null 2>/dev/null
  pkg install -y nodejs-lts curl termux-api </dev/null 2>/dev/null
fi
echo "  Node $(node -v) ✓"

# 2) إنشاء مجلد العمل + تحميل السيرفر (أحدث نسخة دائمًا)
mkdir -p "$HOME/king" && cd "$HOME/king"
echo "• تحميل أحدث نسخة من السيرفر..."
if ! curl -L -s --fail --retry 3 --max-time 90 -o king-server.mjs \
  "https://github.com/abab92100-cloud/King/raw/main/releases/king-server.mjs"; then
  echo "✗ فشل التحميل — أعد المحاولة بعد دقيقة"; exit 1
fi
echo "  حُمّل $(wc -c < king-server.mjs) بايت ✓"

# 3) إيقاف أي نسخة قديمة
pkill -f king-server.mjs 2>/dev/null || true
pkill -f king-watchdog 2>/dev/null || true
sleep 1

# 4) منع نوم المعالج — بدون هذا سيقتل أندرويد كل شيء
termux-wake-lock 2>/dev/null || true
echo "  قفل المعالج مفعل ✓"

# 5) كتابة الحارس — يعيد السيرفر خلال ثانية إن سقط لأي سبب
cat > king-watchdog.sh <<'WATCHDOG'
#!/data/data/com.termux/files/usr/bin/bash
# KING watchdog — يضمن أن السيرفر لا يموت أبدًا
while true; do
  node "$HOME/king/king-server.mjs"
  echo "⚠ السيرفر توقف — إعادة تشغيل خلال ثانية..."
  sleep 1
done
WATCHDOG
chmod +x king-watchdog.sh

# 6) تشغيل الحارس في خلفية Termux (يبقى حتى بعد إغلاق النافذة)
nohup bash king-watchdog.sh > "$HOME/king/server.log" 2>&1 &
echo "  الحارس يعمل في الخلفية ✓"

# 7) تعليمات أندرويد الإلزامية (مرة واحدة)
cat <<'TIP'

══════════ خطوة أخيرة إلزامية (مرة واحدة فقط) ════════
بدونها سيقتل أندرويد السيرفر بعد ~30 دقيقة في الخلفية:

1. إعدادات الجوال ← التطبيقات ← Termux ← البطارية
   ← اختر «غير مقيد / Unrestricted»
2. إعدادات الجوال ← العناية بالبطارية ← التطبيقات غير المقيّدة
   ← فعّل Termux
3. صغّر نافذة Termux (لا تسحبها من التطبيقات الأخيرة)

ثم افتح تطبيق KING-trading — سيكتشف السيرفر تلقائيًا
خلال ثوانٍ وسيتحول لشاشة مرآة «وضع السيرفر: متصل ✓»

البوت الآن يعمل من Termux بلا نوم ولا انقطاع — حتى لو
أُغلق التطبيق نفسه أو انقطع الإنترنت مؤقتًا (يعيد الاتصال وحده).
══════════════════════════════════════════════════════

للاطلاع على سجل السيرفر لاحقًا:
  cat ~/king/server.log | tail -20

لإعادة التشغيل اليدوية (نادرًا ما تحتاجها):
  curl -sL https://github.com/abab92100-cloud/King/raw/main/releases/king-one.sh | bash
TIP

# 8) إظهار نبضة الحياة الأولى
sleep 3
if curl -s --max-time 5 http://127.0.0.1:8787/health >/dev/null 2>&1; then
  echo "✓✓ السيرفر حي الآن على 127.0.0.1:8787 — افتح تطبيق KING-trading"
else
  echo "… السيرفر يقلع — انتظر 10 ثوانٍ ثم افتح التطبيق"
fi
