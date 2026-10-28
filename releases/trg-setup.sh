#!/data/data/com.termux/files/usr/bin/bash
# ================================================================
# TRG-NEW — إعداد سيرفر الأسطول في Termux (أمر مستقل تماماً)
# · سيرفر الأسطول: منسق + 5 محركات، كل عملة بمحرك مستقل
# · يعمل على منفذ 9010 (محركاته على 9011-9015) — لا يمس سيرفري
#   AB-KING (8787) وTRQ (8987) إطلاقاً — التطبيقات الثلاثة مستقلة كلياً
# · حارس ذاتي خاص بـ TRQ يفحص منفذ الأسطول كل 60 ثانية
# · الإقلاع التلقائي بعد إعادة تشغيل الجوال (مدخل boot خاص)
# الحالة والمفاتيح في ~/.trq/ — لا تُمس أبداً
# ================================================================
set -e
echo "══ TRG-NEW — إعداد سيرفر الأسطول (مستقل عن AB-KING وTRQ) ══"

# 1) Node.js
if ! command -v node >/dev/null 2>&1; then
  echo "• تثبيت Node.js..."
  pkg install nodejs -y
fi
echo "✓ Node $(node -v)"

# 2) أحدث نسخة من سيرفر الأسطول
echo "• تحميل أحدث سيرفر أسطول TRG-NEW..."
curl -sL -o ~/trg-server.mjs "https://raw.githubusercontent.com/abab92100-cloud/KING-210/main/releases/trg-server.mjs?t=$(date +%s)"
echo "✓ سيرفر TRG-NEW محدّث"

# 3) قفل اليقظة
termux-wake-lock 2>/dev/null && echo "✓ قفل اليقظة مفعّل" || true

# 4) الحارس الذاتي الخاص بـ TRG-NEW — منفذ 9010 فقط ولا يمس AB-KING وTRQ
cat > ~/trg-watchdog.sh <<'EOF'
#!/data/data/com.termux/files/usr/bin/bash
# حارس أسطول TRG-NEW: منفذ 9010 — نمط الإنهاء trg-server.mjs فقط
while true; do
  if ! curl -s --max-time 8 127.0.0.1:9010/health >/dev/null 2>&1; then
    echo "[$(date '+%H:%M:%S')] أسطول TRG-NEW لا يرد — إنعاش فوري" >> ~/trg-watchdog.log
    termux-wake-lock 2>/dev/null
    pgrep -f trg-server.mjs >/dev/null 2>&1 && pkill -f trg-server.mjs
    sleep 2
    nohup node ~/trg-server.mjs >> ~/trg-server.log 2>&1 &
  else
    termux-wake-lock 2>/dev/null
  fi
  sleep 60
done
EOF
chmod +x ~/trg-watchdog.sh
pgrep -f trg-watchdog.sh >/dev/null 2>&1 && pkill -f trg-watchdog.sh || true
sleep 1
nohup bash ~/trg-watchdog.sh >/dev/null 2>&1 &
echo "✓ حارس TRG-NEW يعمل — يفحص الأسطول كل 60 ثانية"

# 5) إقلاع تلقائي خاص بـ TRG-NEW (مدخل مستقل)
mkdir -p ~/.termux/boot
cat > ~/.termux/boot/trg.sh <<'EOF'
#!/data/data/com.termux/files/usr/bin/bash
termux-wake-lock
pgrep -f trg-server.mjs >/dev/null 2>&1 || nohup node ~/trg-server.mjs >> ~/trg-server.log 2>&1 &
pgrep -f trg-watchdog.sh >/dev/null 2>&1 || nohup bash ~/trg-watchdog.sh >/dev/null 2>&1 &
EOF
chmod +x ~/.termux/boot/trg.sh
echo "✓ إقلاع تلقائي لـ TRG-NEW مُجهّز (Termux:Boot يشغّل الثلاثة مستقلين)"

# 6) تشغيل الأسطول الآن
pgrep -f trg-server.mjs >/dev/null 2>&1 && { echo "• الأسطول يعمل — إعادة تشغيله بالنسخة الجديدة..."; pkill -f trg-server.mjs || true; sleep 2; }
nohup node ~/trg-server.mjs >> ~/trg-server.log 2>&1 &
echo "✓ أسطول TRG-NEW انطلق — المنسق على 9010 ومحركاته على 9011-9015"

# 7) فحص الصحة
sleep 4
echo "• فحص صحة الأسطول..."
curl -s 127.0.0.1:9010/health && echo "" && echo "══ تم — أسطول TRG-NEW يعمل والحارس يحميه (مستقل عن AB-KING وTRQ) ══" || echo "! راجع: cat ~/trg-server.log"
