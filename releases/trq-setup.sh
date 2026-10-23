#!/data/data/com.termux/files/usr/bin/bash
# ================================================================
# TRQ-TRADING — إعداد سيرفر الأسطول في Termux (أمر مستقل تماماً)
# · سيرفر الأسطول: منسق + 5 محركات، كل عملة بمحرك مستقل
# · يعمل على منفذ 8987 (محركاته على 8988-8992) — لا يمس سيرفر
#   AB-KING إطلاقاً (منفذه 8787) — التطبيقان مستقلان كلياً
# · حارس ذاتي خاص بـ TRQ يفحص منفذ الأسطول كل 60 ثانية
# · الإقلاع التلقائي بعد إعادة تشغيل الجوال (مدخل boot خاص)
# الحالة والمفاتيح في ~/.trq/ — لا تُمس أبداً
# ================================================================
set -e
echo "══ TRQ-TRADING — إعداد سيرفر الأسطول (مستقل عن AB-KING) ══"

# 1) Node.js
if ! command -v node >/dev/null 2>&1; then
  echo "• تثبيت Node.js..."
  pkg install nodejs -y
fi
echo "✓ Node $(node -v)"

# 2) أحدث نسخة من سيرفر الأسطول
echo "• تحميل أحدث سيرفر أسطول TRQ..."
curl -sL -o ~/trq-server.mjs "https://raw.githubusercontent.com/abab92100-cloud/KING-210/main/releases/trq-server.mjs?t=$(date +%s)"
echo "✓ سيرفر TRQ محدّث"

# 3) قفل اليقظة
termux-wake-lock 2>/dev/null && echo "✓ قفل اليقظة مفعّل" || true

# 4) الحارس الذاتي الخاص بـ TRQ — منفذ 8987 فقط ولا يمس AB-KING
cat > ~/trq-watchdog.sh <<'EOF'
#!/data/data/com.termux/files/usr/bin/bash
# حارس أسطول TRQ: منفذ 8987 — نمط الإنهاء trq-server.mjs فقط
while true; do
  if ! curl -s --max-time 8 127.0.0.1:8987/health >/dev/null 2>&1; then
    echo "[$(date '+%H:%M:%S')] أسطول TRQ لا يرد — إنعاش فوري" >> ~/trq-watchdog.log
    termux-wake-lock 2>/dev/null
    pgrep -f trq-server.mjs >/dev/null 2>&1 && pkill -f trq-server.mjs
    sleep 2
    nohup node ~/trq-server.mjs >> ~/trq-server.log 2>&1 &
  else
    termux-wake-lock 2>/dev/null
  fi
  sleep 60
done
EOF
chmod +x ~/trq-watchdog.sh
pgrep -f trq-watchdog.sh >/dev/null 2>&1 && pkill -f trq-watchdog.sh || true
sleep 1
nohup bash ~/trq-watchdog.sh >/dev/null 2>&1 &
echo "✓ حارس TRQ يعمل — يفحص الأسطول كل 60 ثانية"

# 5) إقلاع تلقائي خاص بـ TRQ (مدخل مستقل — لا يمس مدخل AB-KING)
mkdir -p ~/.termux/boot
cat > ~/.termux/boot/trq.sh <<'EOF'
#!/data/data/com.termux/files/usr/bin/bash
termux-wake-lock
pgrep -f trq-server.mjs >/dev/null 2>&1 || nohup node ~/trq-server.mjs >> ~/trq-server.log 2>&1 &
pgrep -f trq-watchdog.sh >/dev/null 2>&1 || nohup bash ~/trq-watchdog.sh >/dev/null 2>&1 &
EOF
chmod +x ~/.termux/boot/trq.sh
echo "✓ إقلاع تلقائي لـ TRQ مُجهّز (Termux:Boot يشغّل كليهما مستقلين)"

# 6) تشغيل الأسطول الآن
pgrep -f trq-server.mjs >/dev/null 2>&1 && { echo "• الأسطول يعمل — إعادة تشغيله بالنسخة الجديدة..."; pkill -f trq-server.mjs || true; sleep 2; }
nohup node ~/trq-server.mjs >> ~/trq-server.log 2>&1 &
echo "✓ أسطول TRQ انطلق — المنسق على 8987 ومحركاته على 8988-8992"

# 7) فحص الصحة
sleep 4
echo "• فحص صحة الأسطول..."
curl -s 127.0.0.1:8987/health && echo "" && echo "══ تم — أسطول TRQ يعمل والحارس يحميه (مستقل عن AB-KING) ══" || echo "! راجع: cat ~/trq-server.log"
