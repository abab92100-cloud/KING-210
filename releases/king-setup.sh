#!/data/data/com.termux/files/usr/bin/bash
# ================================================================
# AB-KING — إعداد السيرفر الدائم في Termux (ضغطة واحدة)
# · يثبّت Node.js إن لم يكن موجودًا
# · يحمي Termux من تجميد أندرويد في الخلفية (قفل اليقظة)
# · يجهّز الإقلاع التلقائي بعد إعادة تشغيل الجوال (Termux:Boot)
# · يشغّل حارسًا ذاتيًا: كل 60 ثانية يفحص المحرك وينعشه فور موته
# · أي إقلاع يبدأ بمراجعة كاملة: المراكز والأوامر والجني متطابقة مع المنصة
# الحالة والمفاتيح محفوظة في ~/.ab-king ولا تُمس أبدًا
# ================================================================
set -e
echo "══ AB-KING — إعداد السيرفر الدائم ══"

# 1) Node.js
if ! command -v node >/dev/null 2>&1; then
  echo "• تثبيت Node.js..."
  pkg install nodejs -y
fi
echo "✓ Node $(node -v)"

# 2) أحدث نسخة من السيرفر (بتجاوز الكاش)
echo "• تحميل أحدث سيرفر من المستودع..."
curl -sL -o ~/king-server.mjs "https://raw.githubusercontent.com/abab92100-cloud/KING-210/main/releases/king-server.mjs?t=$(date +%s)"
echo "✓ السيرفر محدّث"

# 3) قفل اليقظة — يمنع أندرويد من تجميد Termux في الخلفية
termux-wake-lock 2>/dev/null && echo "✓ قفل اليقظة مفعّل (Termux لا ينام)" || echo "! pkg install termux-tools ثم أعد المحاولة"

# 4) الحارس الذاتي: يفحص المحرك كل 60 ثانية وينعشه فور موته (الحل النهائي لبقاء الاتصال)
cat > ~/king-watchdog.sh <<'EOF'
#!/data/data/com.termux/files/usr/bin/bash
# حارس AB-KING: المحرك لا يموت — أندرويد يقتله فيُنعش فورًا
while true; do
  if ! curl -s --max-time 8 127.0.0.1:8787/health >/dev/null 2>&1; then
    echo "[$(date '+%H:%M:%S')] المحرك لا يرد — إنعاش فوري" >> ~/king-watchdog.log
    termux-wake-lock 2>/dev/null
    pgrep -f king-server.mjs >/dev/null 2>&1 && pkill -f king-server.mjs
    sleep 2
    nohup node ~/king-server.mjs >> ~/king-server.log 2>&1 &
  else
    # السيرفر حي: تجديد قفل اليقظة دوريًا (بعض الأنظمة تسرّبه)
    termux-wake-lock 2>/dev/null
  fi
  sleep 60
done
EOF
chmod +x ~/king-watchdog.sh
pgrep -f king-watchdog.sh >/dev/null 2>&1 && pkill -f king-watchdog.sh || true
sleep 1
nohup bash ~/king-watchdog.sh >/dev/null 2>&1 &
echo "✓ الحارس الذاتي يعمل — المحرك يُنعش فور موته كل 60 ثانية"

# 5) إقلاع تلقائي بعد إعادة تشغيل الجوال (سيرفر + حارس)
mkdir -p ~/.termux/boot
cat > ~/.termux/boot/king.sh <<'EOF'
#!/data/data/com.termux/files/usr/bin/bash
termux-wake-lock
pgrep -f king-server.mjs >/dev/null 2>&1 || nohup node ~/king-server.mjs >> ~/king-server.log 2>&1 &
pgrep -f king-watchdog.sh >/dev/null 2>&1 || nohup bash ~/king-watchdog.sh >/dev/null 2>&1 &
EOF
chmod +x ~/.termux/boot/king.sh
echo "✓ الإقلاع التلقائي مُجهّز (يعمل بعد تثبيت تطبيق Termux:Boot من متجر F-Droid مرة واحدة)"

# 6) تشغيل السيرفر الآن بأحدث نسخة
if pgrep -f king-server.mjs >/dev/null 2>&1; then
  echo "• السيرفر يعمل — إعادة تشغيله بالنسخة الجديدة..."
  pkill -f king-server.mjs || true
  sleep 2
fi
nohup node ~/king-server.mjs >> ~/king-server.log 2>&1 &
echo "✓ السيرفر انطلق"

# 7) فحص الصحة
sleep 3
echo "• فحص الصحة..."
curl -s 127.0.0.1:8787/health && echo "" && echo "══ تم — المحرك يعمل الآن والحارس يحميه ══" || echo "! راجع السجل: cat ~/king-server.log"
