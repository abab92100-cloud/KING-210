#!/data/data/com.termux/files/usr/bin/bash
# ================================================================
# AB-KING — إعداد سيرفر البوت الدائم في Termux (ضغطة واحدة)
# · يثبّت Node.js إن لم يكن موجودًا
# · يحمي Termux من تجميد أندرويد في الخلفية (قفل اليقظة)
# · يجهّز الإقلاع التلقائي بعد إعادة تشغيل الجوال (Termux:Boot)
# · يشغّل السيرفر إن لم يكن شغالًا ويحوّله للنسخة الأحدث
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

# 2) أحدث نسخة من السيرفر
echo "• تحميل أحدث سيرفر من المستودع..."
curl -sL -o ~/king-server.mjs "https://raw.githubusercontent.com/abab92100-cloud/KING-210/main/releases/king-server.mjs?t=$(date +%s)"
echo "✓ السيرفر محدّث"

# 3) قفل اليقظة — يمنع أندرويد من تجميد Termux في الخلفية
termux-wake-lock 2>/dev/null && echo "✓ قفل اليقظة مفعّل (Termux لا ينام)" || echo "! pkg install termux-tools ثم أعد المحاولة"

# 4) إقلاع تلقائي بعد إعادة تشغيل الجوال
mkdir -p ~/.termux/boot
cat > ~/.termux/boot/king.sh <<'EOF'
#!/data/data/com.termux/files/usr/bin/bash
termux-wake-lock
pgrep -f king-server.mjs >/dev/null 2>&1 || nohup node ~/king-server.mjs >> ~/king-server.log 2>&1 &
EOF
chmod +x ~/.termux/boot/king.sh
echo "✓ الإقلاع التلقائي مُجهّز (يعمل بعد تثبيت تطبيق Termux:Boot من متجر F-Droid مرة واحدة)"

# 5) تشغيل السيرفر الآن إن لم يكن شغالًا
if pgrep -f king-server.mjs >/dev/null 2>&1; then
  echo "• السيرفر يعمل — إعادة تشغيله بالنسخة الجديدة..."
  pkill -f king-server.mjs || true
  sleep 1
fi
nohup node ~/king-server.mjs >> ~/king-server.log 2>&1 &
echo "✓ السيرفر انطلق"

# 6) فحص الصحة
sleep 2
echo "• فحص الصحة..."
curl -s 127.0.0.1:8787/health && echo "" && echo "══ تم — البوت يعمل الآن ومحمي من الخلفية ══" || echo "! راجع السجل: cat ~/king-server.log"
