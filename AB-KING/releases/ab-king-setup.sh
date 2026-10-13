#!/data/data/com.termux/files/usr/bin/bash
# AB-KING — mobile trading server installer
# usage: curl -sL https://raw.githubusercontent.com/OWNER/AB-KING/main/releases/ab-king-setup.sh | bash
echo "═══ AB-KING — mobile server ═══"
if ! curl -sI --max-time 12 https://github.com >/dev/null 2>&1; then
  echo "✗ no internet — check connection"; exit 1; fi
if ! command -v node >/dev/null 2>&1; then
  echo "• installing Node.js..."
  pkg update -y </dev/null
  pkg install -y nodejs-lts </dev/null
fi
echo "  Node $(node -v) ✓"
mkdir -p "$HOME/ab-king" && cd "$HOME/ab-king"
echo "• downloading server..."
if ! curl -L -s --fail --retry 3 --max-time 60 -o ab-king-server.mjs "https://raw.githubusercontent.com/OWNER/AB-KING/main/releases/ab-king-server.mjs"; then
  echo "✗ download failed — retry in a minute"; exit 1; fi
echo "  downloaded $(wc -c < ab-king-server.mjs) bytes ✓"
pkill -f ab-king-server.mjs 2>/dev/null || true
sleep 2
termux-wake-lock 2>/dev/null || true
echo "• server starting — open the AB-KING app and tap server"
while true; do
  node ab-king-server.mjs
  echo "⚠ server stopped — restarting in 3s... (Ctrl+C to quit)"
  sleep 3
done
