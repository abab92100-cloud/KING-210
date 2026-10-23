package com.abking.nativeplugin;

import android.content.Context;
import android.net.ConnectivityManager;
import android.net.Network;
import android.net.NetworkCapabilities;
import android.net.NetworkRequest;
import android.os.Handler;
import android.os.Looper;
import android.os.PowerManager;
import com.getcapacitor.JSObject;
import com.getcapacitor.Plugin;
import com.getcapacitor.PluginCall;
import com.getcapacitor.PluginMethod;
import com.getcapacitor.annotation.CapacitorPlugin;

@CapacitorPlugin(name = "KingNative")
public class KingNative extends Plugin {
    private PowerManager.WakeLock wakeLock = null;
    private ConnectivityManager.NetworkCallback netCallback = null;
    private final Handler mainHandler = new Handler(Looper.getMainLooper());
    private boolean watchdogRunning = false;

    @PluginMethod
    public void acquireWakeLock(PluginCall call) {
        try {
            Context ctx = getContext();
            PowerManager pm = (PowerManager) ctx.getSystemService(Context.POWER_SERVICE);
            if (pm == null) { call.reject("no power service"); return; }
            if (wakeLock == null) {
                wakeLock = pm.newWakeLock(PowerManager.PARTIAL_WAKE_LOCK, "King:keepalive");
                wakeLock.setReferenceCounted(false);
            }
            if (!wakeLock.isHeld()) wakeLock.acquire();
            JSObject ret = new JSObject();
            ret.put("held", wakeLock.isHeld());
            call.resolve(ret);
        } catch (Exception e) {
            call.reject("wakelock: " + e.getMessage());
        }
    }

    @PluginMethod
    public void releaseWakeLock(PluginCall call) {
        try {
            if (wakeLock != null && wakeLock.isHeld()) wakeLock.release();
            JSObject ret = new JSObject();
            ret.put("held", false);
            call.resolve(ret);
        } catch (Exception e) {
            call.reject("release: " + e.getMessage());
        }
    }

    @PluginMethod
    public void heartbeat(PluginCall call) {
        // نبضة حياة من JS: نعيد قفل الاستيقاظ إن سقط (بعض أنظمة أندرويد تنتزعه بعد ساعتين)
        try {
            PowerManager pm = (PowerManager) getContext().getSystemService(Context.POWER_SERVICE);
            if (pm != null && wakeLock != null && !wakeLock.isHeld()) wakeLock.acquire();
        } catch (Exception ignored) { /* best-effort re-acquire */ }
        call.resolve();
    }

    /**
     * مراقب الشبكة الأصلي — يعمل على مستوى النظام (لا يُجمَّد مع WebView في الخلفية).
     * يُخطر JS بحدث "netChange" فور عودة الإنترنت أو انقطاعه، حتى لو كان التطبيق في الخلفية.
     */
    @PluginMethod
    public void startWatchdog(PluginCall call) {
        try {
            if (watchdogRunning) { call.resolve(); return; }
            Context ctx = getContext();
            ConnectivityManager cm = (ConnectivityManager) ctx.getSystemService(Context.CONNECTIVITY_SERVICE);
            if (cm == null) { call.reject("no connectivity service"); return; }

            netCallback = new ConnectivityManager.NetworkCallback() {
                private boolean hadNetwork = false;

                @Override
                public void onAvailable(Network network) {
                    hadNetwork = true;
                    notifyNet(true);
                }

                @Override
                public void onLost(Network network) {
                    // onLost يُطلق لشبكة معينة؛ نتحقق من الحالة الكلية قبل الإخبار
                    mainHandler.post(() -> {
                        boolean stillOnline = isOnline();
                        if (!stillOnline) notifyNet(false);
                    });
                }
            };

            NetworkRequest req = new NetworkRequest.Builder()
                .addCapability(NetworkCapabilities.NET_CAPABILITY_INTERNET)
                .build();
            cm.registerNetworkCallback(req, netCallback);
            watchdogRunning = true;

            JSObject ret = new JSObject();
            ret.put("watchdog", true);
            ret.put("online", isOnline());
            call.resolve(ret);
        } catch (Exception e) {
            call.reject("watchdog: " + e.getMessage());
        }
    }

    private boolean isOnline() {
        try {
            ConnectivityManager cm = (ConnectivityManager) getContext().getSystemService(Context.CONNECTIVITY_SERVICE);
            if (cm == null) return false;
            Network n = cm.getActiveNetwork();
            if (n == null) return false;
            NetworkCapabilities caps = cm.getNetworkCapabilities(n);
            return caps != null && caps.hasCapability(NetworkCapabilities.NET_CAPABILITY_INTERNET);
        } catch (Exception e) {
            return false;
        }
    }

    private void notifyNet(boolean online) {
        JSObject data = new JSObject();
        data.put("online", online);
        data.put("at", System.currentTimeMillis());
        notifyListeners("netChange", data);
    }

    @PluginMethod
    public void isOnline(PluginCall call) {
        JSObject ret = new JSObject();
        ret.put("online", isOnline());
        call.resolve(ret);
    }

    @PluginMethod
    public void isWakeLockHeld(PluginCall call) {
        JSObject ret = new JSObject();
        ret.put("held", wakeLock != null && wakeLock.isHeld());
        call.resolve(ret);
    }
}
