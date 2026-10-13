package com.abking.nativeplugin;

import android.content.Context;
import android.os.PowerManager;
import com.getcapacitor.JSObject;
import com.getcapacitor.Plugin;
import com.getcapacitor.PluginCall;
import com.getcapacitor.PluginMethod;
import com.getcapacitor.annotation.CapacitorPlugin;

@CapacitorPlugin(name = "KingNative")
public class KingNative extends Plugin {

    private PowerManager.WakeLock wakeLock = null;

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
    public void heartbeat(PluginCall call) { call.resolve(); }

    @PluginMethod
    public void startWatchdog(PluginCall call) { call.resolve(); }

    @PluginMethod
    public void isWakeLockHeld(PluginCall call) {
        JSObject ret = new JSObject();
        ret.put("held", wakeLock != null && wakeLock.isHeld());
        call.resolve(ret);
    }
}
