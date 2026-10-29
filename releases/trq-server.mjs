var __defProp = Object.defineProperty;
var __getOwnPropDesc = Object.getOwnPropertyDescriptor;
var __getOwnPropNames = Object.getOwnPropertyNames;
var __hasOwnProp = Object.prototype.hasOwnProperty;
var __esm = (fn, res) => function __init() {
  return fn && (res = (0, fn[__getOwnPropNames(fn)[0]])(fn = 0)), res;
};
var __export = (target, all) => {
  for (var name in all)
    __defProp(target, name, { get: all[name], enumerable: true });
};
var __copyProps = (to, from, except, desc) => {
  if (from && typeof from === "object" || typeof from === "function") {
    for (let key of __getOwnPropNames(from))
      if (!__hasOwnProp.call(to, key) && key !== except)
        __defProp(to, key, { get: () => from[key], enumerable: !(desc = __getOwnPropDesc(from, key)) || desc.enumerable });
  }
  return to;
};
var __toCommonJS = (mod) => __copyProps(__defProp({}, "__esModule", { value: true }), mod);

// src/ws.ts
function forceReconnect(reason) {
  if (stopped) return;
  if (retryTimer) {
    clearTimeout(retryTimer);
    retryTimer = null;
  }
  connectSeq++;
  clearTimers();
  if (ws) {
    try {
      ws.close();
    } catch {
    }
    ws = null;
  }
  streamState.connected = false;
  streamState.connecting = false;
  streamState.error = String(reason || "reconnect");
  attempts = 0;
  connect();
  if (onStatus) onStatus({ ...streamState });
}
async function getBullet() {
  async function tryFetch(base) {
    const res2 = await fetch(base + "/api/v1/bullet-public", { method: "POST", cache: "no-store" });
    if (!res2.ok) throw new Error("bullet http " + res2.status);
    if (!(res2.headers.get("content-type") || "").toLowerCase().includes("json")) throw new Error("bullet not json");
    return res2;
  }
  let res;
  try {
    res = await tryFetch(REST);
  } catch (e) {
    if (REST === DIRECT) throw e;
    REST = DIRECT;
    res = await tryFetch(REST);
  }
  const j = await res.json();
  if (j.code !== "200000") throw new Error(j.msg || "bullet " + j.code);
  const srv2 = (j.data.instanceServers || [])[0] || {};
  return {
    token: j.data.token,
    endpoint: srv2.endpoint,
    pingInterval: srv2.pingInterval || 18e3,
    pingTimeout: srv2.pingTimeout || 1e4
  };
}
function topics(sym) {
  return [
    "/contractMarket/tickerV2:" + sym,
    // السعر اللحظي
    "/contractMarket/level2Depth5:" + sym,
    // دفتر الأوامر (أفضل 5 مستويات)
    "/contractMarket/execution:" + sym
    // الصفقات المنفذة لحظيًا
  ];
}
function subscribe() {
  if (!ws || ws.readyState !== 1 || !symbol) return;
  for (const t of topics(symbol)) {
    ws.send(JSON.stringify({
      id: Date.now() + "" + Math.floor(Math.random() * 1e3),
      type: "subscribe",
      topic: t,
      privateChannel: false,
      response: true
    }));
  }
}
function clearTimers() {
  if (pingTimer) {
    clearInterval(pingTimer);
    pingTimer = null;
  }
  if (watchTimer) {
    clearInterval(watchTimer);
    watchTimer = null;
  }
}
function handleData(j) {
  streamState.lastMsgAt = Date.now();
  streamState.msgs++;
  if (j.type === "pong" || j.type === "ack" || j.type === "welcome") return;
  if (j.type !== "message" || !j.topic || !j.data) return;
  const d = j.data;
  const msgSym = j.topic.includes(":") ? j.topic.split(":")[1] : null;
  if (msgSym && symbol && msgSym !== symbol) return;
  if (j.topic.startsWith("/contractMarket/ticker")) {
    let price = +d.price || 0;
    const bb = +d.bestBidPrice || 0, ba = +d.bestAskPrice || 0;
    if (!price && bb > 0 && ba > 0) price = (bb + ba) / 2;
    if (price > 0 && onMessage) onMessage({ symbol: msgSym, price, side: d.side, size: +d.size || 0 });
  } else if (j.topic.startsWith("/contractMarket/level2")) {
    const bids = (d.bids || []).map((r) => ({ price: +r[0], size: +r[1] })).filter((r) => r.price > 0);
    const asks = (d.asks || []).map((r) => ({ price: +r[0], size: +r[1] })).filter((r) => r.price > 0);
    if (bids.length || asks.length) {
      streamState.bookAt = Date.now();
      if (onMessage) onMessage({ symbol: msgSym, book: { bids, asks } });
    }
  } else if (j.topic.startsWith("/contractMarket/execution")) {
    streamState.tradeAt = Date.now();
    if (onMessage) onMessage({ symbol: msgSym, trade: { side: String(d.side || ""), size: +d.size || 0, price: +d.price || 0 } });
  }
}
async function connect() {
  if (stopped) return;
  const seq = ++connectSeq;
  streamState.connecting = true;
  streamState.error = null;
  if (onStatus) onStatus({ ...streamState });
  try {
    const b = await getBullet();
    if (stopped || seq !== connectSeq) return;
    const socket = new WebSocket(b.endpoint + "?token=" + b.token + "&connectId=" + seq + "_" + Date.now().toString(36));
    ws = socket;
    socket.onopen = () => {
      if (seq !== connectSeq) {
        try {
          socket.close();
        } catch {
        }
        return;
      }
      attempts = 0;
      streamState.connected = true;
      streamState.connecting = false;
      subscribe();
      pingTimer = setInterval(() => {
        try {
          socket.send(JSON.stringify({ id: String(Date.now()), type: "ping" }));
        } catch {
        }
      }, Math.min(8e3, Math.max(5e3, b.pingInterval - 2e3)));
      watchTimer = setInterval(() => {
        if (Date.now() - streamState.lastMsgAt > b.pingInterval + b.pingTimeout + 4e3) {
          try {
            socket.close();
          } catch {
          }
        }
      }, 5e3);
      streamState.lastMsgAt = Date.now();
      if (onStatus) onStatus({ ...streamState });
    };
    socket.onmessage = (ev) => {
      try {
        handleData(JSON.parse(ev.data));
      } catch {
      }
    };
    socket.onerror = () => {
      streamState.error = "\u062E\u0637\u0623 \u0641\u064A \u0627\u0644\u0642\u0646\u0627\u0629";
    };
    socket.onclose = () => {
      if (seq !== connectSeq) return;
      clearTimers();
      streamState.connected = false;
      streamState.connecting = false;
      if (onStatus) onStatus({ ...streamState });
      scheduleRetry();
    };
  } catch (e) {
    streamState.connecting = false;
    streamState.error = String(e?.message || e);
    if (onStatus) onStatus({ ...streamState });
    scheduleRetry();
  }
}
function scheduleRetry() {
  if (stopped) return;
  if (offline) {
    if (retryTimer) clearTimeout(retryTimer);
    retryTimer = null;
    return;
  }
  attempts++;
  streamState.reconnects++;
  const base = Math.min(15e3, 1e3 * Math.pow(1.6, Math.min(attempts, 8)));
  const wait = base + Math.floor(Math.random() * 750);
  if (retryTimer) clearTimeout(retryTimer);
  retryTimer = setTimeout(connect, wait);
}
function startStream(sym, handlers) {
  stopStream();
  stopped = false;
  attempts = 0;
  symbol = sym;
  onMessage = handlers.onMessage;
  onStatus = handlers.onStatus;
  connect();
}
function setStreamSymbol(sym) {
  if (sym === symbol) return;
  const old = symbol;
  symbol = sym;
  if (ws && ws.readyState === 1) {
    if (old) for (const t of topics(old)) {
      try {
        ws.send(JSON.stringify({ id: "u" + Date.now() + t.length, type: "unsubscribe", topic: t, privateChannel: false, response: false }));
      } catch {
      }
    }
    subscribe();
  }
}
function stopStream() {
  stopped = true;
  if (retryTimer) {
    clearTimeout(retryTimer);
    retryTimer = null;
  }
  clearTimers();
  connectSeq++;
  if (ws) {
    try {
      ws.close();
    } catch {
    }
    ws = null;
  }
  streamState.connected = false;
  streamState.connecting = false;
}
var DIRECT, REST, streamState, ws, pingTimer, watchTimer, retryTimer, symbol, onMessage, onStatus, stopped, attempts, connectSeq, offline;
var init_ws = __esm({
  "src/ws.ts"() {
    DIRECT = "https://api-futures.kucoin.com";
    REST = typeof window !== "undefined" && (window.Capacitor || location.protocol === "capacitor:") ? DIRECT : "/kucoin";
    streamState = {
      connected: false,
      connecting: false,
      lastMsgAt: 0,
      bookAt: 0,
      tradeAt: 0,
      msgs: 0,
      reconnects: 0,
      error: null
    };
    ws = null;
    pingTimer = null;
    watchTimer = null;
    retryTimer = null;
    symbol = null;
    onMessage = null;
    onStatus = null;
    stopped = true;
    attempts = 0;
    connectSeq = 0;
    offline = false;
    if (typeof window !== "undefined" && window.Capacitor) {
      const KN = (window.Capacitor.Plugins || {}).KingNative;
      if (KN && KN.addListener) {
        try {
          const sub = KN.addListener("netChange", (d) => {
            const online = !!(d && d.online);
            if (online) {
              offline = false;
              if (stopped) return;
              forceReconnect("\u0627\u0644\u0645\u0631\u0627\u0642\u0628 \u0627\u0644\u0623\u0635\u0644\u064A: \u0639\u0627\u062F \u0627\u0644\u0625\u0646\u062A\u0631\u0646\u062A \u2014 \u0648\u0635\u0644 \u0641\u0648\u0631\u064A");
            } else {
              offline = true;
              if (retryTimer) {
                clearTimeout(retryTimer);
                retryTimer = null;
              }
              streamState.connected = false;
              streamState.error = "\u0627\u0646\u0642\u0637\u0639 \u0627\u0644\u0625\u0646\u062A\u0631\u0646\u062A \u2014 \u0627\u0644\u0645\u0631\u0627\u0642\u0628 \u0627\u0644\u0623\u0635\u0644\u064A \u064A\u0646\u062A\u0638\u0631 \u0639\u0648\u062F\u0629 \u0627\u0644\u0634\u0628\u0643\u0629";
              if (onStatus) onStatus({ ...streamState });
            }
          });
          if (sub && typeof sub.catch === "function") sub.catch(() => {
          });
        } catch {
        }
        try {
          if (KN.isOnline) KN.isOnline().then((r) => {
            offline = !(r && r.online);
          }).catch(() => {
          });
        } catch {
        }
      }
    }
    if (typeof document !== "undefined") {
      document.addEventListener("visibilitychange", () => {
        if (!document.hidden && !stopped) {
          const silent = Date.now() - streamState.lastMsgAt;
          if (!ws || ws.readyState !== 1 || silent > 3e4) {
            forceReconnect("\u0639\u0648\u062F\u0629 \u0627\u0644\u062A\u0637\u0628\u064A\u0642 \u0644\u0644\u0648\u0627\u062C\u0647\u0629 \u0648\u0627\u0644\u0642\u0646\u0627\u0629 \u0635\u0627\u0645\u062A\u0629");
          }
        }
      });
    }
    if (typeof window !== "undefined") {
      window.addEventListener("online", () => {
        offline = false;
        if (stopped) return;
        forceReconnect("\u0639\u0648\u062F\u0629 \u0627\u0644\u0627\u062A\u0635\u0627\u0644 \u0628\u0627\u0644\u0625\u0646\u062A\u0631\u0646\u062A \u2014 \u0625\u0639\u0627\u062F\u0629 \u0648\u0635\u0644 \u0627\u0644\u0642\u0646\u0627\u0629 \u0641\u0648\u0631\u064B\u0627");
      });
      window.addEventListener("offline", () => {
        offline = true;
        streamState.connected = false;
        streamState.error = "\u0627\u0646\u0642\u0637\u0639 \u0627\u0644\u0627\u062A\u0635\u0627\u0644 \u0628\u0627\u0644\u0625\u0646\u062A\u0631\u0646\u062A \u2014 \u0641\u064A \u0627\u0646\u062A\u0638\u0627\u0631 \u0639\u0648\u062F\u0629 \u0627\u0644\u0634\u0628\u0643\u0629";
        if (onStatus) onStatus({ ...streamState });
        if (retryTimer) {
          clearTimeout(retryTimer);
          retryTimer = null;
        }
      });
    }
    if (typeof navigator !== "undefined" && navigator.onLine !== void 0) {
      setInterval(() => {
        if (stopped) return;
        const online = typeof navigator.onLine === "boolean" ? navigator.onLine : true;
        if (online && offline) {
          offline = false;
          forceReconnect("\u0634\u064F\u0648\u0647\u062F\u062A \u0639\u0648\u062F\u0629 \u0627\u0644\u0634\u0628\u0643\u0629 (\u0641\u062D\u0635 \u062F\u0648\u0631\u064A)");
        }
        if (!online && !offline) {
          offline = true;
          if (retryTimer) {
            clearTimeout(retryTimer);
            retryTimer = null;
          }
          streamState.connected = false;
          streamState.error = "\u0627\u0646\u0642\u0637\u0639 \u0627\u0644\u0627\u062A\u0635\u0627\u0644 \u0628\u0627\u0644\u0625\u0646\u062A\u0631\u0646\u062A";
          if (onStatus) onStatus({ ...streamState });
        }
        if (online && !offline && ws && ws.readyState === 1) {
          const silent = Date.now() - streamState.lastMsgAt;
          if (silent > 45e3) forceReconnect("\u0635\u0645\u062A \u0627\u0644\u0642\u0646\u0627\u0629 \u0623\u0643\u062B\u0631 \u0645\u0646 45 \u062B\u0627\u0646\u064A\u0629 \u0623\u062B\u0646\u0627\u0621 \u0627\u0644\u0639\u0645\u0644 \u0641\u064A \u0627\u0644\u062E\u0644\u0641\u064A\u0629");
        }
      }, 1e4);
    }
  }
});

// src/engine.ts
var engine_exports = {};
__export(engine_exports, {
  FLEET_MODE: () => FLEET_MODE,
  S: () => S,
  backgroundPipeline: () => backgroundPipeline,
  batchHoldActive: () => batchHoldActive,
  bgRadarTick: () => bgRadarTick,
  botTick: () => botTick,
  clearKeys: () => clearKeys,
  contractsForLevel: () => contractsForLevel,
  desk: () => desk,
  detectRemoteServer: () => detectRemoteServer,
  dynamicTpNet: () => dynamicTpNet,
  effLevels: () => effLevels,
  emit: () => emit,
  equity: () => equity,
  exposureCapped: () => exposureCapped,
  fetchKlines: () => fetchKlines,
  flattenAt: () => flattenAt,
  fmtPx: () => fmtPx,
  fmtTime: () => fmtTime,
  fmtUsd: () => fmtUsd,
  gapHunterTick: () => gapHunterTick,
  gapScan: () => gapScan,
  gapSignal: () => gapSignal,
  initEngine: () => initEngine,
  levelQtys: () => levelQtys,
  listContracts: () => listContracts,
  liveSync: () => liveSync,
  maybeSwitchSymbol: () => maybeSwitchSymbol,
  memStats: () => memStats,
  memStepMult: () => memStepMult,
  netAlivePing: () => netAlivePing,
  netDownSince: () => netDownSince,
  netIsDown: () => netIsDown,
  newCycle: () => newCycle,
  onStreamTick: () => onStreamTick,
  openNextReady: () => openNextReady,
  pauseBot: () => pauseBot,
  postHarvestReselect: () => postHarvestReselect,
  priceKick: () => priceKick,
  pulseAge: () => pulseAge,
  pushLog: () => pushLog,
  remoteSync: () => remoteSync,
  saveCfg: () => saveCfg,
  saveKeys: () => saveKeys,
  scanRadar: () => scanRadar,
  startBot: () => startBot,
  startSilentKeepAlive: () => startSilentKeepAlive,
  stopBot: () => stopBot,
  studiedEarlyExit: () => studiedEarlyExit,
  studiedTpZone: () => studiedTpZone,
  subscribe: () => subscribe2,
  volBrakeActive: () => volBrakeActive
});
function dynamicTpNet() {
  const hf = S && S.config && S.config.hfMode;
  const posN = S.position ? Math.max(0, (S.position.size || 0) * (S.multiplier || 1) * (S.position.entry || 0)) : 0;
  const bal = Math.max(1, posN || (S.config && S.config.cycleBalance || 100) * 0.8);
  const prof = profOf() || {};
  const vol = Math.max(5e-3, +prof.vol || 0.05);
  const spread = Math.max(0, +prof.spread || 0.02);
  const volMult = clamp(vol / 0.05, 0.6, 3);
  let target = bal * clamp(3e-3 * volMult, 2e-3, 8e-3);
  target = Math.max(target, bal * spread / 100 * 2 + 0.05);
  if (hf) target = Math.min(target, 0.05);
  target = Math.min(target, Math.max(0.3, (S.config && S.config.cycleBalance || 0) * 0.015));
  return Math.max(0.08, Math.round(target * 100) / 100);
}
function tpBase() {
  const fix = Number(S.config.tpUsd);
  if (fix > 0) return fix;
  return dynamicTpNet();
}
function roundTick(p, t) {
  if (!t || t <= 0) return p;
  const n = Math.round(p / t) * t, d = Math.max(0, Math.ceil(-Math.log10(t)));
  return Number(n.toFixed(d));
}
function beep(tone) {
  try {
    if (typeof window === "undefined") return;
    const c = _kaCtx || new (window.AudioContext || window.webkitAudioContext)();
    if (c.state === "suspended") c.resume().catch(() => {
    });
    const o = c.createOscillator(), g = c.createGain();
    o.connect(g);
    g.connect(c.destination);
    const t = c.currentTime;
    if (tone === "bell") {
      o.type = "sine";
      o.frequency.setValueAtTime(1319, t);
      o.frequency.setValueAtTime(1760, t + 0.1);
      g.gain.setValueAtTime(0.16, t);
      g.gain.exponentialRampToValueAtTime(1e-3, t + 0.35);
      o.start(t);
      o.stop(t + 0.36);
    } else if (tone === "alarm") {
      o.type = "square";
      o.frequency.value = 540;
      g.gain.setValueAtTime(0.1, t);
      g.gain.setValueAtTime(0, t + 0.12);
      g.gain.setValueAtTime(0.1, t + 0.18);
      g.gain.setValueAtTime(0, t + 0.32);
      o.start(t);
      o.stop(t + 0.36);
    } else {
      o.type = "sine";
      o.frequency.setValueAtTime(880, t);
      o.frequency.exponentialRampToValueAtTime(660, t + 0.14);
      g.gain.setValueAtTime(0.1, t);
      g.gain.exponentialRampToValueAtTime(1e-3, t + 0.22);
      o.start(t);
      o.stop(t + 0.24);
    }
    if (c !== _kaCtx) setTimeout(() => {
      try {
        c.close();
      } catch (e) {
        logSwallow("silent-catch", e);
      }
    }, 800);
  } catch (e) {
    logSwallow("silent-catch", e);
  }
}
function lnPlugin() {
  try {
    if (typeof window === "undefined") return null;
    return window.Capacitor && window.Capacitor.Plugins && window.Capacitor.Plugins.LocalNotifications;
  } catch (e) {
    return null;
  }
}
function ensureChannels(LN) {
  if (_channelsReady || !LN.createChannel) return;
  _channelsReady = true;
  LN.createChannel({
    id: "king_alerts",
    name: "\u062A\u0646\u0628\u064A\u0647\u0627\u062A KING",
    description: "\u0635\u0641\u0642\u0627\u062A \u0648\u062C\u0646\u064A \u0623\u0631\u0628\u0627\u062D \u0648\u062A\u062D\u0630\u064A\u0631\u0627\u062A",
    importance: 5,
    visibility: 1,
    vibration: true,
    lights: true
  }).catch(() => {
  });
  LN.createChannel({
    id: "king_status",
    name: "\u062D\u0627\u0644\u0629 KING",
    description: "\u0625\u0634\u0639\u0627\u0631 \u0627\u0644\u0628\u0642\u0627\u0621 \u0627\u0644\u062F\u0627\u0626\u0645",
    importance: 2,
    visibility: -1,
    vibration: false
  }).catch(() => {
  });
}
function nativeNotify(title, body, ongoing) {
  const LN = lnPlugin();
  if (!LN) return;
  ensureChannels(LN);
  LN.requestPermissions().then((r) => {
    if (r.display !== "granted") return;
    LN.schedule({ notifications: [{
      id: ongoing ? 7 : Date.now() % 2e9 + 10,
      title,
      body,
      ongoing: !!ongoing,
      autoCancel: !ongoing,
      channelId: ongoing ? "king_status" : "king_alerts"
    }] }).catch(() => {
    });
  }).catch(() => {
  });
}
function startSilentKeepAlive() {
  if (_kaCtx || !(typeof window !== "undefined" && window.Capacitor)) return;
  try {
    const c = new (window.AudioContext || window.webkitAudioContext)();
    _kaCtx = c;
    const o = c.createOscillator(), g = c.createGain();
    g.gain.value = 1e-4;
    o.frequency.value = 18;
    o.connect(g);
    g.connect(c.destination);
    o.start();
    setInterval(() => {
      if (_kaCtx && _kaCtx.state !== "running") _kaCtx.resume().catch(() => {
      });
    }, 15e3);
  } catch (e) {
    logSwallow("silent-catch", e);
  }
}
function fsStart() {
  if (!(typeof window !== "undefined" && window.Capacitor)) return;
  try {
    const FS = (window.Capacitor.Plugins || {}).ForegroundService;
    if (!FS || !FS.startForegroundService) return;
    FS.startForegroundService({
      id: 7,
      title: "KING \u064A\u0639\u0645\u0644",
      body: "\u0645\u062A\u0635\u0644 \u0628\u0627\u0644\u0645\u0646\u0635\u0629 \u0648\u064A\u0631\u0627\u0642\u0628 \u0627\u0644\u0635\u0641\u0642\u0627\u062A \u0628\u0627\u0633\u062A\u0645\u0631\u0627\u0631",
      smallIcon: "ic_launcher",
      silent: true
    }).catch(() => {
    });
  } catch (e) {
    logSwallow("silent-catch", e);
  }
}
function startForegroundSvc() {
  if (!(typeof window !== "undefined" && window.Capacitor)) return;
  try {
    const P = window.Capacitor && window.Capacitor.Plugins || {};
    const FS = P.ForegroundService;
    fsStart();
    if (!_fsTimer) _fsTimer = setInterval(() => {
      try {
        fsStart();
      } catch (e) {
        logSwallow("silent-catch", e);
      }
    }, 3e5);
    if (typeof document !== "undefined") document.addEventListener("visibilitychange", () => {
      if (!document.hidden) {
        try {
          fsStart();
        } catch (e) {
          logSwallow("silent-catch", e);
        }
      }
    });
    try {
      if (FS && FS.requestPermissions && !localStorage.getItem("abking_notif_asked"))
        FS.requestPermissions().then(() => {
          localStorage.setItem("abking_notif_asked", "1");
        }).catch(() => {
        });
    } catch (e) {
      logSwallow("silent-catch", e);
    }
    const BO = P.BatteryOptimization;
    if (BO && BO.isBatteryOptimizationEnabled && !localStorage.getItem("abking_bo_asked"))
      BO.isBatteryOptimizationEnabled().then((r) => {
        if (r && r.enabled && BO.requestIgnoreBatteryOptimization) {
          localStorage.setItem("abking_bo_asked", "1");
          BO.requestIgnoreBatteryOptimization().catch(() => {
          });
        }
      }).catch(() => {
      });
  } catch (e) {
    logSwallow("silent-catch", e);
  }
}
function startNativeKeepAlive() {
  if (!(typeof window !== "undefined" && window.Capacitor)) return;
  const N = (window.Capacitor.Plugins || {}).KingNative;
  if (!N) return;
  try {
    N.acquireWakeLock && N.acquireWakeLock().catch(() => {
    });
  } catch (e) {
    logSwallow("silent-catch", e);
  }
  try {
    N.startWatchdog && N.startWatchdog().catch(() => {
    });
  } catch (e) {
    logSwallow("silent-catch", e);
  }
  if (!window.__kingHb) window.__kingHb = setInterval(() => {
    try {
      N.heartbeat && N.heartbeat().catch(() => {
      });
    } catch (e) {
      logSwallow("silent-catch", e);
    }
  }, 15e3);
  pushLog("server", "\u0642\u0641\u0644 \u0627\u0644\u0645\u0639\u0627\u0644\u062C \u0648\u0627\u0644\u0645\u0646\u0639\u0651\u0627\u0634 \u0627\u0644\u0630\u0627\u062A\u064A \u0641\u0639\u0651\u0627\u0644\u0627\u0646 \u2014 \u0627\u0644\u0645\u062D\u0631\u0643 \u0644\u0627 \u064A\u0646\u0627\u0645 \u0628\u0625\u0637\u0641\u0627\u0621 \u0627\u0644\u0634\u0627\u0634\u0629");
}
function logRevival() {
  try {
    const at = localStorage.getItem("abking_revived");
    if (at) {
      localStorage.removeItem("abking_revived");
      const s = Math.max(1, Math.round((Date.now() - +at) / 1e3));
      pushLog("server", "\u0623\u064F\u0646\u0639\u0634 \u0627\u0644\u062A\u0637\u0628\u064A\u0642 \u062A\u0644\u0642\u0627\u0626\u064A\u064B\u0627 \u0628\u0639\u062F \u062A\u062C\u0645\u064A\u062F \u0627\u0644\u0646\u0638\u0627\u0645 \u0644\u0647 \u2014 \u0627\u0633\u062A\u0624\u0646\u0641 \u0627\u0644\u0639\u0645\u0644 \u062E\u0644\u0627\u0644 " + s + " \u062B");
    }
  } catch (e) {
    logSwallow("silent-catch", e);
  }
}
function clearNativeOngoing() {
  const LN = lnPlugin();
  if (LN) LN.cancel({ notifications: [{ id: 7 }] }).catch(() => {
  });
}
function notify(txt, kind) {
  if (S.sound) {
    if (kind === "win") {
      beep(S.soundTone);
      setTimeout(() => {
        try {
          beep("bell");
        } catch (e) {
          logSwallow("silent-catch", e);
        }
      }, 200);
    } else if (kind === "warn") {
      beep("alarm");
    } else beep(S.soundTone);
    try {
      if (typeof navigator !== "undefined" && navigator.vibrate) navigator.vibrate(kind === "win" ? [80, 60, 140] : 120);
    } catch (e) {
      logSwallow("silent-catch", e);
    }
  }
  pushLog(kind === "warn" ? "error" : "fill", txt);
  nativeNotify("AB-KING", txt);
}
function subscribe2(fn) {
  listeners.add(fn);
  return () => listeners.delete(fn);
}
function emit() {
  for (const fn of [...listeners]) fn();
}
function emitThrottled() {
  if (emitTimer) return;
  const d = typeof document !== "undefined" && document.hidden ? 1500 : 300;
  emitTimer = setTimeout(() => {
    emitTimer = null;
    emit();
  }, d);
}
function toast(m) {
  S.toastMsg = { text: m, at: Date.now() };
  emit();
}
async function remoteCmd(action, payload) {
  const base = R();
  if (!base) return { ok: false, error: "no server" };
  try {
    const r = await fetch(base + "/cmd", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ action, ...payload || {} })
    });
    return await r.json();
  } catch (e) {
    return { ok: false, error: String(e?.message || e) };
  }
}
function relayServerLogs() {
  const logs = S.logs || [];
  if (_remoteLogSeen == null) {
    _remoteLogSeen = Math.max(0, ...logs.map((l) => l.at || 0));
    return;
  }
  const fresh = logs.filter((l) => (l.at || 0) > _remoteLogSeen);
  if (!fresh.length) return;
  _remoteLogSeen = Math.max(_remoteLogSeen, ...fresh.map((l) => l.at || 0));
  const imp = fresh.filter((l) => l.kind === "fill" || l.kind === "error" || /جني|كابح|تصفية|خدش/.test(l.text || "")).slice(0, 4).reverse();
  let sent = 0;
  for (const l of imp) {
    if (sent >= 2) break;
    if (l.text === _relayText && Date.now() - _relayAt < 6e4) continue;
    _relayText = l.text;
    _relayAt = Date.now();
    sent++;
    const win = l.kind === "fill" && /جني|ربح/.test(l.text || "");
    if (S.sound !== false) {
      if (win) {
        beep(S.soundTone);
        setTimeout(() => {
          try {
            beep("bell");
          } catch (e) {
            logSwallow("silent-catch", e);
          }
        }, 200);
      } else if (l.kind === "error") beep("alarm");
      else beep(S.soundTone);
      try {
        navigator.vibrate && navigator.vibrate(win ? [80, 60, 140] : 120);
      } catch (e) {
        logSwallow("silent-catch", e);
      }
    }
    nativeNotify("AB-KING", l.text);
  }
}
async function remoteSync() {
  const base = R();
  if (!base) return false;
  try {
    const __sel = typeof window !== "undefined" && window.__FLEET_SEL || 0;
    const r = await fetch(base + "/state" + (__sel ? "?sel=" + __sel : ""));
    const j = await r.json();
    if (!j || !j.ok) throw new Error("bad state");
    Object.assign(S, j.state);
    _remoteFails = 0;
    relayServerLogs();
    streamState.connected = true;
    streamState.connecting = false;
    streamState.lastMsgAt = Date.now();
    emit();
    return true;
  } catch (e) {
    _remoteFails++;
    streamState.connected = false;
    if (_remoteFails === 5) toast("\u26A0\uFE0F \u0633\u064A\u0631\u0641\u0631 Termux \u0644\u0627 \u064A\u0631\u062F \u2014 \u0634\u063A\u0651\u0644\u0647 \u0645\u0646 \u062C\u062F\u064A\u062F: node king-server.mjs");
    emit();
    return false;
  }
}
async function detectRemoteServer() {
  if (typeof window === "undefined") return false;
  try {
    const c = new AbortController();
    const t = setTimeout(() => c.abort(), 900);
    const r = await fetch("http://127.0.0.1:8987/state", { signal: c.signal });
    clearTimeout(t);
    const j = await r.json();
    if (j && j.ok) {
      window.__ABKING_REMOTE = "http://127.0.0.1:8987";
      return true;
    }
  } catch (e) {
    logSwallow("silent-catch", e);
  }
  return false;
}
function netIsDown() {
  return _netDownAt > 0;
}
function netDownSince() {
  return _netDownAt > 0 ? Math.round((Date.now() - _wasDownStart) / 1e3) : 0;
}
function netAlivePing(ok) {
  if (ok) {
    const wasDown = _netDownAt > 0;
    _netFailStreak = 0;
    _netDownAt = 0;
    if (wasDown) {
      const secs = Math.round((Date.now() - _wasDownStart) / 1e3);
      pushLog("recover", "\u0639\u0648\u062F\u0629 \u0627\u0644\u0627\u062A\u0635\u0627\u0644 \u0628\u0627\u0644\u0645\u0646\u0635\u0629 \u0628\u0639\u062F \u0627\u0646\u0642\u0637\u0627\u0639 " + secs + " \u062B \u2014 \u0628\u062F\u0623\u062A \u0645\u0631\u0627\u062C\u0639\u0629 \u0627\u0644\u0648\u0636\u0639 \u0648\u0625\u0635\u0644\u0627\u062D \u0627\u0644\u0623\u0648\u0627\u0645\u0631");
      liveSync().then(() => pushLog("recover", "\u{1F527} \u0627\u0643\u062A\u0645\u0644\u062A \u0627\u0644\u0645\u0631\u0627\u062C\u0639\u0629 \u0628\u0639\u062F \u0627\u0644\u0627\u0646\u0642\u0637\u0627\u0639 \u2014 \u0627\u0644\u0645\u0631\u0627\u0643\u0632 \u0648\u0627\u0644\u0623\u0648\u0627\u0645\u0631 \u0648\u0627\u0644\u062C\u0646\u064A \u0645\u062A\u0637\u0627\u0628\u0642\u0629 \u0645\u0639 \u0627\u0644\u0645\u0646\u0635\u0629")).catch(() => pushLog("error", "\u062A\u0639\u0630\u0631\u062A \u0645\u0632\u0627\u0645\u0646\u0629 \u0627\u0644\u0645\u0646\u0635\u0629 \u0628\u0639\u062F \u0627\u0644\u0639\u0648\u062F\u0629 \u2014 \u0633\u064A\u062A\u0643\u0631\u0631 \u0627\u0644\u0645\u062D\u0627\u0648\u0644\u0629 \u0643\u0644 5 \u062B"));
      scanRadar().catch(() => {
      });
    }
  } else {
    const wsSilent = Date.now() - (streamState.lastMsgAt || 0) > 2e4;
    _netFailStreak++;
    if (!wsSilent) _netFailStreak = 1;
    if (_netFailStreak >= 4 && wsSilent && _netDownAt === 0) {
      _netDownAt = Date.now();
      _wasDownStart = _netDownAt;
      pushLog("error", "\u0627\u0646\u0642\u0637\u0627\u0639 \u0627\u0644\u0625\u0646\u062A\u0631\u0646\u062A \u0641\u0639\u0644\u064A\u064B\u0627 (\u0642\u0646\u0627\u0629 \u0635\u0627\u0645\u062A\u0629 + \u0637\u0644\u0628\u0627\u062A \u0641\u0627\u0634\u0644\u0629) \u2014 \u0628\u0627\u0646\u062A\u0638\u0627\u0631 \u0627\u0644\u0639\u0648\u062F\u0629");
    }
  }
}
function saveAll() {
  try {
    localStorage.setItem("abking:cfg", JSON.stringify(S.config));
    localStorage.setItem("abking:keys", S.keys ? JSON.stringify(S.keys) : "");
    localStorage.setItem("abking:snd", S.sound ? "1" : "0");
    localStorage.setItem("abking:tone", S.soundTone || "soft");
    const rt = {
      grid: S.grid,
      position: S.position,
      journal: S.journal,
      realizedPnl: S.realizedPnl,
      feesPaid: S.feesPaid,
      fundingPaid: S.fundingPaid || 0,
      cycleHarvested: S.cycleHarvested || 0,
      priceTrail: S.priceTrail,
      cvd: S.cvd,
      huntCount: S.huntCount,
      lastPrice: S.lastPrice,
      markPrice: S.markPrice,
      activeCycle: S.activeCycle,
      status: S.status,
      history: S.history,
      logs: S.logs,
      savedAt: Date.now(),
      // أعلام دورة الجني (قاعدة المالك: إعادة الاختيار بعد الجني) — تُحفظ حتى لا
      // تضيع إعادة الاختيار عند إعادة تشغيل السيرفر/الجوال بعد الجني مباشرة
      _harvestAt: S._harvestAt || 0,
      _reselectDone: !!S._reselectDone,
      lastHuntAt: S.lastHuntAt || 0,
      _dwellAt: S._dwellAt || 0,
      startedAt: S.startedAt || 0
    };
    localStorage.setItem("abking:rt", JSON.stringify(rt));
    localStorage.setItem("abking:mem", JSON.stringify(S.memory || { pairs: {}, cycles: 0, totalPnl: 0 }));
  } catch (e) {
    logSwallow("silent-catch", e);
  }
}
function loadAll() {
  try {
    const cfg = JSON.parse(localStorage.getItem("abking:cfg") || "null");
    if (cfg) S.config = { ...S.config, ...cfg };
    if (!cfg && FLEET_MODE()) S.config.cycleBalance = Math.max(1, Math.round(S.config.cycleBalance / 5 * 100) / 100);
    const ks = localStorage.getItem("abking:keys");
    if (ks) {
      try {
        S.keys = JSON.parse(ks);
      } catch (e) {
        S.keys = null;
      }
    }
    S.sound = localStorage.getItem("abking:snd") !== "0";
    S.soundTone = localStorage.getItem("abking:tone") || "soft";
    const rt = JSON.parse(localStorage.getItem("abking:rt") || "null");
    if (rt) {
      Object.assign(S, {
        grid: rt.grid || [],
        position: rt.position || null,
        journal: rt.journal || [],
        realizedPnl: rt.realizedPnl || 0,
        feesPaid: rt.feesPaid || 0,
        fundingPaid: rt.fundingPaid || 0,
        cycleHarvested: rt.cycleHarvested || 0,
        priceTrail: rt.priceTrail || [],
        cvd: rt.cvd || 0,
        huntCount: rt.huntCount || 0,
        lastPrice: rt.lastPrice ?? null,
        markPrice: rt.markPrice ?? null,
        activeCycle: rt.activeCycle || null,
        status: rt.status || "idle",
        history: rt.history || [],
        logs: rt.logs || [],
        _harvestAt: rt._harvestAt || 0,
        _reselectDone: !!rt._reselectDone,
        lastHuntAt: rt.lastHuntAt || 0,
        _dwellAt: rt._dwellAt || 0,
        startedAt: rt.startedAt || 0
      });
      S._savedAt = rt.savedAt || 0;
      {
        const __bound = Math.max(50, (S.config.cycleBalance || 0) * 3);
        let __n = 0;
        (S.journal || []).forEach((j) => {
          if (j.status === "closed" && typeof j.pnl === "number" && Math.abs(j.pnl) > __bound) {
            j.pnl = 0;
            __n++;
          }
        });
        if (__n > 0) pushLog("server", "\u{1F9FC} \u062A\u0635\u062D\u064A\u062D " + __n + " \u0635\u0641\u0642\u0629 \u0628\u0642\u064A\u0645 \u0634\u0627\u0630\u0629 \u0645\u0646 \u0625\u0635\u062F\u0627\u0631 \u0633\u0627\u0628\u0642 \u2014 \u0635\u064F\u0641\u0651\u0631\u062A \u0642\u064A\u0645\u0647\u0627 \u0627\u0644\u0648\u0647\u0645\u064A\u0629");
      }
      if (S.position) {
        const __oj = (S.journal || []).find((j) => j.status === "open");
        S._batchFills = Math.max(4, __oj && __oj.mergedOrders || 4);
        if (!S._batchHoldAt) {
          S._batchHoldAt = Date.now();
          S._batchStudiedAt = true;
        }
        pushLog("server", "\u{1F9F7} \u062D\u0645\u0627\u064A\u0629 \u0627\u0644\u0625\u0642\u0644\u0627\u0639: \u0645\u0631\u0643\u0632 \u0642\u0627\u0626\u0645 (" + S._batchFills + " \u0623\u0645\u0631) \u2014 \u0627\u0644\u0642\u0627\u0637\u0639 \u0645\u0642\u0641\u0644 \u062D\u062A\u0649 \u0627\u0644\u062C\u0646\u064A \u0627\u0644\u0643\u0627\u0645\u0644");
      }
    }
    if (S.status === "running") {
      if (S._savedAt && Date.now() - S._savedAt < 30 * 60 * 1e3) {
        pushLog("server", "\u26A1 \u0623\u0648\u0642\u0641 \u0627\u0644\u0646\u0638\u0627\u0645 \u0627\u0644\u062A\u0637\u0628\u064A\u0642 \u0644\u062D\u0638\u064A\u064B\u0627 \u2014 \u0627\u0633\u062A\u064F\u0624\u0646\u0641 \u0627\u0644\u0635\u064A\u062F \u062A\u0644\u0642\u0627\u0626\u064A\u064B\u0627 \u0645\u0646 \u062D\u064A\u062B \u062A\u0648\u0642\u0641");
        nativeNotify("AB-KING \u0639\u0627\u062F \u0644\u0644\u0639\u0645\u0644 \u2713", "\u0623\u0648\u0642\u0641 \u0627\u0644\u0646\u0638\u0627\u0645 \u0627\u0644\u062A\u0637\u0628\u064A\u0642 \u0641\u064A \u0627\u0644\u062E\u0644\u0641\u064A\u0629 \u2014 \u0627\u0633\u062A\u064F\u0624\u0646\u0641 \u0627\u0644\u0635\u064A\u062F \u062A\u0644\u0642\u0627\u0626\u064A\u064B\u0627", true);
      } else {
        S.status = "paused";
        pushLog("info", "\u0627\u0633\u062A\u064F\u0639\u064A\u062F\u062A \u0627\u0644\u062C\u0644\u0633\u0629 \u0645\u062A\u0648\u0642\u0641\u0629 \u0645\u0624\u0642\u062A\u064B\u0627 \u2014 \u0627\u0636\u063A\u0637 \xAB\u062A\u0634\u063A\u064A\u0644\xBB \u0644\u0644\u0628\u062F\u0621");
      }
    }
    const mem = JSON.parse(localStorage.getItem("abking:mem") || "null");
    S.memory = { v: 3, pairs: {}, cycles: 0, totalPnl: 0, profiles: mem && mem.profiles || {} };
  } catch (e) {
    logSwallow("silent-catch", e);
  }
}
function pushLog(kind, text) {
  S.logs = [{ id: uid("l"), at: Date.now(), kind, text }, ...S.logs].slice(0, 80);
}
function logSwallow(where, err) {
  try {
    const msg = String(err && err.message || err || "unknown");
    if (/failed to fetch|networkerror|network error|load failed/i.test(msg)) return;
    if (/aborted?|abort/i.test(msg)) return;
    if (/timeout|timed? ?out/i.test(msg)) return;
    const key = where + "|" + msg;
    const now = Date.now();
    if (now - (_swallowAt[key] || 0) < 18e4) return;
    _swallowAt[key] = now;
    pushLog("warn", where + ": " + msg);
  } catch (e2) {
  }
}
function pushJr(row) {
  row.symbol = row.symbol || S.config.displaySymbol;
  S.journal = [row, ...S.journal].slice(0, 120);
}
function sig(ms) {
  const c = new AbortController();
  setTimeout(() => c.abort(), ms);
  return c.signal;
}
async function kcFetch(path, opts) {
  try {
    const _r = await kcFetchInner(path, opts);
    netAlivePing(true);
    return _r;
  } catch (e) {
    if (e?.name !== "AbortError") netAlivePing(false);
    throw e;
  }
}
async function kcFetchInner(path, opts) {
  if (API_BASE === DIRECT_BASE) {
    if (IS_NATIVE) return await fetch(API_BASE + path, opts);
    try {
      return await fetch(API_BASE + path, opts);
    } catch (e) {
      if (e?.name === "AbortError") throw e;
      API_BASE = "/kucoin";
      return await fetch(API_BASE + path, freshOpts(opts));
    }
  }
  let res;
  try {
    res = await fetch(API_BASE + path, opts);
  } catch (e) {
    if (e?.name === "AbortError") throw e;
    API_BASE = DIRECT_BASE;
    return await fetch(API_BASE + path, freshOpts(opts));
  }
  if (res.status === 404 || !(res.headers.get("content-type") || "").toLowerCase().includes("json")) {
    API_BASE = DIRECT_BASE;
    return await fetch(API_BASE + path, freshOpts(opts));
  }
  return res;
}
function freshOpts(opts) {
  if (!opts || !opts.signal) return opts;
  const o = { ...opts };
  o.signal = sig(8e3);
  return o;
}
function normFee(raw, fb) {
  const n = Number(raw);
  if (!Number.isFinite(n) || n <= 0) return fb;
  const frac = n < 0.05 ? n : n / 100;
  return Math.min(3e-3, Math.max(1e-5, frac));
}
async function hmacB64(secret, payload) {
  const key = await crypto.subtle.importKey(
    "raw",
    new TextEncoder().encode(secret),
    { name: "HMAC", hash: "SHA-256" },
    false,
    ["sign"]
  );
  const sig2 = await crypto.subtle.sign("HMAC", key, new TextEncoder().encode(payload));
  return btoa(String.fromCharCode(...new Uint8Array(sig2)));
}
async function kcPublic(path, timeout) {
  const res = await kcFetch(path, {
    cache: "no-store",
    signal: sig(timeout || 3500)
  });
  if (!res.ok) throw new Error("KuCoin " + res.status);
  const j = await res.json();
  if (j.code !== "200000") throw new Error(j.msg || j.code);
  return j.data;
}
async function kcPrivate(method, path, body) {
  if (!S.keys) throw new Error("\u0644\u0627 \u0645\u0641\u0627\u062A\u064A\u062D \u2014 \u0627\u0631\u0628\u0637 KuCoin \u0645\u0646 \u0627\u0644\u0625\u0639\u062F\u0627\u062F\u0627\u062A");
  const ts = String(Date.now());
  const raw = body ? JSON.stringify(body) : "";
  const signStr = ts + method + path + raw;
  const res = await kcFetch(path, {
    method,
    cache: "no-store",
    signal: sig(6e3),
    headers: {
      "Content-Type": "application/json",
      "KC-API-KEY": S.keys.apiKey,
      "KC-API-SIGN": await hmacB64(S.keys.apiSecret, signStr),
      "KC-API-TIMESTAMP": ts,
      "KC-API-PASSPHRASE": await hmacB64(S.keys.apiSecret, S.keys.passphrase),
      "KC-API-KEY-VERSION": "2"
    },
    body: raw || void 0
  });
  const j = await res.json().catch(() => ({ code: "-1", msg: "bad json" }));
  if (j.code !== "200000") throw new Error(j.msg || "KuCoin " + res.status);
  return j.data;
}
function fetchKlines(symbol2, gran) {
  return kcPublic("/api/v1/kline/query?symbol=" + encodeURIComponent(symbol2) + "&granularity=" + gran, 6e3).then((a) => (a || []).map((r) => {
    let t = +r[0];
    if (t < 1e12) t *= 1e3;
    return { t, o: +r[1], h: +r[2], l: +r[3], c: +r[4], v: +r[5] || 0 };
  }).filter((k) => k.t > 0 && k.o > 0).sort((x, y) => x.t - y.t));
}
function listContracts() {
  if (contractsCache && Date.now() - contractsCache.at < 3e5) return Promise.resolve(contractsCache.items);
  return kcPublic("/api/v1/contracts/active", 6e3).then((d) => {
    const items = (d || []).map((c) => ({
      symbol: c.symbol,
      base: String(c.baseCurrency || c.symbol.replace(/USDTM?$/i, "")),
      name: String(c.baseCurrency || "")
    }));
    contractsCache = { at: Date.now(), items };
    return items;
  });
}
function radarScore(x) {
  let sc = 50;
  const why = [];
  if (x.price > 0) {
    if (x.price < 1e-3) {
      sc += 16;
      why.push("\u0635\u0641\u0631\u064A\u0629 \u0645\u062B\u0627\u0644\u064A\u0629");
    } else if (x.price < 0.01) {
      sc += 12;
      why.push("\u0635\u0641\u0631\u064A\u0629");
    } else if (x.price < 0.1) {
      sc += 8;
    } else if (x.price < 1) {
      sc += 3;
    } else if (x.price > 100) {
      sc -= 18;
      why.push("\u063A\u0627\u0644\u064A\u0629 \u2014 \u0644\u0627 \u062A\u0646\u0627\u0633\u0628 \u0631\u0623\u0633 \u0627\u0644\u0645\u0627\u0644");
    } else if (x.price > 10) {
      sc -= 8;
      why.push("\u0633\u0639\u0631 \u0645\u0631\u062A\u0641\u0639");
    }
  }
  if (x.vol >= 0.18 && x.vol <= 1.1) {
    sc += 18;
    why.push("\u062A\u0630\u0628\u0630\u0628 \u0645\u062B\u0627\u0644\u064A");
  } else if (x.vol > 1.1 && x.vol <= 2) {
    sc += 6;
    why.push("\u062A\u0630\u0628\u0630\u0628 \u062D\u0627\u062F");
  } else if (x.vol < 0.18) {
    sc -= 14;
    why.push("\u0633\u0648\u0642 \u0645\u064A\u062A");
  } else {
    sc -= 10;
    why.push("\u062A\u0630\u0628\u0630\u0628 \u0648\u062D\u0634\u064A");
  }
  if (x.spread > 0) {
    if (x.spread <= 0.03) sc += 12;
    else if (x.spread <= 0.08) sc += 6;
    else if (x.spread > 0.2) {
      sc -= 16;
      why.push("\u0633\u0628\u0631\u064A\u062F \u0648\u0627\u0633\u0639");
    }
  }
  if (x.turnover >= 5e7) sc += 10;
  else if (x.turnover >= 1e7) sc += 6;
  else if (x.turnover < 1e6) {
    sc -= 14;
    why.push("\u0633\u064A\u0648\u0644\u0629 \u0636\u0639\u064A\u0641\u0629");
  }
  const a = Math.abs(x.trendPct);
  if (a < 1.2) {
    sc += 8;
    x.state = "\u0639\u0631\u0636\u064A \u0645\u062A\u0630\u0628\u0630\u0628";
  } else if (a < 3.5) {
    sc += 4;
    x.state = x.trendPct > 0 ? "\u0635\u0627\u0639\u062F" : "\u0647\u0627\u0628\u0637";
  } else {
    sc -= 4;
    x.state = x.trendPct > 0 ? "\u0635\u0627\u0639\u062F \u0628\u0642\u0648\u0629" : "\u0647\u0627\u0628\u0637 \u0628\u0642\u0648\u0629";
    why.push("\u0627\u062A\u062C\u0627\u0647 \u062D\u0627\u062F");
  }
  if (Math.abs(x.chg) > 12) {
    sc -= 6;
    why.push("\u062D\u0631\u0643\u0629 \u064A\u0648\u0645 \u0645\u062A\u0637\u0631\u0641\u0629");
  }
  return { score: clamp(Math.round(sc), 0, 100), why };
}
async function backgroundPipeline() {
  if (S.status !== "running") return;
  if (Date.now() - _bgPipeAt < 3e4) return;
  _bgPipeAt = Date.now();
  try {
    const r = S.radar && S.radar.list && S.radar.list.length ? S.radar : await scanRadar();
    if (!r || !r.list || !r.list.length) return;
    const cands = r.list.filter((l) => l.symbol !== S.config.symbol && (l.score || 0) >= 55);
    if (!cands.length) return;
    const nxt = cands[0];
    S._nextReady = true;
    S._nextStudiedAt = Date.now();
    if (S._nextSymbol !== nxt.symbol) {
      S._nextSymbol = nxt.symbol;
      if (Date.now() - (S._bgLogAt || 0) > 6e5) {
        S._bgLogAt = Date.now();
        pushLog("server", "\u{1F9E0} \u0627\u0644\u062E\u0637 \u0627\u0644\u062E\u0644\u0641\u064A \u0627\u0644\u0645\u0633\u062A\u0645\u0631: \u0627\u0644\u0645\u0631\u0634\u062D \u0627\u0644\u062A\u0627\u0644\u064A " + nxt.disp + " (\u062F\u0631\u062C\u0629 " + nxt.score + ") \u0645\u0631\u0634\u0651\u062D \u0628\u0639\u062F \u0627\u0644\u062C\u0646\u064A \u2014 \u064A\u064F\u062E\u062A\u0627\u0631 \u0625\u0646 \u0643\u0627\u0646\u062A \u0645\u0646\u0637\u0642\u062A\u0647 \u0623\u0642\u0648\u0649");
      }
    }
  } catch (e) {
    logSwallow("silent-catch", e);
  }
}
function openNextReady() {
  if (!S._nextSymbol || !S._nextReady) return false;
  if (S.position) return false;
  if (!S._harvestAt || Date.now() - S._harvestAt > 10 * 60 * 1e3) {
    S._nextReady = false;
    S._nextSymbol = null;
    return false;
  }
  if (Date.now() - (S._nextStudiedAt || 0) > 15 * 60 * 1e3) {
    S._nextReady = false;
    S._nextSymbol = null;
    return false;
  }
  if (S._nextSymbol === S.config.symbol) {
    S._nextReady = false;
    return false;
  }
  const sym = S._nextSymbol;
  S._nextReady = false;
  S._nextSymbol = null;
  S._reselectDone = true;
  pushLog("server", "\u26A1 \u0641\u062A\u062D \u0641\u0648\u0631\u064A \u0628\u0639\u062F \u0627\u0644\u062C\u0646\u064A: \u0627\u0644\u0639\u0645\u0644\u0629 \u0627\u0644\u0645\u062F\u0631\u0648\u0633\u0629 \u062E\u0644\u0641\u064A\u064B\u0627 " + sym.replace(/USDTM$/i, "") + " \u062A\u062F\u062E\u0644 \u0627\u0644\u0622\u0646");
  saveCfg({
    symbol: sym,
    leverage: S.config.leverage,
    levels: S.config.levels,
    gridStepPct: S.config.gridStepPct,
    huntPct: S.config.huntPct,
    tpNet: S.config.tpNet,
    tpUsd: S.config.tpUsd,
    cycleBalance: S.config.cycleBalance,
    directionMode: S.config.directionMode,
    mode: S.config.mode,
    sound: S.sound,
    soundTone: S.soundTone
  });
  return true;
}
async function scanRadar() {
  if (S._radarBusy) return S.radar;
  S._radarBusy = true;
  try {
    const all = await kcPublic("/api/v1/contracts/active", 6e3);
    const rows0 = (all || []).filter((c) => c && c.symbol && /USDTM$/i.test(c.symbol) && c.status === "Open").map((c) => ({
      symbol: c.symbol,
      turnover: +(c.turnoverOf24h ?? c.turnover24h ?? c.volumeOf24h ?? c.volume24h ?? 0) || 0,
      price: +(c.lastTradePrice ?? c.markPrice ?? 0) || 0,
      chg: +(c.priceChgPct ?? 0) * 100 || 0,
      funding: +(c.fundingFeeRate ?? 0) || 0,
      fundHrs: +(c.fundingRateGranularity ?? 288e5) / 36e5 || 8,
      nextFundMs: +(c.nextFundingRateTime ?? 0) || 0,
      oi: +(c.openInterest ?? 0) || 0,
      maxLev: +(c.maxLeverage ?? 0) || 0,
      markP: +(c.markPrice ?? 0) || 0,
      idxP: +(c.indexPrice ?? 0) || 0,
      lowP: +(c.lowPrice ?? 0) || 0,
      highP: +(c.highPrice ?? 0) || 0
    })).filter((r) => r.turnover > 0 && r.price > 0);
    const rows = rows0;
    const preScored = rows.map((r) => {
      let sc = 50;
      if (r.turnover >= 5e7) sc += 10;
      else if (r.turnover >= 1e7) sc += 6;
      else if (r.turnover < 3e5) sc -= 25;
      else if (r.turnover < 1e6) sc -= 12;
      const a = Math.abs(r.chg);
      if (a < 1.2) sc += 8;
      else if (a < 3.5) sc += 4;
      else if (a > 8) sc -= 8;
      else sc -= 3;
      const range = r.lowP > 0 ? (r.highP - r.lowP) / r.lowP * 100 : 0;
      if (range >= 4) sc += 10;
      else if (range >= 2) sc += 6;
      else if (range > 0 && range < 0.6) sc -= 6;
      const basis = r.markP > 0 && r.idxP > 0 ? (r.markP - r.idxP) / r.idxP * 100 : 0;
      if (Math.abs(basis) > 0.2) sc += 6;
      if (r.price > 0 && r.price < 0.01) sc += 6;
      else if (r.price > 100) sc -= 10;
      const oiUsd = r.oi * r.price;
      if (r.oi > 0 && r.turnover > 0) {
        const rto = oiUsd / r.turnover;
        if (rto < 0.5) sc += 4;
        else if (rto > 5) sc -= 4;
      }
      return { ...r, preScore: Math.max(0, Math.min(100, Math.round(sc))) };
    }).sort((a, b) => b.preScore - a.preScore);
    const det = await Promise.all(preScored.slice(0, 30).map(async (r) => {
      const out = { ...r, vol: 0, spread: 0, trendPct: 0, price: 0 };
      try {
        const ks = await fetchKlines(r.symbol, 15);
        if (ks.length >= 20) {
          const last = ks.slice(-24);
          let v = 0;
          for (const k of last) v += (k.h - k.l) / k.c;
          out.vol = v / last.length * 100;
          out.price = ks[ks.length - 1].c;
          out.trendPct = (out.price - ks[ks.length - 20].o) / ks[ks.length - 20].o * 100;
        }
      } catch (e) {
        logSwallow("silent-catch", e);
      }
      try {
        const t = await fetchTicker(r.symbol);
        const bb = +t.bestBidPrice || 0, ba = +t.bestAskPrice || 0;
        if (bb > 0 && ba > 0) out.spread = (ba - bb) / ((ba + bb) / 2) * 100;
        if (!out.price) out.price = +t.price || 0;
      } catch (e) {
        logSwallow("silent-catch", e);
      }
      return out;
    }));
    const deepList = det.filter((d) => d.price > 0).map((d) => {
      const s = radarScore(d);
      const fundAnn = (d.funding || 0) * (24 / (d.fundHrs || 8)) * 100;
      const oiUsd = (d.oi || 0) * (d.price || 0);
      const skew = d.chg < 0 && d.funding < 0 ? "\u0634\u0648\u0631\u062A \u0645\u062A\u0643\u062F\u0633 \u2014 \u0645\u0631\u0634\u062D \u0627\u0646\u0639\u0643\u0627\u0633 \u0635\u0627\u0639\u062F" : d.chg > 0 && d.funding > 0 ? "\u0644\u0648\u0646\u063A \u0645\u062A\u0643\u062F\u0633 \u2014 \u062D\u0630\u0631 \u0645\u0646 \u0627\u0646\u0639\u0643\u0627\u0633 \u0647\u0627\u0628\u0637" : (d.funding || 0) !== 0 ? "\u0645\u062D\u0627\u064A\u062F" : "\u2014";
      const basis = d.markP > 0 && d.idxP > 0 ? (d.markP - d.idxP) / d.idxP * 100 : 0;
      const range = d.lowP > 0 ? (d.highP - d.lowP) / d.lowP * 100 : 0;
      const oiToVol = d.turnover > 0 ? oiUsd / d.turnover : 0;
      return {
        symbol: d.symbol,
        disp: d.symbol.replace(/USDTM$/i, "").replace(/^XBT/i, "BTC"),
        price: d.price,
        score: s.score,
        why: s.why,
        state: d.state || "\u2014",
        vol: +d.vol.toFixed(2),
        chg: Math.round(d.chg * 10) / 10,
        turnover: d.turnover,
        funding: +(d.funding || 0).toFixed(6),
        fundHrs: d.fundHrs || 8,
        fundAnnPct: +fundAnn.toFixed(3),
        nextFundMs: d.nextFundMs || 0,
        oi: +d.oi || 0,
        oiUsd,
        oiLev: d.maxLev || 0,
        skew,
        basisPct: +basis.toFixed(3),
        rangePct: +range.toFixed(1),
        oiToVolPct: +(oiToVol * 100).toFixed(0),
        grade: s.score >= 65 ? "\u{1F7E2}" : s.score >= 45 ? "\u{1F7E1}" : "\u{1F534}"
      };
    }).sort((a, b) => b.score - a.score || (b.state === "\u0639\u0631\u0636\u064A \u0645\u062A\u0630\u0628\u0630\u0628" ? 1 : 0) - (a.state === "\u0639\u0631\u0636\u064A \u0645\u062A\u0630\u0628\u0630\u0628" ? 1 : 0) || b.turnover - a.turnover);
    const deepSyms = new Set(deepList.map((x) => x.symbol));
    const rest = preScored.filter((r) => !deepSyms.has(r.symbol)).map((r) => ({
      symbol: r.symbol,
      disp: r.symbol.replace(/USDTM$/i, "").replace(/^XBT/i, "BTC"),
      price: r.price,
      score: r.preScore,
      why: ["full-universe watch"],
      state: "watch",
      vol: 0,
      chg: Math.round(r.chg * 10) / 10,
      turnover: r.turnover,
      funding: +(r.funding || 0).toFixed(6),
      fundHrs: r.fundHrs || 8,
      fundAnnPct: +((r.funding || 0) * (24 / (r.fundHrs || 8)) * 100).toFixed(3),
      nextFundMs: r.nextFundMs || 0,
      oi: +r.oi || 0,
      oiUsd: (r.oi || 0) * (r.price || 0),
      oiLev: r.maxLev || 0,
      skew: "-",
      basisPct: 0,
      rangePct: 0,
      oiToVolPct: 0,
      grade: r.preScore >= 65 ? "\u{1F7E2}" : r.preScore >= 45 ? "\u{1F7E1}" : "\u{1F534}"
    }));
    const list = [...deepList, ...rest];
    const fundRanked = [...list].sort((a, b) => b.fundAnnPct - a.fundAnnPct);
    const oiRanked = [...list].sort((a, b) => b.oiUsd - a.oiUsd).slice(0, 5);
    const gridRanked = [...list].filter((x) => x.turnover > 2e6).sort((a, b) => b.rangePct - a.rangePct).slice(0, 5);
    const basisRanked = [...list].filter((x) => x.turnover > 2e6 && Math.abs(x.basisPct) > 0.1).sort((a, b) => Math.abs(b.basisPct) - Math.abs(a.basisPct)).slice(0, 5);
    S.intel = {
      at: Date.now(),
      hottest: fundRanked.slice(0, 5),
      coldest: fundRanked.slice(-3).reverse(),
      biggestOi: oiRanked,
      bestGrid: gridRanked,
      extremeBasis: basisRanked,
      squeeze: fundRanked.filter((f) => f.fundAnnPct >= 0.05 && f.chg > 3).slice(0, 3),
      rebound: fundRanked.filter((f) => f.fundAnnPct <= -0.03 && f.chg < -3).slice(0, 3)
    };
    let flist = list;
    if (FLEET_MODE()) {
      const taken = S._fleetTaken || [];
      if (taken.length) flist = list.filter((l) => l.symbol === S.config.symbol || !taken.includes(l.symbol));
    }
    S.radar = { at: Date.now(), list: flist };
    const cur = flist.find((l) => l.symbol === S.config.symbol);
    const top = flist[0];
    if (top && cur && top.symbol !== cur.symbol && top.score - cur.score >= 18 && cur.score < 50) {
      if (!S._radarTipAt || Date.now() - S._radarTipAt > 18e5) {
        S._radarTipAt = Date.now();
        pushLog("server", "\u{1F4E1} \u0631\u0627\u062F\u0627\u0631: " + cur.disp + " \u0636\u0639\u064A\u0641\u0629 (" + cur.score + ") \u0628\u064A\u0646\u0645\u0627 " + top.disp + " " + top.state + " \u0628\u062F\u0631\u062C\u0629 " + top.score + " \u2014 \u064A\u064F\u0646\u0635\u062D \u0628\u0627\u0644\u062A\u0628\u062F\u064A\u0644 \u0628\u0639\u062F \u0625\u063A\u0644\u0627\u0642 \u0623\u064A \u0645\u0631\u0643\u0632");
        toast("\u{1F4E1} \u0627\u0644\u0631\u0627\u062F\u0627\u0631 \u064A\u0642\u062A\u0631\u062D " + top.disp + " \u0628\u062F\u0644 " + cur.disp);
        nativeNotify("\u{1F4E1} \u0641\u0631\u0635\u0629 \u0623\u0642\u0648\u0649", top.disp + " " + top.state + " \u2014 \u062F\u0631\u062C\u0629 " + top.score + " \u0645\u0642\u0627\u0628\u0644 " + cur.score + " \u0644\u0639\u0645\u0644\u062A\u0643");
      }
    } else if (top && !cur && top.score >= 70) {
      if (!S._radarTipAt || Date.now() - S._radarTipAt > 18e5) {
        S._radarTipAt = Date.now();
        pushLog("server", "\u{1F4E1} \u0631\u0627\u062F\u0627\u0631: " + top.disp + " " + top.state + " \u0628\u062F\u0631\u062C\u0629 " + top.score + " \u2014 \u0623\u0642\u0648\u0649 \u0641\u0631\u0635\u0629 \u0627\u0644\u0622\u0646");
      }
    }
    emit();
    saveAll();
    return S.radar;
  } catch (e) {
    return S.radar;
  } finally {
    S._radarBusy = false;
  }
}
async function postHarvestReselect() {
  if (S.status !== "running" || S.position) return;
  if (!S._harvestAt || S._reselectDone) return;
  if (Date.now() - S._harvestAt > 15 * 60 * 1e3) {
    S._reselectDone = true;
    return;
  }
  if (Date.now() - S._harvestAt > 5 * 60 * 1e3 && !S._reselectBusy) {
    S._reselectDone = true;
    pushLog("server", "\u23F1 \u0628\u0639\u062F \u0627\u0644\u062C\u0646\u064A: \u062A\u0639\u0630\u0631 \u0627\u0643\u062A\u0645\u0627\u0644 \u0645\u0633\u062D \u0627\u0644\u0631\u0627\u062F\u0627\u0631 \u2014 \u0627\u0633\u062A\u0626\u0646\u0627\u0641 \u0627\u0644\u0635\u064A\u062F \u0639\u0644\u0649 \u0627\u0644\u0639\u0645\u0644\u0629 \u0627\u0644\u062D\u0627\u0644\u064A\u0629");
    return;
  }
  try {
    const r = await scanRadar();
    if (!r || !r.list || !r.list.length) return;
    const cur = r.list.find((l) => l.symbol === S.config.symbol);
    const bestOther = r.list.filter((l) => l.symbol !== S.config.symbol).find((l) => (l.score || 0) >= 55);
    S._reselectDone = true;
    S._nextReady = false;
    S._nextSymbol = null;
    if (!bestOther) {
      pushLog("server", "\u{1F50E} \u0628\u0639\u062F \u0627\u0644\u062C\u0646\u064A: \u0644\u0627 \u0639\u0645\u0644\u0629 \u0623\u0642\u0648\u0649 \u0645\u0646 " + (cur ? cur.disp : S.config.displaySymbol) + " \u0641\u064A \u0627\u0644\u0631\u0627\u062F\u0627\u0631 \u2014 \u0627\u0633\u062A\u0626\u0646\u0627\u0641 \u0627\u0644\u0635\u064A\u062F \u0639\u0644\u0649 \u0627\u0644\u0639\u0645\u0644\u0629 \u0627\u0644\u062D\u0627\u0644\u064A\u0629");
      return;
    }
    const curSc = cur ? cur.score || 0 : 0;
    _lastSwitchAt = Date.now();
    const curDisp = cur ? cur.disp : S.config.displaySymbol;
    pushLog("server", "\u{1F504} \u0628\u0639\u062F \u0627\u0644\u062C\u0646\u064A: \u0627\u0646\u062A\u0642\u0627\u0644 \u0645\u0628\u0627\u0634\u0631 \u0645\u0646 " + curDisp + " (" + curSc + ") \u0625\u0644\u0649 " + bestOther.disp + " (" + bestOther.score + ") \u2014 \u0645\u0646\u0637\u0642\u0629 \u0623\u0642\u0648\u0649 \u2014 \u062A\u064F\u062F\u0631\u0633 \u062B\u0645 \u062A\u064F\u0641\u062A\u062D \u0627\u0644\u0635\u0641\u0642\u0629");
    toast("\u{1F504} \u0628\u0639\u062F \u0627\u0644\u062C\u0646\u064A: " + bestOther.disp + " \u2014 " + bestOther.score);
    nativeNotify("AB-KING \u2014 \u0627\u0646\u062A\u0642\u0627\u0644 \u0628\u0639\u062F \u0627\u0644\u062C\u0646\u064A", bestOther.disp + " \u062F\u0631\u062C\u0629 " + bestOther.score);
    saveCfg({
      symbol: bestOther.symbol,
      leverage: S.config.leverage,
      levels: S.config.levels,
      gridStepPct: S.config.gridStepPct,
      huntPct: S.config.huntPct,
      tpNet: S.config.tpNet,
      tpUsd: S.config.tpUsd,
      cycleBalance: S.config.cycleBalance,
      directionMode: S.config.directionMode,
      mode: S.config.mode,
      sound: S.sound,
      soundTone: S.soundTone
    });
  } catch (e) {
    logSwallow("silent-catch", e);
  }
}
function bgRadarTick() {
  if (!R()) {
    if (Date.now() - _bgScanAt < 5 * 60 * 1e3) return;
    _bgScanAt = Date.now();
    scanRadar().catch(() => {
    });
  }
}
async function gapScan() {
  if (!GAP_MODE_ON()) return null;
  if (Date.now() - _gapScanAt < 60 * 1e3) return _gapStats;
  _gapScanAt = Date.now();
  try {
    const all = await kcPublic("/api/v1/contracts/active", 6e3);
    const liq = (all || []).filter((c) => /USDTM$/.test(c.symbol) && c.status === "Open" && +(c.turnoverOf24h || 0) > 3e6 && +(c.lastTradePrice || 0) > 0 && +(c.indexPrice || 0) > 0);
    const out = [];
    for (const c of liq.slice(0, 60)) {
      const dev = (+c.lastTradePrice - +c.indexPrice) / +c.indexPrice * 100;
      if (dev < -0.15) {
        out.push({
          symbol: c.symbol,
          dev,
          oi: +c.openInterest || 0,
          turn: +c.turnoverOf24h,
          fund: +c.fundingFeeRate || 0,
          last: +c.lastTradePrice,
          idx: +c.indexPrice
        });
      }
    }
    out.sort((a, b) => a.dev - b.dev);
    _gapStats = { at: Date.now(), list: out };
    return _gapStats;
  } catch (e) {
    logSwallow("silent-catch", e);
    return _gapStats;
  }
}
async function gapSignal(symbol2) {
  try {
    const [ixr, pkr] = await Promise.all([
      kcPublic("/api/v1/kline/query?symbol=.K" + symbol2.replace("USDTM", "USDT") + "&granularity=5", 6e3),
      kcPublic("/api/v1/kline/query?symbol=" + symbol2 + "&granularity=5", 6e3)
    ]);
    const idx = {}, perp = {};
    (ixr || []).forEach((k) => idx[k[0]] = +k[4]);
    (pkr || []).forEach((k) => perp[k[0]] = +k[4]);
    const devs = [];
    for (const t in perp) {
      if (idx[t] > 0 && perp[t] > 0) devs.push({ t: +t, d: (perp[t] - idx[t]) / idx[t] * 100, p: perp[t], idx: idx[t] });
    }
    if (devs.length < 27) return null;
    devs.sort((a, b) => a.t - b.t);
    const win = devs.slice(-25, -1);
    const mean = win.reduce((a, b) => a + b.d, 0) / win.length;
    const sd = Math.sqrt(win.reduce((a, b) => a + (b.d - mean) ** 2, 0) / win.length);
    if (!(sd > 1e-3)) return null;
    const cur = devs[devs.length - 1];
    const z = (cur.d - mean) / sd;
    const stable = devs.length > 3 && cur.p >= devs[devs.length - 4].p * 0.9985;
    const deep = cur.d < -0.15 && z < -2;
    const idxNow = cur.idx || cur.p;
    const idx6h = devs.length > 6 ? devs[devs.length - 7].idx || idxNow : idxNow;
    const idxFlat = Math.abs((idxNow - idx6h) / idx6h * 100) < 1.5;
    const ready = deep && stable && idxFlat;
    return { z, dev: cur.d, ready, price: cur.p, exitDev: mean };
  } catch (e) {
    return null;
  }
}
function gapExitCheck(p) {
  const ctx = S._gapCtx;
  if (!ctx) return false;
  const dev = (p - ctx.idx) / ctx.idx * 100;
  if (dev >= ctx.exitDev) return true;
  return false;
}
async function confluenceScore(symbol2, dev) {
  let score = 0;
  try {
    const tr = await kcPublic("/api/v1/trade/history?symbol=" + encodeURIComponent(symbol2), 4e3);
    const trades = (Array.isArray(tr) ? tr : []).slice(0, 100);
    let whaleNet = 0;
    if (trades.length >= 20) {
      const sizes = trades.map((t) => +t.size).sort((a, b) => a - b);
      const med = sizes[Math.floor(sizes.length / 2)] || 1;
      let lastPx = 0;
      for (const t of trades) {
        if (+t.price > 0) lastPx = +t.price;
      }
      if (lastPx > 0) {
        for (const t of trades) {
          if (+t.size > med * 8) whaleNet += (t.side === "buy" ? 1 : -1) * +t.size * lastPx;
        }
      }
    }
    if (whaleNet > 0) score++;
    const bk = await kcPublic("/api/v1/level2/depth20?symbol=" + encodeURIComponent(symbol2), 3e3);
    let imb = 0;
    if (bk && bk.bids && bk.asks) {
      const bd = (bk.bids || []).reduce((a, x) => a + +x[1], 0);
      const ad = (bk.asks || []).reduce((a, x) => a + +x[1], 0);
      if (bd + ad > 0) imb = (bd - ad) / (bd + ad);
    }
    if (imb > 0.05) score++;
  } catch (e) {
    logSwallow("silent-catch", e);
  }
  if (dev < -0.15) score += 2;
  return score;
}
function inPeakWindow() {
  const h = (/* @__PURE__ */ new Date()).getUTCHours();
  return h >= 13 && h <= 19;
}
async function btcLead() {
  try {
    const now = Date.now();
    if (_btcLead && now - _btcLead.t < 6e4) return _btcLead.ret;
    const k = await kcPublic("/api/v1/kline/query?symbol=XBTUSDTM&granularity=1", 3e3);
    const rows = (Array.isArray(k) ? k : []).slice(-3);
    if (rows.length < 2) return 0;
    const last = +rows[rows.length - 1][4], prev = +rows[0][4];
    const ret = prev > 0 ? (last - prev) / prev * 100 : 0;
    _btcLead = { t: now, ret };
    return ret;
  } catch (e) {
    return 0;
  }
}
async function wallReal(symbol2) {
  try {
    const snap = async () => {
      const b = await kcPublic("/api/v1/level2/depth20?symbol=" + encodeURIComponent(symbol2), 3e3);
      if (!b || !b.bids || !b.bids.length) return null;
      let mx = 0, mxP = 0;
      for (const x of b.bids.slice(0, 15)) {
        if (+x[1] > mx) {
          mx = +x[1];
          mxP = +x[0];
        }
      }
      return { wallP: mxP, wallS: mx };
    };
    const s1 = await snap();
    if (!s1 || !(s1.wallS > 0)) return true;
    await new Promise((r) => setTimeout(r, 15e3));
    const s2 = await snap();
    if (!s2) return true;
    const sameZone = Math.abs(s2.wallP - s1.wallP) / Math.max(1e-12, s1.wallP) < 4e-3;
    const stillSized = s2.wallS >= s1.wallS * 0.5;
    return sameZone && stillSized;
  } catch (e) {
    return true;
  }
}
async function gapHunterTick() {
  if (S._gapHuntBusy) return;
  S._gapHuntBusy = true;
  try {
    const st = await gapScan();
    if (!st || !st.list || !st.list.length) return;
    for (const cand of st.list.slice(0, 3)) {
      const sig2 = await gapSignal(cand.symbol);
      if (sig2 && sig2.ready) {
        const cs = await confluenceScore(cand.symbol, sig2.dev);
        if (cs < 3) {
          continue;
        }
        if (!inPeakWindow()) {
          continue;
        }
        const lead = await btcLead();
        if (lead < -0.08) {
          continue;
        }
        if (!await wallReal(cand.symbol)) {
          continue;
        }
        if (cand.symbol !== S.config.symbol) {
          pushLog("server", "\u{1F50D} \u0635\u0627\u0626\u062F \u0627\u0644\u0641\u062C\u0648\u0627\u062A: \u062A\u062D\u0648\u064A\u0644 \u0625\u0644\u0649 " + cand.symbol.replace(/USDTM$/, "") + " \u2014 \u0641\u062C\u0648\u0629 " + sig2.dev.toFixed(2) + "% (z=" + sig2.z.toFixed(1) + ")");
          S.config.symbol = cand.symbol;
          S.config.displaySymbol = cand.symbol.replace(/USDTM$/i, "").replace(/^XBT$/i, "BTC");
          S.multiplier = 1;
          S.tickSize = 0.1;
          S._metaSym = null;
          S._metaAt = 0;
          setStreamSymbol(cand.symbol);
          S.grid = [];
          S.tape = [];
          S.priceTrail = [];
          S.emaFast = S.emaSlow = null;
        }
        S._gapCtx = { idx: cand.idx, exitDev: sig2.exitDev, entryAt: Date.now() };
        S.lastPrice = sig2.price;
        S._metaSym = null;
        S._metaAt = 0;
        try {
          const ct = await kcPublic("/api/v1/contracts/" + encodeURIComponent(cand.symbol), 4e3);
          if (ct && ct.multiplier) {
            S.multiplier = +ct.multiplier;
            S.tickSize = +ct.tickSize || S.tickSize;
            S._metaSym = cand.symbol;
            S._metaAt = Date.now();
          }
        } catch (e) {
          logSwallow("silent-catch", e);
        }
        maybeHunt(sig2.price);
        pushLog("server", "\u2696\uFE0F \u0641\u062C\u0648\u0629 \u0645\u062C\u0647\u0632\u0629: \u062F\u062E\u0648\u0644 \u0637\u0648\u064A\u0644 @ " + fmtPx(sig2.price) + " \u2014 \u062E\u0631\u0648\u062C \u0639\u0646\u062F \u0639\u0648\u062F\u0629 \u0627\u0644\u0641\u062C\u0648\u0629");
        break;
      }
    }
  } catch (e) {
    logSwallow("silent-catch", e);
  } finally {
    S._gapHuntBusy = false;
  }
}
async function maybeSwitchSymbol() {
  if (S.status !== "running") return;
  if (S.position) {
    try {
      await scanRadar();
    } catch (e) {
      logSwallow("silent-catch", e);
    }
    return;
  }
  if (Date.now() - _lastSwitchAt < 45e3) return;
  if (studying()) return;
  try {
    const r = await scanRadar();
    if (!r || !r.list || !r.list.length) return;
    const cur = r.list.find((l) => l.symbol === S.config.symbol);
    const best = r.list.filter((l) => l.symbol !== S.config.symbol).find((l) => (l.score || 0) >= 55);
    if (!best) return;
    const afterHarvest = !!(S._harvestAt && Date.now() - S._harvestAt < 10 * 60 * 1e3);
    const idleMs = Date.now() - (S._dwellAt || S.startedAt || S._savedAt || Date.now());
    const barren = idleMs > 15 * 60 * 1e3;
    if (barren && best) {
      _lastSwitchAt = Date.now();
      const curDisp2 = cur ? cur.disp : S.config.displaySymbol, curSc2 = cur ? cur.score : 0;
      pushLog("server", "\u{1F504} \u062F\u0648\u0631\u0627\u0646 \u0627\u0644\u0628\u0648\u0631\u0629 (" + Math.round(idleMs / 6e4) + " \u062F \u0628\u0644\u0627 \u062A\u0646\u0641\u064A\u0630): " + curDisp2 + " (" + curSc2 + ") \u2192 " + best.disp + " (" + best.score + ") \u2014 \u0644\u0627 \u062C\u0644\u0648\u0633 \u0639\u0644\u0649 \u0639\u0645\u0644\u0629 \u062E\u0627\u0645\u062F\u0629");
      toast("\u{1F504} \u062F\u0648\u0631\u0627\u0646: " + best.disp + " \u2014 " + best.score);
      nativeNotify("AB-KING \u2014 \u062F\u0648\u0631\u0627\u0646 \u0627\u0644\u0628\u0648\u0631\u0629", best.disp + " \u062F\u0631\u062C\u0629 " + best.score + " \u0628\u062F\u0644 " + curDisp2 + " \u0627\u0644\u062E\u0627\u0645\u062F\u0629");
      saveCfg({
        symbol: best.symbol,
        leverage: S.config.leverage,
        levels: S.config.levels,
        gridStepPct: S.config.gridStepPct,
        huntPct: S.config.huntPct,
        tpNet: S.config.tpNet,
        tpUsd: S.config.tpUsd,
        cycleBalance: S.config.cycleBalance,
        directionMode: S.config.directionMode,
        mode: S.config.mode,
        sound: S.sound,
        soundTone: S.soundTone
      });
      return;
    }
    if (cur && (cur.score || 0) >= (best.score || 0)) return;
    if (!afterHarvest && cur && best.score - cur.score < 12) return;
    _lastSwitchAt = Date.now();
    const curDisp = cur ? cur.disp : S.config.displaySymbol, curSc = cur ? cur.score : 0;
    pushLog("server", "\u{1F3AF} \u0627\u0646\u062A\u0642\u0627\u0644 \u0644\u0644\u0623\u0641\u0636\u0644 \u0642\u0628\u0644 \u0627\u0644\u0635\u0641\u0642\u0629: " + curDisp + " (" + curSc + ") \u2192 " + best.disp + " (" + best.score + ") \u2014 \u0643\u0644 \u0627\u0644\u0634\u0631\u0648\u0637 \u0645\u062A\u0648\u0641\u0631\u0629 \u2014 \u062A\u064F\u062F\u0631\u0633 60 \u062B\u0627\u0646\u064A\u0629 \u062B\u0645 \u062A\u064F\u0641\u062A\u062D \u0627\u0644\u0635\u0641\u0642\u0629");
    toast("\u{1F3AF} \u0627\u0644\u0623\u0641\u0636\u0644 \u0627\u0644\u0622\u0646: " + best.disp + " \u2014 " + best.score);
    nativeNotify("AB-KING \u2014 \u0627\u0646\u062A\u0642\u0627\u0644 \u0644\u0644\u0623\u0641\u0636\u0644", best.disp + " \u062F\u0631\u062C\u0629 " + best.score + " \u0642\u0628\u0644 \u0627\u0644\u0635\u0641\u0642\u0629");
    saveCfg({
      symbol: best.symbol,
      leverage: S.config.leverage,
      levels: S.config.levels,
      gridStepPct: S.config.gridStepPct,
      huntPct: S.config.huntPct,
      tpNet: S.config.tpNet,
      tpUsd: S.config.tpUsd,
      cycleBalance: S.config.cycleBalance,
      directionMode: S.config.directionMode,
      mode: S.config.mode,
      sound: S.sound,
      soundTone: S.soundTone
    });
    startBot();
  } catch (e) {
    logSwallow("silent-catch", e);
  }
}
async function exPosition(symbol2) {
  const raw = await kcPrivate("GET", "/api/v1/position?symbol=" + encodeURIComponent(symbol2));
  const p = Array.isArray(raw) ? raw[0] : raw;
  const qty = p ? Number(p.currentQty) || 0 : 0;
  if (qty === 0) return null;
  return {
    side: qty < 0 ? "short" : "long",
    size: Math.abs(qty),
    entry: Number(p.avgEntryPrice) || S.lastPrice || 0,
    // الرافعة الحقيقية المطبّقة على المنصة — قد تخالف المضبوط (CROSS يتجاهل رافعة الأمر)
    leverage: Number(p.realLeverage) || Number(p.leverage) || S.config.leverage,
    unrealized: Number(p.unrealisedPnl) || 0,
    liquidation: Number(p.liquidationPrice) || null,
    openedAt: Date.now()
  };
}
function _mmFromPos(pos) {
  if (!pos) return null;
  if (typeof pos.marginMode === "string" && pos.marginMode) return pos.marginMode.toUpperCase();
  if (typeof pos.crossMode === "boolean") return pos.crossMode ? "CROSS" : "ISOLATED";
  return null;
}
async function placeOrderSmart(body) {
  const sym = body.symbol;
  try {
    const cm = mmCache[sym];
    if (cm && cm.mode && Date.now() - cm.at < 6e5) {
      body = { ...body, marginMode: cm.mode };
    } else {
      const pos = await kcPrivate("GET", "/api/v1/position?symbol=" + encodeURIComponent(sym));
      const mm = _mmFromPos(pos);
      if (mm) {
        mmCache[sym] = { at: Date.now(), mode: mm };
        body = { ...body, marginMode: mm };
      }
    }
  } catch (_) {
    logSwallow("silent-catch", _);
  }
  try {
    return await kcPrivate("POST", "/api/v1/orders", body);
  } catch (e) {
    let m = e?.message || String(e);
    const isFunds = (x) => /insufficient|too high|available margin|available balance/i.test(x);
    const isMode = (x) => /margin mode|330005/i.test(x);
    if (body.postOnly && !isFunds(m) && !isMode(m)) {
      const b3 = { ...body };
      delete b3.postOnly;
      try {
        return await kcPrivate("POST", "/api/v1/orders", b3);
      } catch (e2) {
        m = e2?.message || String(e2);
        body = b3;
      }
    }
    if (isFunds(m)) {
      S._metaAt = 0;
      if (S._metaSym === sym) S._metaSym = null;
      throw e;
    }
    if (!isMode(m)) throw e;
    if (!mmTried[sym]) {
      mmTried[sym] = 1;
      try {
        await kcPrivate(
          "POST",
          "/api/v2/position/batchChangeMarginMode",
          { marginMode: "CROSS", symbols: [sym] }
        );
      } catch (_) {
        logSwallow("silent-catch", _);
      }
    }
    const errs = [m];
    for (const alt of ["CROSS", "ISOLATED", null]) {
      if (alt === body.marginMode) continue;
      const b2 = { ...body };
      if (alt) b2.marginMode = alt;
      else delete b2.marginMode;
      try {
        const r = await kcPrivate("POST", "/api/v1/orders", b2);
        mmCache[sym] = { at: Date.now(), mode: alt };
        return r;
      } catch (e3) {
        const m3 = e3?.message || String(e3);
        if (!errs.includes(m3)) errs.push(m3);
      }
    }
    throw new Error(errs.join(" | "));
  }
}
async function ensureLiveMeta(symbol2) {
  if (S._metaSym === symbol2 && S.multiplier > 0) return;
  try {
    const ct = await fetchContract(symbol2);
    const mul = +ct.multiplier;
    if (!(mul > 0)) throw new Error("no multiplier");
    S.multiplier = mul;
    S.tickSize = +ct.tickSize || S.tickSize;
    S._metaSym = symbol2;
    if (+ct.maxOrderQty > 0) S.maxOrderQty = +ct.maxOrderQty;
    S._metaAt = Date.now();
  } catch (_) {
    throw new Error("\u0628\u064A\u0627\u0646\u0627\u062A \u0627\u0644\u0639\u0642\u062F \u063A\u064A\u0631 \u062C\u0627\u0647\u0632\u0629 \u2014 \u0623\u064F\u062C\u0651\u0644 \u0627\u0644\u0623\u0645\u0631 \u0644\u062D\u064A\u0646 \u062A\u0648\u062B\u064A\u0642 \u0627\u0644\u0645\u0636\u0627\u0639\u0641");
  }
}
async function exPlaceLimit(intent) {
  await ensureLiveMeta(intent.symbol);
  return placeOrderSmart({
    clientOid: intent.clientOid,
    symbol: intent.symbol,
    side: intent.side,
    type: "limit",
    price: String(intent.price),
    size: intent.qty,
    leverage: Number(intent.leverage) || Number(S.config.leverage) || 1,
    timeInForce: "GTC",
    reduceOnly: !!intent.reduceOnly,
    postOnly: true,
    // كل الأوامر الحدّية صانعة سوق: رسوم 0.02% بدل 0.06% — برأس مال صغير الفرق صافٍ حقيقي
    marginMode: "CROSS"
  });
}
async function exPlaceMarket(symbol2, side, qty) {
  await ensureLiveMeta(symbol2);
  return placeOrderSmart({
    clientOid: "hunt_" + Date.now().toString(36),
    symbol: symbol2,
    side,
    type: "market",
    size: qty,
    leverage: Number(S.config.leverage) || 1,
    marginMode: "CROSS"
  });
}
function exCloseQty(symbol2, side, qty) {
  return placeOrderSmart({
    clientOid: "cls_" + Date.now().toString(36) + Math.floor(Math.random() * 1e3),
    symbol: symbol2,
    type: "market",
    side: side === "short" ? "buy" : "sell",
    size: qty,
    reduceOnly: true,
    marginMode: "CROSS"
  });
}
function exClose(symbol2, side) {
  return placeOrderSmart({
    clientOid: "cls_" + Date.now().toString(36),
    symbol: symbol2,
    type: "market",
    side: side === "short" ? "buy" : "sell",
    closeOrder: true,
    reduceOnly: true,
    marginMode: "CROSS"
  });
}
async function exPing() {
  const a = await kcPrivate("GET", "/api/v1/account-overview?currency=USDT");
  return a ? Number(a.accountEquity ?? a.availableBalance ?? 0) : null;
}
async function exPlaceStopGuard() {
  if (S.config.mode !== "live" || !S.keys || !S.position) return;
  const pos = S.position, sh = pos.side === "short";
  const mark = S.markPrice || S.lastPrice || pos.entry || 0;
  if (!(mark > 0)) return;
  let liq = S.exLiqPrice || pos.liquidation || S.liqPrice || 0;
  if (!(liq > 0) || (sh ? liq <= mark * 1.005 : liq >= mark * 0.995)) liq = 0;
  const e = pos.entry || mark;
  const notional = Math.max(1e-9, pos.size * (S.multiplier || 1) * e);
  const dDis = LOSS_STOP_USD / notional * 1.1;
  const cands = [sh ? e * (1 + dDis) : e * (1 - dDis)];
  if (liq > 0) cands.push(sh ? liq * 0.99 : liq * 1.01);
  const gp = roundTick(sh ? Math.min(...cands) : Math.max(...cands), S.tickSize || 1e-10);
  if (!(gp > 0) || (sh ? gp <= mark * 1.003 : gp >= mark * 0.997)) {
    S._guardId = null;
    return;
  }
  if (S._guardId && S._guardPx && Math.abs(gp - S._guardPx) / S._guardPx < 5e-3) return;
  try {
    await exCancelStops(S.config.symbol);
    S._guardId = null;
    const r = await placeOrderSmart({
      clientOid: "grd_" + Date.now().toString(36),
      symbol: S.config.symbol,
      type: "market",
      side: sh ? "buy" : "sell",
      stop: sh ? "up" : "down",
      stopPrice: String(gp),
      stopPriceType: "MP",
      reduceOnly: true,
      closeOrder: true,
      marginMode: "CROSS"
    });
    S._guardId = r && (r.orderId || r.id) || null;
    S._guardPx = gp;
    S._guardAt = Date.now();
    if (!S._guardLogged) {
      S._guardLogged = true;
      pushLog("server", "\u062D\u0627\u0631\u0633 \u062E\u0627\u062F\u0645\u064A \u0645\u0641\u0639\u0651\u0644 \u2014 \u0625\u064A\u0642\u0627\u0641 \u0637\u0648\u0627\u0631\u0626 \u0639\u0644\u0649 \u0627\u0644\u0645\u0646\u0635\u0629 \u0639\u0646\u062F " + fmtPx(gp) + " (\u064A\u0639\u0645\u0644 \u062D\u062A\u0649 \u0644\u0648 \u0646\u0627\u0645 \u0627\u0644\u062A\u0637\u0628\u064A\u0642)");
    }
  } catch (e2) {
    if (!S._guardErrAt || Date.now() - S._guardErrAt > 3e5) {
      S._guardErrAt = Date.now();
      pushLog("error", "\u0627\u0644\u062D\u0627\u0631\u0633 \u0627\u0644\u062E\u0627\u062F\u0645\u064A: " + String(e2?.message || e2));
    }
  }
}
function tapeFlow() {
  const t = S.tape;
  if (!t || t.length < 6) return { bias: 0, whale: 0, whaleVol: 0 };
  let b = 0, s = 0;
  const sizes = t.map((x) => x.size).sort((x, y) => x - y);
  const med = sizes[Math.floor(sizes.length / 2)] || 1;
  let wb = 0, ws2 = 0;
  for (const x of t) {
    if (x.side === "buy") b += x.size;
    else s += x.size;
    if (x.size > med * 5) {
      if (x.side === "buy") wb += x.size;
      else ws2 += x.size;
    }
  }
  const tot = b + s || 1, wtot = wb + ws2 || 1;
  return { bias: (b - s) / tot, whale: (wb - ws2) / wtot, whaleVol: (wb + ws2) / tot };
}
function lastRealFillPx() {
  const f = S.grid.filter((g) => g.status === "filled" && g.filledPrice > 0).sort((a, b) => b.filledAt - a.filledAt);
  if (f.length && Date.now() - (f[0].filledAt || 0) < 30 * 60 * 1e3) return f[0].filledPrice;
  return 0;
}
function spacingGate(p) {
  if (!(p > 0)) return true;
  const last = lastRealFillPx();
  if (!last) return true;
  const hfMin = S.config.hfMode ? 4e-4 : 12e-4;
  const minGap = Math.max(S.config.gridStepPct / 100 * (S.config.hfMode ? 0.25 : 0.75), hfMin);
  return Math.abs(p - last) / last >= minGap;
}
function filledAdds() {
  const o = S.journal.filter((j) => j.status === "open" && (j.symbol === void 0 || j.symbol === S.config.displaySymbol));
  const n = o.reduce((a, j) => a + Math.max(1, j.mergedOrders || 1), 0);
  return n > 0 ? n : S.position ? 1 : 0;
}
function addsBlocked() {
  if (S.status !== "running") return false;
  if (filledAdds() >= effLevels()) return true;
  return !!(S.position && S.regime && S.regime.shock);
}
function adversePct(p) {
  const pos = S.position;
  if (!pos || !p || !pos.entry) return 0;
  return pos.side === "short" ? (p - pos.entry) / pos.entry * 100 : (pos.entry - p) / pos.entry * 100;
}
function lastJumpPct() {
  const t = S.priceTrail;
  if (t.length < 2) return 0;
  const a = t[t.length - 2], b = t[t.length - 1];
  return a > 0 ? Math.abs(b - a) / a * 100 : 0;
}
function waveAdversePct() {
  const pos = S.position, t = S.priceTrail;
  if (!pos || t.length < 10) return 0;
  const w = t.slice(-60), p = t[t.length - 1];
  if (!(p > 0)) return 0;
  if (pos.side === "short") {
    const lo = Math.min(...w);
    return lo > 0 ? (p - lo) / lo * 100 : 0;
  }
  const hi = Math.max(...w);
  return hi > 0 ? (hi - p) / hi * 100 : 0;
}
function bookQuality() {
  const b = S.orderBook, p = S.lastPrice || 0;
  if (!b || !b.bids.length || !b.asks.length || !p) return true;
  const bb = b.bids[0].price, ba = b.asks[0].price;
  if (bb <= 0 || ba <= 0) return true;
  const mid = (bb + ba) / 2, sp = (ba - bb) / mid * 100;
  if (sp > 0.5) return false;
  if (lastJumpPct() > 0.45) return false;
  const short = S.config.direction === "short";
  const wall = short ? b.bids : b.asks, depth = volSum(wall.slice(0, 8));
  const need = contractsForLevel(p) * 2.5;
  return !(depth > 0 && need > 0 && depth < need);
}
function medLevelSz(ob) {
  const s = [...ob.bids.slice(0, 10), ...ob.asks.slice(0, 10)].map((x) => x.size).sort((a, b) => a - b);
  return s.length ? s[Math.floor(s.length / 2)] : 0;
}
function priceKick() {
  const t = S.priceTrail || [];
  if (t.length < 12) return { up: 0, down: 0 };
  const n = Math.min(45, t.length), p0 = t[t.length - n], p1 = t[t.length - 1];
  if (!(p0 > 0)) return { up: 0, down: 0 };
  const chg = (p1 - p0) / p0 * 100;
  return { up: chg > 0 ? chg : 0, down: chg < 0 ? -chg : 0 };
}
function batchHoldActive() {
  if (S.position && S._batchHoldAt && S.lastPrice > 0) {
    const p = S.lastPrice, long = S.position.side === "long";
    const worst = S._batchWorst == null ? p : long ? Math.min(S._batchWorst, p) : Math.max(S._batchWorst, p);
    if (worst !== S._batchWorst) {
      S._batchWorst = worst;
      S._batchWorseAt = Date.now();
    }
  }
  if ((S._batchFills || 0) < 4) return false;
  const H = 36e5;
  if (S._batchHoldAt && (!S._batchHoldLogAt || Date.now() - S._batchHoldLogAt > 18e5)) {
    S._batchHoldLogAt = Date.now();
    const el = Math.floor((Date.now() - S._batchHoldAt) / H), min = Math.floor((Date.now() - S._batchHoldAt) % H / 6e4);
    const calm = S._batchWorseAt ? Math.floor((Date.now() - S._batchWorseAt) / H) : 0;
    pushLog("server", "\u{1F9F7} \u0627\u0644\u0642\u0627\u0637\u0639 \u0645\u0627\u0636\u064D: " + S._batchFills + " \u0623\u0648\u0627\u0645\u0631 \u062F\u062E\u0648\u0644 \u2014 \u0645\u0636\u0649 " + el + "\u0633" + min + "\u062F \u0645\u0646 \u0648\u062C\u0648\u0628 \u0627\u0644\u06403 \u0633\u0627\u0639\u0627\u062A" + (S._batchWorseAt ? Date.now() - S._batchWorseAt > H ? " \u2014 \u0627\u0644\u0627\u0646\u0632\u0644\u0627\u0642 \u0645\u062A\u0648\u0642\u0641 \u0645\u0646\u0630 " + calm + " \u0633\u0627\u0639\u0629" : " \u2014 \u0627\u0644\u0627\u0646\u0632\u0644\u0627\u0642 \u0645\u0627 \u0632\u0627\u0644 \u0646\u0634\u0637\u0627\u064B" : " \u2014 \u0628\u0627\u0646\u062A\u0638\u0627\u0631 \u0627\u0644\u0642\u0631\u0627\u0621\u0629"));
  }
  if (S._batchHoldAt && !S._batchStudiedAt) {
    S._batchStudiedAt = true;
    startStudy(false);
    pushLog("server", "\u{1F50D} \u0628\u062F\u0623\u062A \u062F\u0631\u0627\u0633\u0629 \u0645\u0627 \u0628\u0639\u062F \u0627\u0644\u0642\u0627\u0637\u0639 \u2014 \u0644\u0627 \u0641\u062A\u062D \u0623\u0648\u0627\u0645\u0631 \u062D\u062A\u0649 \u064A\u0643\u062A\u0645\u0644 \u062A\u0642\u064A\u064A\u0645 \u0627\u0644\u0648\u0636\u0639");
  }
  return true;
}
function exposureCapped() {
  const pos = S.position;
  if (!pos) return false;
  const notional = pos.size * (S.multiplier || 1) * (pos.entry || 0);
  return notional > Math.max(20, (S.config.cycleBalance || 0) * 4);
}
function volBrakeActive() {
  if (!S.position) return false;
  const vol = retStdev() || 0;
  const shocked = !!(S.regime && S.regime.shock);
  const oppWave = (S._waveN || 0) >= 3;
  return !!(shocked || vol > 0.4 || oppWave);
}
function retStdev() {
  const t = S.priceTrail;
  if (t.length < 4) return 0;
  const r = [];
  for (let i = 1; i < t.length; i++) if (t[i - 1] > 0) r.push((t[i] - t[i - 1]) / t[i - 1] * 100);
  if (r.length < 3) return 0;
  const m = r.reduce((a, b) => a + b, 0) / r.length;
  return Math.sqrt(r.reduce((s, x) => s + (x - m) ** 2, 0) / r.length);
}
function readRegime() {
  const mom = S.confluence ? S.confluence.momentum : 0;
  const vol = retStdev(), jump = lastJumpPct();
  const shock = jump > 0.9 || Math.abs(mom) > 0.9;
  const up = S.emaFast != null && S.emaSlow != null && S.emaFast > S.emaSlow;
  const dn = S.emaFast != null && S.emaSlow != null && S.emaFast < S.emaSlow;
  let trend = "range";
  if (up && mom > 0.04 && S.biasScore > 3 || mom > 0.12) trend = "up";
  else if (dn && mom < -0.04 && S.biasScore < -3 || mom < -0.12) trend = "down";
  if (Math.abs(mom) < 0.04 && vol < 0.08) trend = "range";
  let vk = "normal";
  if (vol >= 0.22 || shock) vk = "wild";
  else if (vol <= 0.045 && Math.abs(mom) < 0.045) vk = "calm";
  let label = "\u0633\u0648\u0642 \u0639\u0631\u0636\u064A";
  if (shock) label = "\u062A\u063A\u064A\u0631 \u0645\u0641\u0627\u062C\u0626";
  else if (trend === "up") label = vk === "calm" ? "\u0635\u0627\u0639\u062F \u0647\u0627\u062F\u0626" : "\u0627\u062A\u062C\u0627\u0647 \u0635\u0627\u0639\u062F";
  else if (trend === "down") label = vk === "calm" ? "\u0647\u0627\u0628\u0637 \u0647\u0627\u062F\u0626" : "\u0627\u062A\u062C\u0627\u0647 \u0647\u0627\u0628\u0637";
  else if (vk === "wild") label = "\u0639\u0631\u0636\u064A \u0645\u062A\u0630\u0628\u0630\u0628";
  else if (vk === "calm") label = "\u0639\u0631\u0636\u064A \u0647\u0627\u062F\u0626";
  const young = !!(S.startedAt && Date.now() - S.startedAt < SCOUT_MS);
  return {
    trend,
    vol: vk,
    shock,
    label,
    sizeMult: shock ? 0.65 : vk === "wild" ? 0.75 : trend === "range" ? 0.9 : 1,
    stepMult: vk === "wild" || shock ? 1.35 : vk === "calm" ? 0.9 : 1,
    huntMult: shock ? 1.5 : vk === "wild" ? 1.25 : 1,
    maxAdds: shock || vk === "wild" ? 1 : 3,
    scoutOnly: shock || vk === "wild" || young
  };
}
function scoreBias() {
  let sc = 0;
  const rs = [];
  const bv = volSum(S.orderBook.bids), av = volSum(S.orderBook.asks), bt = bv + av;
  if (bt > 0) {
    const im = (bv - av) / bt;
    sc += im * 40;
    if (Math.abs(im) > 0.08) rs.push(im > 0 ? "\u062F\u0641\u062A\u0631 \u0645\u0634\u062A\u0631\u064A\u0646" : "\u062F\u0641\u062A\u0631 \u0628\u0627\u0626\u0639\u064A\u0646");
  }
  let buyT = 0, sellT = 0;
  for (const t of S.tape) {
    if (t.side === "buy") buyT += t.size;
    else sellT += t.size;
  }
  const tt = buyT + sellT;
  if (tt > 0) {
    const im = (buyT - sellT) / tt;
    sc += im * 25;
    if (Math.abs(im) > 0.1) rs.push(im > 0 ? "\u0634\u0631\u064A\u0637 \u0634\u0631\u0627\u0621" : "\u0634\u0631\u064A\u0637 \u0628\u064A\u0639");
  }
  if (S.funding) {
    sc += clamp(-S.funding * 8e4, -20, 20);
    rs.push(S.funding > 0 ? "\u062A\u0645\u0648\u064A\u0644 \u0625\u064A\u062C\u0627\u0628\u064A \u2190 \u0634\u0648\u0631\u062A" : "\u062A\u0645\u0648\u064A\u0644 \u0633\u0644\u0628\u064A \u2190 \u0644\u0648\u0646\u063A");
  }
  if (S.lastPrice && S.emaFast != null && S.emaSlow != null) {
    if (S.lastPrice > S.emaFast && S.emaFast > S.emaSlow) {
      sc += 15;
      rs.push("\u0645\u062A\u0648\u0633\u0637 \u0633\u0631\u064A\u0639 \u0641\u0648\u0642 \u0627\u0644\u0628\u0637\u064A\u0621");
    } else if (S.lastPrice < S.emaFast && S.emaFast < S.emaSlow) {
      sc -= 15;
      rs.push("\u0645\u062A\u0648\u0633\u0637 \u0633\u0631\u064A\u0639 \u062A\u062D\u062A \u0627\u0644\u0628\u0637\u064A\u0621");
    }
  }
  if (S.cvd) {
    sc += Math.sign(S.cvd) * Math.min(20, Math.log10(Math.abs(S.cvd) + 1) * 5);
  }
  return { direction: sc >= 0 ? "long" : "short", score: Math.round(sc * 10) / 10, reasons: rs.slice(0, 3) };
}
function readConfluence() {
  const t = S.priceTrail, w = t.slice(-10);
  const bids = volSum(S.orderBook.bids), asks = volSum(S.orderBook.asks), bd = bids + asks;
  const imb = bd > 0 ? (bids - asks) / bd : 0;
  let mom = 0;
  if (w.length >= 2 && w[0] > 0) mom = (w[w.length - 1] - w[0]) / w[0] * 100;
  let tn = 0, tp = 0;
  for (const r of S.tape) {
    tn += r.size;
    tp += r.price * r.size;
  }
  const vwap = tn > 0 ? tp / tn : null;
  let sc = 0;
  const rs = [];
  if (Math.abs(imb) > 0.08) {
    sc += imb * 40;
    rs.push(imb > 0 ? "\u062F\u0641\u062A\u0631 \u0634\u0631\u0627\u0621 \u063A\u0627\u0644\u0628" : "\u062F\u0641\u062A\u0631 \u0628\u064A\u0639 \u063A\u0627\u0644\u0628");
  }
  if (Math.abs(mom) >= 0.04) {
    sc += mom * 8;
    rs.push(mom > 0 ? "\u0632\u062E\u0645 \u0635\u0627\u0639\u062F" : "\u0632\u062E\u0645 \u0647\u0627\u0628\u0637");
  }
  if (vwap && S.lastPrice) {
    if (S.lastPrice > vwap) {
      sc += 8;
      rs.push("\u0641\u0648\u0642 VWAP");
    } else {
      sc -= 8;
      rs.push("\u062A\u062D\u062A VWAP");
    }
  }
  if (S.emaFast != null && S.emaSlow != null) {
    if (S.emaFast > S.emaSlow) {
      sc += 10;
      rs.push("\u0645\u064A\u0644 \u0635\u0627\u0639\u062F");
    } else {
      sc -= 10;
      rs.push("\u0645\u064A\u0644 \u0647\u0627\u0628\u0637");
    }
  }
  const idle = w.length >= 8 && S.tape.length >= 8 && Math.abs(mom) < 0.04 && Math.abs(imb) < 0.08;
  if (idle) rs.unshift("\u0627\u0644\u0633\u0648\u0642 \u062E\u0627\u0645\u0644");
  return {
    score: Math.round(sc * 10) / 10,
    idle,
    momentum: Math.round(mom * 1e3) / 1e3,
    bookImb: Math.round(imb * 1e3) / 1e3,
    vwap,
    reasons: rs.slice(0, 3)
  };
}
function resolveDirection(now) {
  const mode = S.config.directionMode;
  if (mode === "long" || mode === "short") {
    S.config.direction = mode;
    return mode;
  }
  if (S.status === "paused" || S.status === "stopped") return S.config.direction;
  const bias = scoreBias();
  S.biasScore = bias.score;
  S.biasReasons = bias.reasons;
  const mom = S.confluence ? S.confluence.momentum : 0;
  if (S.position) {
    const flipped = S._flipUntil && now < S._flipUntil && S.config.direction !== S.position.side;
    if (!flipped) S.config.direction = S.position.side;
    return S.config.direction;
  }
  const prev = S.config.direction;
  if (S.regime && S.regime.shock) return prev;
  const flipOk = !S._dirFlipAt || now - S._dirFlipAt > 6e4;
  if (S.regime && S.regime.trend === "up" && bias.score > -6 && mom > -0.04 && !toxicDir("long")) {
    if (prev !== "long") {
      if (!flipOk) return prev;
      S._dirFlipAt = now;
    }
    S.config.direction = "long";
    return "long";
  }
  if (S.regime && S.regime.trend === "down" && bias.score < 6 && mom < 0.04 && !toxicDir("short")) {
    if (prev !== "short") {
      if (!flipOk) return prev;
      S._dirFlipAt = now;
    }
    S.config.direction = "short";
    return "short";
  }
  if (Math.abs(bias.score) < 4) return prev;
  if (bias.direction !== prev && flipOk && Math.abs(bias.score) >= 14 && !toxicDir(bias.direction)) {
    S.config.direction = bias.direction;
    S._dirFlipAt = now;
  } else if (bias.direction === prev) S.config.direction = bias.direction;
  return S.config.direction;
}
function contractsForLevel(p) {
  const q = levelQtys(p, effLevels());
  return q.length ? q[0] : 1;
}
function effLevels() {
  const c = S.config;
  let m = clamp(Math.round(c.levels) || 12, 1, 64);
  const p = S.lastPrice || 0, cv = Math.max(1e-12, (S.multiplier || 1) * p), lev = Math.max(1, c.leverage);
  const total = Math.max(0, c.cycleBalance) * 0.8, denom = (mm) => mm + 0.25 * mm * (mm - 1);
  if (p > 0) while (m > 1 && Math.floor(total / denom(m) * lev / cv) < 1) m--;
  return m;
}
function memStepMult() {
  const lv = sanctionLevel(S.config.direction);
  if (lv >= 2) return 1.4;
  const r = memRec(S.config.direction);
  if (!r || r.n < 6) return 1;
  const wr = smWR(r);
  if (wr >= 0.6 && r.pnl > 0) return 1;
  if (lv === 1) return 1.25;
  return 1;
}
function addStepPct() {
  return Math.max(0.12, S.config.gridStepPct) * memStepMult() * (S.regime ? S.regime.stepMult : 1) * profStepMult();
}
function tooClose(a, b, st) {
  return a > 0 && b > 0 && Math.abs(a - b) / Math.max(a, b) * 100 < st * 0.55;
}
function levelQtys(center, n) {
  const c = S.config;
  if (S._metaSym !== c.symbol || !(S.multiplier > 0)) return [];
  const cv = Math.max(1e-12, S.multiplier * center);
  const lev = Math.max(1, c.leverage);
  const total = Math.max(0, c.cycleBalance) * 0.8;
  const used = S.journal.filter((j) => j.status === "open" && (j.symbol === void 0 || j.symbol === S.config.displaySymbol)).reduce((a, j) => a + j.qty * (S.multiplier || 1) * j.entry, 0) / lev;
  let budget = Math.max(0, total - used) * (S.regime ? S.regime.sizeMult : 1) * memSizeMult();
  if (c.mode === "live" && S.exAvail > 0) budget = Math.min(budget, Math.max(0, S.exAvail * 0.95));
  else if (c.mode === "live" && S.exEquity > 0) budget = Math.min(budget, Math.max(0, S.exEquity * 0.95 - used));
  const denom = (mm) => mm + 0.08 * mm * (mm - 1);
  let m = Math.max(1, n);
  while (m > 1 && Math.floor(budget / denom(m) * lev / cv) < 1) m--;
  const unit = budget / denom(m);
  const out = [];
  let acc = 0;
  for (let i = 0; i < m; i++) {
    let q = Math.max(1, Math.floor(unit * (1 + 0.5 * i) * lev / cv));
    if (S.maxOrderQty > 0) q = Math.min(q, S.maxOrderQty);
    if (acc + q * cv / lev > budget && out.length) break;
    out.push(q);
    acc += q * cv / lev;
  }
  return out;
}
function buildGrid(center, wide, initialCount) {
  const c = S.config, tick = S.tickSize || 1e-10;
  const vol = retStdev() || 0.05;
  const volF = wide ? 1 : Math.max(1, Math.min(3, vol * 10));
  const step = c.gridStepPct / 100 * (wide ? 4 : memStepMult() * (S.regime ? S.regime.stepMult : 1) * volF);
  const stepPct = wide ? Math.max(0.12, c.gridStepPct) * 4 : addStepPct();
  const n = effLevels();
  const qtys = levelQtys(center, n);
  const filledN = filledAdds();
  const entry = S.position ? S.position.entry : center;
  const fills = S.grid.filter((g) => !g.reduceOnly && g.status === "filled").map((g) => g.filledPrice || g.price).filter((v) => v > 0);
  let from = c.direction === "short" ? Math.max(center, entry) : Math.min(center, entry);
  if (fills.length && !wide) {
    if (c.direction === "short") from = Math.max(from, Math.max(...fills) * (1 + step * 0.8));
    else from = Math.min(from, Math.min(...fills) * (1 - step * 0.8));
  }
  const grid = [];
  const liq = S.liqPrice || S.position && S.position.liquidation || 0;
  const spanMin = step * n * 0.9;
  let span = Math.max(spanMin, 0.02);
  if (liq > 0 && !wide) {
    const room = c.direction === "short" ? liq * 0.985 / from - 1 : 1 - liq * 1.015 / from;
    if (room > step * 0.5) span = Math.max(spanMin, room);
  }
  const reach = Math.max(spanMin, Math.min(step * n * 2, 0.12));
  if (span > reach) span = reach;
  let prevDist = 0;
  for (let i = 1; i <= n; i++) {
    let dist = wide ? Math.min(step * i, 0.05) : span * Math.pow(i / n, 1.5);
    if (!wide) {
      const minGap = Math.max(step * (S.config.hfMode ? 0.3 : 1), stepPct / 100 * (S.config.hfMode ? 0.3 : 1));
      if (i === 1) dist = Math.max(dist, minGap);
      else dist = Math.max(dist, prevDist + minGap);
    }
    prevDist = dist;
    const raw = c.direction === "short" ? from * (1 + dist) : from * (1 - dist);
    const price = roundTick(raw, tick);
    if (!(price > 0)) continue;
    if (liq > 0) {
      if (c.direction === "short" && price >= liq * 0.985) continue;
      if (c.direction === "long" && price <= liq * 1.015) continue;
    }
    if (grid.some((g) => tooClose(g.price, price, stepPct))) continue;
    if (c.direction === "short" && price <= center * (1 + step * 0.4)) continue;
    if (c.direction === "long" && price >= center * (1 - step * 0.4)) continue;
    const qi = Math.min(filledN + i - 1, qtys.length - 1);
    const qty = qtys.length ? qtys[Math.max(0, qi)] : contractsForLevel(center);
    const staged = initialCount != null && i > initialCount;
    grid.push({
      id: uid("lvl"),
      clientOid: uid("oid"),
      side: c.direction === "short" ? "sell" : "buy",
      price,
      qty,
      status: staged ? "staged" : "armed",
      reduceOnly: false,
      exchangeOrderId: null,
      filledAt: null,
      origin: "grid",
      createdAt: Date.now()
    });
  }
  return grid.sort((a, b) => b.price - a.price);
}
function armProgressive() {
  if (S.status !== "running") return;
  if (batchHoldActive()) return;
  if (exposureCapped()) {
    if (!S._expCapAt || Date.now() - S._expCapAt > 3e5) {
      S._expCapAt = Date.now();
      pushLog("server", "\u{1F9EE} \u0633\u0642\u0641 \u0627\u0644\u062A\u0639\u0631\u0651\u0636 (4\xD7 \u0631\u0635\u064A\u062F \u0627\u0644\u062F\u0648\u0631\u0629): \u0627\u0644\u0633\u0644\u0645 \u0645\u0643\u062A\u0645\u0644 \u2014 \u0644\u0627 \u0645\u0632\u064A\u062F \u0645\u0646 \u0627\u0644\u0645\u0633\u062A\u0648\u064A\u0627\u062A \u2014 \u0627\u0644\u0635\u0645\u0648\u062F \u0648\u0627\u0644\u062C\u0646\u064A \u0627\u0644\u0645\u062F\u0631\u0648\u0633 \u064A\u062A\u0643\u0641\u0644\u0627\u0646");
    }
    return;
  }
  if (!S.position && S._harvestAt && !S._reselectDone && Date.now() - S._harvestAt < 15 * 60 * 1e3) return;
  if (volBrakeActive()) {
    if (!S._volBrakeAt || Date.now() - S._volBrakeAt > 12e4) {
      S._volBrakeAt = Date.now();
      const vol = retStdev() || 0;
      pushLog("server", "\u{1F30A} \u0641\u0631\u0645\u0644\u0629 \u0627\u0644\u062A\u0642\u0644\u0628: " + (S.regime && S.regime.shock ? "\u0635\u062F\u0645\u0629 \u0646\u0638\u0627\u0645\u064A\u0629" : "\u062A\u0642\u0644\u0628 " + (vol * 100).toFixed(0) + "%") + " \u2014 \u062A\u062C\u0645\u064A\u062F \u062A\u0633\u0644\u064A\u062D \u062F\u062E\u0648\u0644 \u062C\u062F\u064A\u062F \u062D\u062A\u0649 \u064A\u0633\u062A\u0642\u0631 \u0627\u0644\u0633\u0648\u0642 \u062B\u0645 \u062A\u0639\u0648\u062F \u0627\u0644\u0623\u0648\u0627\u0645\u0631");
    }
    return;
  }
  const armed = S.grid.filter((g) => g.status === "armed" || g.status === "open");
  const staged = S.grid.filter((g) => g.status === "staged");
  if (!staged.length) return;
  if (armed.length > 1) return;
  let toArm = armed.length === 0 ? 3 : 2;
  const side = S.config.direction === "short" ? "sell" : "buy";
  for (const g of staged) {
    if (g.side !== side) {
      g.status = "cancelled";
      continue;
    }
    if (toArm <= 0) continue;
    g.status = "armed";
    g.createdAt = Date.now();
    toArm--;
  }
}
function sanitizeAdds() {
  const cap = effLevels();
  if (filledAdds() >= cap) {
    const side = S.position && S.position.side;
    for (const g of S.grid) {
      if (g.reduceOnly || g.filledAt) continue;
      if (g.status !== "armed" && g.status !== "open") continue;
      if (g.lane === "trend" || g.lane === "comp") continue;
      const gs = g.side === "sell" ? "short" : "long";
      if (!side || gs === side) {
        g.status = "cancelled";
        g.exchangeOrderId = null;
      }
    }
    return;
  }
  const st = addStepPct(), px = S.lastPrice || 0;
  if (px > 0) {
    const nz = effLevels(), stf = st / 100;
    const reach = Math.max(stf * nz * 0.9, Math.min(stf * nz * 2, 0.12)) * 1.25;
    for (const g of S.grid) {
      if (g.reduceOnly || g.filledAt || g.lane) continue;
      if (g.status !== "armed" && g.status !== "open") continue;
      if (Math.abs((g.price || 0) - px) / px > reach) {
        g.status = "cancelled";
        if (S.config.mode === "live" && S.keys && g.exchangeOrderId) {
          const id = g.exchangeOrderId;
          g.exchangeOrderId = null;
          exCancelOne(id);
        } else g.exchangeOrderId = null;
      }
    }
  }
  for (const side of ["buy", "sell"]) {
    const live = S.grid.filter((g) => !g.reduceOnly && g.side === side && (g.status === "armed" || g.status === "open")).sort((a, b) => Math.abs((b.price || 0) - px) - Math.abs((a.price || 0) - px));
    const kept = [];
    for (const g of live) {
      if (g.lane === "comp") {
        kept.push(g);
        continue;
      }
      const gap = g.lane ? Math.max(st, S.config.gridStepPct * 4) : st;
      if (kept.length >= cap || kept.some((k) => tooClose(k.price, g.price, gap))) {
        g.status = "cancelled";
        g.exchangeOrderId = null;
        continue;
      }
      kept.push(g);
    }
  }
}
function shouldFill(l, p) {
  if (l.status !== "open" && l.status !== "armed") return false;
  return l.side === "sell" ? p >= l.price : p <= l.price;
}
function feeFor(notional, taker) {
  return notional * (taker ? S.takerFee : S.makerFee);
}
function lotNet(side, entry, exit, qty, fees) {
  return (side === "short" ? 1 : -1) * (entry - exit) * (S.multiplier || 1) * qty - fees;
}
function accrueFunding() {
  const pos = S.position;
  if (!pos || !(pos.size > 0) || !S.funding) return 0;
  const now = Date.now();
  const last = S._fundAt || now;
  const hours = Math.min(8, (now - last) / 36e5);
  if (hours < 0.25) return 0;
  const notional = pos.size * (S.multiplier || 1) * (pos.entry || S.lastPrice || 0);
  const rate = S.funding;
  const payDir = (pos.side === "long" ? 1 : -1) * (rate >= 0 ? 1 : -1);
  const signed = notional * Math.abs(rate) * (hours / 8) * payDir;
  S._fundAt = now;
  S.fundingPaid = Math.round(((S.fundingPaid || 0) + signed) * 1e4) / 1e4;
  return signed;
}
function applyDelta(side, qty, price) {
  const dir2 = side === "sell" ? "short" : "long";
  if (!S.position) {
    S.position = {
      side: dir2,
      size: qty,
      entry: price,
      leverage: S.config.leverage,
      unrealized: 0,
      openedAt: Date.now(),
      liquidation: null
    };
    S._exc = { mae: 0, mfe: 0 };
    S._tpBanked = 0;
    S._trailFloor = 0;
    return { realized: 0, closedQty: 0, addedQty: qty };
  }
  const pos = S.position;
  const same = pos.side === "short" && side === "sell" || pos.side === "long" && side === "buy";
  if (same) {
    const t = pos.size + qty;
    pos.entry = (pos.entry * pos.size + price * qty) / t;
    pos.size = t;
    return { realized: 0, closedQty: 0, addedQty: qty };
  }
  const closeQty = Math.min(qty, pos.size);
  const sgn = pos.side === "short" ? 1 : -1;
  const pnl = sgn * (pos.entry - price) * (S.multiplier || 1) * closeQty;
  S.realizedPnl += pnl;
  pos.size -= closeQty;
  if (pos.size <= 1e-9) S.position = null;
  const leftover = qty - closeQty;
  if (leftover > 1e-9) {
    S.position = {
      side: dir2,
      size: leftover,
      entry: price,
      leverage: S.config.leverage,
      unrealized: 0,
      openedAt: Date.now(),
      liquidation: null
    };
    S._exc = { mae: 0, mfe: 0 };
    S._tpBanked = 0;
    S._trailFloor = 0;
    return { realized: pnl, closedQty: closeQty, addedQty: leftover };
  }
  return { realized: pnl, closedQty: closeQty, addedQty: 0 };
}
function noteEntry(qty, price, fee, source, noCount) {
  const side = S.position ? S.position.side : source === "hunt" ? S.config.direction : "short";
  S._entryCtx = {
    rg: S.regime ? S.regime.shock ? "\u0635\u062F\u0645\u0629" : S.regime.trend === "up" ? "\u0635\u0627\u0639\u062F" : S.regime.trend === "down" ? "\u0647\u0627\u0628\u0637" : "\u0639\u0631\u0636\u064A" : "\u0639\u0631\u0636\u064A",
    ses: sessionOf(Date.now()),
    origin: source === "hunt" ? "\u0635\u064A\u062F" : "\u0634\u0628\u0643\u0629",
    at: Date.now()
  };
  const label = source === "hunt" ? "\u0635\u0641\u0642\u0629" : "\u0634\u0628\u0643\u0629";
  const ex = S.journal.find((j) => j.status === "open" && j.side === side && j.symbol === S.config.displaySymbol);
  if (ex) {
    const t = ex.qty + qty;
    ex.entry = (ex.entry * ex.qty + price * qty) / t;
    ex.qty = t;
    if (!noCount) ex.mergedOrders = (ex.mergedOrders || 1) + 1;
    ex.fees += fee;
    if (S.position) ex.entry = S.position.entry;
    if (ex.source !== label) ex.source = "\u0645\u0631\u0643\u0632";
    if (S.activeCycle) {
      S.activeCycle.fills++;
      S.activeCycle.entry = S.position ? S.position.entry : ex.entry;
    }
    return ex.id;
  }
  const id = uid("jr");
  pushJr({
    id,
    source: label,
    side,
    qty,
    entry: S.position ? S.position.entry : price,
    exit: 0,
    pnl: 0,
    fees: fee,
    openedAt: Date.now(),
    closedAt: 0,
    mergedOrders: 1,
    status: "open"
  });
  if (!S.activeCycle) {
    S.activeCycle = { side, entry: price, openedAt: Date.now(), fills: 1, levels: S.config.levels, pnl: 0 };
  } else {
    S.activeCycle.fills++;
    if (S.position) S.activeCycle.entry = S.position.entry;
  }
  return id;
}
function noteClose(qty, exit, fee, source, pnl, lotId) {
  const lotId_ = lotId;
  const row = (lotId_ ? S.journal.find((j) => j.id === lotId_ && j.status === "open") : null) || S.journal.find((j) => j.status === "open" && (source === "\u0625\u064A\u0642\u0627\u0641" || j.source === source) && j.symbol === S.config.displaySymbol) || S.journal.find((j) => j.status === "open" && j.symbol === S.config.displaySymbol);
  if (row) {
    const share = row.qty > 0 ? row.fees * (qty / row.qty) : row.fees;
    let net = lotNet(row.side, row.entry, exit, qty, share + fee);
    const __bound = Math.max(50, (S.config.cycleBalance || 0) * 3);
    if (Number.isFinite(net) && Math.abs(net) > __bound) {
      pushLog("warn", "\u{1F6E1}\uFE0F \u062F\u0631\u0639 \u0627\u0644\u0625\u0642\u0641\u0627\u0644: \u0631\u0641\u0636 \u0646\u062A\u064A\u062C\u0629 \u0634\u0627\u0630\u0629 " + net.toFixed(2) + "$ (\u062E\u0627\u0631\u062C \u0627\u0644\u062D\u062F \u0627\u0644\u0645\u0627\u062F\u064A " + __bound.toFixed(0) + "$) \u2014 \u0633\u064A\u0627\u0642 \u0645\u062E\u062A\u0644\u0637 \u0645\u0646 \u0627\u0646\u062A\u0642\u0627\u0644 \u0639\u0645\u0644\u0629 \u2014 \u0627\u0644\u0635\u0641 \u064A\u064F\u0642\u0641\u0644 \u0628\u0635\u0641\u0631");
      net = 0;
    }
    row.status = "closed";
    row.exit = exit;
    row.fees += fee;
    row.closedAt = Date.now();
    row.pnl = Math.round((net + (row.partPnl || 0)) * 1e4) / 1e4;
    if (source === "\u0625\u064A\u0642\u0627\u0641") row.source = "\u0625\u064A\u0642\u0627\u0641";
    pnl = net;
  } else {
    const __bound2 = Math.max(50, (S.config.cycleBalance || 0) * 3);
    if (Number.isFinite(pnl) && Math.abs(pnl) > __bound2) {
      pushLog("warn", "\u{1F6E1}\uFE0F \u062F\u0631\u0639 \u0627\u0644\u0625\u0642\u0641\u0627\u0644: \u0631\u0641\u0636 \u0646\u062A\u064A\u062C\u0629 \u0634\u0627\u0630\u0629 " + pnl.toFixed(2) + "$ \u2014 \u0635\u064F\u0641\u0651\u0631\u062A");
      pnl = 0;
    }
    pushJr({
      id: uid("jr"),
      source,
      side: S.position ? S.position.side : S.config.direction,
      qty,
      entry: exit,
      exit,
      pnl,
      fees: fee,
      openedAt: Date.now(),
      closedAt: Date.now(),
      mergedOrders: 1,
      status: "closed"
    });
  }
  if (S.activeCycle) {
    S.activeCycle.pnl += pnl;
    if (!S.position) {
      S.activeCycle = null;
    }
  }
  if (Number.isFinite(pnl) && pnl !== 0) {
    const __bound = Math.max(50, (S.config.cycleBalance || 0) * 3);
    if (Math.abs(pnl) > __bound) {
      pushLog("warn", "\u{1F6E1}\uFE0F \u062F\u0631\u0639 \u0627\u0644\u0631\u0635\u064A\u062F: \u062A\u062C\u0627\u0647\u0644 \u0646\u062A\u064A\u062C\u0629 \u0634\u0627\u0630\u0629 " + pnl.toFixed(2) + "$ (\u062E\u0627\u0631\u062C \u0627\u0644\u062D\u062F \u0627\u0644\u0645\u0627\u062F\u064A " + __bound.toFixed(0) + "$) \u2014 \u0644\u0645 \u062A\u0644\u0645\u0633 \u0631\u0635\u064A\u062F \u0627\u0644\u062F\u0648\u0631\u0629");
    } else {
      S.cycleHarvested = Math.round(((S.cycleHarvested || 0) + pnl) * 1e4) / 1e4;
      S.config.cycleBalance = Math.max(1, Math.round((S.config.cycleBalance + pnl) * 100) / 100);
    }
  }
  {
    const mem = S.memory;
    if (mem && mem.pairs) {
      const pk = S.config.symbol;
      const pr = mem.pairs[pk] || (mem.pairs[pk] = { n: 0, w: 0, l: 0, pnl: 0, grossWin: 0, grossLoss: 0, maxLoseStreak: 0, loseStreak: 0 });
      pr.n++;
      pr.pnl = Math.round(((pr.pnl || 0) + pnl) * 1e4) / 1e4;
      if (pnl >= 0) {
        pr.w++;
        pr.grossWin = Math.round(((pr.grossWin || 0) + pnl) * 1e4) / 1e4;
        pr.loseStreak = 0;
      } else {
        pr.grossLoss = Math.round(((pr.grossLoss || 0) - pnl) * 1e4) / 1e4;
        pr.loseStreak = (pr.loseStreak || 0) + 1;
        pr.maxLoseStreak = Math.max(pr.maxLoseStreak || 0, pr.loseStreak);
      }
      saveAll();
    }
  }
  return pnl;
}
function notePartialClose(qty, exit, fee, lotId) {
  const row = (lotId ? S.journal.find((j) => j.id === lotId && j.status === "open") : null) || S.journal.find((j) => j.status === "open" && (j.symbol === void 0 || j.symbol === S.config.displaySymbol));
  if (!row || !(qty > 0)) return 0;
  const share = row.qty > 0 ? row.fees * Math.min(1, qty / row.qty) : 0;
  const net = lotNet(row.side, row.entry, exit, qty, share + fee);
  row.qty = Math.max(0, row.qty - qty);
  row.fees = Math.max(0, row.fees - share);
  row.partPnl = Math.round(((row.partPnl || 0) + net) * 1e4) / 1e4;
  if (Number.isFinite(net) && net !== 0) {
    S.cycleHarvested = Math.round(((S.cycleHarvested || 0) + net) * 1e4) / 1e4;
    S.config.cycleBalance = Math.max(1, Math.round((S.config.cycleBalance + net) * 100) / 100);
  }
  if (S.activeCycle) {
    S.activeCycle.pnl += net;
    if (!S.position) S.activeCycle = null;
  }
  if (row.qty <= 1e-9) {
    row.status = "closed";
    row.exit = exit;
    row.closedAt = Date.now();
    row.pnl = row.partPnl;
  }
  return net;
}
function armCompEntry(qty) {
  const pos = S.position;
  if (!pos || !(qty > 0)) return;
  if (!spacingGate(pos.entry)) return;
  const side = pos.side === "short" ? "sell" : "buy";
  const px = roundTick(pos.entry, S.tickSize || 1e-10);
  if (!(px > 0)) return;
  const q = Math.max(1, Math.round(qty));
  const tol = Math.max((S.tickSize || 1e-10) * 2, px * 1e-7);
  if (S.grid.some((g) => !g.reduceOnly && (g.status === "armed" || g.status === "open") && g.side === side && Math.abs(g.price - px) <= tol)) return;
  S.grid.push({
    id: uid("cmp"),
    clientOid: uid("oid"),
    side,
    price: px,
    qty: q,
    status: "armed",
    reduceOnly: false,
    exchangeOrderId: null,
    filledAt: null,
    origin: "comp",
    lane: "comp",
    createdAt: Date.now()
  });
  S.grid.sort((a, b) => b.price - a.price);
  pushLog("server", "\u062A\u0639\u0648\u064A\u0636: \u0623\u064F\u0633\u0644\u0651\u062D \u062F\u062E\u0648\u0644 " + side + " @ " + fmtPx(px) + " \u0628\u0643\u0645\u064A\u0629 " + q + " \u2014 \u0625\u0646 \u0639\u0627\u062F \u0627\u0644\u0633\u0639\u0631 \u0644\u0644\u0645\u062A\u0648\u0633\u0637 \u0623\u0639\u0627\u062F \u0634\u0631\u0627\u0621 \u0627\u0644\u0645\u064F\u062C\u0646\u0627\u0629");
}
function sessionOf(ts) {
  const h = new Date(ts).getUTCHours();
  return h < 8 ? "\u0622\u0633\u064A\u0627" : h < 16 ? "\u0623\u0648\u0631\u0648\u0628\u0627" : "\u0623\u0645\u0631\u064A\u0643\u0627";
}
function memStats() {
  const mem = S.memory;
  const o = { n: 0, w: 0, l: 0, pnl: 0, grossWin: 0, grossLoss: 0, maxLoseStreak: 0, pf: 0, winRate: 0, avgWin: 0, avgLoss: 0 };
  if (!mem || !mem.pairs) return o;
  for (const k of Object.keys(mem.pairs)) {
    const r = mem.pairs[k];
    o.n += r.n || 0;
    o.w += r.w || 0;
    o.l += r.l || 0;
    o.pnl += r.pnl || 0;
    o.maxLoseStreak = Math.max(o.maxLoseStreak, r.maxLoseStreak || 0);
    if (r.w && r.pnl > 0) {
    }
  }
  o.winRate = o.n ? o.w / o.n : 0;
  o.pnl = Math.round(o.pnl * 100) / 100;
  let gw = 0, gl = 0, aw = 0, al = 0;
  for (const k of Object.keys(mem.pairs)) {
    const r = mem.pairs[k];
    if (r.pnl > 0) {
      gw += r.pnl;
      aw += r.w ? r.pnl / r.w : 0;
    } else {
      gl += -r.pnl;
      al += r.l ? -r.pnl / r.l : 0;
    }
  }
  o.grossWin = Math.round(gw * 100) / 100;
  o.grossLoss = Math.round(gl * 100) / 100;
  o.pf = gl > 0 ? Math.round(gw / gl * 100) / 100 : gw > 0 ? 99 : 0;
  o.avgWin = Math.round(aw * 100) / 100;
  o.avgLoss = Math.round(al * 100) / 100;
  return o;
}
function memSizeMult() {
  let m = 1;
  const r = memRec(S.config.direction);
  if (r) {
    const st = r.streak || 0;
    if (st >= 3) m = 1.25;
    else if (st <= -3) m = 0.5;
    else if (st <= -2) m = 0.7;
  }
  if (sanctionLevel(S.config.direction) >= 2) m = Math.min(m, 0.5);
  if (S._probation) m = Math.min(m, 0.5);
  return m;
}
function cvdDivergence() {
  const t = S.cvdTrail;
  if (!t || t.length < 15) return 0;
  let mn = Infinity, mx = -Infinity;
  for (const p of t) {
    if (p.cvd < mn) mn = p.cvd;
    if (p.cvd > mx) mx = p.cvd;
  }
  const range = mx - mn;
  if (!(range > 0)) return 0;
  const a = t[Math.max(0, t.length - 25)], b = t[t.length - 1];
  const flow = (b.cvd - a.cvd) / range;
  const mom = S.confluence ? S.confluence.momentum : 0;
  if (mom > 0.05 && flow < -0.4) return -1;
  if (mom < -0.05 && flow > 0.4) return 1;
  return 0;
}
function maybeReport() {
  const now = Date.now();
  if (S.status !== "running" && S.status !== "paused") return;
  if (S._lastReportAt && now - S._lastReportAt < 6 * 3600 * 1e3) return;
  const st = memStats();
  if (!st.n) return;
  S._lastReportAt = now;
  const msg = "\u0635\u0641\u0642\u0627\u062A \u0645\u062A\u0639\u0644\u064E\u0651\u0645\u0629: " + st.n + " \xB7 \u0646\u062C\u0627\u062D " + (st.winRate * 100).toFixed(0) + "% \xB7 \u0639\u0627\u0645\u0644 \u0627\u0644\u0631\u0628\u062D " + st.pf + " \xB7 \u0635\u0627\u0641\u064A \u0627\u0644\u0630\u0627\u0643\u0631\u0629 " + fmtUsd(st.pnl);
  pushLog("info", "\u062A\u0642\u0631\u064A\u0631 \u062F\u0648\u0631\u064A \u2014 " + msg);
  nativeNotify("AB-KING \u2014 \u062A\u0642\u0631\u064A\u0631 \u0627\u0644\u0623\u062F\u0627\u0621", msg);
}
function trailTarget(base, floating) {
  if (!(floating > 0)) return base;
  let t = base;
  while (floating >= t * 2) {
    t = t + Math.max(0.01, t * 0.25);
  }
  return t;
}
function syncPositionTp() {
  if (S.status !== "running" || !S.position) return;
  const pos = S.position, mult = S.multiplier || 1;
  if (!(pos.size > 0)) return;
  const entryFees = S.journal.filter((j) => j.status === "open").reduce((a, j) => a + (j.fees || 0), 0) || pos.size * mult * pos.entry * (S.makerFee || 2e-4);
  const exitFee = pos.size * mult * pos.entry * (S.takerFee || 6e-4);
  const notional = Math.max(1e-9, pos.size * mult * pos.entry);
  const base = tpBase();
  const floatNet = (pos.unrealized || 0) - S.journal.filter((j) => j.status === "open").reduce((a, j) => a + (j.fees || 0), 0) - pos.size * mult * (pos.entry || 0) * (S.takerFee || 6e-4);
  S._trailFloor = Math.max(S._trailFloor || 0, trailTarget(base, floatNet));
  const need = Math.max(S._trailFloor, tpBase()) - (S._tpBanked || 0);
  if (need < 0) {
  }
  const needPos = Math.max(0, need);
  const cov = (entryFees + exitFee + needPos) / notional;
  let want = roundTick(pos.side === "long" ? pos.entry * (1 + cov) : pos.entry * (1 - cov), S.tickSize);
  if (!(want > 0)) return;
  const nowT = Date.now();
  if (!S._zoneAt || nowT - S._zoneAt > 4e3) {
    S._zoneAt = nowT;
    const z = studiedTpZone(pos, S.lastPrice || 0);
    if (z && z.zone > 0) {
      const cap = want + (pos.side === "long" ? 1 : -1) * Math.abs(want - pos.entry) * 0.25;
      S._zone = pos.side === "long" ? Math.min(z.zone, cap) : Math.max(z.zone, cap);
    } else S._zone = 0;
  }
  if (S._zone > 0) {
    const z = roundTick(S._zone, S.tickSize);
    if (pos.side === "long" ? z > want : z < want) {
      if (Math.abs(z - want) > Math.max(S.tickSize * 2, want * 4e-4)) want = z;
    }
  }
  const tickTol = S.tickSize > 0 ? S.tickSize * 0.5 : Math.abs(want) * 1e-9;
  const liveTp = S.grid.filter((g) => g.reduceOnly && (g.status === "armed" || g.status === "open"));
  const remQty = (g) => Math.max(0, g.qty - (g.exFilled || 0));
  const match = liveTp.find((g) => {
    if ((S._tpBanked || 0) > 0 && Math.abs(remQty(g) - pos.size) < 1e-6) return true;
    return Math.abs(g.price - want) <= tickTol && Math.abs(remQty(g) - pos.size) < 1e-6;
  });
  if (match) {
    for (const g of liveTp) {
      if (g !== match) {
        g.status = "cancelled";
        g.exchangeOrderId = null;
      }
    }
    return;
  }
  for (const g of liveTp) {
    g.status = "cancelled";
    g.exchangeOrderId = null;
  }
  S.grid.push({
    id: uid("tp"),
    clientOid: uid("oid"),
    side: pos.side === "short" ? "buy" : "sell",
    price: want,
    qty: pos.size,
    status: "armed",
    reduceOnly: true,
    exchangeOrderId: null,
    filledAt: null,
    origin: "grid",
    createdAt: Date.now(),
    lotId: S.journal.find((j) => j.status === "open")?.id
  });
  S.grid.sort((a, b) => b.price - a.price);
}
function coveringLoser(l, p) {
  const pos = S.position;
  if (!pos || l.reduceOnly) return false;
  const cover = pos.side === "short" && l.side === "buy" || pos.side === "long" && l.side === "sell";
  return cover && adversePct(p) > 0.08;
}
function _harvestCleanup(fp) {
  if (S.config.mode === "live" && S.keys) {
    exCancelAll(S.config.symbol).catch(() => {
    });
    exCancelStops(S.config.symbol).catch(() => {
    });
  }
  S._harvestAt = Date.now();
  S._reselectDone = false;
  S._posPeakNet = null;
  S._batchFills = 0;
  S._batchHoldAt = 0;
  S._batchStudiedAt = false;
  S._batchWorst = null;
  S._batchWorseAt = 0;
  S._batchHoldLogAt = 0;
  S._guardId = null;
  S._tpBanked = 0;
  S._trailFloor = 0;
  for (const g of S.grid) {
    if (g.status === "armed" || g.status === "open" || g.status === "staged") {
      g.status = "cancelled";
      g.exchangeOrderId = null;
    }
  }
  S.gridAnchor = null;
  if (fp > 0) S.huntAnchor = fp;
}
function creditExFill(l, delta, fp) {
  return creditExFillReal(l, delta, fp, null);
}
function creditExFillReal(l, delta, fp, realFee) {
  if (!(delta > 0) || !(fp > 0)) return false;
  l.exFilled = (l.exFilled || 0) + delta;
  const mult = S.multiplier || 1;
  if (!l.reduceOnly) {
    const estFee = feeFor(delta * mult * fp, false);
    if (realFee != null && S._realFeesAt && Math.abs(realFee - estFee) > 1e-9) {
      S.feesPaid = Math.max(0, S.feesPaid - estFee);
      S.feesPaid += realFee;
    } else S.feesPaid += estFee;
    const fee2 = realFee != null && realFee > 0 ? realFee : estFee;
    const dd2 = applyDelta(l.side, delta, fp);
    if (dd2.addedQty > 0) {
      const lotId = noteEntry(
        dd2.addedQty,
        fp,
        fee2,
        l.origin === "hunt" ? "hunt" : "grid",
        !!l._counted
      );
      l._counted = true;
      l.lotId = l.lotId || lotId;
    }
    S.lastAddAt = Date.now();
    if (l.qty - (l.exFilled || 0) > 1e-9) pushLog("server", "\u062A\u0646\u0641\u064A\u0630 \u062C\u0632\u0626\u064A: \u062F\u064F\u062E\u0644 " + delta + " \u0645\u0646 " + l.qty + " @ " + fmtPx(fp) + " \u2014 \u0627\u0644\u0628\u0627\u0642\u064A \u0645\u0639\u0644\u0651\u0642 \u064A\u064F\u0643\u0645\u0644 \u0623\u0648 \u064A\u064F\u0644\u063A\u0649 \u0639\u0646\u062F \u0627\u0644\u062C\u0646\u064A");
    if (S.position && S.status === "running") {
      syncPositionTp();
      backgroundPipeline().catch(() => {
      });
    }
    return false;
  }
  const estFee2 = feeFor(delta * mult * fp, false);
  if (realFee != null && S._realFeesAt && Math.abs(realFee - estFee2) > 1e-9) {
    S.feesPaid = Math.max(0, S.feesPaid - estFee2);
    S.feesPaid += realFee;
  } else S.feesPaid += estFee2;
  const fee = realFee != null && realFee > 0 ? realFee : estFee2;
  const dd = applyDelta(l.side, delta, fp);
  const net = notePartialClose(delta, fp, fee, l.lotId);
  S._tpBanked = Math.round(((S._tpBanked || 0) + net) * 1e4) / 1e4;
  if (!S.position) {
    l.status = "filled";
    l.filledAt = Date.now();
    l.filledPrice = fp;
    const tb = S._tpBanked;
    _harvestCleanup(fp);
    pushLog("server", "\u062C\u0646\u064A \u0645\u0643\u062A\u0645\u0644 \u2713 \u2014 \u0635\u0627\u0641\u064A \u0627\u0644\u062F\u0648\u0631\u0629 " + fmtUsd(tb) + " \u2014 \u0623\u064F\u0644\u063A\u064A\u062A \u0627\u0644\u0623\u0648\u0627\u0645\u0631 \u0648\u062A\u064F\u062F\u0631\u0633 \u0635\u0641\u0642\u0629 \u062C\u062F\u064A\u062F\u0629");
    if (S.status === "running") maybeSwitchSymbol().catch(() => {
    });
    return true;
  }
  const rem = Math.max(0, l.qty - (l.exFilled || 0));
  if (S._tpBanked >= tpBase() - 1e-9 && rem > 0) {
    l.status = "filled";
    l.filledAt = Date.now();
    l.filledPrice = fp;
    if (l.exchangeOrderId) exCancelOne(l.exchangeOrderId);
    const side = S.position.side, q = Math.min(rem, S.position.size);
    if (q > 0) {
      const xp = S.lastPrice || fp;
      const xfee = feeFor(q * mult * xp, true);
      S.feesPaid += xfee;
      applyDelta(l.side, q, xp);
      noteClose(q, xp, xfee, "\u0634\u0628\u0643\u0629", 0, l.lotId);
      exCloseQty(S.config.symbol, side, q).catch((e) => pushLog("error", "\u0625\u063A\u0644\u0627\u0642 \u0627\u0644\u0645\u062A\u0628\u0642\u064A \u0627\u0644\u0631\u0628\u062D\u064A \u0641\u0634\u0644: " + (e.message || e)));
    }
    if (!S.position) {
      const tb = S._tpBanked;
      _harvestCleanup(fp);
      pushLog("server", "\u062C\u0646\u064A \u0645\u0643\u062A\u0645\u0644 \u2713 \u2014 \u0628\u0646\u0643 " + fmtUsd(tb) + " \u0648\u0627\u0644\u0645\u062A\u0628\u0642\u064A \u0628\u0650\u064A\u0639 \u0628\u0633\u0639\u0631 \u0631\u0628\u062D\u064A");
      if (S.status === "running") maybeSwitchSymbol().catch(() => {
      });
      return true;
    }
    return false;
  }
  if (rem > 0) {
    armCompEntry(delta);
    pushLog("server", "\u062C\u0646\u064A \u062C\u0632\u0626\u064A " + fmtUsd(net) + " \u2014 \u0627\u0644\u0645\u062A\u0628\u0642\u064A " + rem + " \u0645\u062D\u0641\u0648\u0638 \u0639\u0646\u062F \u0646\u0641\u0633 \u0627\u0644\u0646\u0642\u0637\u0629 " + fmtPx(l.price) + " \u0648\u0639\u064F\u0648\u0651\u0636 \u0627\u0644\u0645\u064F\u062C\u0646\u0627\u0629 \u0628\u0645\u0646\u0637\u0642\u0629 \u062F\u062E\u0648\u0644 \u0639\u0646\u062F \u0627\u0644\u0645\u062A\u0648\u0633\u0637");
    if (S.status === "running") syncPositionTp();
  }
  return false;
}
function fillLevel(id, fp, taker) {
  const l = S.grid.find((g) => g.id === id);
  if (!l || l.status === "filled") return;
  if (!l.reduceOnly && batchHoldActive()) {
    l.status = "cancelled";
    l.exchangeOrderId = null;
    return;
  }
  if (!l.reduceOnly && coveringLoser(l, fp)) return;
  l.status = "filled";
  l.filledAt = Date.now();
  l.filledPrice = fp;
  const effQty = l.reduceOnly && S.position ? Math.min(l.qty, S.position.size) : l.qty;
  const fee = feeFor(effQty * (S.multiplier || 1) * fp, taker);
  S.feesPaid += fee;
  const d = applyDelta(l.side, effQty, fp);
  if (d.addedQty > 0) {
    if (!l.reduceOnly) {
      S._batchFills = (S._batchFills || 0) + 1;
      if (S._batchFills >= 4 && !S._batchHoldAt) {
        S._batchHoldAt = Date.now();
        S._batchStudiedAt = false;
        cancelPendingAdds();
        pushLog("server", "\u{1F9F7} \u0627\u0644\u0642\u0627\u0637\u0639 \u0627\u0644\u0645\u0631\u062D\u0644\u064A: \u0627\u0643\u062A\u0645\u0644\u062A 4 \u0623\u0648\u0627\u0645\u0631 \u062F\u062E\u0648\u0644 \u2014 \u062A\u0648\u0642\u0641 \u0627\u0644\u0641\u062A\u062D \u062D\u062A\u0649 \u0627\u0644\u0627\u0633\u062A\u0642\u0631\u0627\u0631 + \u062F\u0631\u0627\u0633\u0629 \u062C\u062F\u064A\u062F\u0629 \u062B\u0645 4 \u0623\u062E\u0631\u0649");
      }
    }
    const lotId = noteEntry(d.addedQty, fp, fee, l.origin === "hunt" ? "hunt" : "grid");
    l.lotId = l.lotId || lotId;
    if (l.price > 0) {
      const slipUsd = Math.abs(fp - l.price) * (S.multiplier || 1) * effQty;
      if (slipUsd > 0) {
        S.slipCost = (S.slipCost || 0) + slipUsd;
        if (slipUsd > 1 && (!S._slipLogAt || Date.now() - S._slipLogAt > 6e4)) {
          S._slipLogAt = Date.now();
          pushLog("warn", "\u0627\u0646\u0632\u0644\u0627\u0642 \u062A\u0646\u0641\u064A\u0630 " + slipUsd.toFixed(2) + "$ \u0639\u0646\u062F " + fmtPx(fp) + " (\u0627\u0644\u0645\u0637\u0644\u0648\u0628 " + fmtPx(l.price) + ")");
        }
      }
    }
  }
  if (d.closedQty > 0) {
    const src = l.origin === "hunt" ? "\u0635\u0641\u0642\u0629" : "\u0634\u0628\u0643\u0629";
    const row = l.lotId ? S.journal.find((j) => j.id === l.lotId) : null;
    const net = row ? lotNet(row.side, row.entry, fp, row.qty, row.fees + fee) : d.realized - fee;
    noteClose(d.closedQty, fp, fee, src, net, l.lotId);
    if (l.origin === "hunt" && l.reduceOnly) S.huntOpen = Math.max(0, (S.huntOpen || 0) - 1);
    const parent = S.grid.find((g) => !g.reduceOnly && g.status === "filled" && (l.lotId ? g.lotId === l.lotId : g.origin === l.origin));
    if (parent) parent.status = "cancelled";
    S.huntAnchor = fp;
    S.lastHuntAt = Date.now() - 400;
    if (l.reduceOnly && S.config.mode === "live" && S.keys) {
      const row2 = l.lotId ? S.journal.find((j) => j.id === l.lotId) : null;
      const cSide = row2 ? row2.side : S.position ? S.position.side : S.config.direction;
      if (l.exchangeOrderId) exCancelOne(l.exchangeOrderId);
      (async () => {
        let q = d.closedQty;
        try {
          const lp = await exPosition(S.config.symbol);
          q = lp ? Math.min(q, lp.size) : 0;
        } catch (_) {
          logSwallow("silent-catch", _);
        }
        if (q > 0) await exCloseQty(S.config.symbol, cSide, q).catch((e) => pushLog("error", "\u0625\u063A\u0644\u0627\u0642 \u0627\u0644\u062C\u0646\u064A \u0627\u0644\u062D\u0642\u064A\u0642\u064A \u0641\u0634\u0644: " + (e.message || e)));
      })();
      if (!S.position) {
        _harvestCleanup(fp);
        pushLog("server", "\u062C\u0646\u064A \u0645\u0643\u062A\u0645\u0644 \u2713 \u2014 \u0623\u064F\u063A\u0644\u0642\u062A \u0627\u0644\u0635\u0641\u0642\u0629 \u062D\u0642\u064A\u0642\u064A\u064B\u0627 \u0648\u0623\u064F\u0644\u063A\u064A\u062A \u0623\u0648\u0627\u0645\u0631\u0647\u0627\u060C \u062A\u064F\u062F\u0631\u0633 \u0635\u0641\u0642\u0629 \u062C\u062F\u064A\u062F\u0629");
        if (S.status === "running") maybeSwitchSymbol().catch(() => {
        });
      }
    }
  }
  if (S.position && S.status === "running") syncPositionTp();
  S.lastWorkAt = Date.now();
  notify(
    (l.side === "sell" ? "\u0628\u064A\u0639 " : "\u0634\u0631\u0627\u0621 ") + (l.origin === "hunt" ? "\u0635\u0641\u0642\u0629" : l.reduceOnly ? "\u062C\u0646\u064A" : "\u0634\u0628\u0643\u0629") + " @ " + fmtPx(fp),
    l.reduceOnly && d.closedQty > 0 ? "win" : null
  );
}
function selectDueAdds(due, p) {
  if (addsBlocked()) return [];
  if (studying()) return [];
  if (toxicBlocked(S.config.direction)) return [];
  if (S.position && (lastJumpPct() > 0.4 || !bookQuality())) return [];
  const adds = due.filter((l) => !l.reduceOnly && !coveringLoser(l, p)).sort((a, b) => Math.abs(a.price - p) - Math.abs(b.price - p));
  const now = Date.now();
  return adds.filter(() => !(S.startedAt && now - S.startedAt < SCOUT_MS && filledAdds() >= 1)).filter(() => !(S.lastAddAt && now - S.lastAddAt < ADD_COOLDOWN)).slice(0, 1);
}
function inAddZone(p) {
  if (S.status !== "running" || !p) return false;
  if (addsBlocked()) return false;
  if (lastJumpPct() > 0.4) return false;
  if (!bookQuality()) return false;
  const pos = S.position;
  if (!pos || !pos.entry) return true;
  const step = Math.max(0.12, S.config.gridStepPct * memStepMult());
  const improve = pos.side === "short" ? (p - pos.entry) / pos.entry * 100 : (pos.entry - p) / pos.entry * 100;
  if (improve < step * 0.7) return false;
  const mom = S.confluence ? S.confluence.momentum : 0;
  const r = S.regime || { trend: "range" };
  const against = pos.side === "long" && r.trend === "down" || pos.side === "short" && r.trend === "up";
  if (r.shock && against) return false;
  if (against && (pos.side === "long" && mom < -0.06 || pos.side === "short" && mom > 0.06)) return false;
  if (pos.side === "long" && mom < -0.12) return false;
  if (pos.side === "short" && mom > 0.12) return false;
  const f = tapeFlow();
  if (pos.side === "short" && (f.whale > 0.6 || f.bias > 0.45)) return false;
  if (pos.side === "long" && (f.whale < -0.6 || f.bias < -0.45)) return false;
  return true;
}
function harvestRipe(p) {
  if (!p || S.status === "idle") return;
  if (S._gapCtx && S.position && gapExitCheck(p)) {
    pushLog("server", "\u{1F3AF} \u0635\u0627\u0626\u062F \u0627\u0644\u0641\u062C\u0648\u0627\u062A: \u0639\u0627\u062F\u0629 \u0627\u0644\u0625\u0646\u062F\u064A\u0643\u0633 \u062A\u0645\u062A \u2014 \u062C\u0646\u064A \u0627\u0644\u0645\u0631\u0643\u0632 \u0643\u0627\u0645\u0644\u0627");
    flattenAt(p, "\u0639\u0648\u062F\u0629 \u0625\u0646\u062F\u064A\u0643\u0633").catch(() => {
    });
    S._gapCtx = null;
    return;
  }
  for (const tp of S.grid.filter((g) => g.reduceOnly && (g.status === "open" || g.status === "armed"))) {
    const tol = Math.min(S.tickSize > 0 ? S.tickSize * 0.5 : 1e-12, Math.abs(tp.price) * 5e-4);
    const crossed = tp.side === "sell" ? p >= tp.price - tol : p <= tp.price + tol;
    if (crossed) fillLevel(tp.id, p, true);
  }
  const pv = S._harvPrev;
  if (S.position && pv && pv.px > 0) {
    const dt = Date.now() - pv.at;
    if (dt > 0 && dt < 6e3) {
      const adv = (S.position.side === "short" ? p - pv.px : pv.px - p) / pv.px * 100;
      if (adv >= 0.35) {
        cancelPendingAdds();
        S._waveHoldUntil = Date.now() + 6e4;
        if (!S._advSpkAt || Date.now() - S._advSpkAt > 6e4) {
          S._advSpkAt = Date.now();
          pushLog("server", "\u26A1 \u0627\u0646\u0632\u0644\u0627\u0642 \u0639\u0643\u0633\u064A " + adv.toFixed(2) + "% \u062E\u0644\u0627\u0644 " + Math.round(dt / 1e3) + "\u062B \u2014 \u0633\u064F\u062D\u0628\u062A \u0627\u0644\u0645\u0633\u062A\u0648\u064A\u0627\u062A \u0645\u0646 \u0627\u0644\u0645\u0646\u0635\u0629 60\u062B \u062D\u062A\u0649 \u064A\u0647\u062F\u0623 \u0627\u0644\u0633\u0639\u0631");
        }
      }
    }
  }
  if (S.position && S.status === "running") {
    const wv = waveAdversePct();
    if (wv >= 0.8) {
      cancelPendingAdds();
      S._waveHoldUntil = Date.now() + 6e4;
      if (!S._wavePullAt || Date.now() - S._wavePullAt > 6e4) {
        S._wavePullAt = Date.now();
        pushLog("server", "\u{1F30A} \u0645\u0648\u062C\u0629 \u0645\u0639\u0627\u0643\u0633\u0629 " + wv.toFixed(2) + "% \u062E\u0644\u0627\u0644 60\u062B \u2014 \u0633\u064F\u062D\u0628\u062A \u0627\u0644\u0645\u0633\u062A\u0648\u064A\u0627\u062A \u0645\u0646 \u0627\u0644\u0645\u0646\u0635\u0629 \u0648\u062A\u062C\u0645\u064A\u062F \u0627\u0644\u062A\u0633\u0644\u064A\u062D \u062D\u062A\u0649 \u062A\u0633\u062A\u0642\u0631");
      }
    }
  }
  S._harvPrev = { px: p, at: Date.now() };
}
function trailHuntAnchor(p) {
  if (!p || S.status !== "running") return;
  if (!S.huntAnchor) {
    S.huntAnchor = p;
    return;
  }
  if (S.config.direction === "short") {
    if (p > S.huntAnchor) S.huntAnchor = p;
  } else if (p < S.huntAnchor) S.huntAnchor = p;
}
function huntAligned() {
  const conf = S.confluence || { momentum: 0, idle: false, score: 0 };
  const short = S.config.direction === "short";
  if (!bookQuality()) return false;
  const r = S.regime || { trend: "range", shock: false };
  const withTrend = !S.position && (short && r.trend === "down" && conf.momentum <= -0.06 || !short && r.trend === "up" && conf.momentum >= 0.06);
  if (r.shock && !withTrend) return false;
  if (short && r.trend === "up") return false;
  if (!short && r.trend === "down") return false;
  if (conf.idle && S.tape.length >= 8) return false;
  if (short && conf.momentum > 0.06) return false;
  if (!short && conf.momentum < -0.06) return false;
  if (short && S.biasScore >= 10) return false;
  if (!S.position) {
    const ef = S.emaFast, es = S.emaSlow;
    if (ef && es) {
      const emaUp = ef > es;
      if (short && emaUp) return false;
      if (!short && !emaUp) return false;
    }
    if (short && S.biasScore > 0) return false;
    if (!short && S.biasScore < 0) return false;
  }
  if (!short && S.biasScore <= -10) return false;
  if (short && conf.score >= 12) return false;
  if (!short && conf.score <= -12) return false;
  if (S.position && S.position.side === S.config.direction) {
    const against = S.position.side === "short" && S.biasScore >= 12 || S.position.side === "long" && S.biasScore <= -12;
    if (against) return false;
  }
  const f = tapeFlow();
  if (f.whaleVol > 0.25) {
    if (short && f.whale > 0.5) return false;
    if (!short && f.whale < -0.5) return false;
  }
  if (short && f.bias > 0.35) return false;
  if (!short && f.bias < -0.35) return false;
  const fund = S.funding || 0;
  if (!short && fund > 4e-4) return false;
  if (short && fund < -4e-4) return false;
  const dv = cvdDivergence();
  if (short && dv === 1) return false;
  if (!short && dv === -1) return false;
  const mp = memRec(short ? "short" : "long");
  if (mp && mp.n >= 15 && mp.pnl < 0 && smWR(mp) < 0.3) {
    if (!S._memBlockAt || Date.now() - S._memBlockAt > 6e5) {
      S._memBlockAt = Date.now();
      pushLog("info", "\u0627\u0644\u0630\u0627\u0643\u0631\u0629: \u0627\u062A\u062C\u0627\u0647 " + (short ? "\u0627\u0644\u0634\u0648\u0631\u062A" : "\u0627\u0644\u0644\u0648\u0646\u063A") + " \u0639\u0644\u0649 \u0647\u0630\u0627 \u0627\u0644\u0632\u0648\u062C \u062E\u0627\u0633\u0631 \u0628\u0639\u064A\u0646\u0629 \u0643\u0627\u0641\u064A\u0629 \u2014 \u0645\u0645\u0646\u0648\u0639 \u0645\u0624\u0642\u062A\u064B\u0627");
    }
    return false;
  }
  if (mp) {
    const so = mp.sessions && mp.sessions[sessionOf(Date.now())];
    if (so && so.n >= 8 && so.pnl < 0 && smWR(so) < 0.32) return false;
    const rg = S.regime ? S.regime.shock ? "\u0635\u062F\u0645\u0629" : S.regime.trend === "up" ? "\u0635\u0627\u0639\u062F" : S.regime.trend === "down" ? "\u0647\u0627\u0628\u0637" : "\u0639\u0631\u0636\u064A" : "\u0639\u0631\u0636\u064A";
    const ro = mp.regimes && mp.regimes[rg];
    if (ro && ro.n >= 8 && ro.pnl < 0 && smWR(ro) < 0.32) return false;
  }
  const am = S._actMed || 0;
  if (am >= 10) {
    const cut = Date.now() - 6e4, rate = (S._tradeTs || []).filter((t) => t >= cut).length;
    if (rate < am * 0.5) return false;
  }
  const ob = S.orderBook, lp = S.lastPrice || 0;
  if (ob && ob.bids.length && ob.asks.length && lp > 0) {
    const medL = medLevelSz(ob);
    if (medL > 0) {
      if (!short) {
        for (const a of ob.asks) {
          if (a.price > lp && (a.price - lp) / lp * 100 < 0.15 && a.size > medL * 5) return false;
        }
      } else {
        for (const b of ob.bids) {
          if (b.price < lp && (lp - b.price) / lp * 100 < 0.15 && b.size > medL * 5) return false;
        }
      }
      const nofi = (S._ofi || 0) / (medL * 20);
      if (short && nofi > 0.6) return false;
      if (!short && nofi < -0.6) return false;
    }
  }
  if (S._btcImp != null && Math.abs(S._btcImp) >= 0.1) {
    if (short && S._btcImp > 0) return false;
    if (!short && S._btcImp < 0) return false;
  }
  return true;
}
function dangerVeto(_p) {
  const short = S.config.direction === "short";
  if (Date.now() - (streamState.bookAt || 0) > 8e3) return null;
  if (!bookQuality()) return "\u062F\u0641\u062A\u0631 \u0623\u0648 \u0633\u0628\u0631\u064A\u062F \u0631\u062F\u064A\u0621";
  if (toxicBlocked(S.config.direction)) return "\u0627\u062A\u062C\u0627\u0647 \u0633\u0627\u0645 \u0641\u064A \u0627\u0644\u0630\u0627\u0643\u0631\u0629 (\u064A\u064F\u0639\u0627\u062F \u0627\u062E\u062A\u0628\u0627\u0631\u0647 \u0643\u0644 \u0633\u0627\u0639\u0629)";
  const r = S.regime || { trend: "range", shock: false }, conf = S.confluence || { momentum: 0 };
  if (r.shock) {
    if (short && conf.momentum > 0.3) return "\u0635\u062F\u0645\u0629 \u0635\u0627\u0639\u062F\u0629 \u0639\u0646\u064A\u0641\u0629 \u0636\u062F \u0627\u0644\u0634\u0648\u0631\u062A";
    if (!short && conf.momentum < -0.3) return "\u0635\u062F\u0645\u0629 \u0647\u0627\u0628\u0637\u0629 \u0639\u0646\u064A\u0641\u0629 \u0636\u062F \u0627\u0644\u0644\u0648\u0646\u063A";
  }
  const ob = S.orderBook, lp = S.lastPrice || 0;
  if (ob && ob.bids.length && ob.asks.length && lp > 0) {
    const medL = medLevelSz(ob);
    if (medL > 0) {
      if (!short) {
        for (const a of ob.asks.slice(0, 5)) {
          if (a.price > lp && (a.price - lp) / lp * 100 < 0.07 && a.size > medL * 8) return "\u062C\u062F\u0627\u0631 \u0628\u064A\u0639 \u0644\u0627\u0635\u0642 \u0641\u0648\u0642 \u0627\u0644\u0633\u0639\u0631";
        }
      } else {
        for (const b of ob.bids.slice(0, 5)) {
          if (b.price < lp && (lp - b.price) / lp * 100 < 0.07 && b.size > medL * 8) return "\u062C\u062F\u0627\u0631 \u0634\u0631\u0627\u0621 \u0644\u0627\u0635\u0642 \u062A\u062D\u062A \u0627\u0644\u0633\u0639\u0631";
        }
      }
    }
  }
  const f = tapeFlow();
  if (f.whaleVol > 0.4) {
    if (short && f.whale > 0.7) return "\u0633\u064A\u0644 \u0634\u0631\u0627\u0621 \u062D\u064A\u062A\u0627\u0646 \u0645\u0647\u064A\u0645\u0646";
    if (!short && f.whale < -0.7) return "\u0633\u064A\u0644 \u0628\u064A\u0639 \u062D\u064A\u062A\u0627\u0646 \u0645\u0647\u064A\u0645\u0646";
  }
  const fund = S.funding || 0;
  if (!short && fund > 1e-3) return "\u062A\u0645\u0648\u064A\u0644 \u0645\u0631\u062A\u0641\u0639 \u062C\u062F\u064B\u0627 \u0636\u062F \u0627\u0644\u0644\u0648\u0646\u063A";
  if (short && fund < -1e-3) return "\u062A\u0645\u0648\u064A\u0644 \u0645\u0631\u062A\u0641\u0639 \u062C\u062F\u064B\u0627 \u0636\u062F \u0627\u0644\u0634\u0648\u0631\u062A";
  return null;
}
function huntSignals(p) {
  const short = S.config.direction === "short";
  const sig2 = [];
  const conf = S.confluence || { momentum: 0, score: 0 }, r = S.regime || { trend: "range" };
  if (S.huntAnchor) {
    const pct = (p - S.huntAnchor) / S.huntAnchor * 100, need = effectiveHuntPct();
    if (short ? pct <= -need : pct >= need) sig2.push("\u0645\u0631\u0633\u0627\u0629 " + Math.abs(pct).toFixed(2) + "%");
  }
  const mk = S.markPrice || 0;
  if (mk > 0) {
    const dev = (p - mk) / mk;
    if (Math.abs(dev) >= 15e-4 && (dev > 0 && short || dev < 0 && !short)) sig2.push("\u0641\u062C\u0648\u0629 \u0645\u0627\u0631\u0643");
  }
  if (S._brk && Date.now() - S._brk.at < 1e4) {
    if (S._brk.side === "up" && p < S._brk.level && short || S._brk.side === "down" && p > S._brk.level && !short) {
      sig2.push("\u0641\u062E \u0627\u062E\u062A\u0631\u0627\u0642");
      S._brk = null;
    }
  }
  if (short ? conf.momentum <= -0.05 : conf.momentum >= 0.05) sig2.push("\u0632\u062E\u0645");
  if (short && r.trend === "down" || !short && r.trend === "up") sig2.push("\u0627\u062A\u062C\u0627\u0647");
  const f = tapeFlow();
  if (f.whaleVol > 0.2 && (short && f.whale < -0.3 || !short && f.whale > 0.3)) sig2.push("\u062D\u064A\u062A\u0627\u0646");
  const dv = cvdDivergence();
  if (short && dv === -1 || !short && dv === 1) sig2.push("\u0627\u0646\u062D\u0631\u0627\u0641 \u0633\u064A\u0648\u0644\u0629");
  const ob = S.orderBook;
  if (ob && ob.bids.length) {
    const medL = medLevelSz(ob) || 1, n = (S._ofi || 0) / (medL * 20);
    if (short && n < -0.4 || !short && n > 0.4) sig2.push("\u0636\u063A\u0637 \u0623\u0648\u0627\u0645\u0631");
  }
  if (S._btcImp != null && Math.abs(S._btcImp) >= 0.1 && (short && S._btcImp < 0 || !short && S._btcImp > 0)) sig2.push("\u0642\u0627\u0626\u062F BTC");
  return sig2;
}
function effectiveHuntPct() {
  const hf = S.config.hfMode;
  const base = Math.max(
    hf ? 0.05 : 0.14,
    S.config.huntPct * (hf ? 0.4 : 1),
    profOf() && profOf().spread ? profOf().spread * (hf ? 1.5 : 2.5) : 0
  );
  const mag = Math.abs(S.biasScore);
  const stalled = S.lastWorkAt && Date.now() - S.lastWorkAt > 6e4;
  const mult = Math.min(1.8, (S.regime ? S.regime.huntMult : 1) * memHuntMult() * profStepMult());
  let scaled = base * mult;
  const rg = S.regime;
  if (rg) {
    const sh = S.config.direction === "short";
    const aligned = sh && rg.trend === "down" || !sh && rg.trend === "up";
    if (aligned && !rg.shock) scaled *= 0.8;
  }
  if (S._btcImp != null && Math.abs(S._btcImp) >= 0.1) {
    const sh2 = S.config.direction === "short";
    if (sh2 && S._btcImp < 0 || !sh2 && S._btcImp > 0) scaled *= 0.85;
  }
  if (S._lastWinClose && Date.now() - S._lastWinClose.at < 18e4 && S._lastWinClose.side === S.config.direction) scaled *= 0.7;
  if (S.position && S.position.side !== S.config.direction) return Math.max(0.07, scaled * 0.4);
  if (stalled && huntAligned()) return Math.max(0.1, scaled * 0.5);
  if (mag >= 28 && huntAligned()) return Math.max(0.12, scaled * 0.7);
  return scaled;
}
function huntTooClose(p) {
  const need = Math.max(MIN_HUNT_GAP * 1.5, addStepPct(), effectiveHuntPct() * 0.9, 0.12);
  for (const g of S.grid) {
    if (g.origin !== "hunt" || g.reduceOnly || g.status !== "filled") continue;
    const fp = g.filledPrice || g.price;
    if (fp > 0 && Math.abs(p - fp) / fp * 100 < need) return true;
  }
  return false;
}
function openHuntLots() {
  return S.grid.filter((g) => g.origin === "hunt" && !g.reduceOnly && g.status === "filled").length;
}
function warmedUp() {
  if (!S.startedAt || Date.now() - S.startedAt < 15e3) return false;
  const b = S.orderBook || {};
  if ((b.bids || []).length < 5 || (b.asks || []).length < 5) return false;
  if ((S.tape || []).length < 12) return false;
  return true;
}
function memRec(_dir) {
  return null;
}
function smWR(r) {
  return (r.w + 1) / (r.n + 2);
}
function sanctionLevel(_dir) {
  return 0;
}
function toxicDir(dir2) {
  return sanctionLevel(dir2) >= 2;
}
function toxicBlocked(dir2) {
  const lv = sanctionLevel(dir2);
  S._probation = false;
  if (lv < 3) return false;
  const r = S.memory.pairs[S.config.symbol + ":" + dir2];
  if (!r.probAt || Date.now() - r.probAt > 3600 * 1e3) {
    r.probAt = Date.now();
    S._probation = true;
    pushLog("server", "\u0625\u0639\u0627\u062F\u0629 \u062A\u0623\u0647\u064A\u0644 \u2014 \u0635\u0641\u0642\u0629 \u0627\u0633\u062A\u0643\u0634\u0627\u0641\u064A\u0629 \u0628\u0646\u0635\u0641 \u062D\u062C\u0645 \u062A\u062E\u062A\u0628\u0631 " + (dir2 === "short" ? "\u0627\u0644\u0634\u0648\u0631\u062A" : "\u0627\u0644\u0644\u0648\u0646\u063A") + " \u0627\u0644\u0645\u062D\u0638\u0648\u0631");
    saveAll();
    return false;
  }
  if (!S._toxicLogAt || Date.now() - S._toxicLogAt > 3e5) {
    S._toxicLogAt = Date.now();
    pushLog("server", "\u0627\u062A\u062C\u0627\u0647 \u0633\u0627\u0645 \u0641\u064A \u0627\u0644\u0630\u0627\u0643\u0631\u0629 \u2014 " + (dir2 === "short" ? "\u0627\u0644\u0634\u0648\u0631\u062A" : "\u0627\u0644\u0644\u0648\u0646\u063A") + " \u0645\u062D\u0638\u0648\u0631 (\u064A\u064F\u0639\u0627\u062F \u0627\u062E\u062A\u0628\u0627\u0631\u0647 \u0643\u0644 3 \u0633\u0627\u0639\u0627\u062A)");
  }
  return true;
}
function profOf() {
  const mem = S.memory;
  return mem && mem.profiles ? mem.profiles[S.config.symbol] : null;
}
function profFresh() {
  const p = profOf();
  return p && Date.now() - p.at < 24 * 3600 * 1e3 ? p : null;
}
function startStudy(short) {
  S.studyUntil = Date.now() + (short ? 12e3 : 28e3);
  S.studyAcc = { vols: [], sps: [], act: 0, whales: 0 };
}
function studying() {
  return !!(S.studyUntil && Date.now() < S.studyUntil);
}
function studyTick() {
  if (!S.studyUntil) return;
  if (Date.now() >= S.studyUntil) {
    if (S._postDisaster) {
      const vol = retStdev() || 0, shocked = !!(S.regime && S.regime.shock), whales = tapeFlow().whaleVol || 0;
      if (shocked || vol > 0.2 || whales > 0.6) {
        S.studyUntil = Date.now() + 6e4;
        if (!S._waitLogAt || Date.now() - S._waitLogAt > 6e4) {
          S._waitLogAt = Date.now();
          pushLog("server", "\u23F3 \u0627\u0644\u0633\u0648\u0642 \u0645\u062A\u0630\u0628\u0630\u0628 \u0628\u0639\u0646\u0641 (\u062A\u0642\u0644\u0628/\u0633\u064A\u0648\u0644\u0629) \u2014 \u0625\u0637\u0627\u0644\u0629 \u0627\u0644\u0627\u0646\u062A\u0638\u0627\u0631 60\u062B \u062D\u062A\u0649 \u064A\u0633\u062A\u0642\u0631 \u0642\u0628\u0644 \u0623\u064A \u062F\u062E\u0648\u0644");
        }
        return;
      }
    }
    S._postDisaster = 0;
    finishStudy();
    return;
  }
  const a = S.studyAcc;
  if (!a) return;
  const v = retStdev();
  if (v > 0) a.vols.push(v);
  const b = S.orderBook || {};
  if ((b.bids || []).length && (b.asks || []).length) {
    const bb = b.bids[0].price, aa = b.asks[0].price, mid = (bb + aa) / 2;
    if (mid > 0 && aa > bb) a.sps.push((aa - bb) / mid * 100);
  }
  a.act = (S.tape || []).length;
  if (tapeFlow().whaleVol > 0.28) a.whales++;
}
function finishStudy() {
  S.studyUntil = 0;
  const a = S.studyAcc;
  S.studyAcc = null;
  if (!a) return;
  const vols = a.vols.slice().sort((x, y) => x - y);
  const vol = vols.length ? vols[Math.floor(vols.length / 2)] : 0.05;
  const sp = a.sps.length ? a.sps.reduce((x, y) => x + y, 0) / a.sps.length : 0.02;
  const mem = S.memory = S.memory || { pairs: {}, cycles: 0, totalPnl: 0 };
  mem.profiles = mem.profiles || {};
  mem.profiles[S.config.symbol] = {
    at: Date.now(),
    vol: Math.round(vol * 1e4) / 1e4,
    spread: Math.round(sp * 1e4) / 1e4,
    act: a.act,
    whales: a.whales
  };
  saveAll();
  pushLog("server", "\u0627\u0643\u062A\u0645\u0644\u062A \u062F\u0631\u0627\u0633\u0629 " + S.config.displaySymbol + " \u2014 \u062A\u0642\u0644\u0628 " + vol.toFixed(3) + "% \xB7 \u0633\u0628\u0631\u064A\u062F " + sp.toFixed(3) + "% \xB7 \u0646\u0634\u0627\u0637 \u0634\u0631\u064A\u0637 " + a.act + " \u2014 \u0643\u064F\u064A\u0651\u0641\u062A \u0627\u0644\u062E\u0637\u0648\u0629 \u0648\u0627\u0644\u0635\u064A\u062F \u0639\u0644\u0649 \u062E\u0635\u0627\u0626\u0635\u0647\u0627");
}
function profStepMult() {
  const p = profOf();
  if (!p || !p.vol) return 1;
  return clamp(p.vol / 0.05, 0.7, 2.2);
}
function memHuntMult() {
  const lv = sanctionLevel(S.config.direction);
  if (lv >= 2) return 1.15;
  const r = memRec(S.config.direction);
  if (!r || r.n < 6) return 1;
  const wr = smWR(r);
  if (wr >= 0.6 && r.pnl > 0) return 0.85;
  if (lv === 1) return 1.1;
  return 1;
}
function packHunt(p) {
  if (!(S.multiplier > 0)) {
    pushLog("warn", "\u0628\u0627\u0646\u062A\u0638\u0627\u0631 \u0645\u0648\u0627\u0635\u0641\u0627\u062A \u0627\u0644\u0639\u0642\u062F \u2014 \u0644\u0627 \u062F\u062E\u0648\u0644 \u0628\u062D\u062C\u0645 \u063A\u064A\u0631 \u0645\u0639\u0631\u0648\u0641");
    return null;
  }
  return { side: S.config.direction === "short" ? "sell" : "buy", qty: contractsForLevel(p) };
}
function huntTrigger(p) {
  if (S.status !== "running" || !p) return null;
  if (S._metaSym !== S.config.symbol || !(S.multiplier > 0)) return null;
  if (!warmedUp()) return null;
  const kick = priceKick();
  const againstPump = S.config.direction === "short" && kick.up > 0.8 || S.config.direction === "long" && kick.down > 0.8;
  if (againstPump) {
    if (!S._kickVetoAt || Date.now() - S._kickVetoAt > 12e4) {
      S._kickVetoAt = Date.now();
      pushLog("server", "\u26D4 \u0641\u064A\u062A\u0648 \u0627\u0646\u062F\u0641\u0627\u0639: \u062A\u062D\u0631\u0643 " + (S.config.direction === "short" ? "\u0635\u0627\u0639\u062F" : "\u0647\u0627\u0628\u0637") + " \u0642\u0648\u064A " + (S.config.direction === "short" ? kick.up : kick.down).toFixed(2) + "% \u062E\u0644\u0627\u0644 \u062F\u0642\u064A\u0642\u0629 \u2014 \u0644\u0627 \u062F\u062E\u0648\u0644 \u0636\u062F\u0647 \u062D\u062A\u0649 \u064A\u0633\u062A\u0642\u0631");
    }
    return null;
  }
  if (!S.position && S._harvestAt && !S._reselectDone && Date.now() - S._harvestAt < 15 * 60 * 1e3) return null;
  if (studying()) return null;
  if (!S.position && S._lossStreak && S._lossStreak.side === S.config.direction && S._lossStreak.n >= 2 && Date.now() - S._lossStreak.at < 12e4) return null;
  const flipping = !!(S.position && S.config.direction !== S.position.side);
  if (addsBlocked() && !flipping) return null;
  if (flipping) {
    if (S.lastHuntAt && Date.now() - S.lastHuntAt < 12e4) return null;
    const ageOk = S.position && Date.now() - (S.position.openedAt || 0) > 12e4;
    if (!ageOk && adversePct(p) < 0.3) return null;
    if (toxicBlocked(S.config.direction)) return null;
    return packHunt(p);
  }
  if (toxicBlocked(S.config.direction)) return null;
  if (batchHoldActive()) return null;
  if (S.position && exposureCapped()) return null;
  if (S.position && volBrakeActive()) {
    if (!S._brkVetoAt || Date.now() - S._brkVetoAt > 18e4) {
      S._brkVetoAt = Date.now();
      pushLog("server", "\u{1F512} \u0644\u0627 \u0625\u0636\u0627\u0641\u0627\u062A \u0623\u062B\u0646\u0627\u0621 \u0641\u0631\u0645\u0644\u0629 \u0627\u0644\u062A\u0642\u0644\u0628 \u2014 \u0627\u0644\u0633\u0644\u0645 \u0642\u0627\u0626\u0645 \u0628\u0644\u0627 \u062A\u063A\u0630\u064A\u0629 \u062D\u062A\u0649 \u064A\u0633\u062A\u0642\u0631");
    }
    return null;
  }
  if (S.position) {
    if (!inAddZone(p)) return null;
  }
  if (S.startedAt && Date.now() - S.startedAt < SCOUT_MS && filledAdds() >= 1) return null;
  if (S.lastAddAt && Date.now() - S.lastAddAt < ADD_COOLDOWN) return null;
  trailHuntAnchor(p);
  if (S.lastHuntAt && Date.now() - S.lastHuntAt < (S.config.hfMode ? 3e3 : HUNT_COOLDOWN)) return null;
  const volNow = retStdev() || 0.05;
  const capHunt = S.regime && S.regime.shock ? 2 : volNow > 1.2 ? 2 : volNow < 0.35 ? 4 : MAX_HUNT_OPEN;
  if (openHuntLots() >= capHunt) return null;
  if (huntTooClose(p)) return null;
  if (S.position) {
    const pk = packHunt(p);
    if (!pk) return null;
    const cover = S.position.side === "short" && pk.side === "buy" || S.position.side === "long" && pk.side === "sell";
    return cover ? null : pk;
  }
  const veto = dangerVeto(p);
  if (veto) {
    if (!S._vetoAt || Date.now() - S._vetoAt > 12e4) {
      S._vetoAt = Date.now();
      pushLog("info", "\u0641\u064A\u062A\u0648 \u062E\u0637\u0631 \u0645\u0646\u0639 \u0635\u0641\u0642\u0629: " + veto);
    }
    return null;
  }
  const sig2 = huntSignals(p);
  if (!sig2.length) return null;
  if (sig2.length < 2) {
    const need = effectiveHuntPct();
    const markGap = sig2.some((x) => x.indexOf("\u0641\u062C\u0648\u0629 \u0645\u0627\u0631\u0643") === 0);
    const decentRetrace = sig2.some((x) => x.indexOf("\u0645\u0631\u0633\u0627\u0629") === 0 && parseFloat(x.replace(/[^0-9.]/g, "")) >= need * 0.8);
    const strongSingle = markGap || decentRetrace || sig2.length === 1 && (sig2[0].indexOf("\u0632\u062E\u0645") === 0 || sig2[0].indexOf("\u0627\u062A\u062C\u0627\u0647") === 0 || sig2[0].indexOf("\u062D\u064A\u062A\u0627\u0646") === 0 || sig2[0].indexOf("\u0627\u0646\u062D\u0631\u0627\u0641") === 0 || sig2[0].indexOf("\u0641\u062E \u0627\u062E\u062A\u0631\u0627\u0642") === 0 || sig2[0].indexOf("\u0636\u063A\u0637 \u0623\u0648\u0627\u0645\u0631") === 0);
    if (!strongSingle) return null;
  }
  S._lastSig = sig2.slice(0, 3).join(" \xB7 ");
  return packHunt(p);
}
function maybeHunt(p) {
  const hit = huntTrigger(p);
  if (!hit) return false;
  {
    const W = 2 * 36e5, now = Date.now();
    if (p > 0) {
      if (!S._slideHi || now - (S._slideHiAt || 0) > W) {
        S._slideHi = p;
        S._slideHiAt = now;
      } else if (p > S._slideHi) {
        S._slideHi = p;
        S._slideHiAt = now;
      }
      if (!S._slideLo || now - (S._slideLoAt || 0) > W) {
        S._slideLo = p;
        S._slideLoAt = now;
      } else if (p < S._slideLo) {
        S._slideLo = p;
        S._slideLoAt = now;
      }
    }
  }
  {
    const W = 2 * 36e5, now = Date.now(), long = hit.side !== "short", px = S.lastPrice || p;
    let veto = "", why = "";
    if (long && S._slideHiAt && now - S._slideHiAt < W && px > 0 && px < S._slideHi * 0.98) {
      veto = "coin";
      why = "\u0627\u0644\u0639\u0645\u0644\u0629 \u062A\u062D\u062A \u0642\u0645\u0629 \u0627\u0644\u0633\u0627\u0639\u062A\u064A\u0646 \u0628\u06402%+ \u2014 \u0647\u0628\u0648\u0637 \u0645\u0633\u062A\u062F\u0627\u0645";
    } else if (!long && S._slideLoAt && now - S._slideLoAt < W && px > 0 && px > S._slideLo * 1.02) {
      veto = "coin";
      why = "\u0627\u0644\u0639\u0645\u0644\u0629 \u0641\u0648\u0642 \u0642\u0627\u0639 \u0627\u0644\u0633\u0627\u0639\u062A\u064A\u0646 \u0628\u06402%+ \u2014 \u0635\u0639\u0648\u062F \u0645\u0633\u062A\u062F\u0627\u0645";
    } else if (long && S._btcHiAt && now - S._btcHiAt < W && S._btcPx > 0 && S._btcPx < S._btcHi * 0.985) {
      veto = "btc";
      why = "BTC \u062A\u062D\u062A \u0642\u0645\u062A\u0647 \u0628\u0633\u0627\u0639\u062A\u064A\u0646 \u0628\u06401.5%+ \u2014 \u0633\u0644\u0629 \u0627\u0644\u0645\u064A\u0645\u0632 \u062A\u0646\u0632\u0644 \u0645\u0639\u0627\u064B";
    } else if (!long && S._btcLoAt && now - S._btcLoAt < W && S._btcPx > 0 && S._btcPx > S._btcLo * 1.015) {
      veto = "btc";
      why = "BTC \u0641\u0648\u0642 \u0642\u0627\u0639\u0647 \u0628\u0633\u0627\u0639\u062A\u064A\u0646 \u0628\u06401.5%+ \u2014 \u0627\u0644\u0633\u0644\u0629 \u062A\u0635\u0639\u062F \u0645\u0639\u0627\u064B";
    }
    if (veto) {
      if (Date.now() - (S._slideVetoAt || 0) > 3e5) {
        S._slideVetoAt = Date.now();
        pushLog("server", "\u26D4 \u0641\u064A\u062A\u0648 \u0645\u0646\u0637\u0642\u0629 \u0627\u0644\u062F\u062E\u0648\u0644 (" + (veto === "btc" ? "\u0627\u0644\u0633\u0648\u0642" : "\u0627\u0644\u0639\u0645\u0644\u0629") + "): " + why + " \u2014 \u0644\u0627 \u062F\u062E\u0648\u0644 \u0627\u0644\u0622\u0646");
      }
      return false;
    }
  }
  if (!spacingGate(p)) {
    if (Date.now() - (S._gateLogAt || 0) > 12e4) {
      S._gateLogAt = Date.now();
      pushLog("server", "\u0642\u0641\u0644 \u0627\u0644\u062A\u0628\u0627\u0639\u062F: \u062A\u064F\u062E\u0651\u0631 \u062F\u062E\u0648\u0644 \u0627\u0644\u0635\u064A\u062F \u2014 \u0644\u0644\u0633\u0639\u0629 \u0642\u0631\u064A\u0628\u0629 \u062C\u062F\u064B\u0627 \u0645\u0646 \u0622\u062E\u0631 \u062A\u0646\u0641\u064A\u0630");
    }
    return false;
  }
  S.lastHuntAt = Date.now();
  S.huntAnchor = p;
  const fee = feeFor(hit.qty * (S.multiplier || 1) * p, true);
  S.feesPaid += fee;
  const d = applyDelta(hit.side, hit.qty, p);
  if (d.addedQty > 0) {
    S._batchFills = (S._batchFills || 0) + 1;
    if (S._batchFills >= 4 && !S._batchHoldAt) {
      S._batchHoldAt = Date.now();
      S._batchStudiedAt = false;
      S._batchWorst = null;
      S._batchWorseAt = 0;
      cancelPendingAdds();
      pushLog("server", "\u{1F9F7} \u0627\u0644\u0642\u0627\u0637\u0639 \u0627\u0644\u0645\u0631\u062D\u0644\u064A (\u0635\u064A\u062F): \u0627\u0643\u062A\u0645\u0644\u062A 4 \u0623\u0648\u0627\u0645\u0631 \u062F\u062E\u0648\u0644 \u2014 \u062A\u0648\u0642\u0641 \u0627\u0644\u0641\u062A\u062D \u062D\u062A\u0649 \u064A\u062A\u0648\u0642\u0641 \u0627\u0644\u0627\u0646\u0632\u0644\u0627\u0642 + \u062F\u0631\u0627\u0633\u0629 \u062B\u0645 4 \u0623\u062E\u0631\u0649");
    }
  }
  if (d.closedQty > 0) {
    const of = S.journal.find((j) => j.status === "open")?.fees || 0;
    noteClose(d.closedQty, p, fee, "\u0635\u0641\u0642\u0629", d.realized - of - fee);
    S.huntOpen = Math.max(0, (S.huntOpen || 0) - 1);
  }
  if (d.addedQty > 0) {
    S.huntCount++;
    S.huntOpen = (S.huntOpen || 0) + 1;
    const lotId = noteEntry(d.addedQty, p, fee, "hunt");
    S.grid.push({
      id: uid("hunt"),
      clientOid: uid("oid"),
      side: hit.side,
      price: p,
      qty: d.addedQty,
      status: "filled",
      reduceOnly: false,
      exchangeOrderId: null,
      filledAt: Date.now(),
      origin: "hunt",
      createdAt: Date.now(),
      lotId
    });
    notify("\u0635\u0641\u0642\u0629 \u0641\u0631\u062F\u064A\u0629 #" + S.huntCount + " \u2014 " + (hit.side === "sell" ? "\u0628\u064A\u0639" : "\u0634\u0631\u0627\u0621") + " @ " + fmtPx(p) + (S._lastSig ? " \u2014 " + S._lastSig : ""));
    S._lastSig = null;
  }
  if (S.position && S.status === "running") syncPositionTp();
  S.lastWorkAt = Date.now();
  if (d.addedQty > 0) S.lastAddAt = Date.now();
  return true;
}
function computeLiq() {
  const dir2 = S.position ? S.position.side : S.config.direction;
  const pend = S.grid.filter((g) => !g.reduceOnly && g.status === "open" && (dir2 === "short" ? g.side === "sell" : g.side === "buy"));
  const posQ = S.position ? S.position.size : 0, pendQ = pend.reduce((a, g) => a + g.qty, 0);
  const q = posQ + pendQ;
  if (q <= 0) return null;
  const pendPx = pend.reduce((a, g) => a + g.price * g.qty, 0);
  const entry = posQ > 0 ? (S.position.entry * posQ + pendPx) / q : pendQ > 0 ? pendPx / pendQ : 0;
  if (!entry) return null;
  const mult = S.multiplier || 1, lev = Math.max(1, S.position ? S.position.leverage : S.config.leverage);
  const mmr = lev >= 75 ? 0.025 : lev >= 50 ? 0.012 : lev >= 25 ? 8e-3 : lev >= 10 ? 5e-3 : 4e-3;
  const liqFee = Math.max(S.takerFee, 5e-4);
  const notional = entry * q * mult;
  const curNotional = (S.position ? S.position.size * mult * (S.position.entry || entry) : 0) || notional;
  const eq = Math.max(0, S.exEquity || 0);
  const backing = S.config.mode === "live" ? Math.max(curNotional / lev + Math.max(0, (S.exAvail || 0) * 0.95), eq * 0.95) : Math.max(0, S.config.cycleBalance || 0);
  const margin = backing + Math.max(0, S.position ? S.position.unrealized || 0 : 0);
  const minD = 0.5 / lev;
  if (dir2 === "long") {
    const den2 = q * mult * (1 - mmr - liqFee);
    if (den2 <= 0) return null;
    const lp2 = (notional - margin) / den2;
    return lp2 > 0 && lp2 <= entry * (1 - minD) ? lp2 : null;
  }
  const den = q * mult * (1 + mmr + liqFee);
  if (den <= 0) return null;
  const lp = (notional + margin) / den;
  return lp >= entry * (1 + minD) ? lp : null;
}
function refreshLiq() {
  const local = computeLiq();
  let ex = S.exLiqPrice || 0;
  const e = S.position ? S.position.entry : 0;
  if (ex > 0 && e > 0 && S.position) {
    const sh = S.position.side === "short";
    if (sh ? ex > e * 20 || ex <= e * 1.002 : ex < e * 0.05 || ex >= e * 0.998) ex = 0;
  }
  if (ex > 0 && !S.grid.some((g) => !g.reduceOnly && g.status === "armed" && !g.exchangeOrderId)) {
    S.liqPrice = ex;
  } else S.liqPrice = local;
  if (S.position) S.position.liquidation = S.liqPrice;
}
function markUnrealized(p) {
  if (S.position) {
    S.position.unrealized = (S.position.side === "short" ? 1 : -1) * (S.position.entry - p) * (S.multiplier || 1) * S.position.size;
    const fav = (S.position.side === "short" ? S.position.entry - p : p - S.position.entry) / S.position.entry * 100;
    S._exc = S._exc || { mae: 0, mfe: 0 };
    if (fav >= 0) S._exc.mfe = Math.max(S._exc.mfe, fav);
    else S._exc.mae = Math.max(S._exc.mae, -fav);
  }
  refreshLiq();
}
function liqDanger(p) {
  const pos = S.position, liq = pos ? pos.liquidation || S.liqPrice : S.liqPrice;
  if (!pos || !liq || !p) return false;
  return pos.side === "long" ? p <= liq * 1.015 : p >= liq * 0.985;
}
function reversalAgainst(p) {
  if (!S.position) return false;
  const r = S.regime || { trend: "range" }, mom = S.confluence ? S.confluence.momentum : 0;
  const adv = adversePct(p), long = S.position.side === "long";
  const tA = long ? r.trend === "down" : r.trend === "up";
  const mA = long ? mom <= -0.1 : mom >= 0.1;
  const bA = long ? S.biasScore <= -8 : S.biasScore >= 8;
  if (tA && mA && bA && adv >= 0.65) return true;
  if (tA && mA && adv >= 1.4) return true;
  return false;
}
function cancelPendingAdds() {
  const live = S.config.mode === "live" && S.keys;
  for (const g of S.grid) {
    if (g.lane === "comp" && !batchHoldActive()) continue;
    if (!g.reduceOnly && (g.status === "armed" || g.status === "open") && !g.filledAt) {
      g.status = "cancelled";
      if (live && g.exchangeOrderId) {
        const id = g.exchangeOrderId;
        g.exchangeOrderId = null;
        exCancelOne(id);
      } else g.exchangeOrderId = null;
    }
  }
}
async function patienceLimit(p) {
  if (!p || S.status !== "running" || !S.position) return;
  const hrs = Number(S.config.patienceHrs);
  const usd = Number(S.config.patienceUsd);
  if (!(hrs > 0) && !(usd > 0)) return;
  const ageMs = Date.now() - (S.position.openedAt || Date.now());
  const adv = adversePct(p);
  const floatUsd = -(S.position.unrealized || 0);
  const aged = hrs > 0 && ageMs > hrs * 3600 * 1e3;
  const sunk = usd > 0 && floatUsd >= usd;
  if (!aged && !sunk) return;
  if (S.regime && !S.regime.shock) {
    const against = S.position.side === "short" && S.regime.trend === "down" && adv > 0.6 || S.position.side === "long" && S.regime.trend === "up" && adv > 0.6;
    if (!against) {
      return;
    }
  }
  const why = aged ? "\u0639\u0644\u0642 " + Math.round(ageMs / 36e5) + " \u0633" : "\u0627\u0646\u062D\u062F\u0627\u0631 " + fmtUsd(floatUsd);
  pushLog("server", "\u23F0 \u062D\u062F \u0627\u0644\u0635\u0628\u0631: \u0625\u063A\u0644\u0627\u0642 \u0627\u0644\u0645\u0631\u0643\u0632 \u0627\u0644\u0639\u0627\u0644\u0642 (" + why + ") \u2014 \u062A\u0639\u0648\u062F \u0627\u0644\u062F\u0648\u0631\u0629 \u0645\u0646 \u062C\u062F\u064A\u062F \u0645\u0639 \u062F\u0631\u0627\u0633\u0629");
  await flattenAt(p, "\u062D\u062F \u0627\u0644\u0635\u0628\u0631");
  S._postDisaster = Date.now();
  startStudy(false);
}
async function circuitBreak(p) {
  if (!p || S.status !== "running") return;
  const limit = Math.max(LOSS_STOP_USD, (S.config.cycleBalance || 0) * 0.08);
  const cyc = (S.realizedPnl || 0) + (S.position ? S.position.unrealized : 0) - (S.feesPaid || 0) - (S.slipCost || 0) - (S.fundingPaid || 0);
  if (cyc <= -limit) {
    if (!S._holdLoggedAt || Date.now() - S._holdLoggedAt > 10 * 60 * 1e3) {
      S._holdLoggedAt = Date.now();
      pushLog("server", "\u{1F6E1}\uFE0F \u0635\u0645\u0648\u062F: \u0627\u0644\u062F\u0648\u0631\u0629 \u0639\u0644\u0649 " + fmtUsd(cyc) + " \u2014 \u0644\u0627 \u0625\u0642\u0641\u0627\u0644 \u0628\u062E\u0633\u0627\u0631\u0629 \u2014 \u0627\u0644\u0634\u0628\u0643\u0629 \u062A\u0639\u0648\u0636 \u0639\u0646\u062F \u0627\u0644\u0645\u0646\u0627\u0637\u0642 \u0627\u0644\u0645\u062F\u0631\u0648\u0633\u0629");
    }
    return;
  }
}
async function escapeAdverse(p) {
  if (S.status !== "running" || !S.position || !p) return;
  const pos = S.position;
  let danger = liqDanger(p);
  reversalAgainst(p);
  if (danger) {
    const exl = S.exLiqPrice || 0, e0 = pos.entry || 0;
    if (exl > 0 && e0 > 0) {
      const sane = pos.side === "short" ? exl > e0 * 1.005 : exl < e0 * 0.995;
      const nearEx = pos.side === "short" ? p >= exl * 0.985 : p <= exl * 1.015;
      if (sane && !nearEx) {
        danger = false;
        if (!S._liqFalseAt || Date.now() - S._liqFalseAt > 12e4) {
          S._liqFalseAt = Date.now();
          pushLog("server", "\u0625\u0646\u0630\u0627\u0631 \u062A\u0635\u0641\u064A\u0629 \u0645\u062D\u0644\u064A \u0643\u0627\u0630\u0628 \u2014 \u0627\u0644\u0645\u0646\u0635\u0629 \u062A\u0624\u0643\u062F \u0627\u0644\u062A\u0635\u0641\u064A\u0629 \u0628\u0639\u064A\u062F\u0629 \u0639\u0646\u062F " + fmtPx(exl) + " \u2014 \u0644\u0627 \u0625\u0642\u0641\u0627\u0644");
        }
      }
    }
  }
  const f = tapeFlow(), mom = S.confluence ? S.confluence.momentum : 0;
  const whaleRaw = f.whaleVol > 0.28 && (pos.side === "short" && f.whale > 0.5 && mom > 0.08 || pos.side === "long" && f.whale < -0.5 && mom < -0.08);
  S._waveN = whaleRaw ? (S._waveN || 0) + 1 : 0;
  const posAge = Date.now() - (pos.openedAt || 0);
  const adv = adversePct(p);
  const whaleWave = S._waveN >= 5 && (posAge > 12e4 || adv >= 0.35 || S.regime && S.regime.shock);
  if (whaleRaw && !whaleWave && S._waveN === 5 && (!S._waveLogAt || Date.now() - S._waveLogAt > 6e4)) {
    S._waveLogAt = Date.now();
    pushLog("server", "\u0645\u0648\u062C\u0629 \u062D\u064A\u062A\u0627\u0646 \u0645\u0628\u0643\u0631\u0629 \u2014 \u062A\u064F\u0631\u0627\u0642\u0628 \u0648\u0644\u0627 \u0627\u0646\u0642\u0644\u0627\u0628 (\u0645\u0631\u0643\u0632 \u062D\u062F\u064A\u062B \u0628\u0644\u0627 \u0636\u0631\u0631 \u062D\u0642\u064A\u0642\u064A)");
  }
  const mult = S.multiplier || 1, qty = pos.size || 0;
  const entryFees = S.journal.filter((j) => j.status === "open").reduce((a, j) => a + (j.fees || 0), 0) || qty * mult * pos.entry * (S.makerFee || 2e-4);
  const netAll = (pos.side === "short" ? 1 : -1) * (pos.entry - p) * mult * qty - entryFees - qty * mult * p * (S.takerFee || 6e-4);
  if (!danger) return;
  const was = pos.side, nextSide = was === "long" ? "short" : "long";
  await flattenAt(p, danger ? "\u0645\u0631\u0643\u0632" : "\u0643\u0627\u0628\u062D \u0627\u0644\u0643\u0627\u0631\u062B\u0629");
  if (S.config.directionMode === "auto") S.config.direction = nextSide;
  S.huntAnchor = p;
  S._flipAt = Date.now();
  S.gridAnchor = p;
  S._postDisaster = Date.now();
  startStudy(false);
  if (danger) {
    pushLog("server", "\u26A0\uFE0F \u0642\u0631\u0628 \u0627\u0644\u062A\u0635\u0641\u064A\u0629 \u2014 \u0623\u064F\u063A\u0644\u0642 " + (was === "long" ? "\u0627\u0644\u0644\u0648\u0646\u063A" : "\u0627\u0644\u0634\u0648\u0631\u062A") + " \u0641\u0648\u0631\u064B\u0627 \u2014 \u062F\u0631\u0627\u0633\u0629 \u0627\u0644\u0648\u0636\u0639 \u0642\u0628\u0644 \u0623\u064A \u062F\u062E\u0648\u0644");
    notify("\u0648\u0642\u0641 \u0637\u0627\u0631\u0626: \u0642\u0631\u0628 \u0627\u0644\u062A\u0635\u0641\u064A\u0629 \u2014 \u0623\u064F\u063A\u0644\u0642 \u0627\u0644\u0645\u0631\u0643\u0632 \u0648\u064A\u062F\u0631\u0633 \u0627\u0644\u0633\u0648\u0642 \u0642\u0628\u0644 \u0627\u0644\u0639\u0648\u062F\u0629", "warn");
  } else {
    pushLog("server", "\u26D4 \u0643\u0627\u0628\u062D \u0627\u0644\u0643\u0627\u0631\u062B\u0629: \u0627\u0646\u0639\u0643\u0627\u0633 \u0645\u0624\u0643\u062F \u0628\u0636\u063A\u0637 \u0645\u0639\u0627\u0643\u0633 \u0648\u062E\u0633\u0627\u0631\u0629 " + fmtUsd(-netAll) + " \u2014 \u0623\u064F\u063A\u0644\u0642 \u0627\u0644\u0645\u0631\u0643\u0632 \u2014 \u062F\u0631\u0627\u0633\u0629 \u0648\u0627\u0633\u062A\u0642\u0631\u0627\u0631 \u062B\u0645 \u062A\u0642\u064A\u064A\u0645 \u0627\u0644\u062F\u062E\u0648\u0644 \u0645\u0646 \u062C\u062F\u064A\u062F");
    notify("\u0648\u0642\u0641 \u0627\u0644\u062E\u0633\u0627\u0631\u0629: \u0627\u0646\u0639\u0643\u0627\u0633 \u0645\u0624\u0643\u062F \u2014 \u0623\u064F\u063A\u0644\u0642 \u0627\u0644\u0645\u0631\u0643\u0632 \u0639\u0646\u062F \u062D\u062F \u0627\u0644\u0643\u0627\u0631\u062B\u0629 (" + fmtUsd(-netAll) + ")", "warn");
  }
}
function ensureGrid() {
  if (S.status !== "running") return;
  if (S.grid.length > 400) {
    let dead = 0;
    S.grid = S.grid.filter((g) => {
      if (g.status === "armed" || g.status === "open" || g.status === "filled" || g.status === "staged") return true;
      dead++;
      return dead <= 100;
    });
  }
  const center = S.lastPrice || S.gridAnchor || (S.position ? S.position.entry : 0);
  if (!center) return;
  sanitizeAdds();
  if (S.position && (S._waveHoldUntil && Date.now() < S._waveHoldUntil || volBrakeActive())) {
    syncPositionTp();
    return;
  }
  const dual = S.grid.some((g) => g.lane === "hold" && (g.status === "armed" || g.status === "open")) && S.grid.some((g) => g.lane === "trend" && (g.status === "armed" || g.status === "open"));
  if (dual) {
    if (S.position) syncPositionTp();
    return;
  }
  if (filledAdds() >= effLevels()) {
    if (S.position) syncPositionTp();
    return;
  }
  const sameSide = !!(S.position && S.position.side === S.config.direction);
  const short = S.position && S.position.side === "short";
  const chasing = !!(sameSide && S.gridAnchor) && (short && center > S.gridAnchor || !short && center < S.gridAnchor);
  const jump = lastJumpPct();
  const live = S.grid.filter((g) => !g.reduceOnly && (g.status === "armed" || g.status === "open"));
  const stagedN = S.grid.filter((g) => g.status === "staged").length;
  const drift = S.gridAnchor ? Math.abs(center - S.gridAnchor) / S.gridAnchor : 1;
  const empty = live.length === 0 && stagedN === 0, away = drift > S.config.gridStepPct / 100 * 2.2;
  if (chasing && jump > 0.18) return;
  if (!empty && !away && jump < 0.12) return;
  if (chasing && !empty) {
    const paused = jump > 0.02 && jump < 0.16;
    if (!(paused && inAddZone(center))) return;
  }
  if (S.position && !inAddZone(center) && !empty) {
    if (S.position) syncPositionTp();
    return;
  }
  cancelPendingAdds();
  const keep = S.grid.filter((g) => g.status === "filled" || g.status === "cancelled" || g.status === "staged" || g.reduceOnly && (g.status === "open" || g.status === "armed"));
  S.grid = [...keep, ...buildGrid(center, false, 4)].filter((g) => keep.includes(g) || g.reduceOnly || g.status !== "armed" || !shouldFill(g, center));
  S.gridAnchor = center;
  S.grid.sort((a, b) => b.price - a.price);
}
function studiedTpZone(pos, p) {
  try {
    const dir2 = pos.side === "long" ? 1 : -1;
    const ob = S.orderBook || {};
    const lv = pos.side === "long" ? ob.asks || [] : ob.bids || [];
    if (lv.length >= 8) {
      const sizes = lv.map((l) => +l.size || 0).sort((a, b) => a - b);
      const med = sizes[Math.floor(sizes.length / 2)] || 1;
      for (const l of lv) {
        const lp = +l.price || 0, lsz = +l.size || 0;
        const dist = (lp - p) / p * dir2;
        if (dist <= 0) continue;
        if (dist > 3e-3) break;
        if (lsz > med * 3) return {
          zone: lp * (dir2 > 0 ? 0.9995 : 1.0005),
          dist: dist * 100,
          why: "\u062C\u062F\u0627\u0631 \u0633\u064A\u0648\u0644\u0629 \u0628\u0627\u0631\u0632 \u0639\u0644\u0649 \u0628\u0639\u062F " + (dist * 100).toFixed(2) + "% \u2014 \u0627\u0644\u0627\u0631\u062A\u062F\u0627\u062F \u0645\u0646\u0647 \u0645\u0631\u062C\u062D"
        };
      }
    }
  } catch (e) {
    logSwallow("silent-catch", e);
  }
  return null;
}
function studiedEarlyExit(p) {
  const pos = S.position;
  if (!pos || !p || S.status !== "running") return null;
  if (Date.now() - (S._earlyExitAt || 0) < 9e4) return null;
  const mult = S.multiplier || 1, qty = pos.size || 0;
  const openFees = S.journal.filter((j) => j.status === "open").reduce((a, j) => a + (j.fees || 0), 0) || qty * mult * pos.entry * (S.makerFee || 2e-4);
  const exitFee = qty * mult * p * (S.takerFee || 6e-4);
  const grossNow = (pos.side === "long" ? p - pos.entry : pos.entry - p) * mult * qty;
  const netNow = grossNow - openFees - exitFee;
  if (netNow > 0) S._posPeakNet = Math.max(S._posPeakNet || 0, netNow);
  if (!(netNow >= Math.max(0.08, dynamicTpNet() * 0.2))) return null;
  const dir2 = pos.side === "long" ? 1 : -1;
  let hit = null;
  const ob = S.orderBook || {};
  const lv = pos.side === "long" ? ob.asks || [] : ob.bids || [];
  if (lv.length >= 8) {
    const sizes = lv.map((l) => +l.size || 0).sort((a, b) => a - b);
    const med = sizes[Math.floor(sizes.length / 2)] || 1;
    for (const l of lv) {
      const lp = +l.price || 0, lsz = +l.size || 0;
      const dist = (lp - p) / p * dir2;
      if (dist <= 0) continue;
      if (dist > 18e-4) break;
      if (dist > 0 && dist <= 15e-4 && lsz > med * 3) {
        hit = { netNow, why: "\u062C\u062F\u0627\u0631 \u0633\u064A\u0648\u0644\u0629 \u0628\u0627\u0631\u0632 \u0642\u0628\u0644\u0647 \u0628\u0640" + (dist * 100).toFixed(2) + "% \u2014 \u0627\u0631\u062A\u062F\u0627\u062F \u0645\u0631\u062C\u062D", tag: "wall" };
        break;
      }
    }
  }
  if (!hit) {
    const f = tapeFlow();
    const whaleAgainst = pos.side === "long" ? f.whale < -0.45 : f.whale > 0.45;
    if (f.whaleVol > 0.25 && whaleAgainst && netNow >= Math.max(0.05, dynamicTpNet() * 0.35))
      hit = { netNow, why: "\u062D\u064A\u062A\u0627\u0646 " + (pos.side === "long" ? "\u0634\u0631\u0627\u0621" : "\u0628\u064A\u0639") + " \u0645\u0636\u0627\u062F\u0629 (" + (f.whale > 0 ? "+" : "") + f.whale.toFixed(2) + ") \u062A\u0636\u063A\u0637 \u0639\u0643\u0633 \u0627\u0644\u0635\u0641\u0642\u0629", tag: "whales" };
  }
  if (!hit) {
    const mom = S.confluence ? S.confluence.momentum : 0;
    const momAgainst = pos.side === "long" && mom < -0.05 || pos.side === "short" && mom > 0.05;
    if (momAgainst && netNow >= dynamicTpNet() * 0.5)
      hit = { netNow, why: "\u0632\u062E\u0645 \u0627\u0646\u0642\u0644\u0628 \u0636\u062F \u0627\u0644\u0635\u0641\u0642\u0629 (" + (mom > 0 ? "+" : "") + mom.toFixed(2) + ") \u0628\u0639\u062F \u0631\u0628\u062D \u0646\u0635\u0641 \u0627\u0644\u0647\u062F\u0641", tag: "momentum" };
  }
  if (!hit) {
    const peak = S._posPeakNet || 0, tgt = dynamicTpNet();
    const engaged = peak >= Math.max(0.12, tgt * 0.3);
    const retraced = netNow <= peak * 0.6;
    const worthwhile = netNow >= Math.max(0.08, tgt * 0.08);
    if (engaged && retraced && worthwhile)
      hit = { netNow, why: "\u0630\u0631\u0648\u0629 \u0627\u0644\u0631\u0628\u062D \u0643\u0627\u0646\u062A " + peak.toFixed(2) + "$ \u0648\u062A\u0631\u0627\u062C\u0639\u062A \u0625\u0644\u0649 " + netNow.toFixed(2) + "$ \u2014 \u0642\u0641\u0644 \u0627\u0644\u0631\u0628\u062D \u0642\u0628\u0644 \u0630\u0648\u0628\u0627\u0646\u0647 \u0627\u0644\u0643\u0627\u0645\u0644", tag: "peaklock" };
  }
  if (hit) {
    S._earlyExitAt = Date.now();
    return hit;
  }
  return null;
}
async function flattenAt(p, source) {
  const pos = S.position;
  if (pos && p > 0) {
    const mult = S.multiplier || 1, q = pos.size || 0;
    const openFees = S.journal.filter((j) => j.status === "open").reduce((a, j) => a + (j.fees || 0), 0) || q * mult * pos.entry * (S.makerFee || 2e-4);
    const exitFee = q * mult * p * (S.takerFee || 6e-4);
    const gross = (pos.side === "long" ? p - pos.entry : pos.entry - p) * mult * q;
    const net = gross - openFees - exitFee;
    const sanctioned = source === "\u0625\u064A\u0642\u0627\u0641" || source === "\u0645\u0631\u0643\u0632" || source === "\u0643\u0627\u0628\u062D \u0627\u0644\u0643\u0627\u0631\u062B\u0629";
    if (net < 0.01 && !sanctioned) {
      if (!S._refuseAt || Date.now() - S._refuseAt > 12e4) {
        S._refuseAt = Date.now();
        pushLog("server", "\u{1F6E1}\uFE0F \u0631\u0641\u0636 \u0625\u063A\u0644\u0627\u0642 \u0628\u062E\u0633\u0627\u0631\u0629 (" + source + "): \u0627\u0644\u0635\u0627\u0641\u064A \u0627\u0644\u0622\u0646 " + fmtUsd(net) + " \u2014 \u0627\u0644\u0635\u0645\u0648\u062F \u062D\u062A\u0649 \u0627\u0644\u062C\u0646\u064A \u2014 \u0644\u0627 \u0625\u0642\u0641\u0627\u0644 \u0628\u062E\u0633\u0627\u0631\u0629 \u0623\u0628\u062F\u0627\u064B");
      }
      return;
    }
  }
  if (pos && S.config.mode === "live" && S.keys) {
    exCancelAll(S.config.symbol).catch(() => {
    });
    exCancelStops(S.config.symbol).catch(() => {
    });
    S._guardId = null;
    let closed = false;
    for (let att = 0; att < 5 && !closed; att++) {
      try {
        await exCloseQty(S.config.symbol, pos.side, pos.size);
        closed = true;
      } catch (e) {
        pushLog("error", "\u0645\u062D\u0627\u0648\u0644\u0629 \u0625\u063A\u0644\u0627\u0642 " + (att + 1) + " \u0641\u0634\u0644\u062A: " + String(e?.message || e));
        await new Promise((r) => setTimeout(r, 500));
        try {
          const live = await exPosition(S.config.symbol);
          if (live && live.size > 0) pos.size = live.size;
          else {
            closed = true;
          }
        } catch (e2) {
        }
      }
    }
    if (!closed) pushLog("error", "\u0627\u0644\u0625\u063A\u0644\u0627\u0642 \u0644\u0645 \u064A\u064F\u0624\u0643\u064E\u0651\u062F \u0628\u0639\u062F 5 \u0645\u062D\u0627\u0648\u0644\u0627\u062A \u2014 \u0627\u0644\u062D\u0627\u0631\u0633 \u0627\u0644\u0637\u0627\u0631\u0626 \u0639\u0644\u0649 \u0627\u0644\u0645\u0646\u0635\u0629 \u0647\u0648 \u0627\u0644\u062D\u0645\u0627\u064A\u0629 \u0627\u0644\u0623\u062E\u064A\u0631\u0629");
  }
  if (pos && p > 0) {
    const cs = pos.side === "short" ? "buy" : "sell";
    const fee = feeFor(pos.size * (S.multiplier || 1) * p, true);
    S.feesPaid += fee;
    const d = applyDelta(cs, pos.size, p);
    if (d.closedQty > 0) {
      const of = S.journal.find((j) => j.status === "open")?.fees || 0;
      noteClose(d.closedQty, p, fee, source, d.realized - of - fee);
    }
  }
  for (const row of S.journal) {
    if (row.status === "open") {
      row.status = "closed";
      row.exit = p;
      row.closedAt = Date.now();
      if (source === "\u0625\u064A\u0642\u0627\u0641") row.source = "\u0625\u064A\u0642\u0627\u0641";
      if (!row.pnl) row.pnl = lotNet(row.side, row.entry, p, row.qty, row.fees);
    }
  }
  S.position = null;
  S.huntOpen = 0;
  S._tpBanked = 0;
  S._trailFloor = 0;
  for (const l of S.grid) if (l.status === "open" || l.status === "armed") {
    l.status = "cancelled";
    l.exchangeOrderId = null;
  }
}
function pruneGhosts() {
  if (S.position) return;
  S.huntOpen = 0;
  for (const g of S.grid) {
    if (g.reduceOnly && (g.status === "open" || g.status === "armed")) {
      g.status = "cancelled";
      g.exchangeOrderId = null;
    }
    if (g.status === "filled" && !g.reduceOnly) g.status = "cancelled";
  }
  for (const row of S.journal) {
    if (row.status !== "open") continue;
    row.status = "closed";
    row.exit = S.lastPrice || row.entry;
    row.closedAt = Date.now();
    if (!row.pnl && row.exit) row.pnl = lotNet(row.side, row.entry, row.exit, row.qty, row.fees);
  }
}
function applyMeta(m) {
  S.lastPrice = m.price;
  S.markPrice = m.markPrice || m.price;
  if (m.makerFee) S.makerFee = m.makerFee;
  if (m.takerFee) S.takerFee = m.takerFee;
  if (m.funding != null) S.funding = m.funding;
  if (m.multiplier) S.multiplier = m.multiplier;
  if (m.tickSize) S.tickSize = m.tickSize;
  S.lastTickAt = Date.now();
  if (m.orderBook) S.orderBook = m.orderBook;
  if (m.tape) {
    S.tape = m.tape;
    for (const t of m.tape) S.cvd += t.side === "buy" ? t.size : -t.size;
  }
  if (m.price > 0) {
    S.priceTrail = [...S.priceTrail, m.price].slice(-120);
    S.emaFast = nextEma(S.emaFast, m.price, 2 / 10);
    S.emaSlow = nextEma(S.emaSlow, m.price, 2 / 22);
  }
  S.confluence = readConfluence();
  S.regime = readRegime();
}
function onStreamTick(d) {
  netAlivePing(true);
  if (d.symbol && d.symbol !== S.config.symbol) return;
  const now = Date.now();
  if (d.price > 0) {
    S.lastPrice = d.price;
    S.lastTickAt = now;
    if (!S._lastTrailAt || now - S._lastTrailAt > 400) {
      S.priceTrail = [...S.priceTrail, d.price].slice(-120);
      S.emaFast = nextEma(S.emaFast, d.price, 2 / 10);
      S.emaSlow = nextEma(S.emaSlow, d.price, 2 / 22);
      S._lastTrailAt = now;
      const tt = S.priceTrail;
      if (tt.length >= 20) {
        let hi = -Infinity, lo = Infinity;
        for (let i = 0; i < tt.length - 1; i++) {
          if (tt[i] > hi) hi = tt[i];
          if (tt[i] < lo) lo = tt[i];
        }
        if (hi > -Infinity && d.price > hi) S._brk = { side: "up", at: now, level: hi };
        else if (lo < Infinity && d.price < lo) S._brk = { side: "down", at: now, level: lo };
      }
    }
    if (!S._actAt || now - S._actAt > 6e4) {
      S._actAt = now;
      const cut = now - 6e4, rate = (S._tradeTs || []).filter((t) => t >= cut).length;
      S._actHist = [...S._actHist || [], rate].slice(-30);
      const h = [...S._actHist].sort((a, b) => a - b);
      S._actMed = h[Math.floor(h.length / 2)] || 0;
    }
    if (S.status === "running") {
      try {
        harvestRipe(d.price);
      } catch (e) {
        logSwallow("silent-catch", e);
      }
    }
  }
  if (d.book) {
    const b0 = d.book.bids && d.book.bids[0], a0 = d.book.asks && d.book.asks[0];
    const pb = S._prevBook;
    if (pb && pb.b && pb.a && b0 && a0) {
      let ofi = 0;
      if (b0.price > pb.b.price) ofi += b0.size;
      else if (b0.price < pb.b.price) ofi -= pb.b.size;
      else ofi += b0.size - pb.b.size;
      if (a0.price < pb.a.price) ofi += a0.size;
      else if (a0.price > pb.a.price) ofi -= pb.a.size;
      else ofi -= a0.size - pb.a.size;
      S._ofiWin = [...S._ofiWin || [], ofi].slice(-20);
      S._ofi = S._ofiWin.reduce((x, y) => x + y, 0);
    }
    S._prevBook = { b: b0 ? { price: b0.price, size: b0.size } : null, a: a0 ? { price: a0.price, size: a0.size } : null };
    S.orderBook = d.book;
  }
  if (d.trade && d.trade.price > 0) {
    S.lastPrice = d.trade.price;
    S.lastTickAt = now;
    S.tape = [d.trade, ...S.tape].slice(0, 32);
    S._tradeTs = [...S._tradeTs || [], now].slice(-300);
    S.cvd += d.trade.side === "buy" ? d.trade.size : -d.trade.size;
    if (!S._cvdAt || now - S._cvdAt > 1500) {
      S.cvdTrail = [...S.cvdTrail || [], { t: now, cvd: S.cvd }].slice(-90);
      S._cvdAt = now;
    }
  }
  S.confluence = readConfluence();
  S.regime = readRegime();
  if (S.position) accrueFunding();
  emitThrottled();
}
async function loadMarket(full) {
  const c = S.config.symbol;
  const wsOk = Date.now() - (streamState.lastMsgAt || 0) < 6e3;
  let price = S.lastPrice || 0, mark = S.markPrice || price;
  if (!wsOk) {
    const t = await fetchTicker(c);
    price = +t.price || price;
  }
  if (!S._metaAt || Date.now() - S._metaAt > 6e4) {
    const ct = await fetchContract(c);
    S._metaAt = Date.now();
    mark = +ct.markPrice || mark;
    S.makerFee = normFee(ct.makerFeeRate ?? ct.makerFeeCoefficient, S.makerFee);
    S.takerFee = normFee(ct.takerFeeRate ?? ct.takerFeeCoefficient, S.takerFee);
    S.funding = +ct.fundingFeeRate || 0;
    const _mul = +ct.multiplier, _tk = +ct.tickSize;
    if (_mul > 0) {
      S.multiplier = _mul;
      S._metaSym = c;
    }
    if (_tk > 0) S.tickSize = _tk;
    if (+ct.maxOrderQty > 0) S.maxOrderQty = +ct.maxOrderQty;
    if (!price) price = mark;
  }
  const m = { price, markPrice: mark };
  const bookStale = Date.now() - (streamState.bookAt || 0) > 8e3;
  const tapeStale = Date.now() - (streamState.tradeAt || 0) > 8e3;
  if (full && (bookStale || tapeStale || !S.tape.length)) {
    const [ob, tp] = await Promise.all([fetchBook(c).catch(() => null), fetchTape(c).catch(() => null)]);
    if (ob && (ob.bids.length || ob.asks.length)) m.orderBook = ob;
    else if (!ob) m.dataStale = Date.now();
    if (tp && tp.length && tapeStale) m.tape = tp;
    else if (!tp) m.dataStale = Date.now();
  }
  if (full && c !== "XBTUSDTM") {
    try {
      const bt = await fetchTicker("XBTUSDTM");
      const bp = +bt.price || 0;
      if (bp > 0) {
        const pv = S._btcPrev;
        if (pv && pv.px > 0) {
          const dtMin = Math.max(0.05, (Date.now() - pv.at) / 6e4);
          S._btcImp = Math.round((bp - pv.px) / pv.px * 100 / dtMin * 1e4) / 1e4;
        }
        S._btcPrev = { px: bp, at: Date.now() };
        {
          const W = 2 * 36e5, now = Date.now();
          S._btcPx = bp;
          if (!S._btcHi || now - (S._btcHiAt || 0) > W) {
            S._btcHi = bp;
            S._btcHiAt = now;
          } else if (bp > S._btcHi) {
            S._btcHi = bp;
            S._btcHiAt = now;
          }
          if (!S._btcLo || now - (S._btcLoAt || 0) > W) {
            S._btcLo = bp;
            S._btcLoAt = now;
          } else if (bp < S._btcLo) {
            S._btcLo = bp;
            S._btcLoAt = now;
          }
        }
      }
    } catch (e) {
      logSwallow("silent-catch", e);
    }
  }
  return m;
}
async function liveSync() {
  if (R()) return;
  if (S.config.mode !== "live" || !S.keys) return;
  const sym = S.config.symbol;
  try {
    const pos = await exPosition(sym);
    const inGrace = S.ignoreExchangeUntil && Date.now() < S.ignoreExchangeUntil;
    if (pos) {
      if (inGrace && !S.position) {
      } else if (!S.position) {
        S.position = pos;
        pushLog("recover", "\u0627\u0633\u062A\u064F\u0639\u064A\u062F \u0645\u0631\u0643\u0632 " + pos.side + " \u0645\u0646 \u0627\u0644\u0645\u0646\u0635\u0629");
      } else {
        const prevEntry = S.position.entry, prevSize = S.position.size;
        S.position.size = pos.size;
        S.position.entry = pos.entry;
        S.position.unrealized = pos.unrealized;
        S.position.side = pos.side;
        if (pos.liquidation) S.position.liquidation = pos.liquidation;
        if (pos.entry > 0 && Math.abs(pos.entry - prevEntry) / Math.max(1e-9, prevEntry) > 1e-4) {
          const openRow = S.journal.find((j) => j.status === "open");
          if (openRow) {
            const oldFees = openRow.fees || 0;
            openRow.entry = pos.entry;
            if (openRow.qty > 0 && S.position.size !== prevSize) {
              openRow.qty = S.position.size;
              openRow.fees = oldFees;
            }
          }
          if (S.activeCycle) S.activeCycle.entry = pos.entry;
        }
      }
    } else if (S.position && Date.now() > S.ignoreExchangeUntil) {
      S.position = null;
      S._guardPx = 0;
      S._guardId = null;
      S._tpBanked = 0;
      S._trailFloor = 0;
    }
    if (pos && pos.liquidation) S.exLiqPrice = pos.liquidation;
    if (!S._accAt || Date.now() - S._accAt > 3e4) {
      S._accAt = Date.now();
      kcPrivate("GET", "/api/v1/account-overview?currency=USDT").then((a) => {
        const av = Number(a && a.availableBalance);
        if (av >= 0 && isFinite(av)) S.exAvail = av;
        const eq = Number(a && a.accountEquity);
        if (eq > 0 && isFinite(eq)) S.exEquity = eq;
      }).catch(() => {
      });
    }
    if (pos && pos.leverage > 0) {
      S.exLeverage = pos.leverage;
      const cfgL = S.config.leverage || 1;
      if (Math.abs(pos.leverage - cfgL) / cfgL > 0.05 && (!S._levFixAt || Date.now() - S._levFixAt > 3e5)) {
        S._levFixAt = Date.now();
        kcPrivate(
          "POST",
          "/api/v2/changeCrossUserLeverage",
          { symbol: sym, leverage: String(cfgL) }
        ).then(() => {
          pushLog(
            "server",
            "\u0641\u064F\u0631\u0636\u062A \u0631\u0627\u0641\u0639\u0629 CROSS " + cfgL + "\xD7 \u0639\u0644\u0649 \u0627\u0644\u0645\u0646\u0635\u0629 \u0643\u0645\u0627 \u0636\u0628\u0637\u062A\u0647\u0627"
          );
        }).catch(() => {
        });
      }
    }
    if (!pos) {
      S.exLeverage = null;
      if (!S._mmFixAt || Date.now() - S._mmFixAt > 3e5) {
        S._mmFixAt = Date.now();
        kcPrivate("POST", "/api/v2/position/batchChangeMarginMode", { marginMode: "CROSS", symbols: [sym] }).then(() => {
          delete mmCache[sym];
        }).catch(() => {
        });
        kcPrivate(
          "POST",
          "/api/v2/changeCrossUserLeverage",
          { symbol: sym, leverage: String(S.config.leverage) }
        ).catch(() => {
        });
      }
    }
    if (S.position) exPlaceStopGuard();
  } catch (e) {
    const m = String(e?.message || e);
    if (m !== _syncErrMsg || Date.now() - _syncErrAt > 6e4) {
      _syncErrAt = Date.now();
      _syncErrMsg = m;
      pushLog("error", "\u0642\u0631\u0627\u0621\u0629 \u0627\u0644\u0645\u0631\u0643\u0632: " + m);
    }
  }
  try {
    const exs = await exOrders(sym);
    if (!exs) return;
    const fills = await exFills(sym);
    S._fillsOk = !!fills;
    if (fills) {
      const byOidF = /* @__PURE__ */ new Map();
      const byIdF = /* @__PURE__ */ new Map();
      for (const f of fills) {
        if (!(f.size > 0) || !(f.price > 0)) continue;
        if (f.clientOid) {
          const e1 = byOidF.get(f.clientOid) || { size: 0, cost: 0, fee: 0, n: 0 };
          e1.size += f.size;
          e1.cost += f.size * f.price;
          e1.fee += f.fee;
          e1.n++;
          byOidF.set(f.clientOid, e1);
        }
        if (f.orderId) {
          const e2 = byIdF.get(f.orderId) || { size: 0, cost: 0, fee: 0, n: 0 };
          e2.size += f.size;
          e2.cost += f.size * f.price;
          e2.fee += f.fee;
          e2.n++;
          byIdF.set(f.orderId, e2);
        }
      }
      const agg = (key, map) => {
        if (!key) return null;
        const e = map.get(key);
        if (!e || !(e.size > 0)) return null;
        return { avgPx: e.cost / e.size, size: e.size, fee: e.fee };
      };
      for (const l of S.grid) {
        if (l.status === "cancelled" || l.status === "filled") continue;
        const e = agg(l.clientOid, byOidF) || agg(l.exchangeOrderId, byIdF);
        if (!e) continue;
        const before = l.exFilled || 0;
        if (e.size > before) {
          const delta = e.size - before;
          const actualFee = e.fee * (delta / e.size);
          l._avgFillPx = e.avgPx;
          l._realFillFee = (l._realFillFee || 0) + actualFee;
          const done = creditExFillReal(l, delta, e.avgPx, actualFee);
          if (done) continue;
        } else l._avgFillPx = e.avgPx;
      }
      S._realFeesAt = Date.now();
    }
    const byOid = new Map(exs.map((o) => [o.clientOid || "", o]));
    const byId = new Map(exs.map((o) => [o.orderId, o]));
    for (const l of S.grid) {
      if (l.status === "cancelled" || l.status === "filled") continue;
      const hit = l.clientOid && byOid.get(l.clientOid) || l.exchangeOrderId && byId.get(l.exchangeOrderId);
      if (hit) {
        l.status = "open";
        l.exchangeOrderId = hit.orderId;
        const fs = hit.filledSize || 0;
        let done = false;
        if (fs > (l.exFilled || 0)) {
          const actualPrice = l._avgFillPx > 0 ? l._avgFillPx : hit.price > 0 ? hit.price : l.price;
          done = creditExFill(l, fs - (l.exFilled || 0), actualPrice);
        }
        if (!done && l.status !== "filled" && fs > 0 && fs >= l.qty - 1e-9) {
          l.status = "filled";
          l.filledAt = Date.now();
          l.filledPrice = l.price;
          if (l.reduceOnly && !S.position) {
            const tb = S._tpBanked;
            _harvestCleanup(l.price);
            pushLog("server", "\u062C\u0646\u064A \u0645\u0643\u062A\u0645\u0644 \u2713 \u2014 \u0646\u064F\u0641\u0651\u0630 \u0639\u0644\u0649 \u0627\u0644\u0645\u0646\u0635\u0629 \u0628\u0627\u0644\u0643\u0627\u0645\u0644\u060C \u0635\u0627\u0641\u064A \u0627\u0644\u062F\u0648\u0631\u0629 " + fmtUsd(tb));
            if (S.status === "running") maybeSwitchSymbol().catch(() => {
            });
          }
        }
      } else if (l.status === "open" && !shouldFill(l, S.lastPrice || 0)) {
        l.status = "armed";
        l.exchangeOrderId = null;
        l.exFilled = 0;
      } else if (l.status === "open" && l.exchangeOrderId && shouldFill(l, S.lastPrice || 0) && S._fillsOk === false) {
        const fs2 = l.exFilled || 0;
        if (!(fs2 > 0)) {
          l.status = "cancelled";
          l.exchangeOrderId = null;
          l.exFilled = 0;
          pushLog("warn", "\u0623\u0645\u0631 \u0627\u062E\u062A\u0641\u0649 \u0645\u0646 \u0627\u0644\u0645\u0646\u0635\u0629 \u0648\u0644\u0645 \u064A\u064F\u0646\u0641\u0651\u0638 \u0645\u0646\u0647 \u0634\u064A\u0621 \u2014 \u0644\u0627 \u064A\u064F\u062D\u062C\u0632 \u0623\u064A \u0631\u0628\u062D (\u0645\u0633\u062A\u0648\u0649 @ " + fmtPx(l.price) + ")");
        }
      } else if (l.status === "open" && l.exchangeOrderId && shouldFill(l, S.lastPrice || 0)) {
        const rem = l.qty - (l.exFilled || 0);
        let done = false;
        if (rem > 0) done = creditExFill(l, rem, l._avgFillPx > 0 ? l._avgFillPx : l.price > 0 ? l.price : S.lastPrice || 0);
        if (l.status !== "filled") {
          l.status = "filled";
          l.filledAt = Date.now();
          l.filledPrice = l._avgFillPx > 0 ? l._avgFillPx : l.price;
        }
        if (!done && l.reduceOnly && !S.position) {
          const tb = S._tpBanked;
          _harvestCleanup(l.price);
          pushLog("server", "\u062C\u0646\u064A \u0645\u0643\u062A\u0645\u0644 \u2713 \u2014 \u0646\u064F\u0641\u0651\u0630 \u0639\u0644\u0649 \u0627\u0644\u0645\u0646\u0635\u0629 \u0628\u0627\u0644\u0643\u0627\u0645\u0644\u060C \u0635\u0627\u0641\u064A \u0627\u0644\u062F\u0648\u0631\u0629 " + fmtUsd(tb));
        }
      }
    }
    for (const l of S.grid) {
      if ((l.status === "cancelled" || l.status === "filled") && l.exchangeOrderId) {
        exCancelOne(l.exchangeOrderId);
        l.exchangeOrderId = null;
      }
    }
    const hasLocalLive = S.grid.some((l) => l.status === "armed" || l.status === "open");
    for (const o of exs) {
      if (S.grid.some((l) => l.exchangeOrderId === o.orderId || o.clientOid && l.clientOid === o.clientOid)) continue;
      if (hasLocalLive) continue;
      S.grid.push({
        id: uid("ex"),
        clientOid: o.clientOid || uid("oid"),
        side: o.side,
        price: o.price,
        qty: o.size,
        status: "open",
        reduceOnly: o.reduceOnly,
        exchangeOrderId: o.orderId,
        filledAt: null,
        origin: "grid"
      });
    }
    const want = S.grid.filter((l) => l.status === "armed");
    if (!S.permDenied) for (const l of want) {
      if (l.exchangeOrderId) continue;
      try {
        const r = await exPlaceLimit({
          clientOid: l.clientOid,
          symbol: sym,
          side: l.side,
          price: l.price,
          qty: l.qty,
          reduceOnly: l.reduceOnly,
          leverage: S.config.leverage
        });
        l.status = "open";
        l.exchangeOrderId = r.orderId;
      } catch (e) {
        const m = String(e?.message || e);
        if (/access denied|permission/i.test(m)) {
          if (!S.permDenied) {
            S.permDenied = true;
            pushLog("error", "\u26D4 \u0627\u0644\u0645\u0641\u062A\u0627\u062D \u064A\u0642\u0631\u0623 \u0627\u0644\u0631\u0635\u064A\u062F \u0644\u0643\u0646 \u0628\u0644\u0627 \u0635\u0644\u0627\u062D\u064A\u0629 \u062A\u062F\u0627\u0648\u0644 \u2014 \u0645\u0646 KuCoin: \u0625\u062F\u0627\u0631\u0629 API \u2190 \u062A\u0639\u062F\u064A\u0644 \u0627\u0644\u0645\u0641\u062A\u0627\u062D \u2190 \u0641\u0639\u0651\u0644 \xAB\u0627\u0644\u062A\u062F\u0627\u0648\u0644\xBB \u0644\u0644\u0639\u0642\u0648\u062F \u0627\u0644\u0622\u062C\u0644\u0629 \u062B\u0645 \u0627\u062D\u0641\u0638 \u0627\u0644\u0645\u0641\u0627\u062A\u064A\u062D \u0645\u062C\u062F\u062F\u064B\u0627");
            toast("\u26A0\uFE0F \u0641\u0639\u0651\u0644 \u0635\u0644\u0627\u062D\u064A\u0629 \u0627\u0644\u062A\u062F\u0627\u0648\u0644 \u0641\u064A \u0645\u0641\u062A\u0627\u062D KuCoin");
          }
          break;
        }
        if (Date.now() - _ordErrAt > 6e4) {
          _ordErrAt = Date.now();
          pushLog("error", "\u0623\u0645\u0631 " + l.side + " @ " + fmtPx(l.price) + ": " + m);
        }
      }
    }
    const liveIds = new Set(S.grid.filter((l) => l.exchangeOrderId).map((l) => l.exchangeOrderId));
    for (const o of exs) {
      if (!liveIds.has(o.orderId)) {
        try {
          await kcPrivate("DELETE", "/api/v1/orders/" + o.orderId);
        } catch (e) {
          logSwallow("silent-catch", e);
        }
      }
    }
    if (!S._stopSweepAt || Date.now() - S._stopSweepAt > 3e4) {
      S._stopSweepAt = Date.now();
      const stops = await exStops(sym);
      if (stops) for (const st of stops) {
        if (st.id && st.id !== S._guardId) {
          try {
            await kcPrivate("DELETE", "/api/v1/stopOrders/" + st.id);
          } catch (e) {
            logSwallow("silent-catch", e);
          }
        }
      }
    }
  } catch (e) {
    const m = String(e?.message || e);
    if (m !== _syncErrMsg || Date.now() - _syncErrAt > 6e4) {
      _syncErrAt = Date.now();
      _syncErrMsg = m;
      pushLog("error", "\u0645\u0632\u0627\u0645\u0646\u0629 \u0627\u0644\u0623\u0648\u0627\u0645\u0631: " + m);
    }
  }
}
function heartbeatBeat(p) {
  if (S.status !== "running" || !p) return;
  const now = Date.now();
  if (S._lastBeatAt && now - S._lastBeatAt < 24e4) return;
  if (S.logs.length && now - S.logs[0].at < 24e4) return;
  S._lastBeatAt = now;
  const mom = S.confluence ? S.confluence.momentum : 0, fl = tapeFlow();
  let why;
  if (studying()) why = "\u0623\u064F\u0646\u0647\u064A \u062F\u0631\u0627\u0633\u0629 \u0627\u0644\u0628\u0635\u0645\u0629 (" + Math.max(1, Math.ceil((S.studyUntil - now) / 1e3)) + "\u062B \u0645\u062A\u0628\u0642\u064A\u0629)";
  else if (S.position) {
    const pos = S.position, sgn = pos.side === "short" ? 1 : -1;
    const net = sgn * (pos.entry - p) * (S.multiplier || 1) * pos.size - pos.size * (S.multiplier || 1) * (pos.entry * S.makerFee + p * S.takerFee);
    const tp = S.grid.find((g) => g.reduceOnly && (g.status === "open" || g.status === "armed"));
    why = "\u0623\u062F\u064A\u0631 \u0645\u0631\u0643\u0632\u064B\u0627 \u2014 \u0627\u0644\u0635\u0627\u0641\u064A " + fmtUsd(net) + " \u2014 \u062C\u0646\u064A \u0627\u0644\u0631\u0628\u062D " + (tp ? "@ " + fmtPx(tp.price) + " (\u0635\u0627\u0641\u064A " + fmtUsd(tpBase()) + " \u0644\u0644\u0645\u062C\u0645\u0648\u0639)" : "\u0642\u064A\u062F \u0627\u0644\u0648\u0636\u0639");
  } else {
    const v = dangerVeto(p);
    if (v) why = "\u0641\u064A\u062A\u0648 \u062E\u0637\u0631: " + v;
    else {
      const sig2 = huntSignals(p);
      why = sig2.length ? "\u0625\u0634\u0627\u0631\u0627\u062A \u062A\u062A\u0631\u0627\u0643\u0645: " + sig2.slice(0, 2).join(" \xB7 ") : "\u0644\u0627 \u0625\u0634\u0627\u0631\u0629 \u062F\u062E\u0648\u0644 \u0628\u0639\u062F \u2014 \u0623\u0646\u062A\u0638\u0631 \u0627\u0644\u062A\u0642\u0627\u0621 \u0627\u0644\u0632\u062E\u0645 \u0648\u0627\u0644\u062A\u062F\u0641\u0642";
    }
  }
  pushLog("info", "\u{1F493} \u0623\u0631\u0627\u0642\u0628 " + (S.config.displaySymbol || S.config.symbol) + " @ " + fmtPx(p) + " \u2014 \u0632\u062E\u0645 " + (mom >= 0 ? "+" : "") + mom.toFixed(2) + " \xB7 \u062A\u062F\u0641\u0642 " + (fl.bias >= 0 ? "+" : "") + fl.bias.toFixed(2) + " \u2014 " + why);
}
async function botTick() {
  const running = S.status === "running", paused = S.status === "paused";
  if (!running && !paused && S.status !== "idle") return;
  try {
    const full = S.heartbeat % 5 === 0 || !S.tape.length;
    let m = { price: S.lastPrice || S.markPrice || 0, markPrice: S.markPrice || S.lastPrice || 0 };
    try {
      m = await loadMarket(full);
    } catch (e) {
      logSwallow("silent-catch", e);
    }
    const price = S.lastPrice || m.price || 0;
    if (S.status === "idle") {
      applyMeta(m);
      emit();
      saveAll();
      return;
    }
    S.heartbeat++;
    applyMeta(m);
    studyTick();
    armProgressive();
    bgRadarTick();
    if (S.config.gapMode && !S.position && S.status === "running") {
      gapHunterTick().catch(() => {
      });
    }
    if (S.position && S.status === "running") {
      syncPositionTp();
      backgroundPipeline().catch(() => {
      });
      const ex = studiedEarlyExit(S.lastPrice || 0);
      if (ex) {
        S._earlyExitAt = Date.now();
        pushLog("server", "\u{1F3AF} \u062C\u0646\u064A \u0645\u0628\u0643\u0631 \u0645\u062F\u0631\u0648\u0633 (" + ex.tag + "): " + ex.why + " \u2014 \u0627\u0644\u0628\u064A\u0639 \u0627\u0644\u0622\u0646 \u0628\u0635\u0627\u0641\u064A " + fmtUsd(ex.netNow) + " \u0642\u0628\u0644 \u0627\u0644\u0627\u0631\u062A\u062F\u0627\u062F");
        toast("\u{1F3AF} \u062C\u0646\u064A \u0645\u0628\u0643\u0631: +" + ex.netNow.toFixed(2) + "$ \u0642\u0628\u0644 \u0627\u0644\u0627\u0631\u062A\u062F\u0627\u062F");
        nativeNotify("AB-KING \u2014 \u062C\u0646\u064A \u0645\u0628\u0643\u0631 \u0645\u062F\u0631\u0648\u0633", "\u0628\u064A\u0639 \u0642\u0628\u0644 \u0627\u0644\u0627\u0631\u062A\u062F\u0627\u062F \u0628\u0635\u0627\u0641\u064A +" + ex.netNow.toFixed(2) + "$");
        await flattenAt(S.lastPrice || 0, "\u062C\u0646\u064A \u0645\u0628\u0643\u0631 \u0645\u062F\u0631\u0648\u0633");
        _harvestCleanup(S.lastPrice || 0);
        return;
      }
    }
    if (!S.position && S.status === "running") {
      postHarvestReselect().catch(() => {
      });
      if (openNextReady()) return;
      backgroundPipeline().catch(() => {
      });
      maybeSwitchSymbol().catch(() => {
      });
    }
    const _prevDir = S.config.direction;
    resolveDirection(Date.now());
    if (S.config.direction !== _prevDir) {
      S.gridAnchor = null;
      for (const g of S.grid) {
        if (g.reduceOnly || g.filledAt) continue;
        if (g.status !== "armed" && g.status !== "open") continue;
        const gs = g.side === "sell" ? "short" : "long";
        if (gs !== S.config.direction) {
          g.status = "cancelled";
          g.exchangeOrderId = null;
        }
      }
    }
    if (running && price > 0) {
      await patienceLimit(price);
      await circuitBreak(price);
      await escapeAdverse(price);
      harvestRipe(price);
      const due = S.grid.filter((l) => shouldFill(l, price));
      if (S.config.mode !== "live") for (const l of selectDueAdds(due, price)) {
        const before = filledAdds();
        fillLevel(l.id, l.price, false);
        if (filledAdds() > before) S.lastAddAt = Date.now();
      }
      const hunt = huntTrigger(price);
      if (hunt && !S.permDenied && price > 0) {
        let done = false;
        if (S.config.mode === "live" && S.keys) {
          try {
            await exPlaceMarket(S.config.symbol, hunt.side, hunt.qty);
            done = true;
          } catch (e) {
            const m2 = String(e?.message || e);
            if (/access denied|permission/i.test(m2)) {
              S.permDenied = true;
              pushLog("error", "\u26D4 \u0627\u0644\u0645\u0641\u062A\u0627\u062D \u0628\u0644\u0627 \u0635\u0644\u0627\u062D\u064A\u0629 \u062A\u062F\u0627\u0648\u0644 \u2014 \u0641\u0639\u0651\u0644 \xAB\u0627\u0644\u062A\u062F\u0627\u0648\u0644\xBB \u0644\u0644\u0639\u0642\u0648\u062F \u0627\u0644\u0622\u062C\u0644\u0629 \u0641\u064A KuCoin \u062B\u0645 \u0627\u062D\u0641\u0638 \u0627\u0644\u0645\u0641\u0627\u062A\u064A\u062D \u0645\u062C\u062F\u062F\u064B\u0627");
              toast("\u26A0\uFE0F \u0641\u0639\u0651\u0644 \u0635\u0644\u0627\u062D\u064A\u0629 \u0627\u0644\u062A\u062F\u0627\u0648\u0644 \u0641\u064A \u0645\u0641\u062A\u0627\u062D KuCoin");
            } else {
              if (Date.now() - _huntErrAt > 6e4) {
                _huntErrAt = Date.now();
                pushLog("error", "\u0641\u0634\u0644 \u0635\u064A\u062F \u0627\u0644\u0633\u0648\u0642: " + m2);
              }
            }
          }
        } else done = true;
        if (done) maybeHunt(price);
      }
      ensureGrid();
      harvestRipe(price);
    }
    pruneGhosts();
    markUnrealized(price || m.price);
    if (!S._liveSyncAt || Date.now() - S._liveSyncAt > 5e3) {
      S._liveSyncAt = Date.now();
      try {
        await liveSync();
      } catch (e) {
        logSwallow("silent-catch", e);
      }
    }
    try {
      maybeReport();
    } catch (e) {
      logSwallow("silent-catch", e);
    }
    try {
      heartbeatBeat(price || m.price);
    } catch (e) {
      logSwallow("silent-catch", e);
    }
    emit();
    saveAll();
  } catch (e) {
    S.lastTickAt = Date.now();
    if (!/abort|timeout|429|50[0-4]|fetch/i.test(String(e?.message || ""))) pushLog("error", String(e?.message || e));
  }
}
function startBot() {
  if (R()) {
    remoteCmd("start").then(() => remoteSync());
    return;
  }
  if (S.status === "running") return;
  if (S.config.mode === "live" && !S.keys) {
    toast("\u0627\u0631\u0628\u0637 \u0645\u0641\u0627\u062A\u064A\u062D KuCoin \u0623\u0648\u0644\u0627\u064B \u0645\u0646 \u0627\u0644\u0625\u0639\u062F\u0627\u062F\u0627\u062A");
    return;
  }
  const p = S.lastPrice || S.markPrice;
  if (!p) {
    toast("\u062A\u0639\u0630\u0631 \u0642\u0631\u0627\u0621\u0629 \u0627\u0644\u0633\u0639\u0631 \u2014 \u062A\u062D\u0642\u0642 \u0645\u0646 \u0627\u0644\u0627\u062A\u0635\u0627\u0644 \u0648\u0627\u0644\u0632\u0648\u062C");
    return;
  }
  if (S.status === "paused" && S.position) {
    S.status = "running";
    pushLog("info", "\u0627\u0633\u062A\u0626\u0646\u0627\u0641 \u0627\u0644\u0628\u0648\u062A");
    emit();
    saveAll();
    return;
  }
  S.status = "running";
  S.startedAt = Date.now();
  S.heartbeat = 0;
  const pf = profFresh();
  startStudy(!!pf);
  if (!pf) pushLog("server", "\u062F\u0631\u0627\u0633\u0629 " + S.config.displaySymbol + " 60 \u062B\u0627\u0646\u064A\u0629 \u2014 \u0642\u064A\u0627\u0633 \u0627\u0644\u062A\u0642\u0644\u0628 \u0648\u0627\u0644\u0633\u0628\u0631\u064A\u062F \u0648\u0627\u0644\u0646\u0634\u0627\u0637 \u0642\u0628\u0644 \u0623\u064A \u062F\u062E\u0648\u0644");
  S.huntAnchor = p;
  S.gridAnchor = S.position ? S.position.entry : p;
  S._flipAt = null;
  resolveDirection(Date.now());
  if (!S.grid.filter((g) => !g.reduceOnly && (g.status === "armed" || g.status === "open")).length) {
    S.grid = [
      ...S.grid.filter((g) => g.reduceOnly && (g.status === "open" || g.status === "armed")),
      ...buildGrid(S.position ? S.position.entry : p, false, 4)
    ];
  }
  pushLog("server", "\u062F\u0648\u0631\u0629 \u062C\u062F\u064A\u062F\u0629 " + (S.config.direction === "short" ? "\u0634\u0648\u0631\u062A" : "\u0644\u0648\u0646\u063A") + (S.regime ? " \xB7 " + S.regime.label : "") + " \u2014 \u0634\u0628\u0643\u0629 \u0641\u0639\u0651\u0627\u0644\u0629 " + effLevels() + " \u0645\u0646 " + S.config.levels + " (\u062A\u0631\u0643\u064A\u0632 \u064A\u0646\u0627\u0633\u0628 \u0631\u0623\u0633 \u0627\u0644\u0645\u0627\u0644) \u0648\u0627\u0644\u0635\u064A\u062F \u064A\u0639\u0645\u0644");
  nativeNotify("AB-KING \u064A\u0639\u0645\u0644 \u2713", "\u0627\u0644\u0628\u0648\u062A \u0645\u062A\u0635\u0644 \u0628\u0640 KuCoin \u0648\u064A\u062A\u062F\u0627\u0648\u0644 " + (S.config.displaySymbol || S.config.symbol) + " \u2014 \u064A\u0633\u062A\u0645\u0631 \u062D\u062A\u0649 \u0641\u064A \u0627\u0644\u062E\u0644\u0641\u064A\u0629", true);
  toast("\u0627\u0644\u0628\u0648\u062A \u064A\u0639\u0645\u0644 \u0627\u0644\u0622\u0646");
  emit();
  saveAll();
}
function pauseBot() {
  if (R()) {
    remoteCmd("pause").then(() => remoteSync());
    return;
  }
  if (S.status !== "running") return;
  S.status = "paused";
  for (const l of S.grid) {
    if (!l.reduceOnly && (l.status === "open" || l.status === "armed")) {
      l.status = "cancelled";
      l.exchangeOrderId = null;
    } else if (l.reduceOnly && l.status === "open" && S.config.mode === "live") {
      l.status = "armed";
      l.exchangeOrderId = null;
    }
  }
  if (S.config.mode === "live" && S.keys) exCancelAll(S.config.symbol).catch(() => {
  });
  pushLog("info", "\u0625\u064A\u0642\u0627\u0641 \u0645\u0624\u0642\u062A \u2014 \u0644\u0627 \u0635\u0641\u0642\u0627\u062A \u062C\u062F\u064A\u062F\u0629\u060C \u0627\u0644\u062C\u0646\u064A \u0645\u0633\u062A\u0645\u0631");
  emit();
  saveAll();
}
async function stopBot() {
  if (R()) {
    await remoteCmd("stop");
    await remoteSync();
    return;
  }
  if (S.status === "idle") return;
  const p = S.lastPrice || S.markPrice || 0, prevSide = S.position ? S.position.side : null;
  if (S.config.mode === "live" && S.keys && prevSide) {
    try {
      await exCancelAll(S.config.symbol);
      await exCancelStops(S.config.symbol);
      S._guardId = null;
    } catch (e) {
      logSwallow("silent-catch", e);
    }
    let flat = false;
    for (let i = 0; i < 4 && !flat; i++) {
      try {
        await exClose(S.config.symbol, prevSide);
      } catch (e) {
        logSwallow("silent-catch", e);
      }
      try {
        await new Promise((r) => setTimeout(r, 700));
        const pos = await exPosition(S.config.symbol);
        flat = !pos;
      } catch (e) {
        logSwallow("silent-catch", e);
      }
    }
    S.ignoreExchangeUntil = Date.now() + 15e3;
    if (!flat) {
      pushLog("error", "\u26A0\uFE0F \u0625\u063A\u0644\u0627\u0642 \u0627\u0644\u0645\u0646\u0635\u0629 \u0644\u0645 \u064A\u062A\u0623\u0643\u062F \u0628\u0639\u062F 4 \u0645\u062D\u0627\u0648\u0644\u0627\u062A \u2014 \u0631\u0627\u062C\u0639 \u0627\u0644\u0645\u0631\u0643\u0632 \u064A\u062F\u0648\u064A\u064B\u0627 \u0641\u064A KuCoin \u0641\u0648\u0631\u064B\u0627");
      notify("\u062A\u062D\u0630\u064A\u0631: \u0625\u063A\u0644\u0627\u0642 \u0627\u0644\u0645\u0631\u0643\u0632 \u0639\u0644\u0649 \u0627\u0644\u0645\u0646\u0635\u0629 \u0644\u0645 \u064A\u062A\u0623\u0643\u062F \u2014 \u062A\u062D\u0642\u0642 \u064A\u062F\u0648\u064A\u064B\u0627 \u0641\u0648\u0631\u064B\u0627", "warn");
    }
  }
  await flattenAt(p, "\u0625\u064A\u0642\u0627\u0641");
  S.status = "stopped";
  clearNativeOngoing();
  pushLog("info", "\u0625\u064A\u0642\u0627\u0641 \u2014 \u0623\u064F\u063A\u0644\u0642\u062A \u0643\u0644 \u0627\u0644\u0635\u0641\u0642\u0627\u062A \u0639\u0646\u062F \u0627\u0644\u0633\u0639\u0631 \u0627\u0644\u062D\u0627\u0644\u064A");
  toast("\u062A\u0645 \u0625\u064A\u0642\u0627\u0641 \u0627\u0644\u0628\u0648\u062A");
  emit();
  saveAll();
}
function newCycle() {
  if (R()) {
    remoteCmd("newCycle").then(() => remoteSync());
    return;
  }
  const rolled = Math.max(0.01, Math.round(S.config.cycleBalance * 100) / 100);
  const keep = S.status === "running" || S.status === "paused";
  if (S.config.mode === "live" && S.keys) {
    exCancelAll(S.config.symbol).catch(() => {
    });
    exCancelStops(S.config.symbol).catch(() => {
    });
    S._guardId = null;
    if (S.position) exCloseQty(S.config.symbol, S.position.side, S.position.size).catch((e) => pushLog("error", "\u0625\u063A\u0644\u0627\u0642 \u0645\u0631\u0643\u0632 \u0627\u0644\u062F\u0648\u0631\u0629 \u0627\u0644\u0633\u0627\u0628\u0642\u0629 \u0641\u0634\u0644: " + (e.message || e)));
  }
  S.ignoreExchangeUntil = Date.now() + 12e3;
  const cfg = { ...S.config, cycleBalance: rolled };
  const keys = S.keys, snd = S.sound, tone = S.soundTone;
  Object.assign(S, {
    grid: [],
    position: null,
    journal: [],
    history: [],
    logs: [],
    realizedPnl: 0,
    feesPaid: 0,
    activeCycle: null,
    huntCount: 0,
    huntOpen: 0,
    priceTrail: [],
    emaFast: null,
    emaSlow: null,
    cvd: 0,
    tape: [],
    orderBook: { bids: [], asks: [] },
    status: "idle",
    startedAt: null,
    huntAnchor: null,
    gridAnchor: null,
    liqPrice: null,
    exLiqPrice: null,
    _memBlockAt: 0,
    cycleHarvested: 0,
    fundingPaid: 0,
    slipCost: 0,
    _fundAt: 0,
    _tpBanked: 0,
    _posPeakNet: null,
    _spikeDone: false,
    _posNetPrev: null,
    _spikePosId: null
  });
  S.config = cfg;
  S.keys = keys;
  S.sound = snd;
  S.soundTone = tone;
  pushLog("info", "\u062F\u0648\u0631\u0629 \u062C\u062F\u064A\u062F\u0629 \u0628\u0631\u0635\u064A\u062F $" + rolled.toFixed(2) + " \u2014 \u0645\u064F\u0633\u062D\u062A \u0627\u0644\u0633\u062C\u0644\u0627\u062A \u0648\u0628\u062F\u0623\u062A \u0635\u0641\u062D\u0629 \u0646\u0638\u064A\u0641\u0629");
  toast("\u062F\u0648\u0631\u0629 \u062C\u062F\u064A\u062F\u0629 \u0628\u0631\u0635\u064A\u062F $" + rolled.toFixed(2));
  emit();
  saveAll();
  if (keep) startBot();
}
async function saveKeys(k) {
  if (R()) {
    const r = await remoteCmd("saveKeys", { keys: k });
    await remoteSync();
    if (r && r.ok) toast("\u0623\u064F\u0631\u0633\u0644\u062A \u0627\u0644\u0645\u0641\u0627\u062A\u064A\u062D \u0644\u0644\u0633\u064A\u0631\u0641\u0631 \u2713");
    else toast("\u0627\u0644\u0633\u064A\u0631\u0641\u0631 \u0631\u0641\u0636 \u0627\u0644\u0645\u0641\u0627\u062A\u064A\u062D: " + (r && r.error || "\u062A\u062D\u0642\u0642 \u0645\u0646\u0647\u0627"));
    return !!(r && r.ok);
  }
  if (!k.apiKey || !k.apiSecret || !k.passphrase) {
    toast("\u0623\u062F\u062E\u0644 \u0627\u0644\u0645\u0641\u062A\u0627\u062D \u0648\u0627\u0644\u0633\u0631 \u0648\u0627\u0644\u0639\u0628\u0627\u0631\u0629");
    return false;
  }
  S.keys = k;
  S.linkOk = false;
  saveAll();
  emit();
  try {
    const eq = await exPing();
    S.linkOk = true;
    S.exEquity = eq;
    S.permDenied = false;
    toast("\u062A\u0645 \u0627\u0644\u0631\u0628\u0637 \u2713 \u0627\u0644\u0631\u0635\u064A\u062F $" + (eq != null ? eq.toFixed(2) : "\u2014"));
  } catch (e) {
    S.linkOk = false;
    S.exEquity = null;
    toast("\u062D\u064F\u0641\u0638\u062A \u0627\u0644\u0645\u0641\u0627\u062A\u064A\u062D \u0644\u0643\u0646 \u0641\u0634\u0644 \u0627\u0644\u0627\u062A\u0635\u0627\u0644: " + String(e?.message || e));
  }
  emit();
  return S.linkOk;
}
function clearKeys() {
  if (R()) {
    remoteCmd("clearKeys").then(() => remoteSync());
    return;
  }
  S.keys = null;
  S.linkOk = false;
  S.exEquity = null;
  saveAll();
  emit();
  toast("\u062D\u064F\u0630\u0641\u062A \u0627\u0644\u0645\u0641\u0627\u062A\u064A\u062D");
}
function saveCfg(v) {
  if (R()) {
    remoteCmd("saveCfg", { cfg: v }).then(() => remoteSync());
    return;
  }
  const old = S.config.symbol;
  const oldLv = S.config.leverage, oldN = S.config.levels, oldStep = S.config.gridStepPct, oldBal = S.config.cycleBalance;
  const sym = (v.symbol || old).trim().toUpperCase();
  S.config.symbol = sym;
  S.config.displaySymbol = sym.replace(/USDTM$/i, "").replace(/^XBT$/i, "BTC");
  S.config.leverage = clamp(Math.round(+v.leverage || 3), 1, 100);
  S.config.levels = clamp(Math.round(+v.levels || 12), 1, 64);
  S.config.gridStepPct = clamp(+v.gridStepPct || 0.3, 0.05, 5);
  S.config.huntPct = clamp(+v.huntPct || 0.15, 0.03, 5);
  if (v.tpNet != null && v.tpNet !== "") S.config.tpNet = clamp(+v.tpNet || TP_BASE_NET, 0.03, 50);
  if (v.tpUsd != null && v.tpUsd !== "") S.config.tpUsd = clamp(+v.tpUsd || 0, 0, 1e3);
  S.config.cycleBalance = Math.max(1, +v.cycleBalance || 100);
  S.config.directionMode = v.directionMode;
  S.config.mode = v.mode;
  S.sound = !!v.sound;
  if (v.soundTone) S.soundTone = v.soundTone;
  if (sym !== old) {
    if (S.config.mode === "live" && S.keys) {
      exCancelAll(old).catch(() => {
      });
      exCancelStops(old).catch(() => {
      });
      if (S.position) exCloseQty(old, S.position.side, S.position.size).catch((e) => pushLog("error", "\u0625\u063A\u0644\u0627\u0642 \u0645\u0631\u0643\u0632 \u0627\u0644\u0632\u0648\u062C \u0627\u0644\u0633\u0627\u0628\u0642 \u0641\u0634\u0644: " + (e.message || e)));
      pushLog("server", "\u0623\u064F\u0644\u063A\u064A\u062A \u0623\u0648\u0627\u0645\u0631 \u0648\u0648\u0642\u0641 " + old + " \u0648\u0623\u064F\u063A\u0644\u0642 \u0645\u0631\u0643\u0632\u0647 \u0642\u0628\u0644 \u0627\u0644\u0627\u0646\u062A\u0642\u0627\u0644");
    }
    S.grid = [];
    S.position = null;
    S._tpBanked = 0;
    S._trailFloor = 0;
    S.realizedPnl = 0;
    S.feesPaid = 0;
    S.fundingPaid = 0;
    S.slipCost = 0;
    S._fundAt = 0;
    S.ignoreExchangeUntil = Date.now() + 12e3;
    S.tape = [];
    S.priceTrail = [];
    S.emaFast = S.emaSlow = null;
    S._metaAt = 0;
    S.multiplier = 1;
    S.tickSize = 0.1;
    S._metaSym = null;
    S.maxOrderQty = 0;
    S.lastPrice = null;
    S.markPrice = null;
    S.lastTickAt = null;
    S.cvd = 0;
    S.orderBook = { bids: [], asks: [] };
    S.biasScore = 0;
    S.biasReasons = [];
    S.regime = null;
    S.confluence = null;
    S.heartbeat = 0;
    S.liqPrice = null;
    S.exLiqPrice = null;
    S.huntAnchor = null;
    S.gridAnchor = null;
    S._brk = null;
    S._ofi = 0;
    S._ofiWin = [];
    S._prevBook = null;
    S._btcImp = null;
    S._btcPrev = null;
    S._actHist = [];
    S._actMed = 0;
    S._actAt = 0;
    S._tradeTs = [];
    S._lossStreak = null;
    S._lastSig = null;
    S._vetoAt = 0;
    S._guardPx = 0;
    S._guardLogged = false;
    S._guardId = null;
    setStreamSymbol(sym);
    fetchContract(sym).then((ct) => {
      if (ct && +ct.multiplier > 0 && S.config.symbol === sym) {
        S.multiplier = +ct.multiplier;
        S.tickSize = +ct.tickSize || S.tickSize;
        S._metaSym = sym;
        S._metaAt = Date.now();
        if (+ct.maxOrderQty > 0) S.maxOrderQty = +ct.maxOrderQty;
        pushLog("server", "\u{1F4D0} \u0645\u0648\u0627\u0635\u0641\u0627\u062A " + S.config.displaySymbol + " \u0645\u0648\u062B\u0642\u0629: \u0645\u0636\u0627\u0639\u0641 \u0627\u0644\u0639\u0642\u062F " + S.multiplier + " \u2014 \u0641\u064F\u062A\u062D \u0628\u0627\u0628 \u0627\u0644\u062D\u062C\u0645");
      }
    }).catch(() => {
    });
    const __staleOpen = S.journal.filter((j) => j.status === "open");
    if (__staleOpen.length) {
      S.journal = S.journal.filter((j) => j.status !== "open");
      pushLog("server", "\u{1F9F9} \u0623\u064F\u0644\u063A\u064A\u062A " + __staleOpen.length + " \u0635\u0641\u0642\u0627\u062A \u0627\u0644\u0632\u0648\u062C \u0627\u0644\u0633\u0627\u0628\u0642 \u0627\u0644\u0645\u0641\u062A\u0648\u062D\u0629 \u2014 \u0633\u064A\u0627\u0642 \u0642\u062F\u064A\u0645 \u0644\u0627 \u064A\u064F\u0642\u0641\u0644 \u0628\u0639\u0645\u0644\u0629 \u062C\u062F\u064A\u062F\u0629");
    }
    S._dwellAt = Date.now();
    S._slideHi = null;
    S._slideHiAt = 0;
    S._slideLo = null;
    S._slideLoAt = 0;
    S._slideVetoAt = 0;
    const pf = S.memory && S.memory.profiles && S.memory.profiles[sym];
    startStudy(!!(pf && Date.now() - pf.at < 24 * 3600 * 1e3));
    pushLog("server", "\u0627\u0646\u062A\u0642\u0627\u0644 \u0625\u0644\u0649 " + S.config.displaySymbol + " \u2014 \u0623\u064F\u0644\u063A\u064A\u062A \u062D\u0627\u0644\u0629 \u0627\u0644\u0632\u0648\u062C \u0627\u0644\u0633\u0627\u0628\u0642" + (pf && Date.now() - pf.at < 24 * 3600 * 1e3 ? " \xB7 \u0628\u0635\u0645\u062A\u0647\u0627 \u0645\u0639\u0631\u0648\u0641\u0629 \u2014 \u0625\u062D\u0645\u0627\u0621 \u0642\u0635\u064A\u0631" : " \xB7 \u062A\u064F\u062F\u0631\u0633 60 \u062B\u0627\u0646\u064A\u0629 \u0642\u0628\u0644 \u0623\u064A \u062F\u062E\u0648\u0644"));
  } else if (S.status === "running" && (oldLv !== S.config.leverage || oldN !== S.config.levels || oldStep !== S.config.gridStepPct || oldBal !== S.config.cycleBalance)) {
    cancelPendingAdds();
    if (S.config.mode === "live" && S.keys) exCancelAll(sym).catch(() => {
    });
    const c = S.lastPrice || S.gridAnchor || (S.position ? S.position.entry : 0);
    if (c > 0) {
      const keep = S.grid.filter((g) => g.status === "filled" || g.status === "cancelled" || g.reduceOnly && (g.status === "open" || g.status === "armed"));
      S.grid = [...keep, ...buildGrid(c, false, 4)];
      S.gridAnchor = c;
      S.grid.sort((a, b) => b.price - a.price);
    }
    pushLog("server", "\u0623\u064F\u0639\u064A\u062F \u0628\u0646\u0627\u0621 \u0627\u0644\u0634\u0628\u0643\u0629 \u0628\u0627\u0644\u0645\u0639\u0627\u0645\u0644\u0627\u062A \u0627\u0644\u062C\u062F\u064A\u062F\u0629 \u2014 " + effLevels() + " \u0645\u0633\u062A\u0648\u0649 \u0641\u0639\u0651\u0627\u0644");
  }
  saveAll();
  emit();
  toast("\u062A\u0645 \u062D\u0641\u0638 \u0627\u0644\u0645\u0639\u0627\u0645\u0644\u0627\u062A");
}
function equity() {
  return S.config.cycleBalance + (S.position ? S.position.unrealized : 0);
}
function desk() {
  const j = S.journal;
  const closed = j.filter((r) => r.status !== "open"), open = j.filter((r) => r.status === "open");
  return {
    win: closed.filter((r) => r.pnl > 0),
    lose: closed.filter((r) => r.pnl < 0),
    open,
    winPnl: closed.filter((r) => r.pnl > 0).reduce((a, r) => a + r.pnl, 0),
    losePnl: closed.filter((r) => r.pnl < 0).reduce((a, r) => a + r.pnl, 0),
    fees: S.feesPaid
  };
}
function pulseAge() {
  if (!S.lastTickAt) return "\u0644\u0627 \u0646\u0628\u0636";
  const s = Math.max(0, Math.round((Date.now() - S.lastTickAt) / 1e3));
  return s < 5 ? "\u0627\u0644\u0622\u0646" : s < 60 ? s + " \u062B" : "\u0642\u0628\u0644 " + Math.floor(s / 60) + " \u062F";
}
function initEngine() {
  if (booted) return;
  booted = true;
  if (typeof window !== "undefined") window.__S = S;
  try {
    const LN = lnPlugin();
    if (LN) {
      ensureChannels(LN);
      LN.requestPermissions().catch(() => {
      });
    }
  } catch (e) {
    logSwallow("silent-catch", e);
  }
  if (R()) {
    pushLog("info", "\u0648\u0636\u0639 \u0633\u064A\u0631\u0641\u0631 \u0627\u0644\u062C\u0648\u0627\u0644 \u2014 \u0627\u0644\u0645\u062D\u0631\u0643 \u064A\u0639\u0645\u0644 \u0641\u064A Termux \u0628\u0644\u0627 \u0646\u0648\u0645");
    remoteSync();
    setInterval(remoteSync, 1200);
    setInterval(emit, 500);
    return;
  }
  const _detect = setInterval(async () => {
    if (R()) {
      clearInterval(_detect);
      return;
    }
    const found = await detectRemoteServer().catch(() => false);
    if (found) {
      clearInterval(_detect);
      pushLog("info", "\u062A\u0645 \u0627\u0643\u062A\u0634\u0627\u0641 \u0633\u064A\u0631\u0641\u0631 \u0627\u0644\u062C\u0648\u0627\u0644 \u2014 \u0627\u0644\u062A\u062D\u0648\u0644 \u0644\u0648\u0636\u0639 \u0627\u0644\u0645\u0631\u0622\u0629\u060C \u0627\u0644\u0645\u062D\u0631\u0643 \u0627\u0644\u0645\u062D\u0644\u064A \u062A\u0648\u0642\u0641");
      try {
        S.status = "idle";
        if (S.config.mode !== "live") {
          S.grid = [];
          S.position = null;
        }
        saveAll();
      } catch (e) {
        logSwallow("local-stop", e);
      }
      remoteSync();
      setInterval(remoteSync, typeof window !== "undefined" && window.__FLEET_POLL_SLOW ? 2500 : 1200);
    }
  }, 5e3);
  loadAll();
  if (S.config.symbol === "BTCUSDTM") S.config.symbol = "XBTUSDTM";
  S.config.displaySymbol = S.config.symbol.replace(/USDTM$/i, "").replace(/^XBT$/i, "BTC");
  pushLog("info", "AB-KING \u062C\u0627\u0647\u0632 \u2014 " + (S.config.mode === "live" ? "\u0648\u0636\u0639 LIVE" : "\u0648\u0636\u0639 \u0648\u0631\u0642\u064A"));
  startStream(S.config.symbol, { onMessage: onStreamTick, onStatus: () => emitThrottled() });
  setInterval(botTick, TICK_MS);
  setInterval(emit, 1e3);
  setInterval(() => {
    if (S.status === "idle") botTick().catch(() => {
    });
  }, 6e3);
  const refreshBal = () => {
    if (!S.keys) return;
    exPing().then((eq) => {
      if (eq != null) {
        S.exEquity = eq;
        S.linkOk = true;
      }
    }).catch(() => {
      S.linkOk = false;
    });
  };
  refreshBal();
  setInterval(refreshBal, 2e4);
  setTimeout(() => {
    scanRadar().catch(() => {
    });
  }, 15e3);
  setInterval(() => {
    scanRadar().catch(() => {
    });
  }, 45e3);
  startSilentKeepAlive();
  startForegroundSvc();
  startNativeKeepAlive();
  logRevival();
  if (typeof document !== "undefined") document.addEventListener("visibilitychange", () => {
    if (!document.hidden) {
      botTick().catch(() => {
      });
      if (S.lastPrice) try {
        harvestRipe(S.lastPrice);
      } catch (e) {
        logSwallow("silent-catch", e);
      }
    }
  });
  emit();
}
var TICK_MS, MAX_HUNT_OPEN, HUNT_COOLDOWN, ADD_COOLDOWN, SCOUT_MS, MIN_HUNT_GAP, TP_BASE_NET, LOSS_STOP_USD, clamp, uid, fmtPx, fmtUsd, fmtTime, nextEma, volSum, _channelsReady, _kaCtx, _fsTimer, listeners, emitTimer, R, _remoteFails, _remoteLogSeen, _relayText, _relayAt, S, _netFailStreak, _netDownAt, _wasDownStart, _swallowAt, DIRECT_BASE, IS_SERVER, IS_NATIVE, API_BASE, fetchTicker, fetchContract, _bookFailsAt, _tapeFailsAt, fetchBook, fetchTape, contractsCache, _bgPipeAt, FLEET_MODE, _bgScanAt, _lastSwitchAt, GAP_MODE_ON, _gapScanAt, _gapStats, _btcLead, exOrders, exFills, exStops, mmTried, mmCache, exCancelAll, exCancelOne, exCancelStops, _ordErrAt, _syncErrAt, _syncErrMsg, _huntErrAt, booted;
var init_engine = __esm({
  "src/engine.ts"() {
    init_ws();
    TICK_MS = 1e3;
    MAX_HUNT_OPEN = 3;
    HUNT_COOLDOWN = 1e4;
    ADD_COOLDOWN = 15e3;
    SCOUT_MS = 9e4;
    MIN_HUNT_GAP = 0.08;
    TP_BASE_NET = 0.12;
    LOSS_STOP_USD = 16;
    clamp = (v, a, b) => Math.min(b, Math.max(a, v));
    uid = (p) => p + "_" + Math.random().toString(36).slice(2, 10) + Date.now().toString(36).slice(-4);
    fmtPx = (n) => {
      n = Number(n);
      if (!n || !isFinite(n) || n <= 0) return "\u2014";
      if (n >= 1e3) return n.toLocaleString("en-US", { maximumFractionDigits: 2 });
      if (n >= 1) return n.toFixed(2);
      const d = Math.min(12, Math.max(4, -Math.floor(Math.log10(n)) + 4));
      return n.toFixed(d);
    };
    fmtUsd = (n, d = 2) => {
      const s = n < 0 ? "-" : n > 0 ? "+" : "";
      return s + Math.abs(n).toFixed(d);
    };
    fmtTime = (t) => new Date(t).toLocaleTimeString("en-GB", { hour12: false });
    nextEma = (p, x, k) => p == null ? x : x * k + p * (1 - k);
    volSum = (l) => (l || []).reduce((a, b) => a + (b.size || 0), 0);
    _channelsReady = false;
    _kaCtx = null;
    _fsTimer = null;
    listeners = /* @__PURE__ */ new Set();
    emitTimer = null;
    R = () => typeof window !== "undefined" && window.__ABKING_REMOTE ? String(window.__ABKING_REMOTE) : null;
    _remoteFails = 0;
    _remoteLogSeen = null;
    _relayText = "";
    _relayAt = 0;
    S = {
      keys: null,
      config: {
        symbol: "XBTUSDTM",
        displaySymbol: "BTC",
        leverage: 3,
        gridStepPct: 0.3,
        huntPct: 0.15,
        levels: 12,
        direction: "short",
        directionMode: "auto",
        cycleBalance: 100,
        mode: "paper",
        tpNet: 0.12,
        tpUsd: 1,
        patienceHrs: 24,
        patienceUsd: 0,
        gapMode: false
      },
      status: "idle",
      heartbeat: 0,
      lastPrice: null,
      markPrice: null,
      multiplier: 1,
      tickSize: 0.1,
      makerFee: 2e-4,
      takerFee: 6e-4,
      funding: 0,
      _metaSym: null,
      maxOrderQty: 0,
      exAvail: null,
      exLeverage: null,
      // _metaSym: الرمز الذي وُثّقت مواصفات عقده — لا أمر حقيقي بدونها
      grid: [],
      position: null,
      journal: [],
      logs: [],
      history: [],
      linkOk: false,
      intel: null,
      exEquity: null,
      realizedPnl: 0,
      feesPaid: 0,
      fundingPaid: 0,
      cycleHarvested: 0,
      slipCost: 0,
      priceTrail: [],
      emaFast: null,
      emaSlow: null,
      cvd: 0,
      tape: [],
      biasScore: 0,
      biasReasons: [],
      regime: null,
      confluence: null,
      huntAnchor: null,
      gridAnchor: null,
      lastHuntAt: 0,
      lastAddAt: 0,
      lastWorkAt: 0,
      huntCount: 0,
      liqPrice: null,
      exLiqPrice: null,
      mmr: 4e-3,
      orderBook: { bids: [], asks: [] },
      startedAt: null,
      activeCycle: null,
      studyUntil: 0,
      studyAcc: null,
      // دراسة العملة الجديدة قبل أي دخول
      ignoreExchangeUntil: 0,
      lastTickAt: null,
      sound: true,
      soundTone: "soft",
      permDenied: false,
      toastMsg: null,
      _metaAt: 0,
      _lastTrailAt: 0,
      memory: { pairs: {}, cycles: 0, totalPnl: 0 }
      // الذاكرة القوية — تبقى عبر الدورات ولا تُمسح أبدًا
    };
    _netFailStreak = 0;
    _netDownAt = 0;
    _wasDownStart = 0;
    _swallowAt = {};
    DIRECT_BASE = "https://api-futures.kucoin.com";
    IS_SERVER = typeof process !== "undefined" && !!(process && process.env && process.env.ABKING_SERVER);
    IS_NATIVE = IS_SERVER || typeof window !== "undefined" && (window.Capacitor || location.protocol === "capacitor:");
    API_BASE = IS_NATIVE ? DIRECT_BASE : "/kucoin";
    fetchTicker = (s) => kcPublic("/api/v1/ticker?symbol=" + encodeURIComponent(s), 2500);
    fetchContract = (s) => kcPublic("/api/v1/contracts/" + encodeURIComponent(s), 3500);
    _bookFailsAt = 0;
    _tapeFailsAt = 0;
    fetchBook = (s) => kcPublic("/api/v1/level2/depth20?symbol=" + encodeURIComponent(s), 2500).then((d) => {
      _bookFailsAt = 0;
      return {
        bids: (d.bids || []).map((r) => ({ price: +r[0], size: +r[1] })).filter((r) => r.price > 0),
        asks: (d.asks || []).map((r) => ({ price: +r[0], size: +r[1] })).filter((r) => r.price > 0)
      };
    }).catch((e) => {
      _bookFailsAt = Date.now();
      throw e;
    });
    fetchTape = (s) => kcPublic("/api/v1/trade/history?symbol=" + encodeURIComponent(s), 2500).then((a) => {
      _tapeFailsAt = 0;
      return (a || []).slice(0, 32).map((t) => ({ side: String(t.side || ""), size: +t.size || 0, price: +t.price || 0 }));
    }).catch((e) => {
      _tapeFailsAt = Date.now();
      throw e;
    });
    contractsCache = null;
    S._nextSymbol = null;
    S._nextReady = false;
    _bgPipeAt = 0;
    FLEET_MODE = () => !!(typeof process !== "undefined" && process.env && process.env.ABKING_SLOT);
    _bgScanAt = 0;
    _lastSwitchAt = 0;
    GAP_MODE_ON = () => !!S.config.gapMode;
    _gapScanAt = 0;
    _gapStats = null;
    _btcLead = null;
    exOrders = (symbol2) => kcPrivate("GET", "/api/v1/orders?status=active&symbol=" + encodeURIComponent(symbol2)).then((d) => {
      const a = Array.isArray(d) ? d : d.items || [];
      return a.map((o) => ({
        orderId: o.id,
        clientOid: o.clientOid || null,
        side: o.side === "sell" ? "sell" : "buy",
        price: +o.price,
        size: +o.size,
        filledSize: +(o.filledSize ?? o.dealSize ?? 0) || 0,
        reduceOnly: !!o.reduceOnly
      }));
    }).catch(() => null);
    exFills = (symbol2) => kcPrivate("GET", "/api/v1/fills?symbol=" + encodeURIComponent(symbol2) + "&pageSize=100").then((d) => {
      const a = Array.isArray(d) ? d : d.items || [];
      return a.map((f) => ({
        orderId: f.orderId || "",
        clientOid: f.clientOid || null,
        side: f.side === "sell" ? "sell" : "buy",
        price: +f.price || 0,
        size: +f.size || 0,
        fee: Math.abs(+f.fee || 0),
        feeCurrency: f.feeCurrency || "",
        at: +f.createdAt || 0,
        ts: f.tradeId || ""
      }));
    }).catch(() => null);
    exStops = (symbol2) => kcPrivate("GET", "/api/v1/stopOrders?symbol=" + encodeURIComponent(symbol2)).then((d) => {
      const a = Array.isArray(d) ? d : d.items || [];
      return a.map((o) => ({ id: o.id || o.orderId || "", stopPrice: +(o.stopPrice ?? o.price ?? 0) || 0 }));
    }).catch(() => null);
    mmTried = {};
    mmCache = {};
    exCancelAll = (symbol2) => kcPrivate("DELETE", "/api/v1/orders?symbol=" + encodeURIComponent(symbol2)).catch(() => {
    });
    exCancelOne = (id) => kcPrivate("DELETE", "/api/v1/orders/" + id).catch(() => {
    });
    exCancelStops = (symbol2) => kcPrivate("DELETE", "/api/v1/stopOrders?symbol=" + encodeURIComponent(symbol2)).catch(() => {
    });
    _ordErrAt = 0;
    _syncErrAt = 0;
    _syncErrMsg = "";
    _huntErrAt = 0;
    booted = false;
  }
});

// src/server-shims.ts
import { readFileSync, writeFileSync, existsSync, mkdirSync } from "node:fs";
import { homedir } from "node:os";
import { join } from "node:path";
process.env.ABKING_SERVER = "1";
var dir = join(homedir(), ".ab-king");
try {
  mkdirSync(dir, { recursive: true });
} catch {
}
var __slot = process.env.ABKING_SLOT || "";
var file = join(dir, __slot ? `trq-slot-${__slot}-store.json` : "ab-king-server-store.json");
var data = {};
try {
  if (existsSync(file)) data = JSON.parse(readFileSync(file, "utf8") || "{}");
} catch (e) {
  data = {};
}
var timer = null;
function persist() {
  if (timer) return;
  timer = setTimeout(() => {
    timer = null;
    try {
      writeFileSync(file, JSON.stringify(data));
    } catch {
    }
  }, 300);
}
globalThis.localStorage = {
  getItem: (k) => k in data ? data[k] : null,
  setItem: (k, v) => {
    data[k] = String(v);
    persist();
  },
  removeItem: (k) => {
    delete data[k];
    persist();
  },
  clear: () => {
    data = {};
    persist();
  }
};
console.log("[AB-KING] \u0645\u062E\u0632\u0646 \u0627\u0644\u062D\u0627\u0644\u0629: " + file);

// src/server-entry.ts
init_engine();
import { createServer } from "node:http";
import { spawn } from "node:child_process";
var SRV_VER = "TRQ-TRADING-1.7.0";
var FLEET_N = 5;
var BASE_PORT = 8987;
var SLOT = parseInt(String(process.env.ABKING_SLOT || ""), 10) || 0;
function send(res, code, obj) {
  res.writeHead(code, {
    "Content-Type": "application/json; charset=utf-8",
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Methods": "GET,POST,OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type",
    "Cache-Control": "no-store"
  });
  res.end(JSON.stringify(obj));
}
async function jfetch(url, ms = 2500, payload) {
  const c = new AbortController();
  const t = setTimeout(() => c.abort(), ms);
  try {
    const r = await fetch(url, payload ? { method: "POST", signal: c.signal, headers: { "Content-Type": "application/json" }, body: JSON.stringify(payload) } : { signal: c.signal });
    clearTimeout(t);
    return await r.json();
  } catch {
    clearTimeout(t);
    return null;
  }
}
var PORT = BASE_PORT + SLOT;
if (SLOT > 0) {
  pushLog("server", "\u2705 " + SRV_VER + " \u2014 \u0645\u062D\u0631\u0643 \u0627\u0644\u0623\u0633\u0637\u0648\u0644 \u0631\u0642\u0645 " + SLOT + "/" + FLEET_N + " \u2014 \u0646\u0635\u064A\u0628\u0647 \u0645\u0646 \u0631\u0623\u0633 \u0627\u0644\u0645\u0627\u0644: \xF7" + FLEET_N + " \u062A\u0644\u0642\u0627\u0626\u064A\u0627\u064B \u2014 \u062A\u0628\u062F\u064A\u0644\u0647 \u0648\u062C\u0646\u064A\u0647 \u0645\u0633\u062A\u0642\u0644\u0627\u0646");
  let bootPickDone = false;
  const fleetSync = () => jfetch("http://127.0.0.1:" + BASE_PORT + "/fleet?i=" + SLOT).then((f) => {
    if (f && Array.isArray(f.taken)) {
      S._fleetTaken = f.taken.map((t) => t.sym);
      if (!bootPickDone && !S.position && S.config.symbol === "XBTUSDTM" && S.radar && Array.isArray(S.radar.list) && S.radar.list.length) {
        const pick = S.radar.list[Math.min(SLOT - 1, S.radar.list.length - 1)];
        if (pick && pick.symbol && pick.symbol !== "XBTUSDTM") {
          bootPickDone = true;
          pushLog("server", "\u{1F3AF} \u062A\u0639\u064A\u064A\u0646 \u0627\u0644\u0625\u0642\u0644\u0627\u0639: \u0627\u0644\u0645\u062D\u0631\u0643 " + SLOT + " \u064A\u0628\u062F\u0623 \u0639\u0644\u0649 " + pick.disp + " (\u0627\u0644\u0645\u0631\u062A\u0628\u0629 " + SLOT + " \u0641\u064A \u0627\u0644\u0631\u0627\u062F\u0627\u0631)");
          try {
            (init_engine(), __toCommonJS(engine_exports)).saveCfg({
              symbol: pick.symbol,
              leverage: S.config.leverage,
              levels: S.config.levels,
              gridStepPct: S.config.gridStepPct,
              huntPct: S.config.huntPct,
              cycleBalance: S.config.cycleBalance,
              directionMode: S.config.directionMode,
              mode: S.config.mode
            });
          } catch {
          }
          return;
        }
      }
      const clash = f.taken.find((t) => t.sym === S.config.symbol);
      if (clash && clash.holder < SLOT && !S.position) {
        S._harvestAt = Date.now();
        S._reselectDone = false;
        pushLog("server", "\u{1F6A6} \u062A\u0646\u0648\u064A\u0639 \u0627\u0644\u0623\u0633\u0637\u0648\u0644: \u0639\u0645\u0644\u062A\u064A \u064A\u0633\u0643\u0646\u0647\u0627 \u0627\u0644\u0645\u062D\u0631\u0643 " + clash.holder + " \u2014 \u0627\u0646\u062A\u0642\u0627\u0644\u064A \u0644\u0623\u0641\u0636\u0644 \u0628\u062F\u064A\u0644 \u0645\u062A\u0627\u062D");
      }
    }
  }).catch(() => {
  });
  setTimeout(fleetSync, 2500);
  setInterval(fleetSync, 1e4).unref?.();
  setTimeout(() => {
    liveSync().then(() => pushLog("server", "\u{1F527} \u0627\u0643\u062A\u0645\u0644\u062A \u0645\u0631\u0627\u062C\u0639\u0629 \u0627\u0644\u0648\u0636\u0639 \u0639\u0646\u062F \u0625\u0642\u0644\u0627\u0639 \u0627\u0644\u0645\u062D\u0631\u0643 " + SLOT + " \u2014 \u0645\u062A\u0637\u0627\u0628\u0642 \u0645\u0639 \u0627\u0644\u0645\u0646\u0635\u0629")).catch(() => {
    });
    scanRadar().catch(() => {
    });
  }, 1500 + SLOT * 300);
}
var slotStates = [];
async function pollSlots() {
  for (let i = 1; i <= FLEET_N; i++) {
    const j = await jfetch("http://127.0.0.1:" + (BASE_PORT + i) + "/state", 2e3);
    if (j && j.ok && j.state) slotStates[i] = j.state;
  }
}
setInterval(pollSlots, 1e4).unref?.();
function mergedState(selIn = 0) {
  const live = [];
  for (let i = 1; i <= FLEET_N; i++) if (slotStates[i]) live.push(slotStates[i]);
  if (!live.length) return null;
  const idx = selIn >= 1 && selIn <= FLEET_N && slotStates[selIn] ? selIn : 0;
  const primary = idx ? slotStates[idx] : live.find((x) => x.position) || live[0];
  const st = { ...primary };
  st.journal = live.flatMap((x, li) => (x.journal || []).map((r) => ({ ...r, _slot: li + 1 }))).sort((a, b) => (b.at || 0) - (a.at || 0)).slice(0, 150);
  st.logs = (primary.logs || []).slice(-150);
  st.realizedPnl = live.reduce((a, x) => a + (x.realizedPnl || 0), 0);
  st.feesPaid = live.reduce((a, x) => a + (x.feesPaid || 0), 0);
  st.cycleHarvested = live.reduce((a, x) => a + (x.cycleHarvested || 0), 0);
  st.slipCost = live.reduce((a, x) => a + (x.slipCost || 0), 0);
  st.fundingPaid = live.reduce((a, x) => a + (x.fundingPaid || 0), 0);
  st.config = { ...primary.config || {}, cycleBalance: live.reduce((a, x) => a + (x.config?.cycleBalance || 0), 0) };
  st.status = live.some((x) => x.status === "running") ? "running" : live[0]?.status || "idle";
  st.fleet = live.map((x, idx2) => ({
    slot: idx2 + 1,
    symbol: x.config?.symbol,
    disp: x.config?.displaySymbol,
    position: !!x.position,
    net: x.position ? x.position.unrealized : 0,
    balance: x.config?.cycleBalance,
    running: x.status === "running"
  }));
  return st;
}
var srv = createServer(async (req, res) => {
  try {
    if (req.method === "OPTIONS") {
      res.writeHead(204, {
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "GET,POST,OPTIONS",
        "Access-Control-Allow-Headers": "Content-Type"
      });
      return res.end();
    }
    const u = new URL(req.url || "/", "http://127.0.0.1");
    if (req.method === "GET" && (u.pathname === "/" || u.pathname === "/health"))
      return send(res, 200, { ok: true, name: "TRQ-TRADING Fleet Server", version: SRV_VER, fleet: FLEET_N, at: Date.now() });
    if (SLOT > 0) {
      if (req.method === "GET" && u.pathname === "/state") {
        const lp = S.lastPrice || 0, mult = S.multiplier || 1, taker = S.takerFee || 6e-4;
        const st = {
          ...S,
          journal: (S.journal || []).slice(-30).map((r) => {
            if (r.status !== "open") return r;
            const feeEst = lp ? r.qty * mult * lp * taker : 0;
            const f = lp ? (r.side === "short" ? 1 : -1) * (r.entry - lp) * mult * r.qty - (r.fees || 0) - feeEst : 0;
            return { ...r, floatNet: Math.round((f + Number.EPSILON) * 100) / 100 };
          }),
          logs: (S.logs || []).slice(-80)
        };
        return send(res, 200, { ok: true, at: Date.now(), state: st });
      }
      if (req.method === "POST" && u.pathname === "/cmd") {
        let body = "";
        for await (const chunk of req) body += chunk;
        let j = {};
        try {
          j = JSON.parse(body || "{}");
        } catch {
        }
        const a = String(j.action || "");
        switch (a) {
          case "start":
            startBot();
            return send(res, 200, { ok: true });
          case "pause":
            pauseBot();
            return send(res, 200, { ok: true });
          case "stop":
            await stopBot();
            return send(res, 200, { ok: true });
          case "newCycle":
            newCycle();
            return send(res, 200, { ok: true });
          case "saveCfg":
            saveCfg(j.cfg || {});
            return send(res, 200, { ok: true });
          case "saveKeys": {
            const ok = await saveKeys(j.keys || {});
            return send(res, 200, { ok: !!ok, error: ok ? null : "\u0641\u0634\u0644 \u0627\u0644\u0627\u062A\u0635\u0627\u0644 \u0628\u0627\u0644\u0645\u0646\u0635\u0629 \u2014 \u062A\u062D\u0642\u0642 \u0645\u0646 \u0627\u0644\u0645\u0641\u0627\u062A\u064A\u062D" });
          }
          case "clearKeys":
            clearKeys();
            return send(res, 200, { ok: true });
          case "scanRadar":
            await scanRadar();
            return send(res, 200, { ok: true, radar: S.radar || null });
          default:
            return send(res, 400, { ok: false, error: "\u0625\u062C\u0631\u0627\u0621 \u063A\u064A\u0631 \u0645\u0639\u0631\u0648\u0641: " + a });
        }
      }
      send(res, 404, { ok: false, error: "not found" });
      return;
    }
    if (req.method === "GET" && u.pathname === "/fleet") {
      const i = parseInt(u.searchParams.get("i") || "0", 10) || 0;
      const taken = [];
      for (let k = 1; k <= FLEET_N; k++) {
        if (k === i) continue;
        const st = slotStates[k];
        if (st && st.config && st.config.symbol) taken.push({ sym: st.config.symbol, holder: k });
      }
      return send(res, 200, { ok: true, taken });
    }
    if (req.method === "GET" && u.pathname === "/state") {
      const sel = parseInt(u.searchParams.get("sel") || "0", 10) || 0;
      const st = mergedState(sel);
      if (!st) return send(res, 200, { ok: true, at: Date.now(), state: { status: "idle", heartbeat: Date.now(), logs: [], journal: [], grid: [], config: { cycleBalance: 0 }, fleet: [] } });
      return send(res, 200, { ok: true, at: Date.now(), state: st });
    }
    if (req.method === "POST" && u.pathname === "/cmd") {
      let body = "";
      for await (const chunk of req) body += chunk;
      let j = {};
      try {
        j = JSON.parse(body || "{}");
      } catch {
      }
      const a = String(j.action || "");
      let payload = j;
      if (a === "saveCfg" && j.cfg) {
        const { symbol: symbol2, displaySymbol, ...rest } = j.cfg;
        if (rest.cycleBalance != null) rest.cycleBalance = Math.max(1, Math.round(+rest.cycleBalance / FLEET_N * 100) / 100);
        payload = { ...j, cfg: rest };
      }
      const results = await Promise.all(
        Array.from({ length: FLEET_N }, (_, i) => jfetch("http://127.0.0.1:" + (BASE_PORT + 1 + i) + "/cmd", 5e3, payload))
      );
      const ok = results.some((r) => r && r.ok);
      if (a === "scanRadar") {
        const st = mergedState();
        return send(res, 200, { ok: true, radar: st ? st.radar || null : null });
      }
      return send(res, 200, { ok, fleet: results.map((r) => !!(r && r.ok)) });
    }
    send(res, 404, { ok: false, error: "not found" });
  } catch (e) {
    send(res, 500, { ok: false, error: String(e && e.message || e) });
  }
});
srv.listen(PORT, "127.0.0.1", () => {
  if (SLOT > 0) {
    console.log("[TRQ] \u0645\u062D\u0631\u0643 \u0627\u0644\u0623\u0633\u0637\u0648\u0644 " + SLOT + " \u064A\u0639\u0645\u0644 \u0639\u0644\u0649 http://127.0.0.1:" + PORT);
  } else {
    console.log("[TRQ] \u0645\u0646\u0633\u0642 \u0623\u0633\u0637\u0648\u0644 TRQ-TRADING \u064A\u0639\u0645\u0644 \u0639\u0644\u0649 http://127.0.0.1:" + PORT + " \u2014 \u064A\u064F\u0637\u0644\u0642 " + FLEET_N + " \u0645\u062D\u0631\u0643\u0627\u062A");
    pushLog("server", "\u{1F680} \u0623\u0633\u0637\u0648\u0644 TRQ-TRADING \u0627\u0646\u0637\u0644\u0642 \u2014 " + FLEET_N + " \u0645\u062D\u0631\u0643\u0627\u062A \u0645\u0633\u062A\u0642\u0644\u0629\u060C \u0631\u0623\u0633 \u0627\u0644\u0645\u0627\u0644 \xF7 " + FLEET_N + " \u062A\u0644\u0642\u0627\u0626\u064A\u0627\u064B\u060C \u0643\u0644 \u0639\u0645\u0644\u0629 \u0628\u0645\u062D\u0631\u0643 \u0648\u062C\u0646\u064A \u0645\u0633\u062A\u0642\u0644\u064A\u0646");
    const self = process.argv[1];
    for (let i = 1; i <= FLEET_N; i++) {
      const child = spawn(process.execPath, [self], {
        env: { ...process.env, ABKING_SLOT: String(i) },
        stdio: ["ignore", "inherit", "inherit"],
        detached: false
      });
      child.on("exit", (code) => {
        console.log("[TRQ] \u0645\u062D\u0631\u0643 " + i + " \u062E\u0631\u062C (" + code + ") \u2014 \u0625\u0639\u0627\u062F\u0629 \u062A\u0634\u063A\u064A\u0644\u0647 \u062E\u0644\u0627\u0644 5 \u062B");
        setTimeout(() => {
          try {
            spawn(process.execPath, [self], { env: { ...process.env, ABKING_SLOT: String(i) }, stdio: ["ignore", "inherit", "inherit"], detached: false });
          } catch {
          }
        }, 5e3);
      });
    }
    setTimeout(pollSlots, 4e3);
    setInterval(() => {
      try {
        const st = mergedState();
        const fl = st ? st.fleet || [] : [];
        pushLog("server", "\u{1F4A1} \u0646\u0628\u0636\u0629 \u0627\u0644\u0623\u0633\u0637\u0648\u0644 " + SRV_VER + " \u2014 " + fl.length + "/" + FLEET_N + " \u0645\u062D\u0631\u0643\u0627\u062A \u062D\u064A\u0629: " + (fl.map((f) => (f.disp || "?") + (f.position ? " (\u0645\u0631\u0643\u0632 " + (f.net >= 0 ? "+" : "") + f.net.toFixed(2) + ")" : "")).join(" \xB7 ") || "\u0628\u0627\u0646\u062A\u0638\u0627\u0631 \u0627\u0644\u0625\u0642\u0644\u0627\u0639"));
      } catch {
      }
    }, 30 * 60 * 1e3).unref?.();
  }
});
initEngine();
if (SLOT === 0) console.log("[TRQ] \u0627\u0644\u0645\u0646\u0633\u0642 \u062C\u0627\u0647\u0632 \u2014 \u0623\u0628\u0642\u0650 Termux \u062D\u064A\u064B\u0627 (\u0627\u0644\u062D\u0627\u0631\u0633 \u0627\u0644\u0630\u0627\u062A\u064A \u064A\u062D\u0645\u064A \u0627\u0644\u0623\u0633\u0637\u0648\u0644 \u0643\u0644\u0647)");
