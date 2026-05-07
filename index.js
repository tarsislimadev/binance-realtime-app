const http = require('http');
const path = require('path');
const express = require('express');
const { Server } = require('socket.io');
const Redis = require('ioredis');
const WebSocket = require('ws');

const PORT = Number(process.env.PORT || 3000);
const REDIS_URL = process.env.REDIS_URL || '';
const BINANCE_PREFIX = process.env.BINANCE_PREFIX || 'wss://stream.binance.com:9443';
const PUBLIC_DIR = path.join(__dirname, 'public');
const MAX_HISTORY = 200;

const defaults = {
  urlPrefix: BINANCE_PREFIX,
  endpoint: 'kline',
  symbol: 'BTCUSDT',
  interval: '1m',
};

function nowIso() {
  return new Date().toISOString();
}

function createHistoryStore(limit = MAX_HISTORY) {
  const entries = [];

  return {
    add(entry) {
      entries.push(entry);
      if (entries.length > limit) {
        entries.splice(0, entries.length - limit);
      }
    },
    snapshot() {
      return entries.slice();
    },
  };
}

function sanitizeText(value, fallback = '') {
  if (typeof value === 'string') {
    return value.trim() || fallback;
  }

  if (value === null || value === undefined) {
    return fallback;
  }

  return String(value).trim() || fallback;
}

function normalizeConfig(input = {}) {
  const urlPrefix = sanitizeText(input.urlPrefix, defaults.urlPrefix);
  const endpoint = sanitizeText(input.endpoint, defaults.endpoint);
  const symbol = sanitizeText(input.symbol, defaults.symbol).toUpperCase();
  const interval = sanitizeText(input.interval, defaults.interval);

  return { urlPrefix, endpoint, symbol, interval };
}

function buildStreamName(config) {
  const symbol = config.symbol.toLowerCase();

  switch (config.endpoint) {
    case 'trade':
      return `${symbol}@trade`;
    case 'miniTicker':
      return `${symbol}@miniTicker`;
    case 'ticker':
      return `${symbol}@ticker`;
    case 'depth':
      return `${symbol}@depth@100ms`;
    case 'kline':
    default:
      return `${symbol}@kline_${config.interval}`;
  }
}

function buildStreamUrl(config) {
  const prefix = config.urlPrefix.replace(/\/$/, '');
  return `${prefix}/ws/${buildStreamName(config)}`;
}

function normalizeBinanceKline(raw) {
  const kline = raw && raw.k ? raw.k : {};

  return {
    eventType: sanitizeText(raw?.e, 'unknown'),
    eventTime: Number(raw?.E || Date.now()),
    symbol: sanitizeText(raw?.s || kline.s, 'UNKNOWN'),
    interval: sanitizeText(kline.i || raw?.i, '1m'),
    openTime: Number(kline.t || raw?.t || Date.now()),
    closeTime: Number(kline.T || raw?.T || Date.now()),
    open: sanitizeText(kline.o, '0'),
    close: sanitizeText(kline.c, '0'),
    high: sanitizeText(kline.h, '0'),
    low: sanitizeText(kline.l, '0'),
    volume: sanitizeText(kline.v, '0'),
    trades: Number(kline.n || 0),
    isFinal: Boolean(kline.x),
    quoteVolume: sanitizeText(kline.q, '0'),
    takerBuyBase: sanitizeText(kline.V, '0'),
    takerBuyQuote: sanitizeText(kline.Q, '0'),
    raw,
  };
}

function buildRedisEntries(kline) {
  const baseKey = `binance.klines.${kline.symbol}.${kline.interval}.${kline.openTime}`;

  return [
    [`${baseKey}.eventType.string`, kline.eventType],
    [`${baseKey}.eventTime.number`, String(kline.eventTime)],
    [`${baseKey}.symbol.string`, kline.symbol],
    [`${baseKey}.interval.string`, kline.interval],
    [`${baseKey}.openTime.number`, String(kline.openTime)],
    [`${baseKey}.closeTime.number`, String(kline.closeTime)],
    [`${baseKey}.open.number`, kline.open],
    [`${baseKey}.close.number`, kline.close],
    [`${baseKey}.high.number`, kline.high],
    [`${baseKey}.low.number`, kline.low],
    [`${baseKey}.volume.number`, kline.volume],
    [`${baseKey}.trades.number`, String(kline.trades)],
    [`${baseKey}.isFinal.bool`, String(kline.isFinal)],
    [`${baseKey}.quoteVolume.number`, kline.quoteVolume],
    [`${baseKey}.takerBuyBase.number`, kline.takerBuyBase],
    [`${baseKey}.takerBuyQuote.number`, kline.takerBuyQuote],
    [`${baseKey}.payload.string`, JSON.stringify(kline.raw)],
  ];
}

function createMemoryStore() {
  const values = new Map();

  return {
    kind: 'memory',
    async setEntries(entries) {
      for (const [key, value] of entries) {
        values.set(key, value);
      }
    },
    async ping() {
      return 'PONG';
    },
  };
}

async function createStore() {
  if (!REDIS_URL) {
    return createMemoryStore();
  }

  const client = new Redis(REDIS_URL, {
    lazyConnect: true,
    maxRetriesPerRequest: null,
  });

  try {
    await client.connect();
    return {
      kind: 'redis',
      async setEntries(entries) {
        if (!entries.length) {
          return;
        }

        await client.pipeline(entries.map(([key, value]) => ['set', key, value])).exec();
      },
      async ping() {
        return client.ping();
      },
      client,
    };
  } catch (error) {
    console.warn(`[store] falling back to memory store: ${error.message}`);
    try {
      client.disconnect();
    } catch (disconnectError) {
      void disconnectError;
    }
    return createMemoryStore();
  }
}

function createBinanceBridge({ io, store, history }) {
  let activeSocket = null;
  let reconnectTimer = null;
  let activeConfig = normalizeConfig();
  let shouldReconnect = false;

  function append(kind, message, data = {}) {
    const entry = {
      kind,
      message,
      data,
      timestamp: nowIso(),
    };

    history.add(entry);
    io.emit('app:message', entry);
    return entry;
  }

  function setStatus(state, details = {}) {
    const payload = {
      state,
      config: activeConfig,
      details,
      timestamp: nowIso(),
    };

    io.emit('app:status', payload);
    return payload;
  }

  function stopStream() {
    shouldReconnect = false;

    if (reconnectTimer) {
      clearTimeout(reconnectTimer);
      reconnectTimer = null;
    }

    if (activeSocket) {
      try {
        activeSocket.removeAllListeners();
        activeSocket.close();
      } catch (error) {
        void error;
      }
      activeSocket = null;
    }
  }

  async function handleIncoming(raw) {
    let parsed;

    try {
      parsed = JSON.parse(raw.toString());
    } catch (error) {
      append('error', 'Unable to parse Binance message', { error: error.message });
      io.emit('app:error', { message: error.message, timestamp: nowIso() });
      return;
    }

    const kline = normalizeBinanceKline(parsed);
    const entries = buildRedisEntries(kline);

    try {
      await store.setEntries(entries);
    } catch (error) {
      append('error', 'Failed to persist kline to Redis', { error: error.message });
      io.emit('app:error', { message: error.message, timestamp: nowIso() });
    }

    const payload = {
      kind: 'kline',
      message: `${kline.symbol} ${kline.interval} candle`,
      data: kline,
      redisKeys: entries.map(([key]) => key),
      timestamp: nowIso(),
    };

    history.add(payload);
    io.emit('app:kline', payload);
  }

  const ALLOWED_PREFIXES = new Set([
    BINANCE_PREFIX,
    'wss://data-stream.binance.vision',
  ]);

  function isAllowedPrefix(prefix) {
    try {
      const cleaned = String(prefix || '').trim().replace(/\/$/, '');
      return ALLOWED_PREFIXES.has(cleaned);
    } catch (e) {
      return false;
    }
  }

  function connectStream(config) {
    stopStream();

    activeConfig = normalizeConfig(config);
    // Enforce whitelist: only allow backend to connect to approved Binance prefixes
    if (!isAllowedPrefix(activeConfig.urlPrefix)) {
      const msg = `Rejected stream request: urlPrefix not allowed (${activeConfig.urlPrefix})`;
      append('error', msg, { config: activeConfig });
      io.emit('app:error', { message: msg, timestamp: nowIso() });
      setStatus('rejected', { reason: 'urlPrefix_not_allowed', urlPrefix: activeConfig.urlPrefix });
      return;
    }
    shouldReconnect = true;

    const streamUrl = buildStreamUrl(activeConfig);
    setStatus('connecting', { streamUrl });

    activeSocket = new WebSocket(streamUrl);

    activeSocket.on('open', () => {
      setStatus('connected', { streamUrl });
      append('status', 'Connected to Binance stream', { streamUrl });
    });

    activeSocket.on('message', handleIncoming);

    activeSocket.on('error', error => {
      append('error', 'Binance socket error', { error: error.message });
      io.emit('app:error', { message: error.message, timestamp: nowIso() });
    });

    activeSocket.on('close', (code, reasonBuffer) => {
      const reason = reasonBuffer ? reasonBuffer.toString() : 'closed';
      setStatus('disconnected', { code, reason });
      append('status', 'Binance stream closed', { code, reason });

      if (!shouldReconnect) {
        return;
      }

      reconnectTimer = setTimeout(() => {
        reconnectTimer = null;
        connectStream(activeConfig);
      }, 3000);
    });
  }

  function handleClientMessage(message) {
    const entry = {
      kind: 'chat',
      message: message.text,
      data: message,
      timestamp: nowIso(),
    };

    history.add(entry);
    io.emit('app:message', entry);
  }

  return {
    connectStream,
    stopStream,
    handleClientMessage,
    getConfig() {
      return activeConfig;
    },
    setStatus,
    append,
  };
}

function normalizeClientMessage(payload) {
  if (typeof payload === 'string') {
    return {
      text: payload,
      raw: payload,
    };
  }

  if (payload && typeof payload === 'object') {
    if (typeof payload.text === 'string') {
      return {
        text: payload.text,
        raw: payload,
      };
    }

    if (typeof payload.message === 'string') {
      return {
        text: payload.message,
        raw: payload,
      };
    }
  }

  return {
    text: JSON.stringify(payload),
    raw: payload,
  };
}

async function main() {
  const store = await createStore();
  const history = createHistoryStore();

  const app = express();
  
  // Security and CORS headers
  app.use((request, response, next) => {
    response.setHeader('Access-Control-Allow-Origin', '*');
    response.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
    response.setHeader('Access-Control-Allow-Headers', 'Content-Type');
    response.setHeader('X-Content-Type-Options', 'nosniff');
    response.setHeader('X-Frame-Options', 'SAMEORIGIN');
    next();
  });
  
  app.use(express.json());
  app.use(express.static(PUBLIC_DIR, {
    setHeaders: (response, path) => {
      if (path.endsWith('.html')) {
        response.setHeader('Content-Type', 'text/html; charset=utf-8');
      } else if (path.endsWith('.js')) {
        response.setHeader('Content-Type', 'application/javascript; charset=utf-8');
      } else if (path.endsWith('.css')) {
        response.setHeader('Content-Type', 'text/css; charset=utf-8');
      }
    }
  }));

  app.get('/health', async (_request, response) => {
    try {
      const ping = await store.ping();
      response.json({ ok: true, store: store.kind, ping });
    } catch (error) {
      response.status(503).json({ ok: false, error: error.message });
    }
  });

  app.get('*', (_request, response) => {
    response.sendFile(path.join(PUBLIC_DIR, 'index.html'));
  });

  const server = http.createServer(app);
  
  // Increase max listeners to prevent memory leak warnings
  server.setMaxListeners(0);
  
  const io = new Server(server, {
    path: '/socket.io',
    allowEIO3: true,
    cors: {
      origin: '*',
      methods: ['GET', 'POST'],
      credentials: false,
    },
    transports: ['websocket', 'polling'],
  });
  
  io.setMaxListeners(0);

  // health endpoint to check socket.io availability
  app.get('/socket-io-health', (_req, res) => {
    try {
      const clients = io && io.sockets && io.sockets.sockets ? io.sockets.sockets.size : 0;
      res.json({ ok: true, socketIo: true, clients });
    } catch (err) {
      res.status(500).json({ ok: false, socketIo: false, error: String(err) });
    }
  });

  const bridge = createBinanceBridge({ io, store, history });

  io.on('connection', socket => {
    socket.emit('app:config', bridge.getConfig());
    socket.emit('app:history', history.snapshot());
    bridge.setStatus('client-connected', { socketId: socket.id });

    socket.on('app:configure', payload => {
      const config = normalizeConfig(payload);
      bridge.connectStream(config);
      socket.emit('app:config', config);
      bridge.append('status', 'Client configuration updated', config);
    });

    socket.on('app:message', payload => {
      const message = normalizeClientMessage(payload);
      bridge.handleClientMessage(message);
    });

    socket.on('disconnect', reason => {
      bridge.append('status', 'Client disconnected', { socketId: socket.id, reason });
      bridge.setStatus('client-disconnected', { socketId: socket.id, reason });
    });
  });

  // Bind to 0.0.0.0 explicitly so Codespaces / cloud proxies can reach the socket
  server.listen(PORT, '0.0.0.0', () => {
    console.log(`Server listening on http://localhost:${PORT}`);
    console.log(`Static UI: ${path.join(PUBLIC_DIR, 'index.html')}`);
  });

  process.on('SIGINT', () => {
    bridge.stopStream();
    server.close(() => process.exit(0));
  });

  process.on('SIGTERM', () => {
    bridge.stopStream();
    server.close(() => process.exit(0));
  });
}

main().catch(error => {
  console.error(error);
  process.exit(1);
});