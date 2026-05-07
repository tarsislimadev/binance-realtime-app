# Binance Real-Time App

A small realtime demo that connects to Binance public WebSocket streams, persists recent kline events (in memory or Redis), and exposes a simple Socket.io-powered front-end UI.

This repository is a guest-friendly demo — no Binance API key required. The server connects to public Binance stream endpoints and serves a minimal React UI from the `public` folder.

## Quick demo (for guests)

- Install dependencies:

	npm install

- Start the server (defaults to port `3000`):

	npm start

- Open your browser to: http://localhost:3000

The UI lets you pick a stream (kline, trade, ticker, depth), symbol (e.g. `BTCUSDT`), and interval. Messages and kline events appear in the realtime log.

## Notes for guests

- No API keys are required — the app uses Binance public WebSocket endpoints.
- If no `REDIS_URL` is provided, the app uses an in-memory store (safe for demos).
- Allowed stream prefixes (whitelist):
	- `wss://stream.binance.com:9443`
	- `wss://data-stream.binance.vision`

## Useful endpoints

- Health: `GET /health` — shows store type and ping.
- Socket.io health: `GET /socket-io-health` — reports connected clients.

## Environment

- `PORT` — server port (default: `3000`)
- `REDIS_URL` — optional Redis connection string; if omitted, the app uses memory storage.
- `BINANCE_PREFIX` — optional WebSocket prefix (whitelisted values only)

## Tech stack

- Node.js + Express
- Socket.io for realtime UI
- Optional Redis via `ioredis`
- Vanilla React in `public/app.js` for the demo UI

## Contributing / Feedback

This is a small demo app. If you'd like changes to the guest README or a hosted demo, tell me what you'd like and I can update it.

## License

[MIT](./LICENSE)
