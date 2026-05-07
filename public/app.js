(function () {
  const { createElement: h, useEffect, useMemo, useRef, useState } = React;

  const INITIAL_CONFIG = {
    urlPrefix: 'wss://stream.binance.com:9443',
    endpoint: 'kline',
    symbol: 'BTCUSDT',
    interval: '1m',
  };

  function formatTime(value) {
    try {
      return new Date(value).toLocaleTimeString();
    } catch (error) {
      return String(value);
    }
  }

  function MessageList({ messages }) {
    const listRef = useRef(null);

    useEffect(() => {
      if (listRef.current) {
        listRef.current.scrollTop = listRef.current.scrollHeight;
      }
    }, [messages]);

    return h(
      'ol',
      { className: 'message-list', ref: listRef },
      messages.map((item, index) =>
        h(
          'li',
          { key: `${item.timestamp}-${index}`, className: 'message-item' },
          h(
            'div',
            { className: 'message-meta' },
            h('span', null, item.kind || 'event'),
            h('span', null, formatTime(item.timestamp || Date.now())),
          ),
          h(
            'p',
            { className: `message-body ${item.kind === 'kline' ? 'kline' : ''}` },
            item.message || JSON.stringify(item.data || item, null, 2),
          ),
        ),
      ),
    );
  }

  function App() {
    const [socketState, setSocketState] = useState('connecting');
    const [status, setStatus] = useState({ state: 'booting', details: {}, timestamp: new Date().toISOString() });
    const [config, setConfig] = useState(INITIAL_CONFIG);
    const [messages, setMessages] = useState([]);
    const [draft, setDraft] = useState('');
    const socketRef = useRef(null);
    const configRef = useRef(INITIAL_CONFIG);

    const endpointOptions = useMemo(
      () => [
        { label: 'Kline stream', value: 'kline' },
        { label: 'Trade stream', value: 'trade' },
        { label: 'Mini ticker', value: 'miniTicker' },
        { label: 'Ticker', value: 'ticker' },
        { label: 'Depth', value: 'depth' },
      ],
      [],
    );

    const urlPrefixOptions = useMemo(
      () => [
        { label: 'Binance stream', value: 'wss://stream.binance.com:9443' },
        { label: 'Binance vision', value: 'wss://data-stream.binance.vision' },
      ],
      [],
    );

    useEffect(() => {
      let socket = null;
      let triedPolling = false;

      function attachListeners(s) {
        socketRef.current = s;

        s.on('connect', () => {
          setSocketState('connected');
          setMessages(current => [
            ...current,
            {
              kind: 'status',
              message: 'Socket connected',
              timestamp: new Date().toISOString(),
            },
          ]);
          s.emit('app:configure', configRef.current);
        });

        s.on('disconnect', reason => {
          setSocketState(`disconnected: ${reason}`);
        });

        s.on('app:config', nextConfig => {
          setConfig(previous => ({ ...previous, ...nextConfig }));
        });

        s.on('app:history', history => {
          setMessages(history || []);
        });

        s.on('app:status', nextStatus => {
          setStatus(nextStatus);
        });

        s.on('app:message', message => {
          setMessages(current => [...current, message]);
        });

        s.on('app:kline', message => {
          setMessages(current => [...current, message]);
        });

        s.on('app:error', error => {
          setMessages(current => [
            ...current,
            {
              kind: 'error',
              message: error.message,
              timestamp: error.timestamp || new Date().toISOString(),
            },
          ]);
        });

        s.on('connect_error', err => {
          // if websocket failed, try polling once
          if (!triedPolling) {
            triedPolling = true;
            try {
              s.close();
            } catch (e) {}
            setMessages(current => [...current, { kind: 'status', message: 'WebSocket failed, retrying with polling', timestamp: new Date().toISOString() }]);
            // create new socket using polling only
            socket = window.io({ transports: ['polling'] });
            attachListeners(socket);
          } else {
            setMessages(current => [...current, { kind: 'error', message: `Socket connect_error: ${err && err.message}`, timestamp: new Date().toISOString() }]);
          }
        });
      }

      // initial attempt: prefer websocket but allow polling fallback
      socket = window.io({ transports: ['websocket', 'polling'] });
      attachListeners(socket);

      return () => {
        try { if (socket) socket.close(); } catch (e) {}
      };
    }, []);

    useEffect(() => {
      configRef.current = config;
    }, [config]);

    function updateField(field, value) {
      setConfig(previous => ({ ...previous, [field]: value }));
    }

    function submitConfig(event) {
      event.preventDefault();
      const socket = socketRef.current;

      if (!socket || !socket.connected) {
        setMessages(current => [
          ...current,
          {
            kind: 'error',
            message: 'Socket.io is not connected. Unable to configure backend stream.',
            timestamp: new Date().toISOString(),
          },
        ]);
        return;
      }

      socket.emit('app:configure', configRef.current);
      setMessages(current => [
        ...current,
        {
          kind: 'status',
          message: `Requested backend stream for ${configRef.current.symbol} ${configRef.current.interval}`,
          timestamp: new Date().toISOString(),
        },
      ]);
    }

    function sendChat(event) {
      event.preventDefault();

      const text = draft.trim();
      if (!text) {
        return;
      }

      const socket = socketRef.current;
      const payload = {
        text,
        config,
        timestamp: new Date().toISOString(),
      };

      if (socket) {
        socket.emit('app:message', payload);
      }

      setDraft('');
    }

    return h(
      'main',
      { className: 'page' },
      h(
        'div',
        { className: 'header' },
        h('p', { className: 'eyebrow' }, 'Binance realtime console'),
        h('h1', { className: 'title' }, 'Stream klines, chat over Socket.io, and keep the data in Redis.'),
        h(
          'p',
          { className: 'subtitle' },
          'A mobile-first interface for choosing a Binance stream, watching live kline updates, and sending JSON chat events through the same Node.js process.',
        ),
      ),
      h(
        'div',
        { className: 'shell' },
        h(
          'section',
          { className: 'panel' },
          h(
            'div',
            { className: 'panel-inner' },
            h('h2', { className: 'section-title' }, 'Front-end stream controls'),
            h(
              'form',
              { className: 'form-grid', onSubmit: submitConfig },
              h(
                'div',
                { className: 'field' },
                h('label', { htmlFor: 'urlPrefix' }, 'URL prefix'),
                h(
                  'select',
                  {
                    id: 'urlPrefix',
                    value: config.urlPrefix,
                    onChange: event => updateField('urlPrefix', event.target.value),
                  },
                  urlPrefixOptions.map(option => h('option', { key: option.value, value: option.value }, option.label)),
                ),
              ),
              h(
                'div',
                { className: 'field' },
                h('label', { htmlFor: 'endpoint' }, 'Binance endpoint'),
                h(
                  'select',
                  {
                    id: 'endpoint',
                    value: config.endpoint,
                    onChange: event => updateField('endpoint', event.target.value),
                  },
                  endpointOptions.map(option => h('option', { key: option.value, value: option.value }, option.label)),
                ),
              ),
              h(
                'div',
                { className: 'field' },
                h('label', { htmlFor: 'symbol' }, 'Symbol'),
                h('input', {
                  id: 'symbol',
                  value: config.symbol,
                  onChange: event => updateField('symbol', event.target.value.toUpperCase()),
                  placeholder: 'BTCUSDT',
                }),
              ),
              h(
                'div',
                { className: 'field' },
                h('label', { htmlFor: 'interval' }, 'Interval'),
                h('input', {
                  id: 'interval',
                  value: config.interval,
                  onChange: event => updateField('interval', event.target.value),
                  placeholder: '1m',
                }),
              ),
              h(
                'div',
                { className: 'button-row' },
                h('button', { className: 'button', type: 'submit' }, 'Connect stream'),
                h(
                  'button',
                  {
                    className: 'button secondary',
                    type: 'button',
                    onClick: () => setConfig(INITIAL_CONFIG),
                  },
                  'Reset',
                ),
              ),
            ),
          ),
        ),
        h(
          'section',
          { className: 'right-panel' },
          h(
            'div',
            { className: 'panel messages' },
            h(
              'div',
              { className: 'panel-inner' },
              h('h2', { className: 'section-title' }, 'Realtime messages'),
              h(
                'div',
                { className: 'status-card' },
                h('strong', null, `Socket: ${socketState}`),
                h('span', null, `Stream: ${status.state || 'unknown'}`),
                h('span', null, status.details?.streamUrl || `${config.urlPrefix}/ws/...`),
              ),
            ),
            h(MessageList, { messages }),
            h(
              'form',
              { className: 'chat-form', onSubmit: sendChat },
              h('div', { className: 'chat-row' },
                h('input', {
                  className: 'chat-input',
                  value: draft,
                  onChange: event => setDraft(event.target.value),
                  placeholder: 'Send a message or JSON event',
                }),
                h('span', { className: 'hint' }, 'Messages are sent as Socket.io JSON payloads and echoed back into the log.'),
              ),
              h('div', { className: 'button-row' },
                h('button', { className: 'button', type: 'submit' }, 'Send'),
              ),
            ),
          ),
        ),
      ),
    );
  }

  ReactDOM.createRoot(document.getElementById('root')).render(h(App));
}());