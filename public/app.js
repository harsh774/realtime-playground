/* ---------- Panel helper: log + counters ---------- */
function makePanel(id) {
  const root = document.getElementById(id);
  const logEl = root.querySelector('.log');
  const statsEl = root.querySelector('.stats');
  const p = { lastId: 0, epoch: null, requests: 0, empty: 0, received: 0, totalDelay: 0 };

  function line(text, cls) {
    const li = document.createElement('li');
    li.textContent = text; // textContent, not innerHTML: messages are user input
    if (cls) li.className = cls;
    logEl.prepend(li);
  }

  p.render = () => {
    const avg = p.received ? Math.round(p.totalDelay / p.received) : 0;
    statsEl.textContent =
      `requests: ${p.requests} | empty: ${p.empty} | events: ${p.received} | avg delay: ${avg} ms`;
  };

  p.add = evt => {
    // Server restarted? Its ids start over at 1, so reset our cursor
    // or we'd ignore every new message as "already seen".
    if (p.epoch && evt.epoch !== p.epoch) {
      p.note(`server restarted (new epoch ${evt.epoch}), resetting cursor`);
      p.lastId = 0;
    }
    p.epoch = evt.epoch;

    if (evt.id <= p.lastId) return; // dedupe: replays can resend old events
    p.lastId = evt.id;
    const delay = Date.now() - evt.at; // how late did we hear about it?
    p.received++;
    p.totalDelay += delay;
    line(`#${evt.id} "${evt.text}" (+${delay}ms)`);
    p.render();
  };

  p.note = text => line(text, 'note');
  p.clear = () => logEl.replaceChildren();
  p.render();
  return p;
}

/* ---------- Send form (REST) ---------- */
document.getElementById('sendForm').onsubmit = async e => {
  e.preventDefault(); // stop the browser from reloading the page
  const input = document.getElementById('text');
  const text = input.value.trim();
  if (!text) return;
  await fetch('/api/messages', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ text }),
  });
  input.value = '';
};

/* ---------- 1. Short polling ---------- */
const shortP = makePanel('short');

async function shortPoll() {
  try {
    const res = await fetch(`/api/poll?since=${shortP.lastId}`);
    const { events } = await res.json();
    shortP.requests++;
    if (events.length === 0) shortP.empty++;
    events.forEach(shortP.add);
    shortP.render();
  } catch (err) {
    shortP.note('poll failed: ' + err.message);
  }
  // setTimeout, not setInterval: the next poll waits for this one to
  // finish, so slow responses never pile up on top of each other
  setTimeout(shortPoll, 3000);
}

shortPoll();

/* ---------- 2. Long polling ---------- */
const longP = makePanel('long');

async function longPoll() {
  while (true) {
    try {
      const res = await fetch(`/api/long-poll?since=${longP.lastId}`);
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const body = await res.json();
      longP.requests++;

      if (body.timedOut) {
        longP.empty++; // for long polling, "empty" = timed out with no news
        longP.note(`no news for ${Math.round(body.waitedMs / 1000)}s, asking again`);
      } else if (body.waitedMs > 0) {
        longP.note(`server held the request ${(body.waitedMs / 1000).toFixed(1)}s, then answered`);
      }

      body.events.forEach(longP.add);
      longP.render();
    } catch (err) {
      // Back off on errors. Without this, a dead server means the loop
      // fires thousands of failing requests per second.
      longP.note(`request failed (${err.message}), retrying in 2s`);
      await new Promise(r => setTimeout(r, 2000));
    }
  }
}

longPoll();

/* ---------- 3. SSE ---------- */
const sseP = makePanel('sse');
const es = new EventSource(`/api/sse?since=${sseP.lastId}`);

es.onopen = () => {
  sseP.requests++; // for SSE this counts connections, not messages
  sseP.render();
};

es.addEventListener('hello', e => {
  const { resumedFrom, via } = JSON.parse(e.data);
  sseP.note(`stream opened, server replaying after #${resumedFrom} (via ${via})`);
});

// "update" matches the `event: update` line the server sends
es.addEventListener('update', e => sseP.add(JSON.parse(e.data)));

es.onerror = () => {
  if (es.readyState === EventSource.CONNECTING) {
    sseP.note('stream lost, browser will reconnect by itself in 3s');
  }
};

/* ---------- 4. Socket.io ---------- */
const sockP = makePanel('socket');

const socket = io({
  // A function, so every reconnect sends the CURRENT lastId, not the one from page load
  auth: cb => cb({ lastId: sockP.lastId }),
});

socket.on('connect', () => {
  sockP.requests++; // counts connections
  sockP.render();
  const engine = socket.io.engine;
  sockP.note(`connected via ${engine.transport.name}`);
  engine.once('upgrade', t => sockP.note(`transport upgraded: polling → ${t.name}`));
});

socket.on('hello', ({ resumedFrom }) => sockP.note(`server replaying after #${resumedFrom}`));
socket.on('update', sockP.add);

socket.on('disconnect', reason => {
  sockP.note(`disconnected, reason: "${reason}"`);
  // Socket.io auto-reconnects after network loss, but NOT when the server
  // deliberately kicks you ("io server disconnect"). Then it's on you.
  if (reason === 'io server disconnect') {
    sockP.note('server kicked us, reconnecting manually in 2s');
    setTimeout(() => socket.connect(), 2000);
  }
});

// Send over the socket instead of HTTP POST, and wait for the server's ack
document.getElementById('sendSocket').onclick = () => {
  const input = document.getElementById('text');
  const text = input.value.trim();
  if (!text) return;
  socket.emit('send', { text }, ack => {
    sockP.note(ack.ok ? `sent #${ack.id} over the socket (server acked)` : `rejected: ${ack.error}`);
  });
  input.value = '';
};

// Round-trip time: only possible because the channel goes both ways
setInterval(() => {
  if (!socket.connected) return;
  socket.emit('rtt', Date.now(), sentAt => {
    document.getElementById('rtt').textContent = `round trip: ${Date.now() - sentAt} ms`;
  });
}, 3000);

/* ---------- 5. Chaos controls + server stats ---------- */
const flashEl = document.getElementById('flash');
function flash(text) {
  flashEl.textContent = text;
}

document.getElementById('burst').onclick = async () => {
  const res = await fetch('/api/burst', { method: 'POST' });
  const { ids } = await res.json();
  flash(`Published ${ids.length} messages at once. Compare each panel's request count now.`);
};

document.getElementById('drop').onclick = async () => {
  const res = await fetch('/api/chaos/drop', { method: 'POST' });
  const { dropped } = await res.json();
  flash(
    `Server hung up on ${dropped.sse} SSE stream(s), ${dropped.longPolls} held long-poll(s) ` +
      `and ${dropped.sockets} socket(s). Watch how each panel recovers.`
  );
};

document.getElementById('clear').onclick = () => {
  [shortP, longP, sseP, sockP].forEach(p => p.clear());
};

// The stats bar itself uses plain short polling, every 2s
async function refreshStats() {
  try {
    const s = await (await fetch('/api/stats')).json();
    document.getElementById('serverStats').textContent =
      `Server is holding open → SSE streams: ${s.openSse} | waiting long-polls: ${s.waitingLongPolls} ` +
      `| sockets: ${s.sockets}  ·  total messages: ${s.totalEvents}  ·  epoch: ${s.epoch}`;
  } catch {
    document.getElementById('serverStats').textContent = 'Server unreachable';
  }
}

setInterval(refreshStats, 2000);
refreshStats();