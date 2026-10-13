/** Minimal browser client for the SSE endpoint. No build step, no framework. */
export const SSE_DEMO_PAGE = `<!doctype html>
<html>
<head><meta charset="utf-8"><title>Live notifications</title>
<style>
  body { font: 14px/1.5 ui-monospace, monospace; margin: 2rem; }
  #log { border: 1px solid #ccc; padding: 1rem; min-height: 12rem; }
  .row { padding: .25rem 0; border-bottom: 1px solid #eee; }
  .meta { color: #888; }
</style></head>
<body>
  <h3>Live notifications for user <input id="uid" value="42" size="4"> 
    <button onclick="connect()">connect</button>
    <span id="state" class="meta">disconnected</span>
  </h3>
  <div id="log"></div>
<script>
  let es;
  function connect() {
    if (es) es.close();
    const uid = document.getElementById('uid').value;
    // EventSource is the browser's built-in SSE client. It reconnects on its
    // own if the connection drops -- and silently misses anything published
    // during the gap. That is the PHASE 3 lesson.
    es = new EventSource('/learn-notifications/stream/' + uid);
    es.onopen = () => document.getElementById('state').textContent = 'connected';
    es.onerror = () => document.getElementById('state').textContent = 'reconnecting...';
    es.onmessage = (e) => {
      const n = JSON.parse(e.data);
      const row = document.createElement('div');
      row.className = 'row';
      row.innerHTML = n.title + ' <span class="meta">from ' + n.publishedBy + '</span>';
      document.getElementById('log').prepend(row);
    };
  }
  connect();
</script>
</body></html>`;
