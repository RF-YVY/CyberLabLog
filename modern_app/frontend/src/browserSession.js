export function startBrowserSession({ apiBase, fetcher = fetch, browser = window, navigatorObject = navigator, sessionId = crypto.randomUUID(), intervalMs = 5000 }) {
  let instanceId = "";
  let stopped = false;
  const heartbeat = async () => {
    try {
      const response = await fetcher(`${apiBase}/api/runtime/heartbeat`, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ session_id: sessionId }) });
      if (response.ok) {
        instanceId = (await response.json()).instance_id || "";
        if (stopped) release();
      }
    } catch { /* A later heartbeat reconnects after temporary network interruption. */ }
  };
  const release = () => {
    if (!instanceId) return;
    const url = `${apiBase}/api/runtime/session/close`;
    const body = JSON.stringify({ session_id: sessionId, instance_id: instanceId });
    try {
      if (navigatorObject.sendBeacon?.(url, new Blob([body], { type: "application/json" }))) return;
    } catch { /* Fall back when beacon is unavailable or rejected. */ }
    fetcher(url, { method: "POST", headers: { "Content-Type": "application/json" }, body, keepalive: true }).catch(() => {});
  };
  heartbeat();
  const timer = browser.setInterval(heartbeat, intervalMs);
  browser.addEventListener("pagehide", release);
  browser.addEventListener("pageshow", heartbeat);
  return () => {
    stopped = true;
    browser.clearInterval(timer);
    browser.removeEventListener("pagehide", release);
    browser.removeEventListener("pageshow", heartbeat);
    release();
  };
}
