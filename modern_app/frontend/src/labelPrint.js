export async function withLabelTimeout(operation, message, milliseconds = 30000) {
  let timer;
  try {
    return await Promise.race([operation, new Promise((_, reject) => { timer = setTimeout(() => reject(new Error(message)), milliseconds); })]);
  } finally { clearTimeout(timer); }
}

export function printWindowStatus(target, text, url) {
  if (!target || target.closed) return;
  try {
    target.document.body.textContent = text;
    if (url) {
      const open = target.document.createElement("a");
      open.href = url; open.textContent = "Open label PDF"; open.style.display = "block";
      target.document.body.appendChild(open);
      const download = target.document.createElement("a");
      download.href = `${url}?download=true`; download.textContent = "Download PDF to print"; download.style.display = "block";
      target.document.body.appendChild(download);
    }
  } catch { /* The PDF viewer may already have taken over the tab. */ }
}

export async function openPreparedLabelPdf(images, settings, target, api) {
  if (!target || target.closed) throw new Error("The label print preview was closed. Please try again.");
  printWindowStatus(target, "Creating label PDF…");
  const controller = new AbortController();
  let result;
  try {
    result = await withLabelTimeout(api("/api/evidence-labels/print-pdf", {
      method: "POST", headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ images, width: settings.width, height: settings.height }), signal: controller.signal,
    }), "PDF preparation timed out. Return to CyberLab and try Print Label again.", 45000);
  } finally { controller.abort(); }
  const url = new URL(result.url);
  if (!["http:", "https:"].includes(url.protocol)) throw new Error("CyberLab returned an invalid PDF address.");
  printWindowStatus(target, "Your label PDF is ready. If it does not open automatically, use a link below.", url.href);
  try { target.location.replace(url.href); }
  catch { /* Keep the ordinary HTTP open/download links if browser policy blocks automatic navigation. */ }
}
