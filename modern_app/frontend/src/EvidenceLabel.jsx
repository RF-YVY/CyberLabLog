import React, { useEffect, useLayoutEffect, useRef, useState } from "react";
import { createPortal } from "react-dom";
import { renderToStaticMarkup } from "react-dom/server";
import { zipSync } from "fflate";
import { openPreparedLabelPdf, printWindowStatus, withLabelTimeout, sendLabelsToPrinter } from "./labelPrint.js";
import QRCode from "qrcode";
import { toPng } from "html-to-image";
import { labelFields, savedLabelFromCase, visibleLabelFields, normalizeLabelSettings } from "./evidenceLabel.js";

const emptySettings = normalizeLabelSettings();
const printerSettings = normalizeLabelSettings;

function useLabelPrinters(api, preferred) {
  const [names, setNames] = useState([]), [printer, setPrinter] = useState(""), [message, setMessage] = useState("");
  async function refresh() {
    setMessage("Loading printers…");
    try {
      const result = await withLabelTimeout(api("/api/evidence-labels/printers"), "Printer discovery timed out.", 15000);
      setNames(result.printers || []);
      setPrinter(current => result.printers?.includes(current) ? current : result.printers?.includes(preferred) ? preferred : result.recommended || "");
      setMessage(result.printers?.length ? "" : "No Windows printers found. Open Print PDF is still available.");
    } catch { setMessage("Could not load Windows printers. Refresh printers or use Open Print PDF."); }
  }
  useEffect(() => { refresh(); }, []);
  useEffect(() => { if (preferred && names.includes(preferred)) setPrinter(preferred); }, [preferred, names]);
  return { names, printer, setPrinter, refresh, message };
}

function LabelPrinterSelect({ info }) {
  return <div className="form-actions"><label className="field"><span>Label printer</span><select value={info.printer} onChange={event => info.setPrinter(event.target.value)}><option value="">Select a printer</option>{info.names.map(name => <option key={name} value={name}>{name}</option>)}</select></label><button type="button" className="ghost-action" onClick={info.refresh}>Refresh printers</button>{info.message && <small role="status">{info.message}</small>}</div>;
}

export function EvidenceLabelSettings({ api }) {
  const [settings, setSettings] = useState(emptySettings);
  const [loaded, setLoaded] = useState(false);
  const [message, setMessage] = useState("");
  const printers = useLabelPrinters(api, settings.printer_name);
  useEffect(() => { api("/api/settings/json/evidence_label").then(result => { setSettings(printerSettings(result.value)); setLoaded(true); }).catch(error => setMessage(error.message)); }, []);
  async function loadLogo(file) {
    if (!file) return;
    if (!["image/png", "image/jpeg", "image/webp"].includes(file.type) || file.size > 2 * 1024 * 1024) { setMessage("Choose a PNG, JPEG, or WebP image under 2 MB."); return; }
    try {
      const data = await new Promise((resolve, reject) => { const reader = new FileReader(); reader.onload = () => resolve(reader.result); reader.onerror = reject; reader.readAsDataURL(file); });
      setSettings(current => ({ ...current, logo: data })); setMessage("Logo loaded. Save settings to keep it.");
    } catch { setMessage("Could not read the image."); }
  }
  return <form className="profile-card" onSubmit={async event => { event.preventDefault(); try { await api("/api/settings/json/evidence_label", { method: "PUT", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ value: { ...settings, printer_name: printers.printer } }) }); setMessage("Evidence label settings saved."); } catch (error) { setMessage(error.message); } }}>
    <h3>Evidence Label Settings</h3><p>Agency and division appear at the top of every label. The logo appears centered on the right.</p>
    <fieldset disabled={!loaded} className="label-settings-fields"><div className="form-grid two-col">{[["agency", "Agency title"], ["division", "Division title"]].map(([key, title]) => <label className="field" key={key}><span>{title}</span><input value={settings[key]} onChange={event => setSettings({ ...settings, [key]: event.target.value })} /></label>)}
    <label className="field"><span>Label stock</span><select value={settings.paper} onChange={event => setSettings(current => normalizeLabelSettings({ ...current, paper: event.target.value }))}><option value="dymo550">Dymo 550 — 4″ × 2⁵⁄₁₆″ (landscape)</option><option value="custom">Custom size</option></select></label>
    {settings.paper === "custom" && ["width", "height"].map(key => <label className="field" key={key}><span>Label {key} (inches)</span><input type="number" min="1" max="12" step="any" required value={settings[key]} onChange={event => setSettings({ ...settings, [key]: event.target.value })} /></label>)}
    </div><p>Content fits within the selected dimensions. Labels never grow taller as fields are added.</p>
    <label className="field"><span>Label logo</span><input type="file" accept="image/png,image/jpeg,image/webp" onChange={event => loadLogo(event.target.files?.[0])} /></label>
    {settings.logo && <div><img src={settings.logo} alt="Label logo" style={{ maxWidth: 100, maxHeight: 80 }} /><button type="button" className="ghost-action" onClick={() => setSettings({ ...settings, logo: "" })}>Remove logo</button></div>}
    <LabelPrinterSelect info={printers} />
    <button className="primary-action inline">Save Label Settings</button></fieldset><p role="status">{message}</p>
  </form>;
}

export function EvidenceLabelGenerator({ row, api, onClose, onSaved }) {
  const initial = useRef(savedLabelFromCase(row)).current;
  const [saving, setSaving] = useState(false);
  const [saveMessage, setSaveMessage] = useState("");
  const [values, setValues] = useState(initial.values);
  const [extra, setExtra] = useState(initial.extra);
  const [settings, setSettings] = useState(emptySettings);
  const printers = useLabelPrinters(api, settings.printer_name);
  const [loaded, setLoaded] = useState(false);
  const [error, setError] = useState("");
  const [barcodeError, setBarcodeError] = useState("");
  const [barcode, setBarcode] = useState(initial.barcode);
  const [qrImage, setQrImage] = useState("");
  const [qrPending, setQrPending] = useState(false);
  const [fitError, setFitError] = useState("");
  const label = useRef(null), dialog = useRef(null);
  useEffect(() => { api("/api/settings/json/evidence_label").then(result => { setSettings(printerSettings(result.value)); setLoaded(true); }).catch(error => setError(error.message)); }, []);
  useLayoutEffect(() => {
    setFitError(fitLabelNode(label.current));
  }, [values, extra, settings, qrImage]);
  useEffect(() => {
    let active = true;
    setQrImage(""); setBarcodeError("");
    if (!barcode.trim()) { setQrPending(false); return; }
    setQrPending(true);
    QRCode.toDataURL(barcode, { errorCorrectionLevel: "M", margin: 2, width: 600 })
      .then(url => { if (active) setQrImage(url); })
      .catch(() => { if (active) setBarcodeError("The QR code could not be generated. Try a shorter value."); })
      .finally(() => { if (active) setQrPending(false); });
    return () => { active = false; };
  }, [barcode]);
  useEffect(() => { const previous = document.activeElement; dialog.current?.focus(); return () => previous?.focus(); }, []);
  async function saveLabel() {
    setSaving(true); setError("");
    try {
      const data = { values, extra, barcode };
      if (row.id) await saveLabelRecord(api, row, data);
      await onSaved?.(data);
      setSaveMessage(row.id ? "Label saved for this exhibit." : "Label kept in this entry. Save the case to keep it permanently.");
      return data;
    } catch (error) { setError(`Label save failed: ${error.message}`); throw error; }
    finally { setSaving(false); }
  }
  async function outputLabel(action) {
    if (fitError) return;
    setSaving(true);
    let printWindow;
    try {
      if (action === "pdf") printWindow = openLabelPrintWindow();
      await saveLabel(); setSaving(true);
      const url = await createLabelPng(label.current, settings);
      if (action === "pdf") await openPreparedLabelPdf([url], settings, printWindow, api);
      else if (action === "print") { const result = await sendLabelsToPrinter([url], settings, printers.printer, api); setSaveMessage(`${result.count} label sent to ${result.printer}.`); }
      else downloadUrl(url, `evidence-${safeFilename(values.case || "label")}.png`);
    } catch (error) { printWindowStatus(printWindow, `Could not prepare the label: ${error.message}`); setError(error.message); }
    finally { setSaving(false); }
  }
  return createPortal(<div className="modal-backdrop evidence-label-overlay"><section className="case-modal evidence-label-dialog" role="dialog" aria-modal="true" aria-labelledby="label-title" tabIndex={-1} ref={dialog} onKeyDown={event => {
    if (event.key === "Escape") { event.stopPropagation(); onClose(); }
    if (event.key === "Tab") { const controls = [...dialog.current.querySelectorAll("button:not(:disabled), input, textarea")]; const first = controls[0], last = controls.at(-1); if (event.shiftKey && document.activeElement === first) { event.preventDefault(); last.focus(); } else if (!event.shiftKey && document.activeElement === last) { event.preventDefault(); first.focus(); } }
  }}>
    <header><h2 id="label-title">Generate Evidence Barcode</h2><button className="ghost-action" onClick={onClose}>Close</button></header>
    <p>Blank fields are omitted. Save Label keeps these fields for this exhibit. Printing and downloading also save the label; other case fields remain unchanged.</p>
    <div className="form-grid two-col">{labelFields.map(([key, title]) => <label className="field" key={key}><span>{title}</span>{key === "description" ? <textarea value={values[key]} onChange={event => setValues({ ...values, [key]: event.target.value })} /> : <input type={key === "date" ? "date" : "text"} value={values[key]} onChange={event => setValues({ ...values, [key]: event.target.value })} />}</label>)}</div>
    {extra.map((item, index) => <div className="form-grid two-col" key={index}><label className="field"><span>Extra field name</span><input value={item.label} onChange={event => setExtra(extra.map((entry, i) => i === index ? { ...entry, label: event.target.value } : entry))} /></label><label className="field"><span>Extra field value</span><input value={item.value} onChange={event => setExtra(extra.map((entry, i) => i === index ? { ...entry, value: event.target.value } : entry))} /></label><button className="ghost-action" onClick={() => setExtra(extra.filter((_, i) => i !== index))}>Remove field</button></div>)}
    <button className="ghost-action" onClick={() => setExtra([...extra, { label: "", value: "" }])}>Add field</button>
    <label className="field"><span>QR code value (defaults to case number; include an exhibit identifier if needed)</span><input value={barcode} onChange={event => setBarcode(event.target.value)} /></label>
    {(error || barcodeError) && <p role="alert">{error || barcodeError}</p>}
    <button className="ghost-action" disabled={saving || !loaded} onClick={() => saveLabel().catch(() => {})}>Save Label</button><p role="status">{saveMessage}</p>
    <h3>Label preview</h3><style>{labelStyles}</style><div className="label-preview-scroll"><EvidenceLabelFace labelRef={label} values={values} extra={extra} settings={settings} qrImage={qrImage} /></div>
    {fitError && <p role="alert">{fitError}</p>}
    <p>{settings.paper === "dymo550" ? "Dymo 550 · 4″ × 2⁵⁄₁₆″ landscape" : `Custom label · ${settings.width}″ × ${settings.height}″`}. Print Label sends directly to the selected Windows printer at the label size, with no headers or footers. For Dymo, shipping-label feed direction is applied automatically. Open Print PDF is an alternate option. PNG size: {Math.round(settings.width * 300)} × {Math.round(settings.height * 300)} pixels.</p>
    <LabelPrinterSelect info={printers} />
    <button className="primary-action inline" disabled={saving || !loaded || !printers.printer || qrPending || Boolean(barcodeError) || Boolean(fitError)} onClick={() => outputLabel("print")}>Print Label</button>
    <button className="ghost-action" disabled={saving || !loaded || qrPending || Boolean(barcodeError) || Boolean(fitError)} onClick={() => outputLabel("pdf")}>Open Print PDF</button>
    <button className="ghost-action" disabled={saving || !loaded || qrPending || Boolean(barcodeError) || Boolean(fitError)} onClick={() => outputLabel("download")}>Download PNG</button>
  </section></div>, document.body);
}

const labelStyles = `.label-preview-scroll{overflow-x:auto;margin:12px 0}.evidence-label{box-sizing:border-box!important;width:var(--label-width)!important;height:var(--label-height)!important;min-height:0!important;max-height:var(--label-height)!important;overflow:hidden;display:flex;flex-direction:column;background:white;color:black;padding:0.07in;font:13px Arial,sans-serif;line-height:1.1;overflow-wrap:anywhere;flex-shrink:0}.evidence-label .label-heading{display:flex;align-items:center;gap:6px;border-bottom:1px solid black;padding-bottom:4px;margin-bottom:4px;height:43px;box-sizing:border-box;flex-shrink:0}.evidence-label .label-qr{width:38px;height:38px;object-fit:contain;flex-shrink:0;image-rendering:pixelated}.evidence-label .label-titles{flex:1;min-width:0;text-align:center;font-size:12px;font-weight:700;line-height:1.15}.evidence-label .label-titles>*{display:block;white-space:nowrap;overflow-wrap:normal;font-size:inherit}.evidence-label .label-content{display:flex;align-items:center;gap:6px;flex:1;min-height:0;overflow:hidden}.evidence-label .label-content dl{flex:1;min-width:0;margin:0;align-self:flex-start}.evidence-label .label-content dl>div{display:flex;gap:3px;margin:2px 0}.evidence-label .label-content dt{font-weight:700;flex-shrink:0}.evidence-label .label-content dd{min-width:0;margin:0;white-space:pre-wrap}.evidence-label .label-content .label-exhibit{font-size:1.35em;font-weight:700;margin:0 0 4px}.evidence-label .label-logo{width:27%;height:100%;max-height:1.05in;object-fit:contain;flex-shrink:0}`;

function EvidenceLabelFace({ labelRef, values, extra, settings, qrImage }) {
  return (<div className="evidence-label" ref={labelRef} style={{ "--label-width": `${settings.width}in`, "--label-height": `${settings.height}in` }}>
      {(qrImage || settings.agency.trim() || settings.division.trim()) && <div className="label-heading">
        {qrImage && <img className="label-qr" src={qrImage} alt="Evidence QR code" />}
        <div className="label-titles">{settings.agency.trim() && <strong>{settings.agency}</strong>}{settings.division.trim() && <div>{settings.division}</div>}</div>
      </div>}
      <div className="label-content"><dl>{visibleLabelFields(values, extra).map((item, index) => <div className={item.label === "EXHIBIT" ? "label-exhibit" : ""} key={index}><dt>{({ "Brand/Model": "Brand", Location: "Loc", Description: "Desc", "Investigation Subject": "Suspect" })[item.label] || item.label}:</dt><dd>{item.value}</dd></div>)}</dl>{settings.logo && <img className="label-logo" src={settings.logo} alt="Agency logo" />}</div>
    </div>);
}

function fitLabelNode(node) {
    if (!node) return "";
    for (const title of node.querySelectorAll(".label-titles > *")) {
      let size = 12;
      title.style.fontSize = `${size}px`;
      while (title.scrollWidth > title.clientWidth && size > 1) { size -= 0.25; title.style.fontSize = `${size}px`; }
    }
    const content = node.querySelector(".label-content");
    const fields = content.querySelector("dl");
    let size = 13;
    content.style.fontSize = `${size}px`;
    const overflows = () => fields.scrollHeight > content.clientHeight || fields.scrollWidth > fields.clientWidth;
    while (overflows() && size > 8) { size -= 0.25; content.style.fontSize = `${size}px`; }
    return overflows() ? "This content exceeds the label at a readable size. Shorten the description or remove fields before printing or downloading." : "";
}

function safeFilename(value) { return String(value).replace(/[^a-zA-Z0-9_-]/g, "_"); }

function downloadUrl(url, filename) {
  const link = document.createElement("a"); link.download = filename; link.href = url; link.click();
}

async function saveLabelRecord(api, row, data) {
  const source = row._mode === "progress" || row.source === "progress" ? "in-progress" : "cases";
  return withLabelTimeout(api(`/api/${source}/${row.id}/evidence-label`, { method: "PUT", headers: { "Content-Type": "application/json" }, body: JSON.stringify(data) }), "Saving the label timed out. Check CyberLab is running and try again.");
}

async function createLabelPng(node, settings) {
  const exportLabel = node.cloneNode(true);
  try {
    Object.assign(exportLabel.style, { position: "absolute", left: "-10000px", top: "0" });
    document.body.appendChild(exportLabel);
    await withLabelTimeout(Promise.all([...exportLabel.querySelectorAll("img")].map(img => img.decode())), "A label image could not be loaded. Reload the label logo and try again.", 10000);
    return await withLabelTimeout(toPng(exportLabel, { width: settings.width * 96, height: settings.height * 96, canvasWidth: Math.round(settings.width * 300), canvasHeight: Math.round(settings.height * 300), pixelRatio: 1, skipFonts: true, backgroundColor: "#ffffff", style: { position: "static", left: "auto", top: "auto" } }), "Label image preparation timed out. Please try again.");
  } finally { exportLabel.remove(); }
}

function openLabelPrintWindow() {
  const target = window.open("about:blank", "_blank");
  if (!target) throw new Error("Allow the label print preview to open in a new tab, then try again.");
  target.document.title = "Preparing Evidence Labels";
  target.document.body.textContent = "Preparing label PDF…";
  return target;
}

export async function outputFamilyLabels({ family, api, action, printer, printWindow, onProgress = () => {} }) {
  onProgress("Loading exhibit labels…");
  const [config, rows] = await withLabelTimeout(Promise.all([
    api("/api/settings/json/evidence_label"),
    Promise.all(family.members.map(async member => ({ ...await api(`/api/${member.source === "progress" ? "in-progress" : "cases"}/${member.id}`), source: member.source }))),
  ]), "Loading the case labels timed out. Check CyberLab is running and try again.");
  if (!rows.length) throw new Error("This case has no evidence items.");
  const settings = normalizeLabelSettings(config.value);
  const container = document.createElement("div");
  container.style.cssText = "position:absolute;left:-10000px;top:0";
  container.setAttribute("aria-hidden", "true");
  const style = document.createElement("style"); style.textContent = labelStyles;
  container.appendChild(style); document.body.appendChild(container);
  try {
    const items = [];
    for (const row of rows) {
      const data = savedLabelFromCase(row);
      const qrImage = data.barcode.trim() ? await QRCode.toDataURL(data.barcode, { errorCorrectionLevel: "M", margin: 2, width: 600 }) : "";
      const holder = document.createElement("div");
      holder.innerHTML = renderToStaticMarkup(<EvidenceLabelFace {...data} settings={settings} qrImage={qrImage} />);
      container.appendChild(holder);
      const node = holder.querySelector(".evidence-label");
      const error = fitLabelNode(node);
      if (error) throw new Error(`${row.case_number}: ${error} Open this exhibit’s label to adjust its fields.`);
      items.push({ row, data, node });
    }
    // Validate every label before saving or starting a batch output.
    const urls = [], files = {};
    for (const [index, item] of items.entries()) {
      onProgress(`Preparing label ${index + 1} of ${items.length}…`);
      const url = await createLabelPng(item.node, settings);
      await saveLabelRecord(api, item.row, item.data);
      urls.push(url);
      if (action === "download") {
        const bytes = Uint8Array.from(atob(url.split(",")[1]), char => char.charCodeAt(0));
        files[`${String(index + 1).padStart(3, "0")}-evidence-${safeFilename(item.row.case_number)}.png`] = bytes;
      }
    }
    if (action === "print") {
      const result = await sendLabelsToPrinter(urls, settings, printer, api);
      return `${result.count} labels sent to ${result.printer}.`;
    }
    if (action === "pdf") await openPreparedLabelPdf(urls, settings, printWindow, api);
    else {
      const url = URL.createObjectURL(new Blob([zipSync(files, { level: 0 })], { type: "application/zip" }));
      downloadUrl(url, `evidence-labels-${safeFilename(family.base_case_number)}.zip`);
      setTimeout(() => URL.revokeObjectURL(url), 60000);
    }
    return `${rows.length} labels ${action === "pdf" ? "opened in a PDF print preview with no headers or footers" : "downloaded as separate PNGs in a ZIP file"}.`;
  } finally { container.remove(); }
}

export function FamilyLabelActions({ family, api }) {
  const [busy, setBusy] = useState(false), [message, setMessage] = useState("");
  const [preferred, setPreferred] = useState("");
  const printers = useLabelPrinters(api, preferred);
  useEffect(() => { api("/api/settings/json/evidence_label").then(result => setPreferred(result.value?.printer_name || "")).catch(() => {}); }, []);
  async function output(action) {
    setBusy(true);
    let printWindow;
    try {
      if (action === "pdf") printWindow = openLabelPrintWindow();
      setMessage(await outputFamilyLabels({ family, api, action, printer: printers.printer, printWindow, onProgress: message => { setMessage(message); printWindowStatus(printWindow, message); } }));
    }
    catch (error) { printWindowStatus(printWindow, `Could not prepare the labels: ${error.message}`); setMessage(`Label output failed: ${error.message}`); }
    finally { setBusy(false); }
  }
  return <section aria-label="Case label output"><LabelPrinterSelect info={printers} /><div className="form-actions">
    <button type="button" className="ghost-action" disabled={busy || !printers.printer || !family.members?.length} onClick={() => output("print")}>Print All Labels</button>
    <button type="button" className="ghost-action" disabled={busy || !family.members?.length} onClick={() => output("pdf")}>Open All Labels PDF</button>
    <button type="button" className="ghost-action" disabled={busy || !family.members?.length} onClick={() => output("download")}>Download All PNGs</button>
  </div><small>Print All Labels sends every completed and in-progress exhibit directly to the selected printer. Downloads contain a separate PNG per exhibit in one ZIP file.</small><p role="status">{message}</p></section>;
}
