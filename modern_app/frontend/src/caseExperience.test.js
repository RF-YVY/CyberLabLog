import test from "node:test";
import assert from "node:assert/strict";
import { nextEvidenceForm } from "./evidenceEntry.js";
import { validateCase, numberOrNull } from "./caseValidation.js";
import { labelFromCase, savedLabelFromCase, visibleLabelFields, normalizeLabelSettings } from "./evidenceLabel.js";
import { openPreparedLabelPdf, withLabelTimeout, sendLabelsToPrinter } from "./labelPrint.js";

test("direct label printing sends exact dimensions and the selected printer without a PDF window", async () => {
  let sent;
  const result = await sendLabelsToPrinter(["label-image"], { width: 4, height: 2.3125 }, "DYMO LabelWriter 550", async (path, options) => { sent = { path, body: JSON.parse(options.body) }; return { success: true, count: 1 }; });
  assert.equal(sent.path, "/api/evidence-labels/print-direct");
  assert.deepEqual(sent.body, { images: ["label-image"], width: 4, height: 2.3125, printer_name: "DYMO LabelWriter 550" });
  assert.equal(result.count, 1);
  await assert.rejects(sendLabelsToPrinter([], { width: 4, height: 2 }, "", () => { throw new Error("Must not send"); }), /Select a label printer/);
});

test("print preview uses an ordinary HTTP PDF and leaves working fallback links if Edge blocks navigation", async () => {
  const links = [], requests = [];
  const target = { closed: false, document: { body: { textContent: "Preparing PDF", appendChild: item => links.push(item) }, createElement: () => ({ style: {} }) }, location: { replace: () => { throw new Error("Navigation restricted"); } } };
  await openPreparedLabelPdf(["data:image/png;base64,test"], { width: 4, height: 2.3125 }, target, async (path, options) => { requests.push({ path, options }); return { url: "http://127.0.0.1:8768/api/evidence-labels/print-pdf/abcd.pdf" }; });
  assert.equal(requests[0].path, "/api/evidence-labels/print-pdf");
  assert.deepEqual(JSON.parse(requests[0].options.body), { images: ["data:image/png;base64,test"], width: 4, height: 2.3125 });
  assert.match(target.document.body.textContent, /ready/);
  assert.equal(links.length, 2);
  assert.match(links[0].href, /^http:/);
  assert.match(links[1].href, /download=true$/);
});

test("label preparation rejects stalled operations instead of leaving a waiting tab forever", async () => {
  await assert.rejects(withLabelTimeout(new Promise(() => {}), "Preparation timed out", 5), /Preparation timed out/);
  assert.equal(await withLabelTimeout(Promise.resolve("ready"), "timeout", 5), "ready");
});

test("saved exhibit labels restore extra fields, QR values, and deliberately blank fields", () => {
  const label = { values: { serial: "SN-123", description: "" }, extra: [{ label: "Shelf", value: "B2" }], barcode: "custom-qr" };
  const restored = savedLabelFromCase({ case_number: "CC-26-1234-1", notes: "Do not restore these notes", evidence_label: JSON.stringify(label) });
  assert.equal(restored.values.exhibit, "1");
  assert.equal(restored.values.serial, "SN-123");
  assert.equal(restored.values.description, "");
  assert.deepEqual(restored.extra, label.extra);
  assert.equal(restored.barcode, "custom-qr");
  assert.equal(nextEvidenceForm({ evidence_label: label }, "CC-26-1234-2").evidence_label, null);
});

test("label stock defaults to Dymo shipping labels and preserves explicit custom sizes", () => {
  assert.deepEqual([normalizeLabelSettings().width, normalizeLabelSettings().height], [4, 2.3125]);
  assert.equal(normalizeLabelSettings({ width: 6, height: 4 }).height, 2.3125);
  const custom = normalizeLabelSettings({ paper: "custom", width: "6", height: "4", agency: "Agency", logo: "image" });
  assert.deepEqual([custom.width, custom.height, custom.agency, custom.logo], [6, 4, "Agency", "image"]);
  assert.equal(normalizeLabelSettings({ paper: "custom", width: -1, height: Infinity }).width, 4);
});

test("exhibit is inferred from an evidence suffix without treating the base case number as an exhibit", () => {
  assert.equal(labelFromCase({ case_number: "CC-26-1234-1" }).exhibit, "1");
  assert.equal(labelFromCase({ case_number: "CC-26-1234-2" }).exhibit, "2");
  assert.equal(labelFromCase({ case_number: "CC-26-1234" }).exhibit, "");
  assert.equal(labelFromCase({ case_number: "CC-26-1234-A" }).exhibit, "");
  assert.equal(labelFromCase({ case_number: "CC-26-1234", evidence_number: "7" }).exhibit, "7");
});

test("evidence labels populate case data without mutating the case", () => {
  const row = Object.freeze({ case_number: "26-101-A", start_date: "2026-11-10", agency: "Agency", device_type: "Phone", model: "Model", notes: "Device description", investigation_subject: "Subject" });
  const label = labelFromCase(row);
  assert.equal(label.case, "26-101-A");
  assert.equal(label.date, "2026-11-10");
  assert.equal(label.type, "Phone");
  assert.equal(label.brand, "Model");
  assert.equal(label.description, "Device description");
  assert.equal(label.subject, "Subject");
  label.case = "Edited label";
  assert.equal(row.case_number, "26-101-A");
});

test("blank label fields disappear completely, including empty custom fields", () => {
  const fields = visibleLabelFields({ case: "26-101", serial: "  ", exhibit: "" }, [{ label: "Custodian", value: "Alex" }, { label: "Blank", value: " " }, { label: "", value: "Hidden" }]);
  assert.deepEqual(fields, [{ label: "Case", value: "26-101" }, { label: "Custodian", value: "Alex" }]);
  assert.deepEqual(visibleLabelFields(labelFromCase()), []);
});

test("editing an unresolved location keeps null coordinates instead of creating Null Island", () => {
  for (const value of [null, undefined, "", "   "]) assert.equal(numberOrNull(value), null);
  assert.equal(numberOrNull(0), 0);
  assert.equal(numberOrNull("0"), 0);
  assert.equal(numberOrNull("34.25"), 34.25);
});
import { startBrowserSession } from "./browserSession.js";

test("page navigation releases only its session, never sends a server shutdown", async () => {
  const handlers = new Map(), requests = [], beacons = [];
  const browser = { setInterval: () => 1, clearInterval() {}, addEventListener: (name, fn) => handlers.set(name, fn), removeEventListener: name => handlers.delete(name) };
  const fetcher = async (url, options) => { requests.push([url, options]); return { ok: true, json: async () => ({ instance_id: "server-one" }) }; };
  const stop = startBrowserSession({ apiBase: "http://test", browser, fetcher, sessionId: "page-one", navigatorObject: { sendBeacon: (url, body) => { beacons.push([url, body]); return true; } } });
  await new Promise(resolve => setImmediate(resolve));
  handlers.get("pagehide")();
  assert.equal(beacons[0][0], "http://test/api/runtime/session/close");
  assert.deepEqual(JSON.parse(await beacons[0][1].text()), { session_id: "page-one", instance_id: "server-one" });
  await handlers.get("pageshow")();
  assert.equal(requests.length, 2);
  assert.ok(requests.every(([url]) => !url.endsWith("/shutdown")));
  stop();
  assert.equal(handlers.size, 0);
});

test("next evidence item preserves shared fields without carrying item results or edit identity", () => {
  const original = { id: 17, case_number: "CC-26-123456-1", examiner: "Examiner", investigator: "Investigator", agency: "Agency", investigation_subject: "Subject", start_date: "2026-10-12", end_date: "2026-10-12", notes: "Case notes", forensic_tool: "Tool", device_type: "iOS", model: "iPhone", os: "iOS", volume_size_gb: 128, data_recovered: "Yes", fpr_complete: true, custom_fields: { shared: "value" }, _errors: { case_number: "Old error" } };
  const next = nextEvidenceForm(original, "CC-26-123456-3");
  assert.equal(next.case_number, "CC-26-123456-3");
  for (const key of ["examiner", "investigator", "agency", "investigation_subject", "start_date", "end_date", "notes", "forensic_tool"]) assert.equal(next[key], original[key]);
  for (const key of ["device_type", "model", "os", "volume_size_gb", "data_recovered"]) assert.equal(next[key], "");
  assert.equal(next.fpr_complete, false);
  assert.equal("id" in next, false);
  assert.equal("_errors" in next, false);
  next.custom_fields.shared = "changed";
  assert.equal(original.custom_fields.shared, "value");
});

test("validation accepts optional empty fields and zero coordinates and volume", () => {
  assert.deepEqual(validateCase({ case_number: "CC-26-123456-1", latitude: "", longitude: "", volume_size_gb: "" }), {});
  assert.deepEqual(validateCase({ case_number: "CC-26-123456-1", latitude: 0, longitude: 0, volume_size_gb: 0, start_date: "2026-10-12", end_date: "2026-10-12" }), {});
});

test("validation rejects missing numbers, negative volume, partial or out-of-range GPS and reversed dates", () => {
  const errors = validateCase({ case_number: "  ", volume_size_gb: -1, latitude: 91, longitude: 181, start_date: "2026-10-12", end_date: "2026-10-11" });
  assert.deepEqual(Object.keys(errors).sort(), ["case_number", "end_date", "latitude", "longitude", "volume_size_gb"]);
  assert.ok(validateCase({ case_number: "Case", latitude: 0, longitude: "" }).longitude);
  assert.ok(validateCase({ case_number: "Case", volume_size_gb: "invalid" }).volume_size_gb);
});
