import test from "node:test";
import assert from "node:assert/strict";
import { nextEvidenceForm } from "./evidenceEntry.js";
import { validateCase, numberOrNull } from "./caseValidation.js";

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
