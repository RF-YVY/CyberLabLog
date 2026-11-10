export const labelFields = [["exhibit", "EXHIBIT"], ["case", "Case"], ["date", "Date"], ["type", "Type"], ["brand", "Brand/Model"], ["serial", "S/N"], ["location", "Location"], ["description", "Description"], ["agency", "Agency"], ["subject", "Investigation Subject"]];

export function normalizeLabelSettings(value = {}) {
  const base = { agency: "", division: "", logo: "", ...value };
  // Older settings had no stock selection. Adopt the new default for those.
  if (base.paper !== "custom") return { ...base, paper: "dymo550", width: 4, height: 2.3125 };
  const dimension = (number, fallback) => Number.isFinite(Number(number)) && Number(number) >= 1 && Number(number) <= 12 ? Number(number) : fallback;
  return { ...base, width: dimension(base.width, 4), height: dimension(base.height, 2.3125) };
}

export function labelFromCase(row = {}) {
  const number = String(row.case_number || "").trim();
  // Match the same numeric evidence suffix used by the case-family workflow.
  const suffix = number.match(/^(.+?-\d{3,})-(\d+)$/);
  const exhibit = suffix ? suffix[2] : number ? "1" : row.evidence_number || "";
  return { exhibit, case: row.case_number || "", agency: row.agency || "", date: String(row.start_date || "").slice(0, 10), type: row.device_type || "", brand: row.model || row.brand || "", serial: row.serial_number || "", location: row.storage_location || row.location_name || "", description: row.description || row.notes || "", subject: row.investigation_subject || "" };
}

export function visibleLabelFields(values, extra = []) {
  return [...labelFields.map(([key, label]) => ({ label, value: values[key] })), ...extra].filter(item => String(item.label || "").trim() && String(item.value ?? "").trim());
}

export function savedLabelFromCase(row = {}) {
  let saved = row.evidence_label;
  try { if (typeof saved === "string") saved = JSON.parse(saved); } catch { saved = null; }
  const defaults = labelFromCase(row);
  const values = { ...defaults, ...(saved?.values || {}) };
  if (!String(values.exhibit ?? "").trim()) values.exhibit = defaults.exhibit;
  return {
    values,
    extra: Array.isArray(saved?.extra) ? saved.extra : [],
    barcode: typeof saved?.barcode === "string" ? saved.barcode : row.case_number || "",
  };
}
