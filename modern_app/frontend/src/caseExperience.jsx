import React, { useEffect, useRef, useState } from "react";

export { validateCase } from "./caseValidation";

export function StatusBadge({ value }) {
  const tone = value === "Completed" ? "complete" : ["Critical", "High"].includes(value) ? "urgent" : "active";
  return <span className={`case-status ${tone}`}>{value || "In-Progress"}</span>;
}

export function useListPreferences(key) {
  const [preferences, setPreferences] = useState(() => {
    try { const stored = JSON.parse(localStorage.getItem(key) || "{}"); return { density: stored?.density === "compact" ? "compact" : "comfortable", hidden: Array.isArray(stored?.hidden) ? stored.hidden : [] }; }
    catch { return { density: "comfortable", hidden: [] }; }
  });
  useEffect(() => { localStorage.setItem(key, JSON.stringify(preferences)); }, [key, preferences]);
  return [preferences, setPreferences];
}

export function ListControls({ columns, preferences, setPreferences }) {
  return <div className="list-controls">
    <label>Row spacing <select value={preferences.density} onChange={event => setPreferences(current => ({ ...current, density: event.target.value }))}><option value="comfortable">Comfortable</option><option value="compact">Compact</option></select></label>
    <details className="column-picker"><summary>Columns</summary><div>{columns.map(([key, label]) => <label key={key}><input type="checkbox" disabled={key === "case_number"} checked={key === "case_number" || !preferences.hidden.includes(key)} onChange={event => setPreferences(current => ({ ...current, hidden: event.target.checked ? current.hidden.filter(value => value !== key) : [...current.hidden, key] }))} />{label}</label>)}</div></details>
  </div>;
}

export function IntakeItems({ family, onEdit }) {
  if (!family?.members?.length) return null;
  return <section className="intake-items" aria-label="Saved evidence items"><h3>Saved items for {family.base_case_number}</h3><div className="intake-strip">{family.members.map(row => <button type="button" className="intake-item" key={`${row.source}-${row.id}`} onClick={() => onEdit(row)}><strong>{row.case_number}</strong><span>{[row.device_type, row.model].filter(Boolean).join(" / ") || "Device not entered"}</span><StatusBadge value={row.source === "completed" ? "Completed" : "In-Progress"} /><span>Edit item</span></button>)}</div></section>;
}

export function CaseWorkspace({ family, error, onClose, onEdit, onView, onNext, busy }) {
  const dialogRef = useRef(null);
  useEffect(() => {
    const previous = document.activeElement;
    const dialog = dialogRef.current;
    dialog.querySelector("button")?.focus();
    const handleKey = event => {
      if (event.key === "Escape") onClose();
      if (event.key !== "Tab") return;
      const controls = [...dialog.querySelectorAll("button:not(:disabled), input, select, a[href]")];
      const first = controls[0], last = controls[controls.length - 1];
      if (event.shiftKey && document.activeElement === first) { event.preventDefault(); last?.focus(); }
      if (!event.shiftKey && document.activeElement === last) { event.preventDefault(); first?.focus(); }
    };
    dialog.addEventListener("keydown", handleKey);
    return () => { dialog.removeEventListener("keydown", handleKey); previous?.focus(); };
  }, []);
  const first = family.members?.[0] || {};
  const shared = [["Agency", "agency"], ["Examiner", "examiner"], ["Investigator", "investigator"], ["Subject", "investigation_subject"], ["Offense", "offense_type"]];
  return <div className="modal-backdrop"><section ref={dialogRef} className="case-modal case-workspace" role="dialog" aria-modal="true" aria-labelledby="workspace-title">
    <header className="section-heading"><div><h2 id="workspace-title">Case {family.base_case_number}</h2><p>{family.members.length} evidence items · {family.completed_count} completed · {family.active_count} in progress</p></div><button className="ghost-action" onClick={onClose}>Close</button></header>
    {error && <p role="alert" className="workspace-error">{error}</p>}
    <section className="workspace-shared"><h3>Case information</h3><dl>{shared.map(([label, key]) => {
      const values = [...new Set(family.members.map(row => row[key]).filter(Boolean))];
      return <div key={key}><dt>{label}</dt><dd>{values.join(" / ") || "Not entered"}{values.length > 1 && <small>Varies between items</small>}</dd></div>;
    })}</dl></section>
    <section><h3>Evidence items</h3><div className="workspace-items">{family.members.map(row => <article key={`${row.source}-${row.id}`}><div><strong>{row.case_number}</strong><p>{[row.device_type, row.model].filter(Boolean).join(" / ") || "Device not entered"}</p><StatusBadge value={row.source === "completed" ? "Completed" : "In-Progress"} />{row.source === "progress" && <span className="workflow-label">{row.workflow_status || "Intake"}</span>}</div><div className="form-actions"><button className="ghost-action" onClick={() => onView(row)} disabled={busy}>View</button><button className="ghost-action" onClick={() => onEdit(row)} disabled={busy}>Edit</button></div></article>)}</div></section>
    <footer className="form-actions"><button className="primary-action" onClick={() => onNext(first)} disabled={busy}>Add next evidence item</button></footer>
  </section></div>;
}
