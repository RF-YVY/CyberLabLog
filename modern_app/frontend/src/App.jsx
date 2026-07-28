import React, { useEffect, useMemo, useState } from "react";
import {
  Activity,
  BarChart3,
  CheckCircle2,
  Database,
  FileText,
  FileUp,
  FolderOpen,
  MapPinned,
  MapPin,
  Pencil,
  Plus,
  RefreshCw,
  Save,
  Search,
  Settings,
  ShieldCheck,
  Sparkles,
  Trash2,
  X,
  Copy,
  Download,
  Eye,
  FileCheck2,
  History,
  Layers3,
  LayoutDashboard,
  PackageSearch,
  TriangleAlert,
} from "lucide-react";
import { createRoot } from "react-dom/client";
import { MapContainer, Marker, Popup, TileLayer, useMap } from "react-leaflet";
import L from "leaflet";
import "leaflet/dist/leaflet.css";
import "./styles.css";

function defaultApiBase() {
  if (typeof window === "undefined") {
    return "http://127.0.0.1:8768";
  }
  if (import.meta.env.DEV) {
    return "http://127.0.0.1:8768";
  }
  return window.location.origin;
}

const API_BASE = import.meta.env.VITE_API_BASE || defaultApiBase();

const blankCase = {
  case_number: "",
  examiner: "",
  investigator: "",
  agency: "",
  city_of_offense: "",
  state_of_offense: "",
  start_date: "",
  end_date: "",
  volume_size_gb: "",
  offense_type: "",
  device_type: "",
  model: "",
  os: "",
  forensic_tool: "",
  data_recovered: "",
  fpr_complete: false,
  notes: "",
  custom_fields: {},
  priority: "Medium",
  target_due_date: "",
  workflow_status: "Intake",
};

function defaultCustomReport() {
  const today = new Date();
  const localDate = (value) => `${value.getFullYear()}-${String(value.getMonth() + 1).padStart(2, "0")}-${String(value.getDate()).padStart(2, "0")}`;
  return {
    start_date: localDate(new Date(today.getFullYear(), today.getMonth(), 1)),
    end_date: localDate(today),
    filter_field: "examiner",
    filter_value: "",
    format: "pdf",
  };
}

const builtInNavItems = [
  { key: "cases", label: "Cases", required: true },
  { key: "new", label: "New Case" },
  { key: "progress", label: "In Progress" },
  { key: "reports", label: "Reports" },
  { key: "graphs", label: "Graphs" },
  { key: "map", label: "Map" },
  { key: "settings", label: "Settings", required: true },
];

const caseFieldDefinitions = [
  ["case_number", "Case #"],
  ["examiner", "Examiner"],
  ["investigator", "Investigator"],
  ["agency", "Agency"],
  ["city_of_offense", "City of Offense"],
  ["state_of_offense", "State"],
  ["start_date", "Start Date"],
  ["end_date", "End Date"],
  ["volume_size_gb", "Volume (GB)"],
  ["offense_type", "Offense Type"],
  ["device_type", "Device Type"],
  ["model", "Model"],
  ["os", "OS"],
  ["forensic_tool", "Forensic Tool"],
  ["data_recovered", "Data Recovered"],
  ["fpr_complete", "FPR Complete"],
  ["notes", "Notes"],
  ["priority", "Priority"],
  ["workflow_status", "Workflow"],
  ["target_due_date", "Target Due Date"],
];

const themes = [
  ["cyber-blue", "Cyber Blue"],
  ["signal-dark", "Signal Dark"],
  ["day-shift", "Day Shift"],
  ["nebula-console", "Nebula Console"],
  ["tripwire-neon", "Tripwire Neon"],
  ["light-blue", "Light Blue"],
  ["cyan-hud", "Cyan HUD"],
  ["ember-focus", "Ember Focus"],
  ["soft-relief", "Soft Relief"],
  ["soft-relief-dark", "Soft Relief Dark"],
  ["spectrum-glass", "Spectrum Glass"],
  ["electric-azure", "Electric Azure"],
  ["aqua-command", "Aqua Command"],
  ["violet-flow", "Violet Flow"],
  ["carbon-intelligence", "Carbon Intelligence"],
];

const defaultThemePreferences = {
  theme: "cyber-blue",
  animations_enabled: true,
};

const reportTypeOptions = [
  ["total_summary_pdf", "Total Summary PDF"],
  ["total_summary_pdf_scope", "Scoped Summary PDF"],
  ["total_summary_xlsx", "Total Summary Excel"],
  ["all_cases_pdf", "All Cases PDF"],
  ["graphs_snapshot", "Graph Snapshots"],
  ["map_html", "Map HTML"],
];

const reportOutputFolderOptions = [
  ["reports", "Reports Folder"],
  ...reportTypeOptions,
];

const graphTypeOptions = [
  "Offense Type",
  "Device Type",
  "Agency",
  "Investigator",
  "Forensic Tool",
  "City of Offense",
  "State of Offense",
  "Total Volume by Examiner",
  "Total Volume by Investigator",
  "Total Volume by Agency",
  "Total Volume by Device Type",
];

const deviceTypeOptions = ["", "iOS", "Android", "Windows", "ChromeOS", "USB", "SD", "SSD", "HDD", "SIM", "NAS", "Laptop", "Digital Camera", "Other"];

const deviceTypeAliases = {
  "android": "Android",
  "chrome os": "ChromeOS",
  "chromeos": "ChromeOS",
  "digital camera": "Digital Camera",
  "hdd": "HDD",
  "ios": "iOS",
  "iphone": "iOS",
  "ipad": "iOS",
  "laptop": "Laptop",
  "nas": "NAS",
  "other": "Other",
  "sd": "SD",
  "sdd": "SSD",
  "ssd": "SSD",
  "sim": "SIM",
  "usb": "USB",
  "windows": "Windows",
};

const analyticsGraphCards = {
  offenses: { title: "Top Offenses", metric: "Cases" },
  agencies: { title: "Top Agencies", metric: "Cases" },
  devices: { title: "Device Types", metric: "Cases" },
  examiners: { title: "Examiners", metric: "Cases" },
  investigators: { title: "Investigators", metric: "Cases" },
  cities: { title: "Cities of Offense", metric: "Cases" },
  states: { title: "States of Offense", metric: "Cases" },
  tools: { title: "Forensic Tools", metric: "Cases" },
  models: { title: "Device Models", metric: "Cases" },
  operating_systems: { title: "Operating Systems", metric: "Cases" },
  data_recovered: { title: "Data Recovered", metric: "Cases" },
  volume_by_examiner: { title: "Volume by Examiner", metric: "Volume", valueType: "volume" },
  volume_by_agency: { title: "Volume by Agency", metric: "Volume", valueType: "volume" },
  volume_by_device: { title: "Volume by Device Type", metric: "Volume", valueType: "volume" },
  volume_by_offense: { title: "Volume by Offense", metric: "Volume", valueType: "volume" },
  volume_by_city: { title: "Volume by City", metric: "Volume", valueType: "volume" },
};

const analyticsGraphGroups = [
  ["core", "Core Counts", ["offenses", "agencies", "devices", "examiners"]],
  ["volume", "Volume Totals", ["volume_by_examiner", "volume_by_agency", "volume_by_device", "volume_by_offense"]],
  ["people", "People", ["examiners", "investigators", "agencies", "volume_by_examiner"]],
  ["location", "Location", ["cities", "states", "volume_by_city", "agencies"]],
  ["device_tools", "Devices & Tools", ["devices", "models", "operating_systems", "tools"]],
  ["case_detail", "Case Detail", ["offenses", "data_recovered", "tools", "volume_by_offense"]],
];

const defaultReportConfig = {
  output_dir: "",
  frequency: "manual",
  date_range_mode: "current_week",
  report_types: ["total_summary_pdf", "all_cases_pdf"],
  page_size: "Letter",
  orientation: "Auto",
  recent_only: false,
  recent_days: 31,
  schedule_weekday: "Monday",
  schedule_month_day: "1",
  report_output_dirs: {},
  graph_settings: {
    include_png: true,
    include_csv: false,
    year_filter: "All",
    types: ["Offense Type", "Device Type", "Agency"],
  },
  map_settings: {
    include_completed: true,
    include_in_progress: true,
    include_case_details: true,
    include_data_file: false,
  },
  data_exports: {
    include_completed_csv: false,
    include_in_progress_csv: false,
    include_summary_json: false,
  },
};

function normalizeReportConfig(value) {
  const stored = value && typeof value === "object" ? value : {};
  const legacyOutputDefaults = !Object.prototype.hasOwnProperty.call(stored, "data_exports");
  const merged = {
    ...defaultReportConfig,
    ...stored,
    report_output_dirs: {
      ...defaultReportConfig.report_output_dirs,
      ...(stored.report_output_dirs || {}),
    },
    graph_settings: {
      ...defaultReportConfig.graph_settings,
      ...(stored.graph_settings || {}),
    },
    map_settings: {
      ...defaultReportConfig.map_settings,
      ...(stored.map_settings || {}),
    },
    data_exports: {
      ...defaultReportConfig.data_exports,
      ...(stored.data_exports || {}),
    },
  };
  if (legacyOutputDefaults) {
    merged.graph_settings.include_csv = false;
    merged.map_settings.include_data_file = false;
  }
  return merged;
}

const defaultAppProfile = {
  app_title: "CyberLab Case Tracker",
  organization: "",
  name: "",
};

const defaultUiCustomization = {
  tabs: {},
  custom_tabs: [],
  fields: {},
  custom_fields: [],
};

const defaultMapPreferences = {
  focus: "mississippi",
};

const defaultBrowserPreferences = {
  preferred_browser: "system_default",
};

const browserPreferenceOptions = [
  ["system_default", "System default browser"],
  ["chrome", "Google Chrome"],
  ["edge", "Microsoft Edge"],
  ["auto", "Auto-detect Chrome/Edge"],
];

const mapFocusOptions = [
  ["mississippi", "Mississippi", { bounds: [[30.12, -91.72], [35.02, -88.05]], zoom: 7 }],
  ["united_states", "United States", { bounds: [[24.4, -125.0], [49.4, -66.9]], zoom: 4 }],
  ["case_markers", "Case markers", { zoom: 7 }],
  ["alabama", "Alabama", { bounds: [[30.1, -88.6], [35.1, -84.8]], zoom: 7 }],
  ["arkansas", "Arkansas", { bounds: [[33.0, -94.7], [36.6, -89.6]], zoom: 7 }],
  ["louisiana", "Louisiana", { bounds: [[28.8, -94.1], [33.1, -88.7]], zoom: 7 }],
  ["tennessee", "Tennessee", { bounds: [[34.9, -90.4], [36.7, -81.6]], zoom: 7 }],
  ["world", "World", { bounds: [[-55, -170], [72, 170]], zoom: 2 }],
];

const comboManageOptions = [
  ["examiner", "Examiner"],
  ["investigator", "Investigator"],
  ["agency", "Agency"],
  ["city_of_offense", "City of Offense"],
  ["state_of_offense", "State"],
  ["offense_type", "Offense Type"],
  ["device_type", "Device Type"],
  ["forensic_tool", "Forensic Tool"],
];

function formatValue(value) {
  if (value === null || value === undefined || value === "") return "-";
  return String(value);
}

function formatBytes(bytes) {
  if (!Number.isFinite(bytes)) return "-";
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
}

function formatTimestamp(seconds) {
  if (!seconds) return "-";
  return new Date(seconds * 1000).toLocaleString();
}

function formatVolume(gbValue, digits = 1) {
  const gb = Number(gbValue || 0);
  if (!Number.isFinite(gb)) return "-";
  if (Math.abs(gb) >= 1024) return `${(gb / 1024).toFixed(digits)} TB`;
  return `${gb.toFixed(digits)} GB`;
}

function normalizeDeviceType(value) {
  const cleaned = String(value || "").trim();
  return deviceTypeAliases[cleaned.toLowerCase()] || cleaned;
}

function fileKind(name) {
  const suffix = String(name || "").split(".").pop()?.toUpperCase();
  return suffix && suffix.length <= 5 ? suffix : "FILE";
}

function payloadFromForm(form) {
  return {
    ...form,
    device_type: normalizeDeviceType(form.device_type),
    volume_size_gb: form.volume_size_gb === "" ? null : Number(form.volume_size_gb),
  };
}

function toggleValue(list, value, enabled) {
  const current = Array.isArray(list) ? list : [];
  if (enabled) return current.includes(value) ? current : [...current, value];
  return current.filter((item) => item !== value);
}

function tabLabel(customization, key, fallback) {
  return customization?.tabs?.[key]?.label?.trim() || fallback;
}

function tabVisible(customization, item) {
  if (item.required) return true;
  return customization?.tabs?.[item.key]?.visible !== false;
}

function fieldLabel(customization, key, fallback) {
  return customization?.fields?.[key]?.label?.trim() || fallback;
}

function fieldVisible(customization, key, mode = "completed") {
  if (mode !== "progress" && ["priority", "workflow_status", "target_due_date"].includes(key)) {
    return false;
  }
  return customization?.fields?.[key]?.visible !== false;
}

function parseCustomFields(value) {
  if (!value) return {};
  if (typeof value === "object" && !Array.isArray(value)) return value;
  try {
    const parsed = JSON.parse(String(value));
    return parsed && typeof parsed === "object" && !Array.isArray(parsed) ? parsed : {};
  } catch {
    return {};
  }
}

function formFromRow(row) {
  return {
    ...blankCase,
    ...row,
    custom_fields: parseCustomFields(row.custom_fields),
    volume_size_gb: row.volume_size_gb ?? "",
    device_type: normalizeDeviceType(row.device_type),
    fpr_complete: row.fpr_complete === true || row.fpr_complete === 1,
    data_recovered: row.data_recovered || "",
    priority: row.priority || "Medium",
    workflow_status: row.workflow_status || "Intake",
  };
}

const stickyCaseFields = ["examiner", "investigator", "agency", "city_of_offense", "state_of_offense", "offense_type"];

function retainedCaseDefaults(form) {
  return stickyCaseFields.reduce((next, key) => ({ ...next, [key]: form[key] || "" }), { ...blankCase });
}

function loadDraft(mode) {
  try {
    const value = JSON.parse(localStorage.getItem(`cyberlab-draft-${mode}`) || "null");
    return value?.form ? value : null;
  } catch {
    return null;
  }
}

function hasDraftContent(form) {
  return Boolean(form?.case_number || form?.start_date || form?.model || form?.notes || form?.volume_size_gb);
}

function Field({ label, name, form, setForm, type = "text", options, suggestions = [] }) {
  const value = form[name] ?? "";
  const listId = suggestions.length ? `${name}-suggestions` : undefined;
  const common = {
    value,
    onChange: (event) => setForm((current) => ({ ...current, [name]: event.target.value })),
  };
  return (
    <label className="field">
      <span>{label}</span>
      {options ? (
        <select {...common}>
          {options.map((option) => (
            <option key={option} value={option}>{option || "Select"}</option>
          ))}
        </select>
      ) : (
        <>
          <input type={type} list={listId} {...common} />
          {listId && (
            <datalist id={listId}>
              {suggestions.map((option) => <option key={option} value={option} />)}
            </datalist>
          )}
        </>
      )}
    </label>
  );
}

function CaseForm({
  form,
  setForm,
  onSubmit,
  mode,
  busy,
  onCancelEdit,
  onSaveInProgress,
  onComplete,
  comboValues = {},
  uiCustomization = defaultUiCustomization,
}) {
  const isEditing = Boolean(form.id);
  const label = (key, fallback) => fieldLabel(uiCustomization, key, fallback);
  const visible = (key) => fieldVisible(uiCustomization, key, mode);
  const customFieldDefs = uiCustomization.custom_fields || [];
  const renderField = (key, fallback, props = {}) => (
    visible(key) ? <Field label={label(key, fallback)} name={key} form={form} setForm={setForm} {...props} /> : null
  );
  return (
    <form className="case-form panel-enter" onSubmit={onSubmit}>
      <div className="form-grid">
        {renderField("case_number", "Case #")}
        {renderField("examiner", "Examiner", { suggestions: comboValues.examiner || [] })}
        {renderField("investigator", "Investigator", { suggestions: comboValues.investigator || [] })}
        {renderField("agency", "Agency", { suggestions: comboValues.agency || [] })}
        {renderField("city_of_offense", "City of Offense", { suggestions: comboValues.city_of_offense || [] })}
        {renderField("state_of_offense", "State", { suggestions: comboValues.state_of_offense || [] })}
        {renderField("start_date", "Start Date", { type: "date" })}
        {renderField("end_date", "End Date", { type: "date" })}
        {renderField("volume_size_gb", "Volume (GB)", { type: "number" })}
        {renderField("offense_type", "Offense Type", { suggestions: comboValues.offense_type || [] })}
        {renderField("device_type", "Device Type", { options: deviceTypeOptions })}
        {renderField("model", "Model")}
        {renderField("os", "OS")}
        {renderField("forensic_tool", "Forensic Tool", { suggestions: comboValues.forensic_tool || [] })}
        {mode === "progress" && (
          <>
            {renderField("priority", "Priority", { options: ["Low", "Medium", "High", "Critical"] })}
            {renderField("workflow_status", "Workflow", { options: ["Intake", "Processing", "Reporting", "In Vault", "Ready for Completion"] })}
            {renderField("target_due_date", "Target Due Date", { type: "date" })}
          </>
        )}
        {customFieldDefs.filter((field) => field?.key && field?.visible !== false).map((field) => (
          <Field
            key={field.key}
            label={field.label || field.key}
            name={field.key}
            form={form.custom_fields || {}}
            setForm={(updater) => setForm((current) => ({ ...current, custom_fields: updater(current.custom_fields || {}) }))}
            type={field.type || "text"}
          />
        ))}
      </div>
      {visible("notes") && (
        <label className="field full-width">
          <span>{label("notes", "Notes")}</span>
          <textarea
            value={form.notes || ""}
            onChange={(event) => setForm((current) => ({ ...current, notes: event.target.value }))}
            rows={5}
          />
        </label>
      )}
      <div className="check-row">
        {visible("fpr_complete") && (
          <label>
            <input
              type="checkbox"
              checked={Boolean(form.fpr_complete)}
              onChange={(event) => setForm((current) => ({ ...current, fpr_complete: event.target.checked }))}
            />
            {label("fpr_complete", "FPR Complete")}
          </label>
        )}
        {renderField("data_recovered", "Data Recovered", { options: ["", "Yes", "No"] })}
      </div>
      <div className="form-actions">
        <button className="primary-action" type="submit" disabled={busy}>
          <Save size={17} />
          {isEditing ? "Update Case" : mode === "progress" ? "Save In Progress" : "Save Completed Case"}
        </button>
        {!isEditing && mode === "completed" && onSaveInProgress && (
          <button className="ghost-action" type="button" onClick={onSaveInProgress} disabled={busy}>
            <Save size={17} />
            Place In Progress
          </button>
        )}
        {isEditing && mode === "progress" && onComplete && (
          <button className="complete-action" type="button" onClick={onComplete} disabled={busy}>
            <CheckCircle2 size={17} />
            Completed
          </button>
        )}
        {!isEditing && (
          <button className="ghost-action" type="button" onClick={() => setForm(blankCase)}>
            Clear
          </button>
        )}
        {isEditing && (
          <button className="ghost-action" type="button" onClick={onCancelEdit}>
            <X size={16} />
            Cancel Edit
          </button>
        )}
      </div>
    </form>
  );
}

function App() {
  const [activeTab, setActiveTab] = useState("cases");
  const [theme, setTheme] = useState(() => localStorage.getItem("cyberlab-theme") || "cyber-blue");
  const [animationsEnabled, setAnimationsEnabled] = useState(() => localStorage.getItem("cyberlab-animations") !== "off");
  const [settingsLoaded, setSettingsLoaded] = useState(false);
  const [health, setHealth] = useState(null);
  const [cases, setCases] = useState({ rows: [], total: 0 });
  const [inProgress, setInProgress] = useState({ rows: [], total: 0 });
  const [search, setSearch] = useState("");
  const [sort, setSort] = useState("newest");
  const [busy, setBusy] = useState(false);
  const [status, setStatus] = useState("Ready");
  const [caseForm, setCaseForm] = useState(blankCase);
  const [progressForm, setProgressForm] = useState(blankCase);
  const [reportConfig, setReportConfig] = useState(defaultReportConfig);
  const [customReport, setCustomReport] = useState(defaultCustomReport);
  const [analytics, setAnalytics] = useState({});
  const [analyticsGroup, setAnalyticsGroup] = useState(() => localStorage.getItem("cyberlab-analytics-group") || "core");
  const [mapMarkers, setMapMarkers] = useState([]);
  const [selectedCase, setSelectedCase] = useState(null);
  const [exportResult, setExportResult] = useState(null);
  const [outputFiles, setOutputFiles] = useState({ exists: false, files: [] });
  const [comboValues, setComboValues] = useState({});
  const [logoInfo, setLogoInfo] = useState({ exists: false, path: "" });
  const [markerIconInfo, setMarkerIconInfo] = useState({ exists: false, path: "" });
  const [appInfo, setAppInfo] = useState({ name: "CyberLab Case Tracker", version: "3.0.7", update_available: false });
  const [appProfile, setAppProfile] = useState(defaultAppProfile);
  const [mapPreferences, setMapPreferences] = useState(defaultMapPreferences);
  const [browserPreferences, setBrowserPreferences] = useState(defaultBrowserPreferences);
  const [uiCustomization, setUiCustomization] = useState(defaultUiCustomization);
  const [backups, setBackups] = useState({ backup_dir: "", files: [] });
  const [comboEditor, setComboEditor] = useState({ key: "examiner", value: "" });
  const [schedulerStatus, setSchedulerStatus] = useState({ enabled: false, configured: {} });
  const [dashboard, setDashboard] = useState({ monthly_completed: [], examiner_workload: [] });
  const [dataQuality, setDataQuality] = useState({ issue_count: 0, issues: [] });
  const [caseFamilies, setCaseFamilies] = useState([]);
  const [caseTemplates, setCaseTemplates] = useState([]);
  const [casesView, setCasesView] = useState("cases");
  const [reportPreview, setReportPreview] = useState("");
  const [updateInfo, setUpdateInfo] = useState(null);
  const [workQueue, setWorkQueue] = useState([]);
  const [workQueueFilter, setWorkQueueFilter] = useState(() => localStorage.getItem("cyberlab-work-queue") || "all");
  const [savedDrafts, setSavedDrafts] = useState(() => ({ completed: loadDraft("completed"), progress: loadDraft("progress") }));
  const [showImportWizard, setShowImportWizard] = useState(() => localStorage.getItem("cyberlab-import-wizard-dismissed") !== "1");

  useEffect(() => {
    document.documentElement.dataset.theme = theme;
    localStorage.setItem("cyberlab-theme", theme);
  }, [theme]);

  useEffect(() => {
    document.documentElement.dataset.animations = animationsEnabled ? "on" : "off";
    localStorage.setItem("cyberlab-animations", animationsEnabled ? "on" : "off");
    if (!animationsEnabled) {
      document.querySelectorAll(".shine-surface.shine-active").forEach((surface) => surface.classList.remove("shine-active"));
    }
  }, [animationsEnabled]);

  useEffect(() => {
    if (!settingsLoaded) return;
    api("/api/settings/json/theme_preferences", {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ value: { theme, animations_enabled: animationsEnabled } }),
    }).catch(() => null);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [theme, animationsEnabled, settingsLoaded]);

  useEffect(() => {
    localStorage.setItem("cyberlab-analytics-group", analyticsGroup);
  }, [analyticsGroup]);

  useEffect(() => {
    localStorage.setItem("cyberlab-work-queue", workQueueFilter);
  }, [workQueueFilter]);

  useEffect(() => {
    const timer = window.setTimeout(() => {
      if (!hasDraftContent(caseForm)) return;
      const draft = { form: caseForm, savedAt: new Date().toISOString() };
      localStorage.setItem("cyberlab-draft-completed", JSON.stringify(draft));
      setSavedDrafts((current) => ({ ...current, completed: draft }));
    }, 650);
    return () => window.clearTimeout(timer);
  }, [caseForm]);

  useEffect(() => {
    const timer = window.setTimeout(() => {
      if (!hasDraftContent(progressForm)) return;
      const draft = { form: progressForm, savedAt: new Date().toISOString() };
      localStorage.setItem("cyberlab-draft-progress", JSON.stringify(draft));
      setSavedDrafts((current) => ({ ...current, progress: draft }));
    }, 650);
    return () => window.clearTimeout(timer);
  }, [progressForm]);

  useEffect(() => {
    const shutdownUrl = `${API_BASE}/api/runtime/shutdown`;
    const sendShutdown = () => {
      try {
        navigator.sendBeacon?.(shutdownUrl, new Blob(["{}"], { type: "application/json" }));
      } catch {
        fetch(shutdownUrl, { method: "POST", keepalive: true }).catch(() => {});
      }
    };
    window.addEventListener("pagehide", sendShutdown);
    window.addEventListener("beforeunload", sendShutdown);
    return () => {
      window.removeEventListener("pagehide", sendShutdown);
      window.removeEventListener("beforeunload", sendShutdown);
    };
  }, []);

  async function api(path, options) {
    const response = await fetch(`${API_BASE}${path}`, options);
    if (!response.ok) {
      const detail = await response.text();
      throw new Error(detail || response.statusText);
    }
    return response.json();
  }

  async function refresh() {
    setBusy(true);
    try {
      const [healthData, appInfoData, profileData, uiCustomizationData, mapPreferenceData, browserPreferenceData, themePreferenceData, caseData, progressData, configData, schedulerData, analyticsData, markerData, comboData, logoData, markerIconData, backupData, dashboardData, qualityData, familyData, templateData, workQueueData] = await Promise.all([
        api("/api/health"),
        api("/api/app-info").catch(() => ({ name: "CyberLab Case Tracker", version: "3.0.7", update_available: false })),
        api("/api/settings/json/app_profile").catch(() => ({ value: defaultAppProfile })),
        api("/api/settings/json/ui_customization").catch(() => ({ value: defaultUiCustomization })),
        api("/api/settings/json/map_preferences").catch(() => ({ value: defaultMapPreferences })),
        api("/api/settings/json/browser_preferences").catch(() => ({ value: defaultBrowserPreferences })),
        api("/api/settings/json/theme_preferences").catch(() => ({ value: defaultThemePreferences })),
        api(`/api/cases?search=${encodeURIComponent(search)}&sort=${encodeURIComponent(sort)}&limit=100`),
        api(`/api/in-progress?search=${encodeURIComponent(search)}&limit=100`),
        api("/api/automated-exports/config"),
        api("/api/automated-exports/scheduler").catch(() => ({ enabled: false, configured: {} })),
        api("/api/analytics/summary"),
        api("/api/map/markers"),
        api("/api/settings/combos"),
        api("/api/settings/logo"),
        api("/api/settings/marker-icon"),
        api("/api/backups").catch(() => ({ backup_dir: "", files: [] })),
        api("/api/dashboard").catch(() => ({ monthly_completed: [], examiner_workload: [] })),
        api("/api/data-quality").catch(() => ({ issue_count: 0, issues: [] })),
        api("/api/case-families").catch(() => ({ families: [] })),
        api("/api/templates").catch(() => ({ templates: [] })),
        api("/api/work-queue").catch(() => ({ rows: [] })),
      ]);
      setHealth(healthData);
      setAppInfo(appInfoData || { name: "CyberLab Case Tracker", version: "3.0.7", update_available: false });
      setAppProfile({ ...defaultAppProfile, ...(profileData.value || {}) });
      setUiCustomization({ ...defaultUiCustomization, ...(uiCustomizationData.value || {}) });
      setMapPreferences({ ...defaultMapPreferences, ...(mapPreferenceData.value || {}) });
      setBrowserPreferences({ ...defaultBrowserPreferences, ...(browserPreferenceData.value || {}) });
      if (themePreferenceData.value?.theme) {
        setTheme(themePreferenceData.value.theme);
      }
      if (typeof themePreferenceData.value?.animations_enabled === "boolean") {
        setAnimationsEnabled(themePreferenceData.value.animations_enabled);
      }
      setSettingsLoaded(true);
      setCases(caseData);
      setInProgress(progressData);
      setReportConfig(normalizeReportConfig(configData.value));
      setSchedulerStatus(schedulerData || { enabled: false, configured: {} });
      setAnalytics(analyticsData || { offenses: [], agencies: [], devices: [], examiners: [] });
      setMapMarkers(markerData.markers || []);
      setComboValues(comboData || {});
      setLogoInfo(logoData || { exists: false, path: "" });
      setMarkerIconInfo(markerIconData || { exists: false, path: "" });
      setBackups(backupData || { backup_dir: "", files: [] });
      setDashboard(dashboardData || { monthly_completed: [], examiner_workload: [] });
      setDataQuality(qualityData || { issue_count: 0, issues: [] });
      setCaseFamilies(familyData.families || []);
      setCaseTemplates(templateData.templates || []);
      setWorkQueue(workQueueData.rows || []);
      setStatus("Data refreshed");
    } catch (error) {
      setStatus(`Backend unavailable: ${error.message}`);
    } finally {
      setBusy(false);
    }
  }

  useEffect(() => {
    refresh();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [sort]);

  async function saveCase(event, mode) {
    event.preventDefault();
    const form = mode === "progress" ? progressForm : caseForm;
    await persistCase(form, mode, mode);
  }

  async function saveNewCaseInProgress() {
    await persistCase(caseForm, "progress", "completed");
  }

  async function persistCase(form, mode, draftMode) {
    setBusy(true);
    const isEditing = Boolean(form.id);
    const path = form.id
      ? mode === "progress"
        ? `/api/in-progress/${form.id}`
        : `/api/cases/${form.id}`
      : mode === "progress"
        ? "/api/in-progress"
        : "/api/cases";
    try {
      await api(path, {
        method: form.id ? "PUT" : "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payloadFromForm(form)),
      });
      await saveComboValues(form);
      localStorage.removeItem(`cyberlab-draft-${draftMode}`);
      setSavedDrafts((current) => ({ ...current, [draftMode]: null }));
      if (draftMode === "progress") setProgressForm(isEditing ? blankCase : retainedCaseDefaults(form));
      else setCaseForm(isEditing ? blankCase : retainedCaseDefaults(form));
      setStatus(mode === "progress" && !isEditing ? "Case placed in progress" : "Case saved");
      await refresh();
    } catch (error) {
      setStatus(`Save failed: ${error.message}`);
    } finally {
      setBusy(false);
    }
  }

  async function saveComboValues(form) {
    const entries = ["examiner", "investigator", "agency", "city_of_offense", "state_of_offense", "offense_type", "device_type", "forensic_tool"]
      .map((key) => [key, String(form[key] || "").trim()])
      .filter(([, value]) => value);
    await Promise.all(entries.map(([key, value]) => api(`/api/settings/combos/${key}`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ value }),
    }).catch(() => null)));
  }

  async function removeCase(id, mode = "completed") {
    const label = mode === "progress" ? "in-progress case" : "completed case";
    if (!window.confirm(`Delete this ${label}? This cannot be undone.`)) {
      return;
    }
    setBusy(true);
    try {
      await api(mode === "progress" ? `/api/in-progress/${id}` : `/api/cases/${id}`, { method: "DELETE" });
      setStatus("Case deleted");
      await refresh();
    } catch (error) {
      setStatus(`Delete failed: ${error.message}`);
    } finally {
      setBusy(false);
    }
  }

  async function duplicateCase(row, mode = "completed") {
    setBusy(true);
    try {
      await api(mode === "progress" ? `/api/in-progress/${row.id}/duplicate` : `/api/cases/${row.id}/duplicate`, {
        method: "POST",
      });
      setStatus(`Duplicated ${row.case_number || "case"}`);
      await refresh();
    } catch (error) {
      setStatus(`Duplicate failed: ${error.message}`);
    } finally {
      setBusy(false);
    }
  }

  async function completeCase(id) {
    setBusy(true);
    try {
      await api(`/api/in-progress/${id}/complete`, { method: "POST" });
      setStatus("Moved to completed cases");
      await refresh();
    } catch (error) {
      setStatus(`Completion failed: ${error.message}`);
    } finally {
      setBusy(false);
    }
  }

  async function completeEditedCase() {
    if (!progressForm.id) return;
    if (!window.confirm(`Mark ${progressForm.case_number || "this case"} completed?`)) return;
    setBusy(true);
    try {
      await api(`/api/in-progress/${progressForm.id}/complete`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payloadFromForm(progressForm)),
      });
      await saveComboValues(progressForm);
      localStorage.removeItem("cyberlab-draft-progress");
      setSavedDrafts((current) => ({ ...current, progress: null }));
      setProgressForm(blankCase);
      setStatus("Moved to completed cases");
      await refresh();
    } catch (error) {
      setStatus(`Completion failed: ${error.message}`);
    } finally {
      setBusy(false);
    }
  }

  function editCompletedCase(row) {
    setSelectedCase(null);
    setCaseForm(formFromRow(row));
    setActiveTab("new");
    setStatus(`Editing ${row.case_number || `case ${row.id}`}`);
  }

  function editInProgressCase(row) {
    setSelectedCase(null);
    setProgressForm(formFromRow(row));
    setActiveTab("progress");
    setStatus(`Editing ${row.case_number || `case ${row.id}`}`);
  }

  function cancelInProgressEdit() {
    localStorage.removeItem("cyberlab-draft-progress");
    setSavedDrafts((current) => ({ ...current, progress: null }));
    setProgressForm(blankCase);
    setStatus("Edit canceled");
  }

  function applyCaseTemplate(template, mode) {
    const setForm = mode === "progress" ? setProgressForm : setCaseForm;
    setForm((current) => ({ ...current, ...(template?.payload || {}), id: current.id, case_number: current.case_number }));
    setStatus(`Applied template: ${template.name}`);
  }

  async function saveCaseTemplate(form) {
    const name = window.prompt("Template name");
    if (!name?.trim()) return;
    const excluded = new Set(["id", "case_number", "start_date", "end_date", "volume_size_gb", "notes", "created_at", "updated_at"]);
    const payload = Object.fromEntries(Object.entries(form).filter(([key, value]) => !excluded.has(key) && value !== "" && value !== false));
    try {
      await api("/api/templates", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ name: name.trim(), description: "Reusable case-entry defaults", payload }),
      });
      setStatus(`Saved template: ${name.trim()}`);
      await refresh();
    } catch (error) {
      setStatus(`Template save failed: ${error.message}`);
    }
  }

  async function removeCaseTemplate(template) {
    if (!window.confirm(`Delete the template "${template.name}"?`)) return;
    try {
      await api(`/api/templates/${template.id}`, { method: "DELETE" });
      setStatus("Template deleted");
      await refresh();
    } catch (error) {
      setStatus(`Template delete failed: ${error.message}`);
    }
  }

  async function clearReviewItems(fingerprints) {
    if (!fingerprints.length) return;
    try {
      const result = await api("/api/data-quality/dismiss", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ fingerprints }),
      });
      setDataQuality(result);
      setStatus(`Cleared ${fingerprints.length} review item${fingerprints.length === 1 ? "" : "s"}`);
    } catch (error) {
      setStatus(`Could not clear review items: ${error.message}`);
    }
  }

  async function restoreReviewItems() {
    try {
      const result = await api("/api/data-quality/dismissals", { method: "DELETE" });
      setDataQuality(result);
      setStatus("Restored cleared review items");
    } catch (error) {
      setStatus(`Could not restore review items: ${error.message}`);
    }
  }

  async function openReviewIssue(issue) {
    const record = issue.record_id ? { id: issue.record_id, source: issue.source } : issue.records?.[0];
    if (!record?.id) return;
    try {
      const row = await api(`/api/data-quality/case?source=${encodeURIComponent(record.source)}&record_id=${record.id}`);
      setSelectedCase({ ...row, status: record.source === "progress" ? "In Progress" : "Completed", _mode: record.source === "progress" ? "progress" : "completed" });
    } catch (error) {
      setStatus(`Could not open review case: ${error.message}`);
    }
  }

  async function normalizeReviewIssue(issue) {
    const suggested = issue.variants?.find((value) => value !== value.toLowerCase()) || issue.variants?.[0] || "";
    const canonical = window.prompt(`Use one value for ${String(issue.field || "field").replaceAll("_", " ")}`, suggested);
    if (!canonical?.trim()) return;
    if (!window.confirm(`Replace all listed variants with "${canonical.trim()}" in completed and active cases?`)) return;
    setBusy(true);
    try {
      const result = await api("/api/data-quality/normalize", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ field: issue.field, variants: issue.variants, canonical: canonical.trim() }),
      });
      setStatus(`Normalized ${result.changed} case value${result.changed === 1 ? "" : "s"}`);
      await refresh();
    } catch (error) {
      setStatus(`Normalization failed: ${error.message}`);
    } finally {
      setBusy(false);
    }
  }

  async function mergeReviewDuplicates(issue) {
    const records = issue.records || [];
    if (records.length < 2) return;
    const choices = records.map((record, index) => `${index + 1}. ${record.source === "progress" ? "Active" : "Completed"} / ${record.examiner || "Unassigned"} / ${record.device_type || "No device"} ${record.model || ""} / ${record.created_at || ""}`).join("\n");
    const selected = Number(window.prompt(`Choose the record to keep:\n\n${choices}`, "1"));
    const keeper = records[selected - 1];
    if (!keeper) return;
    if (!window.confirm(`Keep record ${selected} and merge non-empty information from the other ${records.length - 1} record(s)? This removes the duplicate records.`)) return;
    setBusy(true);
    try {
      const result = await api("/api/data-quality/merge-duplicates", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ case_number: issue.case_number, keep_source: keeper.source, keep_id: keeper.id }),
      });
      setStatus(`Merged ${result.removed} duplicate record${result.removed === 1 ? "" : "s"}`);
      await refresh();
    } catch (error) {
      setStatus(`Duplicate merge failed: ${error.message}`);
    } finally {
      setBusy(false);
    }
  }

  function restoreDraft(mode) {
    const draft = savedDrafts[mode];
    if (!draft?.form) return;
    if (mode === "progress") setProgressForm({ ...blankCase, ...draft.form });
    else setCaseForm({ ...blankCase, ...draft.form });
    setStatus("Recovered autosaved draft");
  }

  function discardDraft(mode) {
    localStorage.removeItem(`cyberlab-draft-${mode}`);
    setSavedDrafts((current) => ({ ...current, [mode]: null }));
    setStatus("Autosaved draft discarded");
  }

  async function startNextSubcase(row, mode = "completed") {
    try {
      const result = await api(`/api/case-family/next?case_number=${encodeURIComponent(row.case_number || "")}`);
      const next = {
        ...formFromRow(row),
        id: undefined,
        case_number: result.case_number,
        start_date: "",
        end_date: "",
        volume_size_gb: "",
        device_type: "",
        model: "",
        os: "",
        notes: "",
      };
      if (mode === "progress") {
        setProgressForm(next);
        setActiveTab("progress");
      } else {
        setCaseForm(next);
        setActiveTab("new");
      }
      setSelectedCase(null);
      setStatus(`Ready for ${result.case_number}`);
    } catch (error) {
      setStatus(`Could not create next subcase: ${error.message}`);
    }
  }

  async function checkPortableUpdate() {
    setBusy(true);
    try {
      const result = await api("/api/update/info");
      setUpdateInfo(result);
      setStatus(result.update_available ? `Update ${result.version} is available` : "Application is current");
    } catch (error) {
      setStatus(`Update check failed: ${error.message}`);
    } finally {
      setBusy(false);
    }
  }

  async function downloadPortableUpdate() {
    if (!window.confirm("Download the verified portable update beside this application?")) return;
    setBusy(true);
    try {
      const result = await api("/api/update/download", { method: "POST" });
      setUpdateInfo((current) => ({ ...(current || {}), ...result }));
      setStatus(result.downloaded ? `Update saved: ${result.path}` : result.message);
    } catch (error) {
      setStatus(`Update download failed: ${error.message}`);
    } finally {
      setBusy(false);
    }
  }

  async function saveReportConfig(event) {
    event.preventDefault();
    await persistReportConfig("Automated report settings saved");
  }

  async function persistReportConfig(successMessage = "Automated report settings saved") {
    setBusy(true);
    try {
      await api("/api/automated-exports/config", {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ value: reportConfig }),
      });
      setStatus(successMessage);
      await refresh();
    } catch (error) {
      setStatus(`Report settings failed: ${error.message}`);
    } finally {
      setBusy(false);
    }
  }

  async function saveAppProfile(event) {
    event.preventDefault();
    setBusy(true);
    try {
      await api("/api/settings/json/app_profile", {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ value: appProfile }),
      });
      setStatus("Application profile saved");
      await refresh();
    } catch (error) {
      setStatus(`Profile save failed: ${error.message}`);
    } finally {
      setBusy(false);
    }
  }

  async function saveUiCustomization(next = uiCustomization) {
    setUiCustomization(next);
    try {
      await api("/api/settings/json/ui_customization", {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ value: next }),
      });
      setStatus("Interface customization saved");
    } catch (error) {
      setStatus(`Customization save failed: ${error.message}`);
    }
  }

  function updateTabCustomization(key, patch) {
    const next = {
      ...uiCustomization,
      tabs: {
        ...(uiCustomization.tabs || {}),
        [key]: { ...(uiCustomization.tabs?.[key] || {}), ...patch },
      },
    };
    saveUiCustomization(next);
  }

  function updateFieldCustomization(key, patch) {
    const next = {
      ...uiCustomization,
      fields: {
        ...(uiCustomization.fields || {}),
        [key]: { ...(uiCustomization.fields?.[key] || {}), ...patch },
      },
    };
    saveUiCustomization(next);
  }

  function addCustomTab() {
    const label = window.prompt("Custom tab name");
    if (!label?.trim()) return;
    const key = `tab_${Date.now()}`;
    const next = {
      ...uiCustomization,
      custom_tabs: [...(uiCustomization.custom_tabs || []), { key, label: label.trim(), content: "", visible: true }],
    };
    saveUiCustomization(next);
    setActiveTab(`custom:${key}`);
  }

  function updateCustomTab(key, patch) {
    const next = {
      ...uiCustomization,
      custom_tabs: (uiCustomization.custom_tabs || []).map((tab) => tab.key === key ? { ...tab, ...patch } : tab),
    };
    saveUiCustomization(next);
  }

  function removeCustomTab(key) {
    const next = {
      ...uiCustomization,
      custom_tabs: (uiCustomization.custom_tabs || []).filter((tab) => tab.key !== key),
    };
    saveUiCustomization(next);
    if (activeTab === `custom:${key}`) setActiveTab("settings");
  }

  function addCustomField() {
    const label = window.prompt("Custom field label");
    if (!label?.trim()) return;
    const key = `custom_${Date.now()}`;
    const next = {
      ...uiCustomization,
      custom_fields: [...(uiCustomization.custom_fields || []), { key, label: label.trim(), type: "text", visible: true }],
    };
    saveUiCustomization(next);
  }

  function updateCustomField(key, patch) {
    const next = {
      ...uiCustomization,
      custom_fields: (uiCustomization.custom_fields || []).map((field) => field.key === key ? { ...field, ...patch } : field),
    };
    saveUiCustomization(next);
  }

  function removeCustomField(key) {
    const next = {
      ...uiCustomization,
      custom_fields: (uiCustomization.custom_fields || []).filter((field) => field.key !== key),
    };
    saveUiCustomization(next);
  }

  async function updateMapFocus(focus) {
    const next = { ...mapPreferences, focus };
    setMapPreferences(next);
    try {
      await api("/api/settings/json/map_preferences", {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ value: next }),
      });
      setStatus("Map focus saved");
    } catch (error) {
      setStatus(`Map focus save failed: ${error.message}`);
    }
  }

  async function updateBrowserPreference(preferred_browser) {
    const next = { ...browserPreferences, preferred_browser };
    setBrowserPreferences(next);
    try {
      await api("/api/settings/json/browser_preferences", {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ value: next }),
      });
      setStatus("Browser preference saved for next launch");
    } catch (error) {
      setStatus(`Browser preference save failed: ${error.message}`);
    }
  }

  async function addComboOption(event) {
    event.preventDefault();
    const value = comboEditor.value.trim();
    if (!value) return;
    setBusy(true);
    try {
      const result = await api(`/api/settings/combos/${comboEditor.key}`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ value }),
      });
      setComboValues((current) => ({ ...current, [comboEditor.key]: result.values || [] }));
      setComboEditor((current) => ({ ...current, value: "" }));
      setStatus("Dropdown value saved");
    } catch (error) {
      setStatus(`Dropdown save failed: ${error.message}`);
    } finally {
      setBusy(false);
    }
  }

  async function deleteComboOption(value) {
    if (!value) return;
    setBusy(true);
    try {
      const result = await api(`/api/settings/combos/${comboEditor.key}?value=${encodeURIComponent(value)}`, {
        method: "DELETE",
      });
      setComboValues((current) => ({ ...current, [comboEditor.key]: result.values || [] }));
      setStatus("Dropdown value removed");
    } catch (error) {
      setStatus(`Dropdown delete failed: ${error.message}`);
    } finally {
      setBusy(false);
    }
  }

  async function renameComboOption(value) {
    const nextValue = window.prompt("Rename dropdown value", value);
    if (!nextValue || nextValue.trim() === value) return;
    setBusy(true);
    try {
      const result = await api(`/api/settings/combos/${comboEditor.key}`, {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ old_value: value, new_value: nextValue.trim() }),
      });
      setComboValues((current) => ({ ...current, [comboEditor.key]: result.values || [] }));
      setStatus("Dropdown value renamed");
    } catch (error) {
      setStatus(`Dropdown rename failed: ${error.message}`);
    } finally {
      setBusy(false);
    }
  }

  async function createBackup() {
    setBusy(true);
    try {
      const result = await api("/api/backups/create", { method: "POST" });
      setStatus(`Backup created: ${result.backup?.name || "complete"}`);
      const backupData = await api("/api/backups");
      setBackups(backupData);
    } catch (error) {
      setStatus(`Backup failed: ${error.message}`);
    } finally {
      setBusy(false);
    }
  }

  async function createEncryptedBackup() {
    const password = window.prompt("Encrypted backup password (minimum 8 characters)");
    if (!password) return;
    const confirmation = window.prompt("Confirm encrypted backup password");
    if (confirmation !== password) {
      setStatus("Backup passwords did not match");
      return;
    }
    setBusy(true);
    try {
      const result = await api("/api/backups/encrypted", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ password }),
      });
      setStatus(`Encrypted backup created: ${result.backup?.name || "complete"}`);
      setBackups(await api("/api/backups"));
    } catch (error) {
      setStatus(`Encrypted backup failed: ${error.message}`);
    } finally {
      setBusy(false);
    }
  }

  async function restoreBackup(path) {
    if (String(path).toLowerCase().endsWith(".clbackup")) {
      const password = window.prompt("Password for this encrypted backup");
      if (!password) return;
      if (!window.confirm("Restore this encrypted portable backup? A safety database backup will be created first.")) return;
      setBusy(true);
      try {
        await api("/api/backups/encrypted/restore", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ path, password }),
        });
        setStatus("Encrypted backup verified and restored");
        await refresh();
      } catch (error) {
        setStatus(`Encrypted restore failed: ${error.message}`);
      } finally {
        setBusy(false);
      }
      return;
    }
    if (!window.confirm("Restore this backup database? The current database will be backed up first.")) {
      return;
    }
    setBusy(true);
    try {
      await api("/api/backups/restore", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ path }),
      });
      setStatus("Backup restored");
      await refresh();
    } catch (error) {
      setStatus(`Restore failed: ${error.message}`);
    } finally {
      setBusy(false);
    }
  }

  async function createSupportBundle() {
    setBusy(true);
    try {
      const result = await api("/api/backups/support-bundle", { method: "POST" });
      setStatus(`Support bundle created: ${result.bundle?.name || "complete"}`);
      const backupData = await api("/api/backups");
      setBackups(backupData);
    } catch (error) {
      setStatus(`Support bundle failed: ${error.message}`);
    } finally {
      setBusy(false);
    }
  }

  function updateReportConfig(key, value) {
    setReportConfig((current) => ({ ...current, [key]: value }));
  }

  function updateNestedReportConfig(section, key, value) {
    setReportConfig((current) => ({
      ...current,
      [section]: {
        ...(current[section] || {}),
        [key]: value,
      },
    }));
  }

  function toggleReportType(value, enabled) {
    setReportConfig((current) => ({
      ...current,
      report_types: toggleValue(current.report_types, value, enabled),
    }));
  }

  function toggleGraphType(value, enabled) {
    setReportConfig((current) => ({
      ...current,
      graph_settings: {
        ...(current.graph_settings || {}),
        types: toggleValue(current.graph_settings?.types, value, enabled),
      },
    }));
  }

  function updateReportOutputDir(reportType, value) {
    setReportConfig((current) => ({
      ...current,
      report_output_dirs: {
        ...(current.report_output_dirs || {}),
        [reportType]: value,
      },
    }));
  }

  function customReportUrl({ download = false, format = customReport.format } = {}) {
    const params = new URLSearchParams({
      start_date: customReport.start_date,
      end_date: customReport.end_date,
      filter_field: customReport.filter_field,
      filter_value: customReport.filter_value,
      format,
      download: download ? "true" : "false",
      t: String(Date.now()),
    });
    return `${API_BASE}/api/reports/custom?${params}`;
  }

  function previewCustomReport() {
    if (!customReport.start_date || !customReport.end_date) {
      setStatus("Choose a start and end date for the custom report");
      return;
    }
    setReportPreview(customReportUrl({ format: "pdf" }));
  }

  function exportCustomReport() {
    if (!customReport.start_date || !customReport.end_date) {
      setStatus("Choose a start and end date for the custom report");
      return;
    }
    window.open(customReportUrl({ download: true }), "_blank", "noopener");
    setStatus(`Preparing custom ${customReport.format.toUpperCase()} report`);
  }

  async function chooseOutputFolder(reportType = null, label = "Output Folder") {
    setBusy(true);
    try {
      const currentPath = reportType ? reportConfig.report_output_dirs?.[reportType] : reportConfig.output_dir;
      const result = await api("/api/files/select-folder", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          initial_dir: currentPath || reportConfig.output_dir || "",
          title: `Select ${label}`,
        }),
      });
      if (!result.path) {
        setStatus("Folder selection canceled");
        return;
      }
      if (reportType) {
        updateReportOutputDir(reportType, result.path);
      } else {
        setReportConfig((current) => ({ ...current, output_dir: result.path }));
        await loadOutputFiles(result.path);
      }
      setStatus(`Selected folder: ${result.path}`);
    } catch (error) {
      setStatus(`Folder selection failed: ${error.message}`);
    } finally {
      setBusy(false);
    }
  }

  async function loadOutputFiles(path = reportConfig.output_dir) {
    if (!path) {
      setOutputFiles({ exists: false, files: [] });
      return;
    }
    try {
      const result = await api("/api/files/list-output", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ path }),
      });
      setOutputFiles(result);
    } catch (error) {
      setStatus(`Could not read output folder: ${error.message}`);
    }
  }

  async function uploadFile(path, file, successLabel = "Import complete") {
    if (!file) return;
    if (path.startsWith("/api/import/")) {
      const ok = window.confirm("Importing legacy data will create a safety backup first, then update the active application data. Continue?");
      if (!ok) return;
    }
    const formData = new FormData();
    formData.append("file", file);
    setBusy(true);
    try {
      await api(path, { method: "POST", body: formData });
      setStatus(successLabel);
      if (path.startsWith("/api/import/")) {
        localStorage.setItem("cyberlab-import-wizard-dismissed", "1");
        setShowImportWizard(false);
      }
      await refresh();
    } catch (error) {
      setStatus(`Import failed: ${error.message}`);
    } finally {
      setBusy(false);
    }
  }

  async function runAutomatedExports() {
    setBusy(true);
    setStatus("Running automated exports...");
    try {
      const result = await api("/api/automated-exports/run", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({}),
      });
      setExportResult(result);
      setStatus("Automated exports complete");
      await loadOutputFiles(reportConfig.output_dir);
    } catch (error) {
      setStatus(`Export failed: ${error.message}`);
    } finally {
      setBusy(false);
    }
  }

  async function runNativeExports() {
    setBusy(true);
    setStatus("Running native exports...");
    try {
      const result = await api("/api/native-exports/run", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ value: reportConfig }),
      });
      setExportResult(result);
      const pdfCount = result.pdf_files?.length || 0;
      const reportsPath = result.report_dirs?.reports ? ` / Reports: ${result.report_dirs.reports}` : "";
      setStatus(`Native exports complete: ${result.files?.length || 0} files, ${pdfCount} PDFs${reportsPath}`);
      await loadOutputFiles(result.output_dir || reportConfig.output_dir);
    } catch (error) {
      setStatus(`Native export failed: ${error.message}`);
    } finally {
      setBusy(false);
    }
  }

  const stats = health?.stats || {};
  const totalVolume = useMemo(() => {
    return Number(stats.total_volume_gb || 0);
  }, [stats.total_volume_gb]);
  const focusedActiveRows = useMemo(() => {
    const rows = workQueue.length ? workQueue : inProgress.rows;
    const today = new Date();
    today.setHours(0, 0, 0, 0);
    const daysUntil = (value) => {
      if (!value) return null;
      const date = new Date(`${value}T00:00:00`);
      return Number.isNaN(date.getTime()) ? null : Math.round((date - today) / 86400000);
    };
    if (workQueueFilter === "due_soon") return rows.filter((row) => daysUntil(row.target_due_date) !== null && daysUntil(row.target_due_date) >= 0 && daysUntil(row.target_due_date) <= 7);
    if (workQueueFilter === "overdue") return rows.filter((row) => daysUntil(row.target_due_date) !== null && daysUntil(row.target_due_date) < 0);
    if (workQueueFilter === "awaiting_evidence") return rows.filter((row) => Number(row.evidence_count || 0) === 0);
    if (workQueueFilter === "ready_close") return rows.filter((row) => /ready|complete/i.test(String(row.workflow_status || "")) || Boolean(row.fpr_complete));
    if (workQueueFilter === "mine") return rows.filter((row) => String(row.examiner || "").trim().toLowerCase() === String(appProfile.name || "").trim().toLowerCase());
    return rows;
  }, [appProfile.name, inProgress.rows, workQueue, workQueueFilter]);
  const activeAnalyticsGroup = useMemo(() => {
    return analyticsGraphGroups.find(([key]) => key === analyticsGroup) || analyticsGraphGroups[0];
  }, [analyticsGroup]);
  const visibleAnalyticsCards = useMemo(() => {
    return (activeAnalyticsGroup?.[2] || []).map((key) => ({
      key,
      ...(analyticsGraphCards[key] || { title: key, metric: "Cases" }),
      rows: analytics?.[key] || [],
    }));
  }, [activeAnalyticsGroup, analytics]);
  const headerSubtitle = useMemo(() => {
    const organization = String(appProfile.organization || "").trim();
    const name = String(appProfile.name || "").trim();
    if (organization && name) return `${organization} / ${name}`;
    if (organization) return organization;
    if (name) return name;
    return "Digital forensic case management";
  }, [appProfile.organization, appProfile.name]);
  const appTitle = String(appProfile.app_title || "").trim() || "CyberLab Case Tracker";
  const navItems = useMemo(() => {
    const builtIns = builtInNavItems
      .filter((item) => tabVisible(uiCustomization, item))
      .map((item) => [item.key, tabLabel(uiCustomization, item.key, item.label)]);
    const customTabs = (uiCustomization.custom_tabs || [])
      .filter((tab) => tab?.key && tab?.visible !== false)
      .map((tab) => [`custom:${tab.key}`, tab.label || tab.key]);
    return [...builtIns, ...customTabs];
  }, [uiCustomization]);
  const appVersionLabel = `v${appInfo.version || "3.0.7"}`;

  useEffect(() => {
    if (navItems.length && !navItems.some(([key]) => key === activeTab)) {
      setActiveTab(navItems[0][0]);
    }
  }, [activeTab, navItems]);

  useEffect(() => {
    const clearPointerShine = () => {
      document.querySelectorAll(".shine-surface.shine-active").forEach((surface) => {
        surface.classList.remove("shine-active");
      });
    };
    const clearWhenHidden = () => {
      if (document.hidden) {
        clearPointerShine();
      }
    };
    window.addEventListener("blur", clearPointerShine);
    document.addEventListener("visibilitychange", clearWhenHidden);
    return () => {
      window.removeEventListener("blur", clearPointerShine);
      document.removeEventListener("visibilitychange", clearWhenHidden);
    };
  }, []);

  function trackPointerShine(event) {
    if (!animationsEnabled) return;
    const surface = event.target.closest?.(".shine-surface");
    if (!surface || !event.currentTarget.contains(surface)) {
      clearPointerShine(event.currentTarget);
      return;
    }
    event.currentTarget.querySelectorAll(".shine-surface.shine-active").forEach((activeSurface) => {
      if (activeSurface !== surface) {
        activeSurface.classList.remove("shine-active");
      }
    });
    const rect = surface.getBoundingClientRect();
    surface.style.setProperty("--shine-x", `${event.clientX - rect.left}px`);
    surface.style.setProperty("--shine-y", `${event.clientY - rect.top}px`);
    surface.classList.add("shine-active");
  }

  function handlePointerShineOut(event) {
    const surface = event.target.closest?.(".shine-surface");
    if (!surface || !event.currentTarget.contains(surface)) {
      return;
    }
    const nextSurface = event.relatedTarget?.closest?.(".shine-surface");
    if (nextSurface !== surface) {
      surface.classList.remove("shine-active");
    }
    if (!event.relatedTarget || !event.currentTarget.contains(event.relatedTarget)) {
      clearPointerShine(event.currentTarget);
    }
  }

  function clearPointerShine(root) {
    root.querySelectorAll?.(".shine-surface.shine-active").forEach((surface) => {
      surface.classList.remove("shine-active");
    });
  }

  return (
    <main
      className="app-shell"
      onPointerMove={trackPointerShine}
      onPointerOut={handlePointerShineOut}
      onPointerLeave={(event) => clearPointerShine(event.currentTarget)}
    >
      <header className="topbar panel-enter shine-surface">
        <div className="brand">
          <div className={`brand-mark ${logoInfo.exists ? "has-logo" : ""}`}>
            {logoInfo.exists ? (
              <img
                src={`${API_BASE}/api/settings/logo/image?path=${encodeURIComponent(logoInfo.path || "logo")}&v=${logoInfo.version || 0}`}
                alt="Application logo"
              />
            ) : (
              <ShieldCheck size={25} />
            )}
          </div>
          <div>
            <h1>{appTitle}</h1>
            <p className="brand-subtitle">
              <span>{headerSubtitle}</span>
              <span className="version-pill">{appVersionLabel}</span>
              {appInfo.update_available && (
                <a className="update-badge" href={appInfo.url || `https://github.com/${appInfo.repository || "RF-YVY/CyberLabLog"}`} target="_blank" rel="noreferrer">
                  Update available
                </a>
              )}
            </p>
          </div>
        </div>
        <div className="top-actions">
          <div className="status-pill">
            <Activity size={16} className={busy ? "spin" : ""} />
            <span>{status}</span>
          </div>
        </div>
      </header>

      {showImportWizard && !Number(stats.completed_cases || 0) && !Number(stats.in_progress_cases || 0) && (
        <section className="import-wizard panel-enter shine-surface">
          <div>
            <h2><FileUp size={18} /> Legacy Data Setup</h2>
            <p className="muted">Bring forward your existing CyberLab database, app_data folder, report logo, and map marker. A safety backup is created before import when an active database already exists.</p>
          </div>
          <div className="wizard-actions">
            <label className="file-action">
              <Database size={16} />
              Import caselog_gui_v6.db
              <input type="file" accept=".db" onChange={(event) => uploadFile("/api/import/database", event.target.files?.[0], "Legacy database imported")} />
            </label>
            <label className="file-action">
              <FileUp size={16} />
              Import app_data zip
              <input type="file" accept=".zip" onChange={(event) => uploadFile("/api/import/app-data", event.target.files?.[0], "Legacy app_data imported")} />
            </label>
            <label className="file-action">
              <FileUp size={16} />
              Choose Logo
              <input type="file" accept=".png,.jpg,.jpeg,image/png,image/jpeg" onChange={(event) => uploadFile("/api/settings/logo", event.target.files?.[0], "Report logo updated")} />
            </label>
            <label className="file-action">
              <MapPin size={16} />
              Choose Marker
              <input type="file" accept=".bmp,.png,.jpg,.jpeg,image/bmp,image/png,image/jpeg" onChange={(event) => uploadFile("/api/settings/marker-icon", event.target.files?.[0], "Map marker updated")} />
            </label>
            <button
              className="ghost-action"
              type="button"
              onClick={() => {
                localStorage.setItem("cyberlab-import-wizard-dismissed", "1");
                setShowImportWizard(false);
              }}
            >
              Continue without import
            </button>
          </div>
        </section>
      )}

      <section className="hero-grid">
        <div className="metric-card delay-1 shine-surface">
          <Database size={18} />
          <span>Completed Cases</span>
          <strong>{stats.completed_cases ?? 0}</strong>
        </div>
        <div className="metric-card delay-2 shine-surface">
          <BarChart3 size={18} />
          <span>In Progress</span>
          <strong>{stats.in_progress_cases ?? 0}</strong>
        </div>
        <div className="metric-card delay-3 shine-surface">
          <FileText size={18} />
          <span>Total Volume</span>
          <strong>{formatVolume(totalVolume)}</strong>
        </div>
        <div className="metric-card wide delay-4 shine-surface">
          <span>Legacy Data</span>
          <strong>{stats.database_exists ? "Connected" : "Ready for import"}</strong>
          <small>{health?.database || "Place caselog_gui_v6.db in the app folder or import it in Settings."}</small>
        </div>
      </section>

      <section className="workspace">
        <aside className="side-panel shine-surface">
          {navItems.map(([key, label]) => (
            <button key={key} className={`nav-button ${activeTab === key ? "active" : ""}`} onClick={() => setActiveTab(key)}>
              {label}
            </button>
          ))}

          <div className="import-panel active-summary shine-surface">
            <h2><Sparkles size={15} /> Active Cases</h2>
            {inProgress.rows.slice(0, 4).map((row) => (
              <button key={row.id} type="button" className="active-case-link" onClick={() => editInProgressCase(row)}>
                <strong>{formatValue(row.case_number)}</strong>
                <span>{formatValue(row.workflow_status)} / {formatValue(row.priority)}</span>
              </button>
            ))}
            {!inProgress.rows.length && <p>No in-progress cases.</p>}
          </div>
        </aside>

        <section className="content-panel panel-enter shine-surface" key={activeTab}>
          {activeTab === "cases" && (
            <>
              <Toolbar search={search} setSearch={setSearch} sort={sort} setSort={setSort} refresh={refresh} busy={busy} />
              <OperationalOverview dashboard={dashboard} quality={dataQuality} />
              <div className="segmented-control cases-view-control" aria-label="Case view">
                <button className={casesView === "cases" ? "active" : ""} onClick={() => setCasesView("cases")}><Database size={15} /> Cases</button>
                <button className={casesView === "families" ? "active" : ""} onClick={() => setCasesView("families")}><Layers3 size={15} /> Families</button>
                <button className={casesView === "quality" ? "active" : ""} onClick={() => setCasesView("quality")}><FileCheck2 size={15} /> Review <span>{dataQuality.issue_count || 0}</span></button>
              </div>
              {casesView === "cases" && (
                <CaseTable
                  rows={cases.rows}
                  total={cases.total}
                  onView={(row) => setSelectedCase({ ...row, status: "Completed", _mode: "completed" })}
                  onEdit={editCompletedCase}
                  onDuplicate={(row) => duplicateCase(row)}
                  onDelete={(id) => removeCase(id)}
                  uiCustomization={uiCustomization}
                />
              )}
              {casesView === "families" && <CaseFamilyList families={caseFamilies} onNext={(row) => startNextSubcase(row)} />}
              {casesView === "quality" && <DataQualityList quality={dataQuality} onDismiss={clearReviewItems} onRestore={restoreReviewItems} onOpen={openReviewIssue} onNormalize={normalizeReviewIssue} onMerge={mergeReviewDuplicates} />}
            </>
          )}

          {activeTab === "new" && (
            <section className="content-pad">
              <h2><Plus size={18} /> {caseForm.id ? "Edit Completed Case" : "New Case"}</h2>
              {savedDrafts.completed && !hasDraftContent(caseForm) && <DraftRecoveryBanner draft={savedDrafts.completed} onRestore={() => restoreDraft("completed")} onDiscard={() => discardDraft("completed")} />}
              <CaseTemplateBar templates={caseTemplates} form={caseForm} onApply={(template) => applyCaseTemplate(template, "completed")} onSave={() => saveCaseTemplate(caseForm)} onDelete={removeCaseTemplate} />
              <CaseForm
                form={caseForm}
                setForm={setCaseForm}
                onSubmit={(event) => saveCase(event, "completed")}
                onSaveInProgress={saveNewCaseInProgress}
                onCancelEdit={() => setCaseForm(blankCase)}
                mode="completed"
                busy={busy}
                comboValues={comboValues}
                uiCustomization={uiCustomization}
              />
            </section>
          )}

          {activeTab === "progress" && (
            <section className="content-pad">
              {progressForm.id ? (
                <>
                  <div className="section-heading">
                    <div>
                      <h2><Pencil size={18} /> Edit In-Progress Case</h2>
                      <p className="muted">Update the active case or mark it completed.</p>
                    </div>
                  </div>
                  <CaseTemplateBar templates={caseTemplates} form={progressForm} onApply={(template) => applyCaseTemplate(template, "progress")} onSave={() => saveCaseTemplate(progressForm)} onDelete={removeCaseTemplate} />
                  <CaseForm
                    form={progressForm}
                    setForm={setProgressForm}
                    onSubmit={(event) => saveCase(event, "progress")}
                    onCancelEdit={cancelInProgressEdit}
                    onComplete={completeEditedCase}
                    mode="progress"
                    busy={busy}
                    comboValues={comboValues}
                    uiCustomization={uiCustomization}
                  />
                </>
              ) : (
                <>
                  {savedDrafts.progress && <DraftRecoveryBanner draft={savedDrafts.progress} onRestore={() => restoreDraft("progress")} onDiscard={() => discardDraft("progress")} />}
                  <div className="progress-list-heading">
                    <div>
                      <h2><FileCheck2 size={18} /> In-Progress Cases</h2>
                      <p className="muted">Review active work, open case details, or choose Edit to continue a case.</p>
                    </div>
                    <select value={workQueueFilter} onChange={(event) => setWorkQueueFilter(event.target.value)} aria-label="Focused work queue">
                      <option value="all">All active</option>
                      <option value="due_soon">Due within 7 days</option>
                      <option value="overdue">Overdue</option>
                      <option value="awaiting_evidence">Awaiting evidence</option>
                      <option value="ready_close">Ready to close</option>
                      <option value="mine">My active cases</option>
                    </select>
                  </div>
                  <div className="progress-search-row">
                    <div className="search-box">
                      <Search size={17} />
                      <input
                        value={search}
                        onChange={(event) => setSearch(event.target.value)}
                        onKeyDown={(event) => event.key === "Enter" && refresh()}
                        placeholder="Search active cases..."
                      />
                    </div>
                    <button className="icon-button" onClick={refresh} disabled={busy} title="Refresh">
                      <RefreshCw size={17} className={busy ? "spin" : ""} />
                    </button>
                  </div>
                  <InProgressTable
                    rows={focusedActiveRows}
                    total={inProgress.total}
                    onView={(row) => setSelectedCase({ ...row, status: "In Progress", _mode: "progress" })}
                    onEdit={editInProgressCase}
                    onDuplicate={(row) => duplicateCase(row, "progress")}
                    onComplete={(row) => completeCase(row.id)}
                    onDelete={(id) => removeCase(id, "progress")}
                    uiCustomization={uiCustomization}
                  />
                </>
              )}
            </section>
          )}

          {activeTab === "reports" && (
            <section className="content-pad report-grid">
              <form className="custom-report-builder" onSubmit={(event) => { event.preventDefault(); exportCustomReport(); }}>
                <div className="custom-report-heading">
                  <div><h2><FileCheck2 size={18} /> Custom Workload Report</h2><p className="muted">Completed devices whose End Date falls within the selected period.</p></div>
                  <button className="ghost-action compact" type="button" onClick={() => setCustomReport(defaultCustomReport())}>Current month</button>
                </div>
                <div className="custom-report-fields">
                  <label className="field"><span>From</span><input type="date" value={customReport.start_date} onChange={(event) => setCustomReport((current) => ({ ...current, start_date: event.target.value }))} /></label>
                  <label className="field"><span>Through</span><input type="date" value={customReport.end_date} onChange={(event) => setCustomReport((current) => ({ ...current, end_date: event.target.value }))} /></label>
                  <label className="field"><span>Report By</span><select value={customReport.filter_field} onChange={(event) => setCustomReport((current) => ({ ...current, filter_field: event.target.value, filter_value: "" }))}><option value="examiner">Examiner</option><option value="investigator">Investigator</option><option value="agency">Agency</option></select></label>
                  <label className="field"><span>{customReport.filter_field === "agency" ? "Agency" : customReport.filter_field === "investigator" ? "Investigator" : "Examiner"}</span><select value={customReport.filter_value} onChange={(event) => setCustomReport((current) => ({ ...current, filter_value: event.target.value }))}><option value="">All</option>{(comboValues[customReport.filter_field] || []).map((value) => <option key={value} value={value}>{value}</option>)}</select></label>
                  <label className="field"><span>Format</span><select value={customReport.format} onChange={(event) => setCustomReport((current) => ({ ...current, format: event.target.value }))}><option value="pdf">PDF</option><option value="csv">CSV</option></select></label>
                </div>
                <div className="form-actions custom-report-actions">
                  <button className="ghost-action" type="button" onClick={previewCustomReport}><Eye size={16} /> Preview PDF</button>
                  <button className="primary-action inline" type="submit"><Download size={16} /> Export {customReport.format.toUpperCase()}</button>
                </div>
              </form>
              <form onSubmit={saveReportConfig}>
                <h2><FileText size={18} /> Automated Reports</h2>
                <div className="form-grid two-col">
                  <label className="field path-field">
                    <span>Output Folder</span>
                    <div className="path-input-row">
                      <input
                        value={reportConfig.output_dir || ""}
                        onChange={(event) => setReportConfig((current) => ({ ...current, output_dir: event.target.value }))}
                        placeholder="Choose output folder"
                      />
                      <button className="ghost-action compact" type="button" onClick={() => chooseOutputFolder(null, "Output Folder")} disabled={busy}>
                        <FolderOpen size={16} /> Choose
                      </button>
                    </div>
                  </label>
                  <Field label="Frequency" name="frequency" form={reportConfig} setForm={(fn) => setReportConfig(fn)} options={["manual", "daily", "weekly", "monthly"]} />
                  <Field label="Date Scope" name="date_range_mode" form={reportConfig} setForm={(fn) => setReportConfig(fn)} options={["all", "current_week", "current_month"]} />
                  <Field label="Page Size" name="page_size" form={reportConfig} setForm={(fn) => setReportConfig(fn)} options={["Letter", "Legal", "A4"]} />
                  <Field label="Orientation" name="orientation" form={reportConfig} setForm={(fn) => setReportConfig(fn)} options={["Auto", "Portrait", "Landscape"]} />
                  <Field label="Recent Days" name="recent_days" type="number" form={reportConfig} setForm={(fn) => setReportConfig(fn)} />
                  <Field label="Schedule Time" name="schedule_time" type="time" form={reportConfig} setForm={(fn) => setReportConfig(fn)} />
                  <Field label="Weekly Day" name="schedule_weekday" form={reportConfig} setForm={(fn) => setReportConfig(fn)} options={["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]} />
                  <Field label="Monthly Day" name="schedule_month_day" type="number" form={reportConfig} setForm={(fn) => setReportConfig(fn)} />
                </div>
                <div className="settings-checks compact-checks">
                  <div className="scheduler-heading">
                    <strong className="settings-checks-title">Scheduler</strong>
                    <button className="ghost-action compact" type="button" onClick={() => persistReportConfig("Report schedule saved")} disabled={busy}><Save size={15} /> Save Schedule</button>
                  </div>
                  <label>
                    <input
                      type="checkbox"
                      checked={Boolean(reportConfig.enable_schedule)}
                      onChange={(event) => setReportConfig((current) => ({ ...current, enable_schedule: event.target.checked }))}
                    />
                    Run scheduled exports while the app is open
                  </label>
                  <p className="muted">
                    {schedulerStatus.configured?.enabled
                      ? `Enabled: ${schedulerStatus.configured.frequency || "manual"} at ${schedulerStatus.configured.schedule_time || "08:00"}`
                      : "Disabled"}
                    {schedulerStatus.last_run ? ` / Last run ${new Date(schedulerStatus.last_run).toLocaleString()}` : ""}
                    {schedulerStatus.last_error ? ` / Error: ${schedulerStatus.last_error}` : ""}
                  </p>
                </div>
                <div className="settings-checks compact-checks">
                  <strong className="settings-checks-title">Date Filters</strong>
                  <label>
                    <input
                      type="checkbox"
                      checked={Boolean(reportConfig.recent_only)}
                      onChange={(event) => setReportConfig((current) => ({ ...current, recent_only: event.target.checked }))}
                    />
                    Limit exports to recent cases
                  </label>
                </div>
                <div className="settings-checks">
                  <strong className="settings-checks-title">Reports</strong>
                  {reportTypeOptions.map(([value, label]) => (
                    <label key={value}>
                      <input
                        type="checkbox"
                        checked={(reportConfig.report_types || []).includes(value)}
                        onChange={(event) => toggleReportType(value, event.target.checked)}
                      />
                      {label}
                    </label>
                  ))}
                </div>
                <div className="settings-checks">
                  <strong className="settings-checks-title">Graph Types</strong>
                  {graphTypeOptions.map((value) => (
                    <label key={value}>
                      <input
                        type="checkbox"
                        checked={(reportConfig.graph_settings?.types || []).includes(value)}
                        onChange={(event) => toggleGraphType(value, event.target.checked)}
                      />
                      {value}
                    </label>
                  ))}
                </div>
                <div className="settings-checks">
                  <strong className="settings-checks-title">Output Options</strong>
                  <label><input type="checkbox" checked={Boolean(reportConfig.graph_settings?.include_png)} onChange={(event) => updateNestedReportConfig("graph_settings", "include_png", event.target.checked)} /> Graph PNG</label>
                  <label><input type="checkbox" checked={Boolean(reportConfig.graph_settings?.include_csv)} onChange={(event) => updateNestedReportConfig("graph_settings", "include_csv", event.target.checked)} /> Graph CSV</label>
                  <label><input type="checkbox" checked={Boolean(reportConfig.map_settings?.include_data_file)} onChange={(event) => updateNestedReportConfig("map_settings", "include_data_file", event.target.checked)} /> Map data files</label>
                  <label><input type="checkbox" checked={Boolean(reportConfig.map_settings?.include_case_details)} onChange={(event) => updateNestedReportConfig("map_settings", "include_case_details", event.target.checked)} /> Map case details</label>
                </div>
                <div className="settings-checks">
                  <strong className="settings-checks-title">Raw Data Files</strong>
                  <label><input type="checkbox" checked={Boolean(reportConfig.data_exports?.include_completed_csv)} onChange={(event) => updateNestedReportConfig("data_exports", "include_completed_csv", event.target.checked)} /> Completed cases CSV</label>
                  <label><input type="checkbox" checked={Boolean(reportConfig.data_exports?.include_in_progress_csv)} onChange={(event) => updateNestedReportConfig("data_exports", "include_in_progress_csv", event.target.checked)} /> In-progress cases CSV</label>
                  <label><input type="checkbox" checked={Boolean(reportConfig.data_exports?.include_summary_json)} onChange={(event) => updateNestedReportConfig("data_exports", "include_summary_json", event.target.checked)} /> Summary JSON</label>
                </div>
                <div className="per-report-grid">
                  <strong className="settings-checks-title">Optional Output Folders</strong>
                  <p className="muted">Leave blank to use the main output folder. These are preserved for the export engine as we finish the native scheduler.</p>
                  {reportOutputFolderOptions.map(([value, label]) => (
                    <label className="field path-field" key={value}>
                      <span>{label}</span>
                      <div className="path-input-row">
                        <input
                          value={reportConfig.report_output_dirs?.[value] || ""}
                          onChange={(event) => updateReportOutputDir(value, event.target.value)}
                          placeholder="Optional folder path"
                        />
                        <button className="ghost-action compact" type="button" onClick={() => chooseOutputFolder(value, label)} disabled={busy}>
                          <FolderOpen size={16} /> Choose
                        </button>
                      </div>
                    </label>
                  ))}
                </div>
                <div className="form-actions">
                  <button className="primary-action inline" type="submit" disabled={busy}><Save size={17} /> Save Settings</button>
                  <button className="ghost-action" type="button" onClick={runNativeExports} disabled={busy}><Sparkles size={17} /> Run Export Now</button>
                  <button className="ghost-action" type="button" onClick={() => setReportPreview(`${API_BASE}/api/reports/preview?report_type=all_cases_pdf&t=${Date.now()}`)} disabled={busy}><Eye size={17} /> Preview PDF</button>
                  <button className="ghost-action" type="button" onClick={() => loadOutputFiles()} disabled={!reportConfig.output_dir}><FolderOpen size={17} /> View Output</button>
                </div>
              </form>
              <div className="report-summary-card">
                <h2>Saved Profile</h2>
                <dl className="config-list">
                  <dt>Frequency</dt><dd>{reportConfig.frequency || "weekly"}</dd>
                  <dt>Date scope</dt><dd>{reportConfig.date_range_mode || "current_week"}</dd>
                  <dt>Schedule</dt><dd>{reportConfig.frequency === "monthly" ? `Day ${reportConfig.schedule_month_day || 1}` : reportConfig.frequency === "weekly" ? reportConfig.schedule_weekday || "Monday" : reportConfig.frequency || "manual"}</dd>
                  <dt>Recent only</dt><dd>{reportConfig.recent_only ? `${reportConfig.recent_days || 31} days` : "No"}</dd>
                  <dt>Output</dt><dd>{reportConfig.output_dir || "Legacy app_data/automated_reports"}</dd>
                </dl>
                {exportResult && (
                  <div className="run-result">
                    <strong>Last run</strong>
                    <span>
                      {exportResult.engine === "native"
                        ? `Native export completed with ${exportResult.files?.length || 0} files`
                        : `${exportResult.ok ? "Completed" : "Failed"} with exit code ${exportResult.returncode ?? 0}`}
                    </span>
                  </div>
                )}
                <div className="output-files">
                  <div className="output-files-head">
                    <h3>Recent Output</h3>
                    <button className="icon-button" type="button" onClick={() => loadOutputFiles()} disabled={!reportConfig.output_dir} title="Refresh output folder">
                      <RefreshCw size={15} />
                    </button>
                  </div>
                  {!outputFiles.exists && <p className="empty-copy">No output folder loaded yet.</p>}
                  {outputFiles.exists && !outputFiles.files.length && <p className="empty-copy">Output folder is empty.</p>}
                  {outputFiles.exists && outputFiles.files.slice(0, 10).map((file) => (
                    <article className="output-file" key={file.path}>
                      <div>
                        <b>{fileKind(file.name)}</b>
                        <strong>{file.name}</strong>
                      </div>
                      <span>{formatBytes(file.size)} / {formatTimestamp(file.modified)}</span>
                    </article>
                  ))}
                </div>
              </div>
            </section>
          )}

          {activeTab === "graphs" && (
            <section className="content-pad analytics-grid">
              <div className="analytics-head">
                <div>
                  <h2><BarChart3 size={18} /> Analytics Preview</h2>
                  <p className="muted">Preview the same graph families available in report exports.</p>
                </div>
                <label className="graph-picker">
                  <span>Graph Set</span>
                  <select value={analyticsGroup} onChange={(event) => setAnalyticsGroup(event.target.value)}>
                    {analyticsGraphGroups.map(([value, label]) => (
                      <option key={value} value={value}>{label}</option>
                    ))}
                  </select>
                </label>
              </div>
              {visibleAnalyticsCards.map((card) => (
                <BarList
                  key={card.key}
                  title={card.title}
                  metric={card.metric}
                  rows={card.rows}
                  valueType={card.valueType}
                />
              ))}
            </section>
          )}

          {activeTab === "map" && (
            <section className="content-pad map-preview">
              <div className="map-head">
                <div>
                  <h2><MapPinned size={18} /> Map View</h2>
                </div>
                <label className="map-focus-picker">
                  <span>Focal Point</span>
                  <select value={mapPreferences.focus || "mississippi"} onChange={(event) => updateMapFocus(event.target.value)}>
                    {mapFocusOptions.map(([value, label]) => <option key={value} value={value}>{label}</option>)}
                  </select>
                </label>
              </div>
              <InteractiveMap markers={mapMarkers} markerIconInfo={markerIconInfo} mapPreferences={mapPreferences} />
            </section>
          )}

          {activeTab === "settings" && (
            <section className="content-pad settings-grid">
              <div className="settings-column">
                <h2><Settings size={18} /> Appearance</h2>
                <form className="profile-card" onSubmit={saveAppProfile}>
                  <h3>Application Profile</h3>
                  <div className="form-grid two-col compact-profile-grid">
                    <label className="field">
                      <span>Application Header</span>
                      <input
                        value={appProfile.app_title || ""}
                        onChange={(event) => setAppProfile((current) => ({ ...current, app_title: event.target.value }))}
                        placeholder="CyberLab Case Tracker"
                      />
                    </label>
                    <label className="field">
                      <span>Agency/Organization</span>
                      <input
                        value={appProfile.organization || ""}
                        onChange={(event) => setAppProfile((current) => ({ ...current, organization: event.target.value }))}
                        placeholder="Agency or organization name"
                      />
                    </label>
                    <label className="field">
                      <span>Name</span>
                      <input
                        value={appProfile.name || ""}
                        onChange={(event) => setAppProfile((current) => ({ ...current, name: event.target.value }))}
                        placeholder="Examiner or report contact"
                      />
                    </label>
                  </div>
                  <button className="primary-action inline" type="submit" disabled={busy}><Save size={16} /> Save Profile</button>
                </form>
                <label className="field">
                  <span>Theme</span>
                  <select value={theme} onChange={(event) => setTheme(event.target.value)}>
                    {themes.map(([value, label]) => <option key={value} value={value}>{label}</option>)}
                  </select>
                </label>
                <label className="setting-card toggle-setting">
                  <span><strong>Interface Animations</strong><small className="muted">Logo movement, pane highlights, border sweeps, and entry transitions.</small></span>
                  <input type="checkbox" checked={animationsEnabled} onChange={(event) => setAnimationsEnabled(event.target.checked)} />
                </label>
                <label className="field setting-card">
                  <span>Preferred Browser</span>
                  <select value={browserPreferences.preferred_browser || "system_default"} onChange={(event) => updateBrowserPreference(event.target.value)}>
                    {browserPreferenceOptions.map(([value, label]) => <option key={value} value={value}>{label}</option>)}
                  </select>
                  <small className="muted">Applies the next time the app is launched.</small>
                </label>
                <label className="field setting-card">
                  <span>Default Map Focus</span>
                  <select value={mapPreferences.focus || "mississippi"} onChange={(event) => updateMapFocus(event.target.value)}>
                    {mapFocusOptions.map(([value, label]) => <option key={value} value={value}>{label}</option>)}
                  </select>
                </label>
                <div className="profile-card">
                  <h3>Tabs</h3>
                  <div className="customization-list">
                    {builtInNavItems.map((item) => (
                      <article className="customization-row" key={item.key}>
                        <label>
                          <input
                            type="checkbox"
                            checked={tabVisible(uiCustomization, item)}
                            disabled={item.required}
                            onChange={(event) => updateTabCustomization(item.key, { visible: event.target.checked })}
                          />
                          Show
                        </label>
                        <input
                          value={tabLabel(uiCustomization, item.key, item.label)}
                          onChange={(event) => updateTabCustomization(item.key, { label: event.target.value })}
                        />
                      </article>
                    ))}
                    {(uiCustomization.custom_tabs || []).map((tab) => (
                      <article className="customization-row wide" key={tab.key}>
                        <label>
                          <input
                            type="checkbox"
                            checked={tab.visible !== false}
                            onChange={(event) => updateCustomTab(tab.key, { visible: event.target.checked })}
                          />
                          Show
                        </label>
                        <input value={tab.label || ""} onChange={(event) => updateCustomTab(tab.key, { label: event.target.value })} />
                        <textarea value={tab.content || ""} onChange={(event) => updateCustomTab(tab.key, { content: event.target.value })} rows={2} placeholder="Custom tab text" />
                        <button className="ghost-action" type="button" onClick={() => removeCustomTab(tab.key)}><Trash2 size={14} /> Remove</button>
                      </article>
                    ))}
                  </div>
                  <button className="ghost-action" type="button" onClick={addCustomTab}><Plus size={15} /> Add Tab</button>
                </div>
                <div className="profile-card">
                  <h3>Case Fields</h3>
                  <div className="customization-list">
                    {caseFieldDefinitions.map(([key, fallback]) => (
                      <article className="customization-row" key={key}>
                        <label>
                          <input
                            type="checkbox"
                            checked={fieldVisible(uiCustomization, key, "progress")}
                            onChange={(event) => updateFieldCustomization(key, { visible: event.target.checked })}
                          />
                          Show
                        </label>
                        <input
                          value={fieldLabel(uiCustomization, key, fallback)}
                          onChange={(event) => updateFieldCustomization(key, { label: event.target.value })}
                        />
                      </article>
                    ))}
                    {(uiCustomization.custom_fields || []).map((field) => (
                      <article className="customization-row" key={field.key}>
                        <label>
                          <input
                            type="checkbox"
                            checked={field.visible !== false}
                            onChange={(event) => updateCustomField(field.key, { visible: event.target.checked })}
                          />
                          Show
                        </label>
                        <input value={field.label || ""} onChange={(event) => updateCustomField(field.key, { label: event.target.value })} />
                        <select value={field.type || "text"} onChange={(event) => updateCustomField(field.key, { type: event.target.value })}>
                          <option value="text">Text</option>
                          <option value="number">Number</option>
                          <option value="date">Date</option>
                        </select>
                        <button className="ghost-action" type="button" onClick={() => removeCustomField(field.key)}><Trash2 size={14} /> Remove</button>
                      </article>
                    ))}
                  </div>
                  <button className="ghost-action" type="button" onClick={addCustomField}><Plus size={15} /> Add Field</button>
                </div>
              </div>
              <div className="settings-column">
                <h2><Database size={18} /> Data Tools</h2>
                <div className="logo-picker">
                  <div>
                    <strong>Report Logo</strong>
                    <span>{logoInfo.exists ? "Using app_data/logo.png" : "No report logo configured"}</span>
                  </div>
                  {logoInfo.exists && (
                    <img
                      src={`${API_BASE}/api/settings/logo/image?path=${encodeURIComponent(logoInfo.path || "logo")}&v=${logoInfo.version || 0}`}
                      alt="Report logo preview"
                    />
                  )}
                  <label className="file-action">
                    <FileUp size={16} />
                    Choose Logo
                    <input type="file" accept=".png,.jpg,.jpeg,image/png,image/jpeg" onChange={(event) => uploadFile("/api/settings/logo", event.target.files?.[0], "Report logo updated")} />
                  </label>
                </div>
                <div className="logo-picker">
                  <div>
                    <strong>Map Marker Icon</strong>
                    <span>{markerIconInfo.exists ? "Using app_data/marker_icon.png" : "No custom marker icon configured"}</span>
                  </div>
                  {markerIconInfo.exists && (
                    <img
                      src={`${API_BASE}/api/settings/marker-icon/image?path=${encodeURIComponent(markerIconInfo.path || "marker")}&v=${markerIconInfo.version || 0}`}
                      alt="Map marker preview"
                    />
                  )}
                  <label className="file-action">
                    <FileUp size={16} />
                    Choose Marker
                    <input type="file" accept=".bmp,.png,.jpg,.jpeg,image/bmp,image/png,image/jpeg" onChange={(event) => uploadFile("/api/settings/marker-icon", event.target.files?.[0], "Map marker updated")} />
                  </label>
                </div>
                <form className="profile-card" onSubmit={addComboOption}>
                  <h3>Dropdown Values</h3>
                  <div className="form-grid two-col compact-profile-grid">
                    <label className="field">
                      <span>Field</span>
                      <select value={comboEditor.key} onChange={(event) => setComboEditor((current) => ({ ...current, key: event.target.value }))}>
                        {comboManageOptions.map(([value, label]) => <option key={value} value={value}>{label}</option>)}
                      </select>
                    </label>
                    <label className="field">
                      <span>New Value</span>
                      <input value={comboEditor.value} onChange={(event) => setComboEditor((current) => ({ ...current, value: event.target.value }))} />
                    </label>
                  </div>
                  <button className="primary-action inline" type="submit" disabled={busy || !comboEditor.value.trim()}><Save size={16} /> Add Value</button>
                  <div className="combo-preview">
                    {(comboValues[comboEditor.key] || []).map((value) => (
                      <span className="combo-chip" key={value}>
                        <b>{value}</b>
                        <button
                          type="button"
                          onClick={() => renameComboOption(value)}
                          disabled={busy}
                          title={`Rename ${value}`}
                          aria-label={`Rename ${value}`}
                        >
                          <Pencil size={12} />
                        </button>
                        <button
                          type="button"
                          onClick={() => deleteComboOption(value)}
                          disabled={busy}
                          title={`Remove ${value}`}
                          aria-label={`Remove ${value}`}
                        >
                          <X size={13} />
                        </button>
                      </span>
                    ))}
                    {!(comboValues[comboEditor.key] || []).length && <p className="empty-copy">No saved values yet.</p>}
                  </div>
                </form>
                <div className="profile-card">
                  <h3>Backup & Restore</h3>
                  <p className="muted">Backups are stored in {backups.backup_dir || "app_data/backups"}.</p>
                  <div className="form-actions">
                    <button className="primary-action inline" type="button" onClick={createBackup} disabled={busy}><Database size={16} /> Backup Now</button>
                    <button className="ghost-action" type="button" onClick={createEncryptedBackup} disabled={busy}><ShieldCheck size={16} /> Encrypted Portable</button>
                    <button className="ghost-action" type="button" onClick={createSupportBundle} disabled={busy}><FileText size={16} /> Support Bundle</button>
                  </div>
                  <div className="backup-list">
                    {(backups.files || []).slice(0, 6).map((file) => (
                      <article className="backup-file" key={file.path}>
                        <div>
                          <strong>{file.name}</strong>
                          <span>{formatBytes(file.size)} / {formatTimestamp(file.modified)}</span>
                        </div>
                        <button className="ghost-action" type="button" onClick={() => restoreBackup(file.path)} disabled={busy}>Restore</button>
                      </article>
                    ))}
                    {!(backups.files || []).length && <p className="empty-copy">No backups found.</p>}
                  </div>
                </div>
                <div className="profile-card update-panel">
                  <h3>Portable Application Update</h3>
                  <p className="muted">Checks the GitHub release and downloads a verified single EXE beside the current application. No installer or administrator access is required.</p>
                  {updateInfo && (
                    <dl className="config-list compact-config">
                      <dt>Installed</dt><dd>v{updateInfo.current_version || appInfo.version}</dd>
                      <dt>Latest</dt><dd>{updateInfo.version || "Unavailable"}</dd>
                      <dt>Status</dt><dd>{updateInfo.update_available ? "Update available" : "Current"}</dd>
                      {updateInfo.path && <><dt>Saved</dt><dd>{updateInfo.path}</dd></>}
                      {updateInfo.sha256 && <><dt>SHA-256</dt><dd className="hash-value">{updateInfo.sha256}</dd></>}
                    </dl>
                  )}
                  <div className="form-actions">
                    <button className="ghost-action" type="button" onClick={checkPortableUpdate} disabled={busy}><RefreshCw size={16} /> Check</button>
                    {updateInfo?.update_available && <button className="primary-action inline" type="button" onClick={downloadPortableUpdate} disabled={busy}><Download size={16} /> Download EXE</button>}
                  </div>
                </div>
                <div className="profile-card">
                  <h3>Legacy Import</h3>
                <label className="file-action">
                  <FileUp size={16} />
                  Import DB
                  <input type="file" accept=".db" onChange={(event) => uploadFile("/api/import/database", event.target.files?.[0])} />
                </label>
                <label className="file-action">
                  <FileUp size={16} />
                  Import app_data zip
                  <input type="file" accept=".zip" onChange={(event) => uploadFile("/api/import/app-data", event.target.files?.[0])} />
                </label>
                </div>
              </div>
            </section>
          )}
          {activeTab.startsWith("custom:") && (
            <section className="content-pad">
              {(() => {
                const key = activeTab.slice("custom:".length);
                const tab = (uiCustomization.custom_tabs || []).find((item) => item.key === key);
                return (
                  <>
                    <h2><FileText size={18} /> {tab?.label || "Custom Tab"}</h2>
                    <p className="custom-tab-copy">{tab?.content || "No content configured for this tab yet."}</p>
                  </>
                );
              })()}
            </section>
          )}
        </section>
      </section>
      {selectedCase && (
        <CaseDetailModal
          row={selectedCase}
          onClose={() => setSelectedCase(null)}
          onEdit={() => selectedCase._mode === "progress" ? editInProgressCase(selectedCase) : editCompletedCase(selectedCase)}
          onDuplicate={() => duplicateCase(selectedCase, selectedCase._mode === "progress" ? "progress" : "completed")}
          uiCustomization={uiCustomization}
          api={api}
          onNextSubcase={() => startNextSubcase(selectedCase, selectedCase._mode === "progress" ? "progress" : "completed")}
          onChanged={refresh}
        />
      )}
      {reportPreview && <ReportPreviewModal url={reportPreview} onClose={() => setReportPreview("")} />}
    </main>
  );
}

function OperationalOverview({ dashboard, quality }) {
  const monthly = dashboard.monthly_completed || [];
  const maxMonthly = Math.max(1, ...monthly.map((item) => Number(item.value || 0)));
  return (
    <details className="operations-panel">
      <summary><LayoutDashboard size={16} /> Operational snapshot <span>{dashboard.overdue_count || 0} overdue / {quality.issue_count || 0} review items</span></summary>
      <div className="operations-body">
        <dl className="operations-metrics">
          <div><dt>Case families</dt><dd>{dashboard.family_count || 0}</dd></div>
          <div><dt>Evidence items</dt><dd>{dashboard.evidence_count || 0}</dd></div>
          <div><dt>Active 30+ days</dt><dd>{dashboard.active_aging_count || 0}</dd></div>
          <div><dt>Avg. turnaround</dt><dd>{dashboard.average_turnaround_days || 0} days</dd></div>
        </dl>
        <div className="monthly-strip" aria-label="Completed cases by month">
          {monthly.map((item) => <i key={item.label} style={{ height: `${Math.max(8, Number(item.value || 0) / maxMonthly * 100)}%` }} title={`${item.label}: ${item.value}`} />)}
          {!monthly.length && <span>No monthly history available.</span>}
        </div>
      </div>
    </details>
  );
}

function CaseTemplateBar({ templates, onApply, onSave, onDelete }) {
  const [selected, setSelected] = useState("");
  const template = templates.find((item) => String(item.id) === selected);
  return (
    <div className="template-bar">
      <PackageSearch size={17} />
      <select value={selected} onChange={(event) => setSelected(event.target.value)} aria-label="Case template">
        <option value="">Case template</option>
        {templates.map((item) => <option key={item.id} value={item.id}>{item.name}</option>)}
      </select>
      <button className="ghost-action compact" type="button" disabled={!template} onClick={() => onApply(template)}>Apply</button>
      <button className="ghost-action compact" type="button" onClick={onSave}><Save size={15} /> Save current</button>
      {template && <button className="icon-button" type="button" onClick={() => onDelete(template)} title="Delete template"><Trash2 size={15} /></button>}
    </div>
  );
}

function DraftRecoveryBanner({ draft, onRestore, onDiscard }) {
  return (
    <div className="draft-recovery">
      <div><Save size={16} /><span><strong>Unfinished draft available</strong><small>Autosaved {draft.savedAt ? new Date(draft.savedAt).toLocaleString() : "recently"}</small></span></div>
      <div><button className="primary-action inline" type="button" onClick={onRestore}>Restore</button><button className="ghost-action compact" type="button" onClick={onDiscard}>Discard</button></div>
    </div>
  );
}

function CaseFamilyList({ families, onNext }) {
  return (
    <div className="family-list">
      <div className="list-heading"><span>{families.length} case families</span><span>Grouped by base case number</span></div>
      {families.map((family) => (
        <article className="family-row" key={family.base_case_number}>
          <div><strong>{family.base_case_number}</strong><span>{family.device_count} devices / {family.completed_count} completed / {family.active_count} active</span></div>
          <div className="family-volume">{formatVolume(family.total_volume_gb)}</div>
          <button className="icon-button" type="button" onClick={() => window.open(`${API_BASE}/api/case-family/report?case_number=${encodeURIComponent(family.base_case_number)}`, "_blank", "noopener")} title="Open case-family PDF"><FileText size={15} /></button>
          <button className="ghost-action compact" type="button" onClick={() => onNext(family.members?.[family.members.length - 1] || { case_number: family.base_case_number })}><Plus size={15} /> Next device</button>
        </article>
      ))}
      {!families.length && <p className="empty-copy content-pad">No multi-device case families found yet.</p>}
    </div>
  );
}

function DataQualityList({ quality, onDismiss, onRestore, onOpen, onNormalize, onMerge }) {
  const [selected, setSelected] = useState([]);
  const issues = quality.issues || [];
  const visibleFingerprints = issues.map((issue) => issue.fingerprint);
  const allSelected = Boolean(issues.length) && visibleFingerprints.every((fingerprint) => selected.includes(fingerprint));

  useEffect(() => {
    setSelected((current) => current.filter((fingerprint) => visibleFingerprints.includes(fingerprint)));
  }, [quality.issue_count]); // eslint-disable-line react-hooks/exhaustive-deps

  function toggleSelected(fingerprint, checked) {
    setSelected((current) => checked ? [...new Set([...current, fingerprint])] : current.filter((value) => value !== fingerprint));
  }

  return (
    <div className="quality-list">
      <div className="quality-toolbar">
        <label className="quality-select-all">
          <input type="checkbox" checked={allSelected} disabled={!issues.length} onChange={(event) => setSelected(event.target.checked ? visibleFingerprints : [])} />
          Select all
        </label>
        <div className="quality-summary">
          <span className="severity high">{quality.high_count || 0} high</span>
          <span className="severity medium">{quality.medium_count || 0} medium</span>
          <span className="severity low">{quality.low_count || 0} low</span>
        </div>
        <button className="ghost-action compact" type="button" disabled={!selected.length} onClick={() => onDismiss(selected)}><CheckCircle2 size={15} /> Clear selected ({selected.length})</button>
        {quality.dismissed_count > 0 && <button className="ghost-action compact" type="button" onClick={onRestore}><RefreshCw size={15} /> Restore cleared ({quality.dismissed_count})</button>}
      </div>
      {issues.map((issue, index) => (
        <article className="quality-row" key={`${issue.case_number}-${issue.type}-${index}`}>
          <input type="checkbox" checked={selected.includes(issue.fingerprint)} onChange={(event) => toggleSelected(issue.fingerprint, event.target.checked)} aria-label={`Select ${issue.message}`} />
          <TriangleAlert size={16} />
          <div><strong>{issue.message}</strong><span>{issue.case_number || "All records"} / {issue.source}</span></div>
          <span className={`severity ${issue.severity}`}>{issue.severity}</span>
          <div className="review-actions">
            {(issue.record_id || issue.records?.length) && <button className="icon-button" type="button" onClick={() => onOpen(issue)} title="Open affected case" aria-label={`Open ${issue.case_number}`}><Eye size={14} /></button>}
            {issue.type === "inconsistent" && <button className="icon-button" type="button" onClick={() => onNormalize(issue)} title="Normalize these values" aria-label={`Normalize ${issue.field}`}><Sparkles size={14} /></button>}
            {issue.type === "duplicate" && <button className="icon-button" type="button" onClick={() => onMerge(issue)} title="Merge duplicate records" aria-label={`Merge duplicates for ${issue.case_number}`}><Layers3 size={14} /></button>}
            <button className="icon-button" type="button" onClick={() => onDismiss([issue.fingerprint])} title="Clear review item" aria-label={`Clear ${issue.message}`}><X size={14} /></button>
          </div>
        </article>
      ))}
      {!quality.issue_count && <p className="empty-copy content-pad">No data-quality issues detected.</p>}
    </div>
  );
}

function ReportPreviewModal({ url, onClose }) {
  return (
    <div className="modal-backdrop" role="presentation" onClick={onClose}>
      <section className="report-preview-modal panel-enter" role="dialog" aria-modal="true" onClick={(event) => event.stopPropagation()}>
        <header><div><h2>Report Preview</h2><p className="muted">Generated from current data without writing to the export folders.</p></div><button className="icon-button" onClick={onClose} title="Close"><X size={18} /></button></header>
        <iframe src={url} title="Generated report preview" />
      </section>
    </div>
  );
}

function CaseDetailModal({ row, onClose, onEdit, onDuplicate, onNextSubcase, onChanged, api, uiCustomization = defaultUiCustomization }) {
  const [detailTab, setDetailTab] = useState("overview");
  const [evidence, setEvidence] = useState([]);
  const [audit, setAudit] = useState([]);
  const [family, setFamily] = useState({ members: [] });
  const [showEvidenceForm, setShowEvidenceForm] = useState(false);
  const [evidenceForm, setEvidenceForm] = useState({ case_number: row.case_number || "", evidence_number: "", item_type: "", serial_number: "", storage_location: "", received_date: "", status: "In Custody", description: "" });

  async function loadDetails() {
    const number = encodeURIComponent(row.case_number || "");
    const [evidenceData, auditData, familyData] = await Promise.all([
      api(`/api/evidence?case_number=${number}`).catch(() => ({ evidence: [] })),
      api(`/api/audit?case_number=${number}`).catch(() => ({ events: [] })),
      api(`/api/case-family?case_number=${number}`).catch(() => ({ members: [] })),
    ]);
    setEvidence(evidenceData.evidence || []);
    setAudit(auditData.events || []);
    setFamily(familyData || { members: [] });
  }

  useEffect(() => { loadDetails(); }, [row.case_number]); // eslint-disable-line react-hooks/exhaustive-deps

  async function addEvidence(event) {
    event.preventDefault();
    await api("/api/evidence", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(evidenceForm) });
    setEvidenceForm((current) => ({ ...current, evidence_number: "", item_type: "", serial_number: "", storage_location: "", received_date: "", description: "" }));
    setShowEvidenceForm(false);
    await loadDetails();
    onChanged?.();
  }

  async function addCustody(item) {
    const eventType = window.prompt("Custody event (Received, Transferred, Examined, Returned)", "Transferred");
    if (!eventType?.trim()) return;
    const person = window.prompt("Person receiving or handling the evidence", "") || "";
    const location = window.prompt("Location", item.storage_location || "") || "";
    await api(`/api/evidence/${item.id}/custody`, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ event_type: eventType.trim(), person, location }) });
    await loadDetails();
  }

  async function removeEvidence(item) {
    if (!window.confirm(`Delete evidence ${item.evidence_number || item.item_type || "item"}?`)) return;
    await api(`/api/evidence/${item.id}?case_number=${encodeURIComponent(row.case_number || "")}`, { method: "DELETE" });
    await loadDetails();
    onChanged?.();
  }

  const builtInFields = [
    ["Status", "Status", row.status],
    ["case_number", "Case #", row.case_number],
    ["Created", "Created", row.created_at],
    ["examiner", "Examiner", row.examiner],
    ["investigator", "Investigator", row.investigator],
    ["agency", "Agency", row.agency],
    ["city_of_offense", "City/State", `${formatValue(row.city_of_offense)}, ${formatValue(row.state_of_offense)}`],
    ["start_date", "Dates", `${formatValue(row.start_date)} to ${formatValue(row.end_date)}`],
    ["volume_size_gb", "Volume", row.volume_size_gb ? formatVolume(row.volume_size_gb) : ""],
    ["offense_type", "Offense", row.offense_type],
    ["device_type", "Device", row.device_type],
    ["model", "Model", row.model],
    ["os", "OS", row.os],
    ["forensic_tool", "Forensic Tool", row.forensic_tool],
    ["data_recovered", "Data Recovered", row.data_recovered],
    ["fpr_complete", "FPR Complete", row.fpr_complete ? "Yes" : "No"],
  ];
  const customValues = parseCustomFields(row.custom_fields);
  const fields = [
    ...builtInFields
      .filter(([key]) => ["Status", "Created"].includes(key) || fieldVisible(uiCustomization, key, "progress"))
      .map(([key, fallback, value]) => [fieldLabel(uiCustomization, key, fallback), value]),
    ...(uiCustomization.custom_fields || [])
      .filter((field) => field?.key && field.visible !== false)
      .map((field) => [field.label || field.key, customValues[field.key]]),
  ];
  return (
    <div className="modal-backdrop" role="presentation" onClick={onClose}>
      <section className="case-modal panel-enter" role="dialog" aria-modal="true" onClick={(event) => event.stopPropagation()}>
        <header>
          <div>
            <h2>{formatValue(row.case_number)}</h2>
            <p className="muted">{formatValue(row.agency)} / {formatValue(row.offense_type)}</p>
          </div>
          <button className="icon-button" onClick={onClose} title="Close"><X size={18} /></button>
        </header>
        <div className="modal-tabs segmented-control">
          <button className={detailTab === "overview" ? "active" : ""} onClick={() => setDetailTab("overview")}>Overview</button>
          <button className={detailTab === "evidence" ? "active" : ""} onClick={() => setDetailTab("evidence")}>Evidence <span>{evidence.length}</span></button>
          <button className={detailTab === "family" ? "active" : ""} onClick={() => setDetailTab("family")}>Family <span>{family.device_count || 0}</span></button>
          <button className={detailTab === "history" ? "active" : ""} onClick={() => setDetailTab("history")}><History size={14} /> History</button>
        </div>
        <div className="case-modal-body">
          {detailTab === "overview" && <>
            <div className="detail-grid">
              {fields.map(([label, value]) => <div key={label}><span>{label}</span><strong>{formatValue(value)}</strong></div>)}
            </div>
            {fieldVisible(uiCustomization, "notes", "progress") && <div className="notes-panel"><span>{fieldLabel(uiCustomization, "notes", "Notes")}</span><p>{formatValue(row.notes)}</p></div>}
          </>}
          {detailTab === "evidence" && <div className="evidence-workspace">
            <div className="section-action-row"><p className="muted">Evidence and chain-of-custody activity for this case.</p><button className="primary-action inline" onClick={() => setShowEvidenceForm((value) => !value)}><Plus size={15} /> Add evidence</button></div>
            {showEvidenceForm && <form className="evidence-form" onSubmit={addEvidence}>
              <Field label="Evidence #" name="evidence_number" form={evidenceForm} setForm={setEvidenceForm} />
              <Field label="Item Type" name="item_type" form={evidenceForm} setForm={setEvidenceForm} />
              <Field label="Serial Number" name="serial_number" form={evidenceForm} setForm={setEvidenceForm} />
              <Field label="Storage" name="storage_location" form={evidenceForm} setForm={setEvidenceForm} />
              <Field label="Received" name="received_date" type="date" form={evidenceForm} setForm={setEvidenceForm} />
              <Field label="Status" name="status" form={evidenceForm} setForm={setEvidenceForm} options={["In Custody", "Examining", "Returned", "Released"]} />
              <div className="form-actions"><button className="primary-action inline" type="submit"><Save size={15} /> Save</button><button className="ghost-action" type="button" onClick={() => setShowEvidenceForm(false)}>Cancel</button></div>
            </form>}
            {evidence.map((item) => <article className="evidence-item" key={item.id}>
              <div className="evidence-heading"><div><strong>{item.evidence_number || "Unnumbered item"}</strong><span>{formatValue(item.item_type)} / {formatValue(item.serial_number)}</span></div><span className="status-tag">{item.status}</span></div>
              <p>{item.description || item.storage_location || "No description recorded."}</p>
              <div className="custody-timeline">{(item.custody_events || []).map((event) => <div key={event.id}><i /><span><b>{event.event_type}</b> {event.person || ""}<small>{event.event_at} {event.location ? `/ ${event.location}` : ""}</small></span></div>)}</div>
              <div className="form-actions"><button className="ghost-action compact" onClick={() => addCustody(item)}><History size={14} /> Custody event</button><button className="icon-button" onClick={() => removeEvidence(item)} title="Delete evidence"><Trash2 size={14} /></button></div>
            </article>)}
            {!evidence.length && !showEvidenceForm && <p className="empty-copy">No evidence items recorded.</p>}
          </div>}
          {detailTab === "family" && <div className="family-detail">
            <div className="section-action-row"><div><strong>{family.base_case_number || row.case_number}</strong><p className="muted">{family.device_count || 1} related devices / {formatVolume(family.total_volume_gb || row.volume_size_gb)}</p></div><div className="form-actions"><button className="ghost-action" onClick={() => window.open(`${API_BASE}/api/case-family/report?case_number=${encodeURIComponent(family.base_case_number || row.case_number)}`, "_blank", "noopener")}><FileText size={15} /> Family PDF</button><button className="primary-action inline" onClick={onNextSubcase}><Plus size={15} /> Next device</button></div></div>
            {(family.members || []).map((member) => <article key={`${member.source}-${member.id}`}><strong>{member.case_number}</strong><span>{formatValue(member.device_type)} / {formatValue(member.model)}</span><span>{member.source}</span></article>)}
          </div>}
          {detailTab === "history" && <div className="audit-timeline">
            {audit.map((event) => <article key={event.id}><i /><div><strong>{event.summary || event.action}</strong><span>{event.created_at} / {event.actor || "Local user"}</span>{event.changes && Object.keys(event.changes).length > 0 && <small>{Object.keys(event.changes).join(", ")}</small>}</div></article>)}
            {!audit.length && <p className="empty-copy">No recorded history for this case.</p>}
          </div>}
        </div>
        {detailTab === "overview" && (
          <footer className="form-actions">
            <button className="primary-action" onClick={onEdit}><Pencil size={16} /> Edit</button>
            <button className="ghost-action" onClick={onDuplicate}><Copy size={16} /> Duplicate</button>
            <button className="ghost-action" onClick={onNextSubcase}><Layers3 size={16} /> Next device</button>
          </footer>
        )}
      </section>
    </div>
  );
}

function markerSizeForZoom(zoom) {
  const level = Number(zoom || 6);
  if (level <= 4) {
    return 8;
  }
  if (level <= 5) {
    return 10;
  }
  if (level <= 6) {
    return 13;
  }
  if (level <= 7) {
    return 17;
  }
  if (level <= 8) {
    return 22;
  }
  return Math.min(44, 22 + (level - 8) * 5);
}

function ZoomAwareMarkers({ markers, markerIconInfo }) {
  const map = useMap();
  const [zoom, setZoom] = useState(map.getZoom());
  useEffect(() => {
    const updateZoom = () => setZoom(map.getZoom());
    map.on("zoomend", updateZoom);
    return () => map.off("zoomend", updateZoom);
  }, [map]);

  const size = markerSizeForZoom(zoom);
  return (
    <>
      {markers.map((marker) => {
        const count = Number(marker.case_count || 0);
        const icon = markerIconInfo?.exists
          ? L.icon({
              iconUrl: `${API_BASE}/api/settings/marker-icon/image?path=${encodeURIComponent(markerIconInfo.path || "marker")}&v=${markerIconInfo.version || 0}`,
              iconSize: [size, size],
              iconAnchor: [size / 2, size / 2],
              popupAnchor: [0, -size / 2],
              className: "custom-map-marker",
            })
          : L.divIcon({
              className: "cyber-map-marker",
              html: `<span>${count}</span>`,
              iconSize: [size, size],
              iconAnchor: [size / 2, size / 2],
              popupAnchor: [0, -size / 2],
            });
        return (
          <Marker
            key={`${marker.city}-${marker.state}`}
            position={[Number(marker.latitude), Number(marker.longitude)]}
            icon={icon}
          >
            <Popup>
              <strong>{marker.city}, {marker.state}</strong>
              <br />
              {marker.case_count} cases
              <br />
              {formatVolume(marker.total_volume_gb)}
            </Popup>
          </Marker>
        );
      })}
    </>
  );
}

function MapFocusController({ markers, mapPreferences }) {
  const map = useMap();
  const focus = mapPreferences?.focus || "mississippi";

  useEffect(() => {
    const option = mapFocusOptions.find(([value]) => value === focus)?.[2] || mapFocusOptions[0][2];
    const geocoded = (markers || []).filter((marker) => marker.latitude && marker.longitude);

    if (focus === "case_markers" && geocoded.length) {
      const bounds = geocoded.map((marker) => [Number(marker.latitude), Number(marker.longitude)]);
      map.fitBounds(bounds, { padding: [38, 38], maxZoom: 9 });
      return;
    }

    if (option.bounds) {
      map.fitBounds(option.bounds, { padding: [26, 26] });
      return;
    }

    map.setView([32.7, -89.5], option.zoom || 6);
  }, [focus, markers, map]);

  return null;
}

function InteractiveMap({ markers, markerIconInfo, mapPreferences }) {
  const geocodedMarkers = useMemo(() => {
    return (markers || []).filter((marker) => marker.latitude && marker.longitude);
  }, [markers]);
  const selectedFocus = mapFocusOptions.find(([value]) => value === (mapPreferences?.focus || "mississippi"))?.[2] || mapFocusOptions[0][2];
  const center = selectedFocus.bounds
    ? [
        (Number(selectedFocus.bounds[0][0]) + Number(selectedFocus.bounds[1][0])) / 2,
        (Number(selectedFocus.bounds[0][1]) + Number(selectedFocus.bounds[1][1])) / 2,
      ]
    : [32.7, -89.5];

  return (
    <div className="interactive-map-grid">
      <div className="map-card">
        <MapContainer center={center} zoom={selectedFocus.zoom || 6} scrollWheelZoom className="leaflet-map">
          <TileLayer
            attribution="&copy; OpenStreetMap contributors"
            url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
          />
          <MapFocusController markers={geocodedMarkers} mapPreferences={mapPreferences} />
          <ZoomAwareMarkers markers={geocodedMarkers} markerIconInfo={markerIconInfo} />
        </MapContainer>
      </div>
      <div className="marker-grid">
        {(markers || []).slice(0, 18).map((marker) => (
          <article className="marker-card" key={`${marker.city}-${marker.state}`}>
            <MapPin size={16} />
            <strong>{marker.city}, {marker.state}</strong>
            <span>{marker.case_count} cases / {formatVolume(marker.total_volume_gb)}</span>
            <small>{marker.latitude && marker.longitude ? `${Number(marker.latitude).toFixed(3)}, ${Number(marker.longitude).toFixed(3)}` : "Not geocoded"}</small>
          </article>
        ))}
      </div>
    </div>
  );
}

function Toolbar({ search, setSearch, sort, setSort, refresh, busy }) {
  return (
    <div className="toolbar">
      <div className="search-box">
        <Search size={17} />
        <input
          value={search}
          onChange={(event) => setSearch(event.target.value)}
          onKeyDown={(event) => event.key === "Enter" && refresh()}
          placeholder="Search cases, agencies, offenses..."
        />
      </div>
      <select value={sort} onChange={(event) => setSort(event.target.value)} aria-label="Sort cases">
        <option value="newest">Newest to oldest</option>
        <option value="oldest">Oldest to newest</option>
        <option value="start_newest">Start date newest</option>
        <option value="start_oldest">Start date oldest</option>
        <option value="case_number">Case # A to Z</option>
        <option value="agency">Agency A to Z</option>
        <option value="offense">Offense A to Z</option>
      </select>
      <button className="icon-button" onClick={refresh} disabled={busy} title="Refresh">
        <RefreshCw size={17} className={busy ? "spin" : ""} />
      </button>
    </div>
  );
}

function BarList({ title, rows, metric = "Cases", valueType = "count" }) {
  const max = Math.max(...(rows || []).map((row) => row.value), 1);
  const formatter = valueType === "volume" ? (value) => formatVolume(value) : (value) => Number(value || 0).toLocaleString();
  return (
    <section className="bar-list">
      <header>
        <h3>{title}</h3>
        <span>{metric}</span>
      </header>
      {(rows || []).map((row) => (
        <div className="bar-row" key={row.label}>
          <div>
            <strong>{row.label}</strong>
            <span>{formatter(row.value)}</span>
          </div>
          <i style={{ width: `${Math.max(6, (row.value / max) * 100)}%` }} />
        </div>
      ))}
      {!(rows || []).length && <p className="empty-copy">No data available.</p>}
    </section>
  );
}

function CaseTable({ rows, total, onView, onEdit, onDuplicate, onDelete, uiCustomization = defaultUiCustomization }) {
  const columns = [
    ["case_number", "Case #", (row) => row.case_number],
    ["created_at", "Created", (row) => row.created_at],
    ["examiner", "Examiner", (row) => row.examiner],
    ["agency", "Agency", (row) => row.agency],
    ["offense_type", "Offense", (row) => row.offense_type],
    ["city_of_offense", "City", (row) => row.city_of_offense],
    ["device_type", "Device", (row) => row.device_type],
  ].filter(([key]) => key === "created_at" || fieldVisible(uiCustomization, key, "completed"));
  return (
    <>
      <div className="table-meta">
        <span>{total || 0} cases</span>
        <span>Edit or delete completed cases</span>
      </div>
      <div className="case-table">
        <table>
          <thead>
            <tr>
              {columns.map(([key, fallback]) => <th key={key}>{fieldLabel(uiCustomization, key, fallback)}</th>)}
              <th>Actions</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((row) => (
              <tr key={row.id}>
                {columns.map(([key, , reader]) => <td key={key}>{formatValue(reader(row))}</td>)}
                <td className="table-actions">
                  <button className="table-action" onClick={() => onView(row)} title="View"><Eye size={15} /></button>
                  <button className="table-action" onClick={() => onEdit(row)} title="Edit"><Pencil size={15} /></button>
                  <button className="table-action" onClick={() => onDuplicate(row)} title="Duplicate"><Copy size={15} /></button>
                  <button className="table-action danger" onClick={() => onDelete(row.id)} title="Delete"><Trash2 size={15} /></button>
                </td>
              </tr>
            ))}
            {!rows.length && (
              <tr>
                <td colSpan={columns.length + 1} className="empty-state">No cases loaded yet.</td>
              </tr>
            )}
          </tbody>
        </table>
      </div>
    </>
  );
}

function InProgressTable({ rows, total, onView, onEdit, onDuplicate, onComplete, onDelete, uiCustomization = defaultUiCustomization }) {
  const columns = [
    ["case_number", "Case #", (row) => row.case_number],
    ["examiner", "Examiner", (row) => row.examiner],
    ["investigator", "Investigator", (row) => row.investigator],
    ["agency", "Agency", (row) => row.agency],
    ["device_type", "Device", (row) => row.device_type],
    ["workflow_status", "Workflow", (row) => row.workflow_status],
    ["priority", "Priority", (row) => row.priority],
    ["target_due_date", "Due", (row) => row.target_due_date],
  ].filter(([key]) => ["workflow_status", "priority", "target_due_date"].includes(key) || fieldVisible(uiCustomization, key, "progress"));
  return (
    <>
      <div className="table-meta">
        <span>{rows.length === total ? `${total || 0} active cases` : `${rows.length} of ${total || 0} active cases`}</span>
        <span>Edit a case to update or complete it</span>
      </div>
      <div className="case-table">
        <table>
          <thead>
            <tr>
              {columns.map(([key, fallback]) => <th key={key}>{fieldLabel(uiCustomization, key, fallback)}</th>)}
              <th>Actions</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((row) => (
              <tr key={row.id}>
                {columns.map(([key, , reader]) => <td key={key}>{formatValue(reader(row))}</td>)}
                <td className="table-actions">
                  <button className="table-action" onClick={() => onView(row)} title="View"><Eye size={15} /></button>
                  <button className="table-action" onClick={() => onEdit(row)} title="Edit"><Pencil size={15} /></button>
                  <button className="table-action" onClick={() => onDuplicate(row)} title="Duplicate"><Copy size={15} /></button>
                  <button className="table-action complete" onClick={() => onComplete(row)} title="Complete"><CheckCircle2 size={15} /></button>
                  <button className="table-action danger" onClick={() => onDelete(row.id)} title="Delete"><Trash2 size={15} /></button>
                </td>
              </tr>
            ))}
            {!rows.length && (
              <tr>
                <td colSpan={columns.length + 1} className="empty-state">No active cases match this view.</td>
              </tr>
            )}
          </tbody>
        </table>
      </div>
    </>
  );
}

createRoot(document.getElementById("root")).render(<App />);
