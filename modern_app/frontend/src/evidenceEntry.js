export function nextEvidenceForm(form, caseNumber) {
  const { id, _nextEvidenceNotice, _errors, ...shared } = form;
  return {
    ...shared,
    case_number: caseNumber,
    device_type: "",
    model: "",
    os: "",
    volume_size_gb: "",
    data_recovered: "",
    fpr_complete: false,
    custom_fields: { ...(form.custom_fields || {}) },
  };
}
