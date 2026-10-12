export function validateCase(form) {
  const errors = {};
  if (!form.case_number?.trim()) errors.case_number = "Enter a case number.";
  if (form.volume_size_gb !== "" && form.volume_size_gb != null && (!Number.isFinite(Number(form.volume_size_gb)) || Number(form.volume_size_gb) < 0)) errors.volume_size_gb = "Enter a volume of zero or more GB.";
  for (const [key, limit] of [["latitude", 90], ["longitude", 180]]) {
    if (form[key] !== "" && form[key] != null && (!Number.isFinite(Number(form[key])) || Math.abs(Number(form[key])) > limit)) errors[key] = `Enter a value between -${limit} and ${limit}.`;
  }
  if (Boolean(form.latitude !== "" && form.latitude != null) !== Boolean(form.longitude !== "" && form.longitude != null)) {
    errors.latitude ||= "Enter both coordinates, or leave both empty.";
    errors.longitude ||= "Enter both coordinates, or leave both empty.";
  }
  if (form.start_date && form.end_date && form.end_date < form.start_date) errors.end_date = "End date must be on or after the start date.";
  return errors;
}
