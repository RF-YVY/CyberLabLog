# CyberLab Case Tracker v3.0.15

Fixes city locations appearing at **0.00, 0.00** and being skipped by **Refresh Missing Locations**.

- Blank GPS fields on edited cases stay blank instead of becoming zero. Saving a zero/zero pair cannot overwrite a valid city cache.
- Refresh now treats zero/zero, partial, non-finite, and out-of-range pairs as unresolved. Existing zero/zero cache entries are retried.
- Good cached coordinates or a valid record for the same named location are reused before an online lookup.
- Repairs persist to both completed and in-progress records, and the interface reports how many case records were repaired.
- Existing precise GPS coordinates are preserved, including valid pairs with one zero. Snapshot-checked writes avoid overwriting a concurrent manual correction.
- Live and exported maps use the same pair-validation/fallback rules and avoid placing unresolved cities at Null Island.
- A database backup is saved in `app_data/backups` before the refresh repair operation. Unresolved locations remain listed for retry when an online lookup fails.

**After updating:** open **Map → Refresh Missing Locations** once. Existing zero/zero records will be repaired when a valid cached location or lookup is available. Older exported maps should be regenerated.

Update through **Settings → Portable Application Update → Check → Download & Install**.

Validation: 48 backend tests and 5 frontend tests pass. The packaged GUI repaired three invalid records across completed/in-progress tables while preserving a precise location. The pre-repair backup retained the original zeros, the exported map retained all four test cases at valid coordinates, and a repeat refresh required no further repairs. Poisoned-cache retries and failed lookups are covered with mocked geocoding.
