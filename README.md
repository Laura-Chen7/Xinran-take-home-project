# Data Import Assignment (Solution)

This repository contains my solution for transforming three input spreadsheets (constituents, emails, donations) into two client-ready output CSVs:

- `data/output/cuebox_constituents.csv`
- `data/output/cuebox_tags.csv`

> Note: `data/input/` is intentionally git-ignored. The repository includes the generated output CSVs and the full code used to produce them.

---

## How to Run

### 1) Setup environment

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -U pip pandas numpy requests
```

### 2) Place input files

Put the three input CSVs here:

- `data/input/constituents.csv`
- `data/input/emails.csv`
- `data/input/donations.csv`

### 3) Run transform

```bash
python main.py
```

This writes:
- `data/output/cuebox_constituents.csv`
- `data/output/cuebox_tags.csv`

---

## Assumptions & Decisions

### Constituent Type: Person vs Company
- If the input `Company` field is non-empty, the record is classified as `Company`.
- Otherwise, the record is classified as `Person`.

Rationale: the input contains an explicit Company field; using it provides a clear, deterministic rule and avoids “guessing” from name strings.

### Missing Names for Person records
CueBox requires `CB First Name` and `CB Last Name` if `CB Constituent Type = Person`. In the input, some records have missing first/last name (both missing together).

Decision:
- For Person records with missing names, I populate placeholders:
  - `CB First Name = "Unknown"`
  - `CB Last Name = "Unknown-<PatronID>"`

Rationale: this preserves the record for client review/import while keeping placeholders non-personal and traceable to the original record.

### Emails
Requirements:
- Email fields must be standardized and well formatted for a valid domain.
- Email 2 must not equal Email 1.
- Email 2 cannot be present if Email 1 is not present.

Decision:
- Candidate emails are taken in priority order:
  1) `Primary Email` from the constituents sheet (if valid)
  2) Additional emails from the emails sheet (file order)
- Emails are standardized (lowercased, trimmed) and validated conservatively.
- Invalid emails are dropped (output as empty strings).

Rationale: avoid “fixing” typos in emails without client confirmation; keep output compliant with formatting requirements.

### Donations
Decision:
- `Refunded` donations are excluded from lifetime totals and “most recent donation” metrics.
- Donation amounts are parsed from currency strings like `$3,000.00` and output formatted as `$#,###.##`.
- If a constituent has no (paid) donations, donation metric fields are empty strings.

Rationale: refunded rows should not inflate totals; requirement asks for donation metrics and allows empties when none exist.

### Tags + Tag Mapping API
- Input tags are comma-separated strings. I split, trim whitespace, and de-duplicate per constituent.
- I call the provided API to map `name -> mapped_name`. If the API is unavailable, tags are left as-is.
- After mapping, tags are de-duplicated again because multiple raw tags may map to the same output tag (e.g., “Top Donor” -> “Major Donor”).

The tags output file counts **unique constituents** per mapped tag.

---

## QA Process

I performed a combination of automated and manual QA:

1) **Automated validation** (`src/validate.py`):
- checks required columns exist
- validates Person/Company required fields
- validates email rules (email2 requires email1, email1 != email2, formatting)
- validates allowed values for `CB Title` (Mr./Mrs./Ms./Dr./empty)
- validates tag output schema

2) **Spot checks**:
- random sample of Person vs Company classifications
- sample of records with missing names to confirm placeholder rule is applied
- ensure refunded donations do not contribute to totals
- ensure tag mapping merges multiple raw tags correctly

---
### QA Findings & Resolutions (Data Anomalies)

During QA I found a few input inconsistencies that required explicit handling decisions.

#### 1) Orphan Patron IDs appearing only in `donations.csv` (e.g., Patron ID = 1234)
**Finding**

Some Patron IDs exist in `donations.csv` but do not appear in `constituents.csv` or `emails.csv`.

**Risk**
If the output is built strictly from `constituents.csv`, these donors would be dropped entirely and their lifetime / recent donation metrics would be lost.

**Resolution**
I expanded the “ID universe” to include the union of Patron IDs across all three inputs (`constituents`, `emails`, `donations`).
- For IDs missing from `constituents.csv`, the output row is still created so donation metrics are preserved.
- Fields that rely on missing constituent data (name/company/title/tags/background) remain empty or use the same conservative placeholder rules required by CueBox.

**Impact**
Donation totals are preserved for all Patron IDs present in donation history, even if the donor record is missing in the other input files.

---

#### 2) Duplicate Patron IDs in `constituents.csv` with conflicting attributes (e.g., Patron ID = 1288)
**Finding**
Some Patron IDs appear multiple times in `constituents.csv`. In certain cases core attributes conflict (e.g., different first/last names for the same Patron ID).

**Risk**
The company expects one constituent record per Constituent ID. If duplicates are not resolved, the output may contain multiple rows for the same ID, leading to ambiguous imports and mismatched joins.

**Resolution**
I deduplicate `constituents.csv` to enforce one row per Patron ID using a deterministic rule:
- Keep the row with the most recent `Date Entered` (ties resolved deterministically, e.g., stable file order).

**Impact**
Each `CB Constituent ID` appears once in the output. When duplicates exist, older/conflicting values may be dropped in favor of the most recent record. But in real work, I would ask the customer regarding their needs first.

---
#### 3) API Mapping Problem

**Finding**

The map specifies to map 'Major Donor 2021' and 'Top Donor' to 'Major Donor', but does not specify tag "Major Donor 2022" in the mapping. This leads to a new separate tag 'Major Donor 2022' in my final tag counts output. I might assume this API is an outdated version and will ask the clients their requirement to keep the 'Major Donor 2022' tag or merge.
---

#### Recommended Verification Checks
- Confirm `CB Constituent ID` is unique in the final output.
- Reconcile donation sums per Patron ID between `donations.csv` and `CB Lifetime Donation Amount`.
- Review any Patron IDs flagged as “duplicates” or “orphan donation-only IDs” if client wants a manual merge policy.


## AI Tool Usage

I used AI tools for:
- brainstorming an initial project structure (module boundaries, validation checklist)
- quick syntax reminders (some basic str operation functions)
- API setup and mapping (use of request.get(), resp.raise_for_status())

I did **not** rely on AI to generate the final transformation logics without reviewing them against the requirements and the actual input data. I manually validated all requirements, edge cases, and outputs.

---

## Repository Structure

- `main.py` - CLI entrypoint; reads inputs and writes outputs
- `src/transform.py` - transformation logic (emails, donations, tags, mapping)
- `src/validate.py` - basic validation checks
- `data/output/` - generated output CSVs (committed)
- `data/input/` - input CSVs (git-ignored)
