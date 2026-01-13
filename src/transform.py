"""
Transformation logic for the data import project.

Inputs:
- constituents.csv: one row per constituent (person or company)
- emails.csv: one row per email address per constituent (Patron ID + Email)
- donations.csv: one row per donation (Patron ID + amount/date/etc)

Outputs:
- Constituents CSV (client sign-off + import)
- Tags CSV (tag counts)
"""
from typing import Dict, List, Optional, Tuple
import pandas as pd
import numpy as np

# We keep requests optional: transformation still works without network access.
try:
    import requests  # type: ignore
except Exception:  # pragma: no cover
    requests = None  # type: ignore


TAG_MAPPING_URL = "https://6719768f7fc4c5ff8f4d84f1.mockapi.io/api/v1/tags"


def _clean_str(x) -> str:
    if pd.isna(x):
        return ""
    return str(x).strip()


def standardize_email(raw: str) -> str:
    """Lowercase and trim common wrappers."""
    s = _clean_str(raw).lower()
    # Remove common wrappers like "<email>" or quotes
    s = s.strip(" <>\"'")
    return s


def is_valid_email(email: str) -> bool:
    """
    Check for a valid email:
    - one '@'
    - at least one '.' in domain
    - no spaces
    """
    e = standardize_email(email)
    if not e or " " in e:
        return False
    if e.count("@") != 1:
        return False
    local, domain = e.split("@", 1)
    if not local or not domain:
        return False
    if "." not in domain:
        return False
    return True


def normalize_salutation(raw: str) -> str:
    """
    Title to be one of: Mr., Mrs., Ms., Dr., or empty string.
    Input may contain 'Mr', 'Dr', 'Rev', 'Mr. and Mrs.', etc.
    """
    s = _clean_str(raw)
    if not s:
        return ""
    s = s.replace(".", "").strip().lower()

    mapping = {
        "mr": "Mr.",
        "mrs": "Mrs.",
        "ms": "Ms.",
        "dr": "Dr.",
    }
    return mapping.get(s, "")


def parse_date_any(raw: str) -> str:
    """
    Parse a date string into YYYY-MM-DD.
    Returns empty string if missing.
    """
    s = _clean_str(raw)
    if not s:
        return ""
    dt = pd.to_datetime(s, errors="coerce", infer_datetime_format=True)
    if pd.isna(dt):
        return ""
    return dt.date().isoformat()


def parse_currency_to_float(raw: str) -> Optional[float]:
    """
    Remove currency symbols and commas for USD Amount
    """
    s = _clean_str(raw)
    if not s:
        return None

    s2 = s.replace("$", "").replace(",", "").strip()
    try:
        return float(s2)
    except ValueError:
        return None


def format_currency(amount: Optional[float]) -> str:
    """
    Add currency symbols back to the Amount
    """  
    if amount is None or (isinstance(amount, float) and np.isnan(amount)):
        return ""
    return f"${amount:,.2f}"


def split_tags(raw: str) -> List[str]:
    """
    Split tags from a comma-separated string, trim whitespace, drop empties, de-duplicate.
    """
    s = _clean_str(raw)
    if not s:
        return []
    parts = [p.strip() for p in s.split(",")]
    # Drop empties while preserving order, then de-dupe preserving order
    out: List[str] = []
    seen = set()
    for p in parts:
        if not p:
            continue
        key = p.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(p)
    return out


def fetch_tag_mapping(url: str = TAG_MAPPING_URL, timeout_s: int = 10) -> Dict[str, str]:
    """
    Fetch mapping of original tag name -> desired mapped_name from the provided API.
    Returns {} if requests isn't installed or fetch fails.
    """
    if requests is None:
        return {}
    try:
        resp = requests.get(url, timeout=timeout_s)
        resp.raise_for_status()
        data = resp.json()
        mapping = {}
        for row in data:
            name = _clean_str(row.get("name", ""))
            mapped = _clean_str(row.get("mapped_name", ""))
            if name and mapped:
                # normalize keys for robust matching
                mapping[name.strip().lower()] = mapped.strip()
        return mapping
    except Exception:
        return {}


def apply_tag_mapping(tags: List[str], mapping: Dict[str, str]) -> List[str]:
    """
    Apply API mapping to tag list. If tag not in mapping, keep original.
    After mapping, de-duplicate (because multiple originals may map to same target).
    """
    out: List[str] = []
    seen = set()
    for t in tags:
        key = t.strip().lower()
        mapped = mapping.get(key, t)
        mapped_clean = _clean_str(mapped)
        if not mapped_clean:
            continue
        k2 = mapped_clean.lower()
        if k2 in seen:
            continue
        seen.add(k2)
        out.append(mapped_clean)
    return out


def classify_constituent_type(row: pd.Series) -> str:
    """
    Decide if constituent is Person or Company.
    Rules:
    - If Company column is non-empty => Company
    - Else => Person
    """
    company = _clean_str(row.get("Company", ""))
    return "Company" if company else "Person"


def fill_missing_person_names(patron_id: str, first: str, last: str) -> Tuple[str, str]:
    """
    First + last for Person. If missing, we use a non-personal placeholder.
    """
    f = _clean_str(first)
    l = _clean_str(last)
    if f and l:
        return f, l
    return "Unknown", f"Unknown-{patron_id}"


def build_email_columns(
    constituents: pd.DataFrame,
    emails: pd.DataFrame,
) -> pd.DataFrame:
    """
    Build CB Email 1 and CB Email 2 per Patron ID.

    Priority order:
    1) Primary Email (from constituents) if valid
    2) Additional emails from emails table (in file order) if valid

    Constraints:
    - Email 2 must be different from Email 1
    - Email 2 cannot be present if Email 1 is empty
    """
    # Start from primary email in constituents
    base = constituents[["Patron ID", "Primary Email"]].copy()
    base["Patron ID"] = base["Patron ID"].astype(str)

    # Collect candidate emails per patron in order
    candidates: Dict[str, List[str]] = {pid: [] for pid in base["Patron ID"].tolist()}

    for _, r in base.iterrows():
        pid = str(r["Patron ID"])
        pe = standardize_email(_clean_str(r.get("Primary Email", "")))
        if pe and is_valid_email(pe):
            candidates[pid].append(pe)

    # Add from emails table
    em = emails.copy()
    em["Patron ID"] = em["Patron ID"].astype(str)
    em["Email_std"] = em["Email"].map(standardize_email)

    for _, r in em.iterrows():
        pid = str(r["Patron ID"])
        email = r["Email_std"]
        if not email or not is_valid_email(email):
            continue
        if pid not in candidates:
            candidates[pid] = []
        candidates[pid].append(email)

    # Deduplicate while preserving order
    out_rows = []
    for pid, arr in candidates.items():
        seen = set()
        uniq = []
        for x in arr:
            if x in seen:
                continue
            seen.add(x)
            uniq.append(x)
        email1 = uniq[0] if len(uniq) >= 1 else ""
        email2 = ""
        if email1 and len(uniq) >= 2:
            email2 = uniq[1]
            if email2 == email1:
                email2 = ""
        out_rows.append({"Patron ID": pid, "CB Email 1 (Standardized)": email1, "CB Email 2 (Standardized)": email2})

    return pd.DataFrame(out_rows)


def build_donation_metrics(donations: pd.DataFrame) -> pd.DataFrame:
    """
    Compute donation metrics per Patron ID:
    - lifetime donation amount (sum)
    - most recent donation date
    - most recent donation amount
    """
    df = donations.copy()
    df["Patron ID"] = df["Patron ID"].astype(str)
    df["amount_num"] = df["Donation Amount"].map(parse_currency_to_float)
    df["date_dt"] = pd.to_datetime(df["Donation Date"], errors="coerce")

    # Filter to paid only if Status exists
    if "Status" in df.columns:
        df = df[df["Status"].fillna("").str.lower() == "paid"].copy()

    # Drop rows with missing amount or date
    df = df.dropna(subset=["amount_num", "date_dt"])

    if df.empty:
        return pd.DataFrame(columns=[
            "Patron ID",
            "CB Lifetime Donation Amount",
            "CB Most Recent Donation Date",
            "CB Most Recent Donation Amount",
        ])

    # Lifetime sum
    lifetime = df.groupby("Patron ID", as_index=False)["amount_num"].sum().rename(columns={"amount_num": "lifetime_sum"})

    # Most recent (by date; if tie, take the largest amount)
    df_sorted = df.sort_values(["Patron ID", "date_dt", "amount_num"], ascending=[True, False, False])
    recent = df_sorted.groupby("Patron ID", as_index=False).first()[["Patron ID", "date_dt", "amount_num"]]
    recent = recent.rename(columns={"date_dt": "recent_date", "amount_num": "recent_amount"})

    out = lifetime.merge(recent, on="Patron ID", how="outer")
    out["CB Lifetime Donation Amount"] = out["lifetime_sum"].map(lambda x: format_currency(float(x)) if pd.notna(x) else "")
    out["CB Most Recent Donation Date"] = out["recent_date"].map(lambda x: x.date().isoformat() if pd.notna(x) else "")
    out["CB Most Recent Donation Amount"] = out["recent_amount"].map(lambda x: format_currency(float(x)) if pd.notna(x) else "")

    return out[["Patron ID", "CB Lifetime Donation Amount", "CB Most Recent Donation Date", "CB Most Recent Donation Amount"]]


def build_background_information(constituents: pd.DataFrame) -> pd.Series:
    """
    Construct CB Background Information.
    Requirement says include job title and marital status if present.
    Input only provides a 'Title' field that appears to be job title.
    """
    def make_row(job_title: str) -> str:
        jt = _clean_str(job_title)
        if not jt:
            return ""
        return f"Job Title: {jt}"

    return constituents["Title"].map(make_row)


def transform(
    constituents: pd.DataFrame,
    emails: pd.DataFrame,
    donations: pd.DataFrame,
    tag_mapping: Optional[Dict[str, str]] = None,
) -> Tuple[pd.DataFrame, pd.DataFrame, Dict[str, object]]:
    """
    Main transformation entrypoint.

    Returns:
    - constituents_df
    - tags_df
    - notes dict (useful for README/QA summaries)
    """
    c = constituents.copy()
    def dedupe_constituents_latest(c: pd.DataFrame) -> pd.DataFrame:
        c = c.copy()
        c["Patron ID"] = c["Patron ID"].astype(str)
        c["_date_entered_dt"] = pd.to_datetime(c["Date Entered"], errors="coerce")

        # Sort different names with same Patron ID(EXAMPLE: ID1288):
        c["_row"] = np.arange(len(c))
        c = c.sort_values(["Patron ID", "_date_entered_dt", "_row"], ascending=[True, False, False])

        # Use the name info with the latest date
        c = c.drop_duplicates(subset=["Patron ID"], keep="first")

        return c.drop(columns=["_date_entered_dt", "_row"])
    c = dedupe_constituents_latest(constituents)

    mapping = tag_mapping if tag_mapping is not None else fetch_tag_mapping()

    # Prepare email columns
    email_cols = build_email_columns(c, emails)

    donation_metrics = build_donation_metrics(donations)

    # Build a base table of ALL Patron IDs that appear in any input
    all_ids = pd.Index(c["Patron ID"]).union(email_cols["Patron ID"]).union(donation_metrics["Patron ID"])
    base = pd.DataFrame({"CB Constituent ID": all_ids.astype(str)})

    # Tags (mapped) per patron for both outputs
    raw_tags = c["Tags"].map(split_tags)
    mapped_tags = raw_tags.map(lambda lst: apply_tag_mapping(lst, mapping))
    c["_mapped_tags"] = mapped_tags

    # Constituent classification + names
    ctype = c.apply(classify_constituent_type, axis=1)
    c["CB Constituent Type"] = ctype

    # Start output from the full ID universe
    out = base.merge(c, left_on="CB Constituent ID", right_on="Patron ID", how="left")

    # Now build required output columns (safe even when c is missing -> NaNs)
    out["CB Constituent Type"] = out.apply(
        lambda r: classify_constituent_type(r) if pd.notna(r.get("Patron ID")) else "Person",
        axis=1
    )

    out["CB Created At"] = out["Date Entered"].map(parse_date_any)
    out["CB Title"] = out["Salutation"].map(normalize_salutation)

    # Tags
    out["_mapped_tags"] = out["Tags"].fillna("").map(split_tags).map(lambda lst: apply_tag_mapping(lst, mapping))
    out["CB Tags"] = out["_mapped_tags"].map(lambda lst: ", ".join(lst))

    # Background info
    out["CB Background Information"] = out["Title"].map(lambda x: f"Job Title: {_clean_str(x)}" if _clean_str(x) else "")

    # Company / Person fields
    out["CB Company Name"] = ""
    out["CB First Name"] = ""
    out["CB Last Name"] = ""

    is_company = out["CB Constituent Type"] == "Company"
    out.loc[is_company, "CB Company Name"] = out.loc[is_company, "Company"].map(_clean_str)

    is_person = ~is_company
    # fill names (only for rows that have Patron ID in constituents; otherwise placeholder)
    def _person_name_row(r):
        pid = r["CB Constituent ID"]
        first = r.get("First Name", "")
        last = r.get("Last Name", "")
        return fill_missing_person_names(str(pid), first, last)

    filled = out.loc[is_person].apply(_person_name_row, axis=1)
    out.loc[is_person, "CB First Name"] = [t[0] for t in filled]
    out.loc[is_person, "CB Last Name"] = [t[1] for t in filled]

    # Keep only needed core columns before merging other tables
    out = out[[
        "CB Constituent ID",
        "CB Constituent Type",
        "CB First Name",
        "CB Last Name",
        "CB Company Name",
        "CB Created At",
        "CB Title",
        "CB Tags",
        "CB Background Information",
        "_mapped_tags",
    ]]

    out = out.merge(email_cols, left_on="CB Constituent ID", right_on="Patron ID", how="left").drop(columns=["Patron ID"])
    out = out.merge(donation_metrics, left_on="CB Constituent ID", right_on="Patron ID", how="left").drop(columns=["Patron ID"])


    # If never donated => blanks (already blanks via merge NaNs)
    for col in ["CB Lifetime Donation Amount", "CB Most Recent Donation Date", "CB Most Recent Donation Amount"]:
        if col in out.columns:
            out[col] = out[col].fillna("")

    # Enforce email2 rule: must not exist if email1 missing
    m_no_e1 = out["CB Email 1 (Standardized)"].fillna("") == ""
    out.loc[m_no_e1, "CB Email 2 (Standardized)"] = ""

    # Tags output: counts of constituents per tag (post-mapping, per-constituent de-duped)
    tag_rows = []
    for pid, tags in zip(c["Patron ID"], c["_mapped_tags"]):
        for t in tags:
            tag_rows.append({"Patron ID": pid, "CB Tag Name": t})
    tags_df = pd.DataFrame(tag_rows)
    if tags_df.empty:
        tags_out = pd.DataFrame(columns=["CB Tag Name", "CB Tag Count"])
    else:
        # Unique tag per patron, then count patrons per tag
        tags_unique = tags_df.drop_duplicates(subset=["Patron ID", "CB Tag Name"])
        tags_out = tags_unique.groupby("CB Tag Name", as_index=False)["Patron ID"].nunique().rename(columns={"Patron ID": "CB Tag Count"})
        tags_out = tags_out.sort_values(["CB Tag Count", "CB Tag Name"], ascending=[False, True])

    notes = {
        "tag_mapping_loaded": bool(mapping),
        "num_constituents": int(len(out)),
        "num_companies": int(is_company.sum()),
        "num_people": int(is_person.sum()),
        "num_people_with_placeholder_names": int(((out["CB Constituent Type"] == "Person") & (out["CB First Name"] == "Unknown")).sum()),
        "donations_paid_rows": int((donations["Status"].fillna("").str.lower() == "paid").sum()) if "Status" in donations.columns else None,
        "donations_refunded_rows": int((donations["Status"].fillna("").str.lower() == "refunded").sum()) if "Status" in donations.columns else None,
    }

    # Final column ordering per requirements
    final_cols = [
        "CB Constituent ID",
        "CB Constituent Type",
        "CB First Name",
        "CB Last Name",
        "CB Company Name",
        "CB Created At",
        "CB Email 1 (Standardized)",
        "CB Email 2 (Standardized)",
        "CB Title",
        "CB Tags",
        "CB Background Information",
        "CB Lifetime Donation Amount",
        "CB Most Recent Donation Date",
        "CB Most Recent Donation Amount",
    ]
    # Keep only columns that exist (robustness)
    out = out[[c for c in final_cols if c in out.columns]]

    return out, tags_out, notes
