"""
Lightweight validation for output CSVs.

This is not meant to be exhaustive, but to catch common issues that break imports
or violate explicit requirements.
"""

from typing import List
import pandas as pd
from .transform import is_valid_email


def validate_constituents(df: pd.DataFrame) -> List[str]:
    issues: List[str] = []

    # Required columns
    required_cols = [
        "CB Constituent ID",
        "CB Constituent Type",
        "CB Created At",
        "CB Email 1 (Standardized)",
        "CB Email 2 (Standardized)",
        "CB Title",
    ]
    for c in required_cols:
        if c not in df.columns:
            issues.append(f"Missing required column: {c}")

    # Constituent type values
    if "CB Constituent Type" in df.columns:
        bad = df[~df["CB Constituent Type"].isin(["Person", "Company"])]
        if not bad.empty:
            issues.append(f"Invalid CB Constituent Type values found: {bad['CB Constituent Type'].unique().tolist()}")

    # Person required names; Company required company name
    if set(["CB Constituent Type", "CB First Name", "CB Last Name", "CB Company Name"]).issubset(df.columns):
        people = df[df["CB Constituent Type"] == "Person"]
        bad_people = people[(people["CB First Name"].fillna("").str.strip() == "") | (people["CB Last Name"].fillna("").str.strip() == "")]
        if not bad_people.empty:
            issues.append(f"{len(bad_people)} Person rows missing first/last name")

        companies = df[df["CB Constituent Type"] == "Company"]
        bad_companies = companies[companies["CB Company Name"].fillna("").str.strip() == ""]
        if not bad_companies.empty:
            issues.append(f"{len(bad_companies)} Company rows missing company name")

    # Email rules
    if "CB Email 1 (Standardized)" in df.columns and "CB Email 2 (Standardized)" in df.columns:
        e1 = df["CB Email 1 (Standardized)"].fillna("").astype(str)
        e2 = df["CB Email 2 (Standardized)"].fillna("").astype(str)

        # email2 cannot exist without email1
        bad_e2 = df[(e1.str.strip() == "") & (e2.str.strip() != "")]
        if not bad_e2.empty:
            issues.append(f"{len(bad_e2)} rows have Email2 present but Email1 missing")

        # email1 and email2 must be different
        bad_same = df[(e1.str.strip() != "") & (e1 == e2)]
        if not bad_same.empty:
            issues.append(f"{len(bad_same)} rows have Email1 equal to Email2")

        # validate formatting if present
        invalid_e1 = [x for x in e1.unique().tolist() if x.strip() and not is_valid_email(x)]
        invalid_e2 = [x for x in e2.unique().tolist() if x.strip() and not is_valid_email(x)]
        if invalid_e1:
            issues.append(f"Invalid Email1 values present (examples): {invalid_e1[:5]}")
        if invalid_e2:
            issues.append(f"Invalid Email2 values present (examples): {invalid_e2[:5]}")

    # Title allowed set
    if "CB Title" in df.columns:
        allowed = {"Mr.", "Mrs.", "Ms.", "Dr.", ""}
        bad_title = df[~df["CB Title"].fillna("").isin(allowed)]
        if not bad_title.empty:
            issues.append(f"Invalid CB Title values found: {bad_title['CB Title'].unique().tolist()}")

    return issues


def validate_tags(df: pd.DataFrame) -> List[str]:
    issues: List[str] = []
    if df.empty:
        return issues

    for col in ["CB Tag Name", "CB Tag Count"]:
        if col not in df.columns:
            issues.append(f"Missing required column: {col}")

    if "CB Tag Count" in df.columns:
        bad = df[pd.to_numeric(df["CB Tag Count"], errors="coerce").isna()]
        if not bad.empty:
            issues.append("Non-numeric CB Tag Count values found")

    return issues
