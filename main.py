from __future__ import annotations

import argparse
import os
import pandas as pd

from src.transform import transform, fetch_tag_mapping
from src.validate import validate_constituents, validate_tags


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Data import assignment transformer")
    p.add_argument("--constituents", default="data/input/constituents.csv")
    p.add_argument("--emails", default="data/input/emails.csv")
    p.add_argument("--donations", default="data/input/donations.csv")
    p.add_argument("--out-constituents", default="data/output/output_constituents.csv")
    p.add_argument("--out-tags", default="data/output/output_tags.csv")
    p.add_argument("--no-network", action="store_true", help="Do not call tag mapping API; keep tags as-is")
    return p.parse_args()


def main() -> int:
    args = parse_args()

    constituents = pd.read_csv(args.constituents)
    emails = pd.read_csv(args.emails)
    donations = pd.read_csv(args.donations)

    mapping = {} if args.no_network else fetch_tag_mapping()

    out_constituents, out_tags, notes = transform(
        constituents=constituents,
        emails=emails,
        donations=donations,
        tag_mapping=mapping,
    )

    # Validate
    issues = []
    issues.extend(validate_constituents(out_constituents))
    issues.extend(validate_tags(out_tags))

    os.makedirs(os.path.dirname(args.out_constituents), exist_ok=True)
    os.makedirs(os.path.dirname(args.out_tags), exist_ok=True)

    out_constituents.to_csv(args.out_constituents, index=False)
    out_tags.to_csv(args.out_tags, index=False)

    print("Wrote:")
    print(" -", args.out_constituents, f"({len(out_constituents)} rows)")
    print(" -", args.out_tags, f"({len(out_tags)} rows)")
    print("Notes:", notes)

    if issues:
        print("\nVALIDATION ISSUES:")
        for i in issues:
            print(" -", i)
        # Non-zero exit so it's obvious something needs attention
        return 2

    print("\nValidation passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
