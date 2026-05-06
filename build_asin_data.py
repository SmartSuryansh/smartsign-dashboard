"""
build_asin_data.py
==================
Generates asin_data.json from the OOS YoY xlsx workbook.

Usage:
    python build_asin_data.py <path_to_xlsx> [output_json]

The script reads the 'OOS YoY Mon' sheet and produces a compact JSON optimized
for the asin.html dashboard. Run this every Monday after Lesa's team uploads
the new OOS file.

YoY Inactive logic (per Rashmi):
  An ASIN qualifies for "Month-X YoY Inactive" if BOTH:
    1. Was alive in 2025 same month: Spend > 0 OR Total Sales > 0
    2. EITHER any weekly Status snapshot in that 2026 month = "Inactive"
       OR  Spend_2026 == 0 AND Total_Sales_2026 == 0  (the brain rule)
"""

import json
import sys
import math
from datetime import datetime
import pandas as pd
import numpy as np

# ----- Config -----
SOURCE_SHEET = 'OOS YoY Mon'
HEADER_ROW = 2          # 0-indexed: row 2 contains the column names
DATE_ROW = 1            # row 1 contains the per-week dates for status/qty
ANCHOR_ROW = 0          # row 0 contains the per-month anchors for business metrics
DATA_START_ROW = 3      # ASIN rows start at row 3

# Map status snapshot dates -> calendar month buckets for YoY Inactive logic
MONTH_WEEKS = {
    'jan':  ['2026-01-05', '2026-01-12', '2026-01-19', '2026-01-27'],
    'feb':  ['2026-02-03', '2026-02-09', '2026-02-16', '2026-02-23'],
    'mar':  ['2026-03-02', '2026-03-09', '2026-03-16', '2026-03-23', '2026-03-30'],
    'apr':  ['2026-04-06', '2026-04-13', '2026-04-20', '2026-04-27'],
}
MONTH_LABELS = {'jan': 'January', 'feb': 'February', 'mar': 'March', 'apr': 'April'}

# Source has typos in row-0 anchors (col 36 says 2026-01-25 but is actually Jan 2025).
# We pair anchors positionally: 1st = this-yr, 2nd = last-yr, alternating.
MONTH_KEYS_ORDERED = ['jan', 'feb', 'mar', 'apr']


def shorten(s, n=120):
    if not isinstance(s, str):
        return s
    return s if len(s) <= n else s[:n - 1] + '\u2026'


def safe_num(v):
    """Convert a cell to a number; non-finite/missing -> 0.0."""
    if v is None:
        return 0.0
    try:
        x = float(v)
        if not math.isfinite(x):
            return 0.0
        return x
    except (TypeError, ValueError):
        return 0.0


def safe_int(v):
    return int(round(safe_num(v)))


def short_status(s):
    """Compress status to single chars to save JSON bytes."""
    if s is None or (isinstance(s, float) and math.isnan(s)):
        return 'U'
    s = str(s).strip().lower()
    if s == 'active':
        return 'A'
    if s == 'inactive':
        return 'I'
    return 'U'


def build(xlsx_path, out_path='asin_data.json'):
    print(f'Reading {xlsx_path} ...')
    raw = pd.read_excel(xlsx_path, sheet_name=SOURCE_SHEET, header=None)
    print(f'  {len(raw)} rows x {len(raw.columns)} cols loaded')

    header = raw.iloc[HEADER_ROW]
    date_row = raw.iloc[DATE_ROW]
    anchor_row = raw.iloc[ANCHOR_ROW]

    # ----- Locate the 8 month-anchor positions (4 months x 2 years, alternating) -----
    anchor_positions = [
        i for i in range(len(anchor_row)) if pd.notna(anchor_row.iloc[i])
    ]
    if len(anchor_positions) != 8:
        raise ValueError(f'Expected 8 month anchors, found {len(anchor_positions)}')

    # Pair them as (this_yr, last_yr) per month, in declared order
    month_anchor_cols = {}
    for idx, mkey in enumerate(MONTH_KEYS_ORDERED):
        month_anchor_cols[mkey] = {
            '26': anchor_positions[idx * 2],
            '25': anchor_positions[idx * 2 + 1],
        }

    print('  Month anchor cols:', month_anchor_cols)

    METRIC_ORDER = ['Spend', 'Paid Orders', 'Ad Sales', 'Sessions', 'Total Sales', 'Total Orders']
    # Each anchor is followed by 6 metrics in the order above

    # ----- Locate weekly Status & Qty columns -----
    status_cols = []  # list of (col_idx, date_str)
    qty_cols = []
    for i in range(len(header)):
        h = str(header.iloc[i]).strip() if pd.notna(header.iloc[i]) else ''
        d = date_row.iloc[i]
        if h == 'Status' and pd.notna(d):
            status_cols.append((i, pd.Timestamp(d).strftime('%Y-%m-%d')))
        elif h == 'Qty' and pd.notna(d):
            qty_cols.append((i, pd.Timestamp(d).strftime('%Y-%m-%d')))

    print(f'  Found {len(status_cols)} status weeks, {len(qty_cols)} qty weeks')

    status_dates = [d for _, d in status_cols]
    qty_dates = [d for _, d in qty_cols]
    # Map status date -> col idx for quick lookup in YoY logic
    status_date_to_col = dict(status_cols)

    # ----- Locate master attribute columns by header name -----
    name_to_col = {}
    for i in range(len(header)):
        h = str(header.iloc[i]).strip() if pd.notna(header.iloc[i]) else ''
        # Use first occurrence of each non-empty name (skip Status/Qty which repeat)
        if h and h not in ('Status', 'Qty', 'Spend', 'Paid Orders', 'Ad Sales',
                          'Sessions', 'Sessions ', 'Total Sales', 'Total Orders'):
            if h not in name_to_col:
                name_to_col[h] = i

    print('  Master attr columns:', name_to_col)

    # ----- Slice the data block -----
    data = raw.iloc[DATA_START_ROW:].reset_index(drop=True)
    n = len(data)

    # ----- Pre-extract numeric arrays for each business metric -----
    biz_arrays = {}  # (mkey, year, metric_index) -> np.array of n
    for mkey, yr_cols in month_anchor_cols.items():
        for yr in ('26', '25'):
            base = yr_cols[yr]
            for k, met in enumerate(METRIC_ORDER):
                col_idx = base + k
                arr = pd.to_numeric(data.iloc[:, col_idx], errors='coerce').fillna(0).values.astype(float)
                biz_arrays[(mkey, yr, k)] = arr

    # ----- Pre-extract status & qty arrays -----
    status_arr = {}  # date -> np.array of single-char codes
    for col_idx, d in status_cols:
        s = data.iloc[:, col_idx].apply(short_status).values
        status_arr[d] = s

    qty_arr = {}
    for col_idx, d in qty_cols:
        qty_arr[d] = pd.to_numeric(data.iloc[:, col_idx], errors='coerce').fillna(0).values

    # ----- YoY Inactive flag computation per ASIN per month -----
    yoyi_flags = {}  # mkey -> np.bool array
    yoyi_breakdown = {}  # mkey -> dict
    for mkey, weeks in MONTH_WEEKS.items():
        spend_25 = biz_arrays[(mkey, '25', 0)]   # Spend
        sales_25 = biz_arrays[(mkey, '25', 4)]   # Total Sales
        spend_26 = biz_arrays[(mkey, '26', 0)]
        sales_26 = biz_arrays[(mkey, '26', 4)]

        alive_25 = (spend_25 > 0) | (sales_25 > 0)

        inactive_wk = np.zeros(n, dtype=bool)
        for w in weeks:
            inactive_wk = inactive_wk | (status_arr[w] == 'I')

        zero_26 = (spend_26 == 0) & (sales_26 == 0)
        qual = alive_25 & (inactive_wk | zero_26)

        yoyi_flags[mkey] = qual

        # Track detection breakdown
        wk_only = alive_25 & inactive_wk & ~zero_26
        zero_only = alive_25 & zero_26 & ~inactive_wk
        both_sig = alive_25 & inactive_wk & zero_26

        yoyi_breakdown[mkey] = {
            'qualified': int(qual.sum()),
            'detected_by_status_only': int(wk_only.sum()),
            'detected_by_zero_rule_only': int(zero_only.sum()),
            'detected_by_both': int(both_sig.sum()),
        }

    # ----- Build per-ASIN records -----
    print(f'  Building {n} ASIN records...')
    asins = []
    parent_idx = {}  # parent ASIN -> [child idx,...]

    # Helper: get column value
    def gc(name, row_i):
        if name not in name_to_col:
            return None
        v = data.iloc[row_i, name_to_col[name]]
        return None if pd.isna(v) else v

    skipped = 0
    for i in range(n):
        cid = gc('Child Asin', i)
        if not cid:
            skipped += 1
            continue
        cid = str(cid).strip()
        if not cid:
            skipped += 1
            continue

        pasin = gc('Parent Asin', i)
        psku = gc('Parent Sku', i)
        ssku = gc('Seller Sku', i)
        sku = gc('SKU', i)
        cat = gc('Category', i)
        scat = gc('SubCategory', i)
        own = gc('Ownership', i)
        ttl = gc('Title', i)
        prc = gc('Price', i)
        ful = gc('Fulfillment0channel', i)
        rmk = gc('Remark', i)
        opn = gc('Open Date', i)

        # Normalize fulfillment (some cells are 0 instead of blank)
        if ful is not None and not isinstance(ful, str):
            ful = None

        # Open date -> ISO yyyy-mm-dd
        opn_iso = None
        if opn is not None:
            try:
                opn_iso = pd.Timestamp(opn).strftime('%Y-%m-%d')
            except Exception:
                pass

        # Build status array (18 codes)
        stat = ''.join(status_arr[d][i] for d in status_dates)
        # Build qty array (18 ints)
        qty = [int(qty_arr[d][i]) for d in qty_dates]

        # Build biz block (compact: per-period array of 6 metrics)
        biz = {}
        for mkey in MONTH_KEYS_ORDERED:
            for yr in ('26', '25'):
                vals = [round(float(biz_arrays[(mkey, yr, k)][i]), 2) for k in range(6)]
                # Convert paid orders / sessions / total orders to ints
                vals[1] = int(round(vals[1]))
                vals[3] = int(round(vals[3]))
                vals[5] = int(round(vals[5]))
                biz[f'{mkey}{yr}'] = vals

        # YoY Inactive flags
        yoyi = [mkey for mkey in MONTH_KEYS_ORDERED if yoyi_flags[mkey][i]]

        rec = {
            'id': cid,
            'pa': str(pasin).strip() if pasin else None,
            'ps': str(psku).strip() if psku else None,
            'ss': str(ssku).strip() if ssku else None,
            'sk': str(sku).strip() if sku else None,
            'c':  str(cat).strip() if cat else None,
            'sc': str(scat).strip() if scat else None,
            'o':  str(own).strip() if own else None,
            't':  shorten(str(ttl).strip(), 140) if ttl else None,
            'f':  ful,
            'p':  round(safe_num(prc), 2) if prc is not None else None,
            'od': opn_iso,
            'r':  shorten(str(rmk).strip(), 80) if rmk else None,
            'st': stat,
            'q':  qty,
            'b':  biz,
            'yi': yoyi,
        }
        asins.append(rec)

        if rec['pa']:
            parent_idx.setdefault(rec['pa'], []).append(len(asins) - 1)

    print(f'  Built {len(asins)} ASIN records ({skipped} skipped — no Child ASIN)')

    # ----- Aggregate summary stats -----
    n_asins = len(asins)
    latest_status_date = status_dates[-1]
    n_active_latest = sum(1 for a in asins if a['st'][-1] == 'A')
    n_inactive_latest = sum(1 for a in asins if a['st'][-1] == 'I')

    # YoY Inactive impact summary (per month)
    yoyi_summary = []
    for mkey in MONTH_KEYS_ORDERED:
        flagged_idx = np.where(yoyi_flags[mkey])[0]
        impact = {}
        for k, met in enumerate(METRIC_ORDER):
            v25 = float(biz_arrays[(mkey, '25', k)][flagged_idx].sum())
            v26 = float(biz_arrays[(mkey, '26', k)][flagged_idx].sum())
            impact[met] = {'25': round(v25, 2), '26': round(v26, 2)}
        yoyi_summary.append({
            'month': MONTH_LABELS[mkey],
            'mkey': mkey,
            'qualified': int(yoyi_flags[mkey].sum()),
            'detection': yoyi_breakdown[mkey],
            'impact': impact,
        })

    # Distinct values for filter dropdowns
    distinct = {
        'category': sorted({a['c'] for a in asins if a['c']}),
        'subcategory': sorted({a['sc'] for a in asins if a['sc']}),
        'ownership': sorted({a['o'] for a in asins if a['o']}),
        'fulfillment': sorted({a['f'] for a in asins if a['f']}),
    }

    # Aggregate timeline (for the small overview chart)
    timeline = []
    for d in status_dates:
        row = status_arr[d]
        active_n = int((row == 'A').sum())
        inactive_n = int((row == 'I').sum())
        timeline.append({
            'wk': d,
            'a': active_n,
            'i': inactive_n,
        })

    output = {
        'meta': {
            'generated_at': datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ'),
            'source_file': xlsx_path.split('/')[-1],
            'total_asins': n_asins,
            'status_dates': status_dates,
            'qty_dates': qty_dates,
            'months': [{'k': k, 'label': MONTH_LABELS[k]} for k in MONTH_KEYS_ORDERED],
            'metric_order': METRIC_ORDER,
            'biz_keys': [f'{m}{y}' for m in MONTH_KEYS_ORDERED for y in ('26', '25')],
        },
        'summary': {
            'total': n_asins,
            'active_latest': n_active_latest,
            'inactive_latest': n_inactive_latest,
            'latest_week': latest_status_date,
            'yoyi_total_unique': int(np.any(np.stack([yoyi_flags[m] for m in MONTH_KEYS_ORDERED]), axis=0).sum()),
        },
        'distinct': distinct,
        'yoyi_summary': yoyi_summary,
        'timeline': timeline,
        'asins': asins,
    }

    print(f'  Writing {out_path} ...')
    with open(out_path, 'w') as fp:
        json.dump(output, fp, separators=(',', ':'), default=str)

    import os
    sz_mb = os.path.getsize(out_path) / 1024 / 1024
    print(f'  Done. {sz_mb:.2f} MB written.')
    return output


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print('Usage: python build_asin_data.py <xlsx_path> [output_json]')
        sys.exit(1)
    src = sys.argv[1]
    dst = sys.argv[2] if len(sys.argv) > 2 else 'asin_data.json'
    build(src, dst)
