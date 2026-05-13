"""
build_asin_data.py
==================
Generates asin_data.json from the OOS YoY xlsx workbook.

Usage:
    python build_asin_data.py <path_to_xlsx> [output_json]

YoY Inactive logic (matches the per-month YoY Inactive sheets in the workbook):
  An ASIN qualifies for "Month-X YoY Inactive" if BOTH:
    1. Status = "Inactive" in any weekly snapshot during that month in 2026
    2. Any of the 6 business metrics (Spend / Paid Orders / Ad Sales / Sessions /
       Total Sales / Total Orders) > 0 in either 2025 OR 2026 for that same month.

Reverse-engineered from the per-month YoY Inactive sheets to match exactly.
"""

import json
import sys
import math
from datetime import datetime
import pandas as pd
import numpy as np

SOURCE_SHEET = 'OOS YoY Mon'
HEADER_ROW = 2
DATE_ROW = 1
ANCHOR_ROW = 0
DATA_START_ROW = 3

MONTH_WEEKS = {
    'jan':  ['2026-01-05', '2026-01-12', '2026-01-19', '2026-01-27'],
    'feb':  ['2026-02-03', '2026-02-09', '2026-02-16', '2026-02-23'],
    'mar':  ['2026-03-02', '2026-03-09', '2026-03-16', '2026-03-23', '2026-03-30'],
    'apr':  ['2026-04-06', '2026-04-13', '2026-04-20', '2026-04-27'],
}
MONTH_LABELS = {'jan': 'January', 'feb': 'February', 'mar': 'March', 'apr': 'April'}
MONTH_KEYS_ORDERED = ['jan', 'feb', 'mar', 'apr']


def shorten(s, n=140):
    if not isinstance(s, str): return s
    return s if len(s) <= n else s[:n - 1] + '\u2026'

def safe_num(v):
    if v is None: return 0.0
    try:
        x = float(v)
        return 0.0 if not math.isfinite(x) else x
    except (TypeError, ValueError):
        return 0.0

def short_status(s):
    if s is None or (isinstance(s, float) and math.isnan(s)): return 'U'
    s = str(s).strip().lower()
    if s == 'active': return 'A'
    if s == 'inactive': return 'I'
    return 'U'


def build(xlsx_path, out_path='asin_data.json'):
    print(f'Reading {xlsx_path} ...')
    raw = pd.read_excel(xlsx_path, sheet_name=SOURCE_SHEET, header=None)
    print(f'  {len(raw)} rows x {len(raw.columns)} cols loaded')

    header = raw.iloc[HEADER_ROW]
    date_row = raw.iloc[DATE_ROW]
    anchor_row = raw.iloc[ANCHOR_ROW]

    anchor_positions = [i for i in range(len(anchor_row)) if pd.notna(anchor_row.iloc[i])]
    if len(anchor_positions) != 8:
        raise ValueError(f'Expected 8 month anchors, found {len(anchor_positions)}')

    month_anchor_cols = {}
    for idx, mkey in enumerate(MONTH_KEYS_ORDERED):
        month_anchor_cols[mkey] = {
            '26': anchor_positions[idx * 2],
            '25': anchor_positions[idx * 2 + 1],
        }
    print('  Month anchor cols:', month_anchor_cols)

    METRIC_ORDER = ['Spend', 'Paid Orders', 'Ad Sales', 'Sessions', 'Total Sales', 'Total Orders']

    status_cols = []
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

    name_to_col = {}
    for i in range(len(header)):
        h = str(header.iloc[i]).strip() if pd.notna(header.iloc[i]) else ''
        if h and h not in ('Status','Qty','Spend','Paid Orders','Ad Sales','Sessions','Sessions ','Total Sales','Total Orders'):
            if h not in name_to_col: name_to_col[h] = i
    print('  Master attr columns:', name_to_col)

    data = raw.iloc[DATA_START_ROW:].reset_index(drop=True)
    n = len(data)

    biz_arrays = {}
    for mkey, yr_cols in month_anchor_cols.items():
        for yr in ('26', '25'):
            base = yr_cols[yr]
            for k, met in enumerate(METRIC_ORDER):
                col_idx = base + k
                arr = pd.to_numeric(data.iloc[:, col_idx], errors='coerce').fillna(0).values.astype(float)
                biz_arrays[(mkey, yr, k)] = arr

    status_arr = {}
    for col_idx, d in status_cols:
        s = data.iloc[:, col_idx].apply(short_status).values
        status_arr[d] = s
    qty_arr = {}
    for col_idx, d in qty_cols:
        qty_arr[d] = pd.to_numeric(data.iloc[:, col_idx], errors='coerce').fillna(0).values

    # CORRECTED YoY Inactive logic (matches Excel sheets exactly):
    yoyi_flags = {}
    yoyi_breakdown = {}
    for mkey, weeks in MONTH_WEEKS.items():
        inactive_wk = np.zeros(n, dtype=bool)
        for w in weeks:
            inactive_wk = inactive_wk | (status_arr[w] == 'I')
        any_activity = np.zeros(n, dtype=bool)
        for yr in ('25', '26'):
            for k in range(6):
                any_activity = any_activity | (biz_arrays[(mkey, yr, k)] > 0)
        qual = inactive_wk & any_activity
        yoyi_flags[mkey] = qual
        yoyi_breakdown[mkey] = {
            'qualified': int(qual.sum()),
            'inactive_in_month': int(inactive_wk.sum()),
            'inactive_with_zero_activity': int((inactive_wk & ~any_activity).sum()),
        }
    print('  YoY Inactive per month:', {k: v['qualified'] for k, v in yoyi_breakdown.items()})

    print(f'  Building {n} ASIN records...')
    asins = []
    skipped = 0

    def gc(name, row_i):
        if name not in name_to_col: return None
        v = data.iloc[row_i, name_to_col[name]]
        return None if pd.isna(v) else v

    for i in range(n):
        cid = gc('Child Asin', i)
        if not cid:
            skipped += 1; continue
        cid = str(cid).strip()
        if not cid:
            skipped += 1; continue

        pasin = gc('Parent Asin', i); psku = gc('Parent Sku', i)
        ssku = gc('Seller Sku', i); sku = gc('SKU', i)
        cat = gc('Category', i); scat = gc('SubCategory', i)
        own = gc('Ownership', i); ttl = gc('Title', i)
        prc = gc('Price', i); ful = gc('Fulfillment0channel', i)
        rmk = gc('Remark', i); opn = gc('Open Date', i)

        if ful is not None and not isinstance(ful, str):
            ful = None

        opn_iso = None
        if opn is not None:
            try: opn_iso = pd.Timestamp(opn).strftime('%Y-%m-%d')
            except: pass

        stat = ''.join(status_arr[d][i] for d in status_dates)
        qty = [int(qty_arr[d][i]) for d in qty_dates]

        biz = {}
        for mkey in MONTH_KEYS_ORDERED:
            for yr in ('26', '25'):
                vals = [round(float(biz_arrays[(mkey, yr, k)][i]), 2) for k in range(6)]
                vals[1] = int(round(vals[1]))
                vals[3] = int(round(vals[3]))
                vals[5] = int(round(vals[5]))
                biz[f'{mkey}{yr}'] = vals

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

    print(f'  Built {len(asins)} ASIN records ({skipped} skipped)')

    n_asins = len(asins)
    latest_status_date = status_dates[-1]
    n_active_latest = sum(1 for a in asins if a['st'][-1] == 'A')
    n_inactive_latest = sum(1 for a in asins if a['st'][-1] == 'I')

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

    distinct = {
        'category': sorted({a['c'] for a in asins if a['c']}),
        'subcategory': sorted({a['sc'] for a in asins if a['sc']}),
        'ownership': sorted({a['o'] for a in asins if a['o']}),
        'fulfillment': sorted({a['f'] for a in asins if a['f']}),
    }

    timeline = []
    for d in status_dates:
        row = status_arr[d]
        timeline.append({'wk': d, 'a': int((row=='A').sum()), 'i': int((row=='I').sum())})

    output = {
        'meta': {
            'generated_at': datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ'),
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
