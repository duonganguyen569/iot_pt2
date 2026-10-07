"""Per-device/session cleaning. Retrospective IQR and interpolation, not a predictive model."""
import numpy as np
import pandas as pd
from iot.config import SENSORS, RANGES


def process(raw, rule='10s'):
    if raw.empty:
        return pd.DataFrame()
    output = []
    for (device, run), group in raw.groupby(['device_id', 'run_id']):
        g = group.copy().sort_values('sent_at')
        g.index = pd.to_datetime(g.sent_at, unit='s', utc=True)
        clean = pd.DataFrame(index=g.index)
        flags = pd.DataFrame(index=g.index)
        for s in SENSORS:
            x = g[s].astype(float)
            lo, hi = RANGES[s]
            bad = (x < lo) | (x > hi)
            valid = x.mask(bad).dropna()
            if len(valid) >= 8:
                q1, q3 = valid.quantile([.25, .75])
                iqr = q3-q1
                if iqr > 1e-9:
                    bad |= (x < q1-1.5*iqr) | (x > q3+1.5*iqr)
            flags[s+'_missing'] = x.isna().astype(int)
            flags[s+'_outlier'] = bad.astype(int)
            clean[s] = x.mask(bad)
        result = clean.resample(rule).mean()
        counts = g['seq'].resample(rule).count()
        result['sample_count'] = counts
        result['empty_bin'] = (counts == 0).astype(int)
        aggregated_flags = flags.resample(rule).sum()
        for col in aggregated_flags:
            result[col] = aggregated_flags[col]
        for s in SENSORS:
            before = result[s].isna()
            # Fill at most 3 consecutive internal bins, never a long outage or edge.
            runs = before.ne(before.shift()).cumsum()
            gap_size = before.groupby(runs).transform('sum')
            estimate = result[s].interpolate(method='time', limit_area='inside')
            result[s] = result[s].where(~(before & (gap_size <= 3)), estimate)
            result[s+'_imputed'] = (before & result[s].notna()).astype(int)
            result[s+'_rolling_mean'] = result[s].rolling(5, min_periods=1).mean()
            result[s+'_delta'] = result[s].diff()
            lo, hi = RANGES[s]
            result[s+'_normalized'] = (result[s]-lo)/(hi-lo)
        result['device_id'], result['run_id'] = device, run
        result['timestamp'] = result.index.strftime('%Y-%m-%dT%H:%M:%SZ')
        output.append(result.reset_index(drop=True))
    out = pd.concat(output, ignore_index=True)
    # JSON/SQLite/Influx boundary: NaN is missing, never silently converted to zero.
    return out.replace([np.inf, -np.inf], np.nan).astype(object).where(pd.notna(out), None)
