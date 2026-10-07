def summarize(raw):
    if raw.empty:
        return {'unique_messages': 0, 'sequence_gaps': 0}
    gaps = 0
    for _, g in raw.groupby(['device_id', 'run_id']):
        # Only between first and last observed seq; includes intentional skips and invalid JSON.
        gaps += int(g.seq.max()-g.seq.min()+1-g.seq.nunique())
    result = dict(unique_messages=len(raw), sequence_gaps=gaps,
                latency_mean_ms=float(raw.latency_ms.mean()),
                latency_p95_ms=float(raw.latency_ms.quantile(.95)),
                latency_max_ms=float(raw.latency_ms.max()))

    if 'first_processed_at' in raw:
        delay = (raw.first_processed_at-raw.sent_at).dropna()*1000
        if not delay.empty:
            result['to_processed_mean_ms'] = float(delay.mean())
            result['to_processed_p95_ms'] = float(delay.quantile(.95))
    return result
