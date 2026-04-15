import json, statistics
from pathlib import Path

results = {}
for cfg_dir in sorted(Path('output/logs').iterdir()):
    if not cfg_dir.is_dir():
        continue
    fd_algo, repl = '?', '?'
    lats = []
    for trial_dir in sorted(cfg_dir.iterdir()):
        if not trial_dir.is_dir():
            continue
        inj = trial_dir / 'injector.jsonl'
        if not inj.exists():
            continue
        for line in open(inj):
            try:
                e = json.loads(line)
                if e.get('event') == 'run_start':
                    fd_algo = e.get('fd_algo', '?')
                    repl    = e.get('repl_mode', '?')
                if e.get('event') == 'detection_result':
                    lat = e.get('detection_latency_ms')
                    if lat:
                        lats.append(lat)
            except Exception:
                pass
    if lats:
        results[cfg_dir.name] = {'fd': fd_algo, 'repl': repl, 'lats': lats}

header = "{:32s} | {:5s} | {:5s} | {:2s} | {:6s} | {:4s} | {}".format(
    'Config', 'fd', 'repl', 'N', 'median', 'min', 'max')
print(header)
print('-' * 75)
for cfg, v in sorted(results.items()):
    med = statistics.median(v['lats'])
    row = "{:32s} | {:5s} | {:5s} | {:2d} | {:6.0f} | {:4d} | {}".format(
        cfg, v['fd'], v['repl'], len(v['lats']),
        med, min(v['lats']), max(v['lats']))
    print(row)
