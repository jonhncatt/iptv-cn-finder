# Chinese IPTV Finder

This tool builds a verified M3U playlist of public mainland China core channels.
The default scope is intentionally narrow: only CCTV and mainland satellite TV channels.
Channel names and groups are written in Chinese wherever possible.
It is aimed at open playlists and channel indexes that are already publicly exposed.
It does not scrape private services or bypass paywalls.

## What it does

- pulls candidate streams from the public `iptv-org` dataset
- narrows the scope to mainland CCTV and satellite TV channels
- normalizes channel names and groups into Chinese labels like `央视` and `卫视`
- probes each stream over HTTP/HLS and keeps only the working ones
- runs an extra stability pass so flaky entries are dropped
- writes a clean M3U file plus a JSON report

## Requirements

- macOS or Linux
- Python 3.10+
- optional: `ffprobe` for deeper validation (`brew install ffmpeg`)

## Quick start

```bash
cd /Users/zhoudali/Desktop/iptv
python3 find_cn_streams.py --verbose
```

That writes:

- `output/chinese-public-verified.m3u`
- `output/chinese-public-report.json`

By default the script probes the target scope with two successful checks required per channel.
It skips raw IP-hosted streams unless you explicitly add `--allow-ip-hosts`, because those entries are much more likely to be unstable.

## Useful commands

Probe more aggressively:

```bash
python3 find_cn_streams.py --limit 0 --workers 32 --timeout 10 --verbose
```

Include raw IP sources too:

```bash
python3 find_cn_streams.py --allow-ip-hosts --limit 0 --verbose
```

Relax the stability filter:

```bash
python3 find_cn_streams.py --stability-checks 1 --retries 0 --verbose
```

Require HD-ish streams:

```bash
python3 find_cn_streams.py --min-quality 720 --verbose
```

Use `ffprobe` if installed:

```bash
python3 find_cn_streams.py --ffprobe --limit 150 --verbose
```

## Notes

- Some public streams are geo-restricted or unstable. Re-run the script when channels stop working.
- The generated M3U preserves `User-Agent` and `Referrer` hints when they exist.
- The default output is intentionally conservative. If very few channels pass, that usually means the public sources are currently unstable or blocked from your network.
