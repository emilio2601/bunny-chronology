import os
import sys
import json
import glob
import re

from collections import defaultdict
from typing import Dict, List, Tuple


# Hardcoded group exclusions (case-insensitive match against computed group names)
EXCLUDED_GROUPS = {
  # example: "windows", "ios", "Google Cast"
}

# Hardcoded exact platform exclusions (exact raw string match before grouping)
# e.g., "Windows 10 (Unknown Ed)", "Android 8.1.0 (Pixel 2)"
EXCLUDED_PLATFORMS_EXACT = {
  "iOS 5.1.1 (iPod4,1)",
  "iOS 7.1.1 (iPad2,1)",
  "iOS 6.1.3 (iPod4,1)",
  "Android OS 4.1.1 API 16 (samsung, SGH-I747M)",
  "Android OS 4.2.2 API 17 (TCT, ALCATEL ONE TOUCH 5036A)",
  "Android OS 5.0.2 API 21 (motorola, XT1032)",
  "Android-tablet OS 5.0.2 API 21 (samsung, SM-P350)",
  "Windows 10 (10.0.10586; x64)",
}

# Minimum total plays required for a platform group to be reported
MIN_GROUP_PLAYS = 25


def iter_history_records(folder: str):
  pattern = os.path.join(folder, "*.json")
  files = sorted(glob.glob(pattern))
  for path in files:
    with open(path, "r", encoding="utf-8") as f:
      try:
        data = json.load(f)
        if isinstance(data, list):
          for row in data:
            yield row
          continue
      except Exception:
        f.seek(0)
        for line in f:
          line = line.strip()
          if not line:
            continue
          try:
            row = json.loads(line)
            yield row
          except Exception:
            continue


def normalize_platform_to_group(platform: str) -> str:
  """Group similar platforms into normalized buckets.

  Examples:
  - "Windows 10 (Unknown Ed)" -> "Windows 10"
  - "Android 8.1.0 (Pixel 2)" -> "Android 8"
  - "Android OS 6.0.1 API 23)" -> "Android 6"
  - "Android-tablet OS 6.0.1 API 23" -> "Android 6"
  - "iOS 14.7.1 (iPhone12,3)" -> "iOS 14"
  - "web_player windows 10;chrome 87..." -> "Web Player Windows 10"
  - "web_player linux ;firefox 62..." -> "Web Player Linux"
  - "Google Cast (Chromecast)" -> "Google Cast"
  - "Mac OS X 13.6.9 (x86_64)" -> "macOS 13"
  - Fallback: the original string trimmed
  """
  if not platform:
    return "Unknown"

  p = platform.strip().rstrip(")")
  # Remove square-bracket metadata like "[arm 2]"
  p = re.sub(r"\s*\[[^\]]*\]\s*", " ", p).strip()
  pl = p.lower()

  # Handle web_player style strings: "web_player <os>;browser ...;..."
  if pl.startswith("web_player") or pl.startswith("web player"):
    rest = p.split(" ", 1)[1] if " " in p else ""
    os_token = rest.split(";", 1)[0].strip()
    os_token_l = os_token.lower()
    # Windows major
    m = re.search(r"windows\s+(\d+)", os_token_l)
    if m:
      return f"Web Player Windows {m.group(1)}"
    # macOS / Mac OS
    m = re.search(r"(?:mac\s*os\s*x|macos)\s+(\d+)", os_token_l)
    if m:
      return f"Web Player macOS {m.group(1)}"
    # Linux
    if "linux" in os_token_l:
      return "Web Player Linux"
    # iOS
    m = re.search(r"ios\s+(\d+)", os_token_l)
    if m:
      return f"Web Player iOS {m.group(1)}"
    # Android
    m = re.search(r"android\s+(\d+)", os_token_l)
    if m:
      return f"Web Player Android {m.group(1)}"
    # Fallback
    cleaned = re.sub(r"\s*\([^\)]*\)\s*", "", os_token).strip()
    cleaned = cleaned or "Unknown"
    return f"Web Player {cleaned.title()}"

  # Handle Partner device strings (e.g., Tesla, Roku TV, Google Cast Voice/Group)
  if p.lower().startswith("partner"):
    pl = p.lower()
    # Tesla partners
    if "tesla" in pl:
      return "Tesla"
    # Roku TV partners
    if "roku" in pl:
      return "Roku TV"
    # Google Cast partners (voice/group variants)
    if "cast" in pl:
      if "group" in pl:
        return "Google Cast Group"
      return "Google Cast"
    # Fallback to cleaned partner string without details
    cleaned = re.sub(r"\s*\([^\)]*\)\s*", "", p).strip()
    return cleaned or "Partner"

  # Google Cast
  if pl.startswith("google cast"):
    return "Google Cast"

  # Windows X -> Windows X
  m = re.match(r"^Windows\s+(\d+)\b", p, flags=re.IGNORECASE)
  if m:
    return f"Windows {m.group(1)}"

  # Android 8.1.0 -> Android 8
  m = re.match(r"^Android\s+(\d+)(?:[\.\s]|$)", p, flags=re.IGNORECASE)
  if m:
    return f"Android {m.group(1)}"

  # Android OS / Android-tablet OS X.Y.Z -> Android X
  m = re.match(r"^Android(?:-tablet)?\s+OS\s+(\d+)", p, flags=re.IGNORECASE)
  if m:
    return f"Android {m.group(1)}"

  # iOS 14.7.1 -> iOS 14
  m = re.match(r"^iOS\s+(\d+)(?:[\.\s]|$)", p, flags=re.IGNORECASE)
  if m:
    return f"iOS {m.group(1)}"

  # macOS / Mac OS X / OS X 13.6 -> macOS 13
  m = re.match(r"^(?:Mac\s*OS\s*X|macOS|OS\s*X)\s+(\d+)(?:[\.\s]|$)", p, flags=re.IGNORECASE)
  if m:
    return f"macOS {m.group(1)}"

  # Linux (Ubuntu 22.04) -> Linux
  if pl.startswith("linux"):
    return "Linux"

  # Partner device strings: group by partner vendor and platform name
  if pl.startswith("partner"):
    tail = p[len("Partner"):].strip()
    # prefix before first semicolon holds tokens (e.g., "sonos_imx6 Sonos")
    sem_idx = tail.find(";")
    prefix = tail if sem_idx == -1 else tail[:sem_idx]
    rest = "" if sem_idx == -1 else tail[sem_idx+1:]
    vendor_token = prefix.strip().split()[-1] if prefix.strip().split() else ""
    model_field = rest.split(";")[0].strip() if rest else ""

    vendor_clean = vendor_token.replace("_", " ").strip()
    model_clean = model_field.replace("_", " ").strip()

    # Special-case Google Cast Group
    if "google cast group" in model_clean.lower():
      return "Google Cast Group"
    if "google cast" in vendor_clean.lower() or "google cast" in model_clean.lower():
      return "Google Cast"

    group_vendor = vendor_clean.title() if vendor_clean else "Partner"
    if model_clean:
      return f"{group_vendor} {model_clean}"
    return group_vendor

  # Fallback: strip parentheticals and extra whitespace
  p = re.sub(r"\s*\([^\)]*\)\s*", "", p).strip()
  return p or "Unknown"


def build_track_key_and_name(row: dict) -> Tuple[str, str]:
  uri = row.get("spotify_track_uri")
  track = row.get("master_metadata_track_name")
  artist = row.get("master_metadata_album_artist_name")

  if track and artist:
    display = f"{track} - {artist}"
  elif track:
    display = track
  elif uri:
    display = uri
  else:
    display = "(Unknown Track)"

  key = uri if uri else f"name:{display}"
  return key, display


def main():
  if len(sys.argv) < 2:
    print("Usage: python history_top_songs_by_platform_grouped.py <folder>")
    sys.exit(1)

  folder = sys.argv[1]

  group_to_counts: Dict[str, Dict[str, int]] = defaultdict(lambda: defaultdict(int))
  track_display: Dict[str, str] = {}

  # Preserve group output order by first appearance
  group_order: List[str] = []
  seen_groups = set()

  for row in iter_history_records(folder):
    platform = row.get("platform") or "Unknown"

    # Exact-match exclusions on raw platform strings
    if platform in EXCLUDED_PLATFORMS_EXACT:
      continue

    group = normalize_platform_to_group(platform)

    # Exclusions (case-insensitive)
    if group.lower() in {g.lower() for g in EXCLUDED_GROUPS}:
      continue

    if group not in seen_groups:
      group_order.append(group)
      seen_groups.add(group)

    key, name = build_track_key_and_name(row)
    track_display[key] = name
    group_to_counts[group][key] += 1

  # Output: for each group (first-seen order), show top 10 by plays
  for group in group_order:
    counts = group_to_counts.get(group, {})
    if not counts:
      continue
    total_group_plays = sum(counts.values())
    if total_group_plays < MIN_GROUP_PLAYS:
      continue
    sorted_items = sorted(counts.items(), key=lambda kv: (-kv[1], track_display.get(kv[0], kv[0])))
    top10 = sorted_items[:10]

    print(f"\nGroup: {group} (Total plays: {total_group_plays})")
    for idx, (key, plays) in enumerate(top10, start=1):
      name = track_display.get(key, key)
      print(f"{idx}. {name} - {plays} plays")


if __name__ == "__main__":
  main()


