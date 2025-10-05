import os
import sys
import json
import glob

from collections import defaultdict
from typing import Dict, List, Tuple


def iter_history_records(folder: str):
  """Yield history records from all JSON files under folder.

  Supports both a single large JSON array and line-delimited JSON.
  Files are processed in sorted filename order; platform order is based on
  first appearance in the encountered data.
  """
  pattern = os.path.join(folder, "*.json")
  files = sorted(glob.glob(pattern))
  for path in files:
    with open(path, "r", encoding="utf-8") as f:
      # Try to parse as a big JSON array first
      try:
        data = json.load(f)
        if isinstance(data, list):
          for row in data:
            yield row
          continue
      except Exception:
        # fallback to line-delimited
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


def build_track_key_and_name(row: dict) -> Tuple[str, str]:
  """Return a stable track key and human-readable display name.

  Prefer the Spotify track URI as key when available. For display, show
  "<track> - <artist>" when available, otherwise fallback progressively.
  """
  uri = row.get("spotify_track_uri")
  track = row.get("master_metadata_track_name")
  artist = row.get("master_metadata_album_artist_name")

  display: str
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
    print("Usage: python history_top_songs_by_platform.py <folder>")
    sys.exit(1)

  folder = sys.argv[1]

  # platform -> (track_key -> count)
  platform_to_counts: Dict[str, Dict[str, int]] = defaultdict(lambda: defaultdict(int))
  # track_key -> display name
  track_display: Dict[str, str] = {}

  # Preserve platforms in order of first appearance
  platform_order: List[str] = []
  seen_platforms = set()

  for row in iter_history_records(folder):
    platform = row.get("platform") or "Unknown"
    if platform not in seen_platforms:
      platform_order.append(platform)
      seen_platforms.add(platform)

    key, name = build_track_key_and_name(row)
    track_display[key] = name
    platform_to_counts[platform][key] += 1

  # Output: for each platform (in first-seen order), show top 10 by plays
  for platform in platform_order:
    counts = platform_to_counts.get(platform, {})
    if not counts:
      continue
    # sort by plays desc, then by name for tie-breaker
    sorted_items = sorted(counts.items(), key=lambda kv: (-kv[1], track_display.get(kv[0], kv[0])))
    top10 = sorted_items[:20]

    print(f"\nPlatform: {platform}")
    for idx, (key, plays) in enumerate(top10, start=1):
      name = track_display.get(key, key)
      print(f"{idx}. {name} - {plays} plays")


if __name__ == "__main__":
  main()


