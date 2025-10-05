import os
import sys
import json
import glob

from collections import defaultdict
from typing import Dict, List, Tuple
from tabulate import tabulate


# Exact platform exclusions (raw string exact match)
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

# Consistency metric cap per year: min(plays_in_year, CONSISTENCY_CAP_PER_YEAR)
CONSISTENCY_CAP_PER_YEAR = 10


def iter_history_records(folder: str):
  """Yield history records from all JSON files under folder.

  Supports both a single large JSON array and line-delimited JSON.
  Files are processed in sorted filename order.
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


def extract_year(ts: str) -> str:
  if not ts or len(ts) < 4:
    return "Unknown"
  return ts[:4]


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
    print("Usage: python history_top_by_year.py <folder>")
    sys.exit(1)

  folder = sys.argv[1]

  # song and artist counts per year and global
  song_counts_by_year: Dict[str, Dict[str, int]] = defaultdict(lambda: defaultdict(int))
  artist_counts_by_year: Dict[str, Dict[str, int]] = defaultdict(lambda: defaultdict(int))
  global_song_counts: Dict[str, int] = defaultdict(int)
  global_artist_counts: Dict[str, int] = defaultdict(int)
  # global per-artist album counts (lead artist from history JSON)
  artist_album_counts: Dict[str, Dict[str, int]] = defaultdict(lambda: defaultdict(int))
  # per-artist song counts (global)
  artist_song_counts: Dict[str, Dict[str, int]] = defaultdict(lambda: defaultdict(int))

  # display maps
  track_display: Dict[str, str] = {}

  def print_table(title: str, headers: List[str], rows: List[List[object]]):
    print(title)
    print(tabulate(rows, headers=headers, tablefmt="github"))
    print("")

  def strip_artist_from_display(display: str, artist_name: str) -> str:
    if not display:
      return display
    suffix = f" - {artist_name}" if artist_name else ""
    if suffix and display.endswith(suffix):
      return display[:-len(suffix)]
    parts = display.rsplit(" - ", 1)
    if len(parts) == 2 and artist_name and parts[1].lower() == artist_name.lower():
      return parts[0]
    return parts[0] if len(parts) == 2 else display

  for row in iter_history_records(folder):
    platform = row.get("platform")
    if platform in EXCLUDED_PLATFORMS_EXACT:
      continue

    # Only count plays strictly over 30 seconds
    try:
      ms_played = int(row.get("ms_played") or 0)
    except Exception:
      ms_played = 0
    if ms_played <= 30000:
      continue

    ts = row.get("ts") or ""
    year = extract_year(ts)

    song_key, song_name = build_track_key_and_name(row)
    artist_name = row.get("master_metadata_album_artist_name") or "(Unknown Artist)"
    album_name = row.get("master_metadata_album_album_name") or "(Unknown Album)"

    track_display[song_key] = song_name

    # Increment counts
    song_counts_by_year[year][song_key] += 1
    artist_counts_by_year[year][artist_name] += 1
    global_song_counts[song_key] += 1
    global_artist_counts[artist_name] += 1
    artist_album_counts[artist_name][album_name] += 1
    artist_song_counts[artist_name][song_key] += 1

  # Output per year top 100 songs and artists (sorted by count desc, tie by name)
  for year in sorted(song_counts_by_year.keys()):
    # Songs (Top 100)
    song_items = list(song_counts_by_year[year].items())
    song_items.sort(key=lambda kv: (-kv[1], track_display.get(kv[0], kv[0])))
    rows = [[idx, track_display.get(key, key), cnt] for idx, (key, cnt) in enumerate(song_items[:100], start=1)]
    print_table(f"Year: {year} — Top 100 Songs", ["#", "Song", "Plays"], rows)

    # Artists (Top 100)
    artist_items = list(artist_counts_by_year[year].items())
    artist_items.sort(key=lambda kv: (-kv[1], kv[0]))
    rows = [[idx, name, cnt] for idx, (name, cnt) in enumerate(artist_items[:100], start=1)]
    print_table(f"Year: {year} — Top 100 Artists", ["#", "Artist", "Plays"], rows)

  # Global
  print("Global Totals")
  global_song_items = list(global_song_counts.items())
  global_song_items.sort(key=lambda kv: (-kv[1], track_display.get(kv[0], kv[0])))
  rows = [[idx, track_display.get(key, key), cnt] for idx, (key, cnt) in enumerate(global_song_items[:100], start=1)]
  print_table("Top 100 Songs (Global)", ["#", "Song", "Plays"], rows)

  global_artist_items = list(global_artist_counts.items())
  global_artist_items.sort(key=lambda kv: (-kv[1], kv[0]))
  rows = [[idx, name, cnt] for idx, (name, cnt) in enumerate(global_artist_items[:100], start=1)]
  print_table("Top 100 Artists (Global)", ["#", "Artist", "Plays"], rows)
  
  # Helper: compute gini for flatness
  def compute_gini(counts: List[int]) -> float:
    arr = [c for c in counts if c > 0]
    n = len(arr)
    if n == 0:
      return 0.0
    arr.sort()
    cum = 0
    for i, x in enumerate(arr, start=1):
      cum += i * x
    total = sum(arr)
    if total == 0:
      return 0.0
    g = (2 * cum) / (n * total) - (n + 1) / n
    return max(0.0, min(1.0, g))

  # For top 10 global artists, print all info contiguously: summary, top songs, top albums
  top10_artists = [name for name, _ in global_artist_items[:10]]
  for artist_name in top10_artists:
    total_artist_plays = global_artist_counts.get(artist_name, 0)
    counts = list(artist_song_counts.get(artist_name, {}).values())
    num_songs = len([c for c in counts if c > 0])
    g = compute_gini(counts)
    diversity = (1.0 - 1.0 / num_songs) if num_songs > 1 else 0.0
    flatness = (1.0 - g) * diversity

    # Header line (not a table) to avoid empty table output
    print(f"Artist: {artist_name} — {total_artist_plays} plays, flatness {flatness:.3f}")
    print("")

    # Top songs (table), exclude songs with < 10 plays
    per_songs_all = list(artist_song_counts.get(artist_name, {}).items())
    per_songs = [(k, v) for (k, v) in per_songs_all if v >= 10]
    per_songs.sort(key=lambda kv: (-kv[1], track_display.get(kv[0], kv[0])))
    rows = []
    for idx, (song_key, cnt) in enumerate(per_songs[:100], start=1):
      disp_full = track_display.get(song_key, song_key)
      title_only = strip_artist_from_display(disp_full, artist_name)
      rows.append([idx, title_only, cnt])
    print_table(f"Top 100 Songs for Artist: {artist_name}", ["#", "Song", "Plays"], rows)

    # Top albums (table), exclude albums with < 10 plays
    per_albums_all = list(artist_album_counts.get(artist_name, {}).items())
    per_albums = [(k, v) for (k, v) in per_albums_all if v >= 10]
    per_albums.sort(key=lambda kv: (-kv[1], kv[0]))
    rows = [[idx, album, cnt] for idx, (album, cnt) in enumerate(per_albums[:10], start=1)]
    print_table(f"Top 10 Albums for Artist: {artist_name}", ["#", "Album", "Plays"], rows)

  # Consistency metric: favor songs with steady plays across many years
  # score(song) = sum_years min(count_in_year, CONSISTENCY_CAP_PER_YEAR)
  song_year_counts: Dict[str, Dict[str, int]] = defaultdict(dict)
  for year, counts in song_counts_by_year.items():
    for song_key, cnt in counts.items():
      song_year_counts[song_key][year] = song_year_counts[song_key].get(year, 0) + cnt

  consistency_scores: List[Tuple[str, int, int, int]] = []  # (song_key, score, years_active, total)
  for song_key, year_map in song_year_counts.items():
    years_active = len(year_map)
    total = sum(year_map.values())
    score = sum(min(cnt, CONSISTENCY_CAP_PER_YEAR) for cnt in year_map.values())
    consistency_scores.append((song_key, score, years_active, total))

  consistency_scores.sort(key=lambda x: (-x[1], -x[2], -x[3], track_display.get(x[0], x[0])))

  rows = [[idx, track_display.get(song_key, song_key), score, years_active, total] for idx, (song_key, score, years_active, total) in enumerate(consistency_scores[:25], start=1)]
  print_table("Top 25 Songs by Consistency (cap per year = %d)" % CONSISTENCY_CAP_PER_YEAR, ["#", "Song", "Score", "Years Active", "Total Plays"], rows)

  print("\n")

  # Artist flatness metric: (1 - Gini) scaled by diversity (1 - 1/num_songs)
  artist_flatness: List[Tuple[str, float, int, int]] = []  # (artist, flatness, num_songs, total)
  for artist, song_map in artist_song_counts.items():
    counts = list(song_map.values())
    num_songs = len([c for c in counts if c > 0])
    total = sum(counts)
    if num_songs == 0 or total == 0:
      continue
    g = compute_gini(counts)
    diversity = (1.0 - 1.0 / num_songs) if num_songs > 1 else 0.0
    flatness = (1.0 - g) * diversity
    artist_flatness.append((artist, flatness, num_songs, total))

  # Restrict global flatness to artists with > 250 plays
  artist_flatness = [t for t in artist_flatness if t[3] > 250]
  artist_flatness.sort(key=lambda x: (-x[1], -x[2], -x[3], x[0]))
  rows = [[idx, artist, f"{flatness:.3f}", num_songs, total] for idx, (artist, flatness, num_songs, total) in enumerate(artist_flatness[:50], start=1)]
  print_table("Top 50 Artists by Flatness (>250 plays, higher = flatter distribution)", ["#", "Artist", "Flatness", "Songs", "Plays"], rows)

  bottom = sorted(artist_flatness, key=lambda x: (x[1], -x[2], -x[3], x[0]))[:50]
  rows = [[idx, artist, f"{flatness:.3f}", num_songs, total] for idx, (artist, flatness, num_songs, total) in enumerate(bottom, start=1)]
  print_table("Bottom 50 Artists by Flatness (>250 plays, lower = more skewed)", ["#", "Artist", "Flatness", "Songs", "Plays"], rows)


if __name__ == "__main__":
  main()


