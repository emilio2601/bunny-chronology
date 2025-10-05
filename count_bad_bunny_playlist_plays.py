import os
import sys
import json
import glob
import dotenv
import spotipy

from collections import defaultdict
from typing import Dict, List, Tuple, Set
from spotipy.oauth2 import SpotifyOAuth


dotenv.load_dotenv()


# Default to the Bad Bunny playlist provided
HARDCODED_PLAYLIST_ID = "3cwfW1Gn2qABuaD6ryiSZS"
# Default exclude playlist ("all reggaeton")
HARDCODED_EXCLUDE_PLAYLIST_ID = "5gTwnL2iQATKAYeHaEoo0I"


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

# Old criteria: lead artist names considered as "Bad Bunny" (case-insensitive match)
OLD_CRITERIA_ARTIST_NAMES = {
  "Bad Bunny",
}


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


def _normalize_key(title: str, artist: str) -> str:
  return f"{(title or '').strip().lower()}||{(artist or '').strip().lower()}"


def fetch_playlist_tracks(sp: spotipy.Spotify, playlist_id: str) -> Tuple[Dict[str, str], Set[str], Dict[str, str], Dict[str, str]]:
  """Return:
  - uri_to_display: track uri -> "<track> - <lead artist>"
  - uri_set: set of track uris
  - name_artist_key_to_display: normalized "title||artist" -> display
  - display_to_key: display -> normalized key
  """
  uri_to_display: Dict[str, str] = {}
  uri_set: Set[str] = set()
  name_artist_key_to_display: Dict[str, str] = {}
  display_to_key: Dict[str, str] = {}
  limit = 100
  offset = 0
  while True:
    page = sp.playlist_items(playlist_id, limit=limit, offset=offset)
    items = page.get("items", [])
    for it in items:
      track = it.get("track")
      if not track:
        continue
      uri = track.get("uri")
      name = track.get("name") or uri
      if not uri:
        continue
      # lead artist is first in list
      artists = track.get("artists") or []
      lead_artist = artists[0].get("name") if artists else ""
      display = f"{name} - {lead_artist}" if lead_artist else name
      uri_set.add(uri)
      uri_to_display[uri] = display
      key = _normalize_key(name, lead_artist)
      if key and key not in name_artist_key_to_display:
        name_artist_key_to_display[key] = display
      if display and key:
        display_to_key[display] = key
    if page.get("next") is None:
      break
    offset += limit
  return uri_to_display, uri_set, name_artist_key_to_display, display_to_key


def fetch_playlist_keys(sp: spotipy.Spotify, playlist_id: str) -> Set[str]:
  """Return set of normalized "title||lead artist" keys for a playlist."""
  keys: Set[str] = set()
  limit = 100
  offset = 0
  while True:
    page = sp.playlist_items(playlist_id, limit=limit, offset=offset)
    items = page.get("items", [])
    for it in items:
      track = it.get("track")
      if not track:
        continue
      name = track.get("name") or ""
      artists = track.get("artists") or []
      lead_artist = artists[0].get("name") if artists else ""
      key = _normalize_key(name, lead_artist)
      if key:
        keys.add(key)
    if page.get("next") is None:
      break
    offset += limit
  return keys


def extract_year(ts: str) -> str:
  if not ts or len(ts) < 4:
    return "Unknown"
  return ts[:4]


def main():
  if len(sys.argv) < 2:
    print("Usage: python count_bad_bunny_playlist_plays.py <history_folder> [playlist_id] [exclude_playlist_id]")
    sys.exit(1)

  folder = sys.argv[1]
  playlist_id = sys.argv[2] if len(sys.argv) > 2 else HARDCODED_PLAYLIST_ID
  exclude_playlist_id = sys.argv[3] if len(sys.argv) > 3 else HARDCODED_EXCLUDE_PLAYLIST_ID

  auth_manager = SpotifyOAuth(scope='playlist-read-private,playlist-read-collaborative', redirect_uri='http://localhost:8080/callback')
  sp = spotipy.Spotify(auth_manager=auth_manager)

  print(f"Loading playlist {playlist_id}...")
  uri_to_display, uri_set, key_to_display, display_to_key = fetch_playlist_tracks(sp, playlist_id)

  print(f"Loading exclude playlist {exclude_playlist_id}...")
  exclude_keys = fetch_playlist_keys(sp, exclude_playlist_id)

  # counts keyed by display string
  song_counts: Dict[str, int] = defaultdict(int)
  song_counts_old: Dict[str, int] = defaultdict(int)
  total_plays = 0
  # per-year counts for playlist-qualified plays
  song_counts_by_year: Dict[str, Dict[str, int]] = defaultdict(lambda: defaultdict(int))

  print(f"Scanning history in {folder} (plays > 30s, exact platform exclusions applied)...")
  for row in iter_history_records(folder):
    platform = row.get("platform")
    if platform in EXCLUDED_PLATFORMS_EXACT:
      continue

    try:
      ms_played = int(row.get("ms_played") or 0)
    except Exception:
      ms_played = 0
    if ms_played <= 30000:
      continue

    uri = row.get("spotify_track_uri")
    display_key: str = ""
    matched = False
    if uri and uri in uri_set:
      display_key = uri_to_display.get(uri, uri)
      matched = True
    else:
      # Try fallback by name + lead artist
      row_title = row.get("master_metadata_track_name") or ""
      row_artist = row.get("master_metadata_album_artist_name") or ""
      nk = _normalize_key(row_title, row_artist)
      if nk and nk in key_to_display:
        display_key = key_to_display[nk]
        matched = True
        # record mapping for later exclusion matching
        if display_key and display_key not in display_to_key:
          display_to_key[display_key] = nk
    if not matched:
      continue

    song_counts[display_key] += 1
    total_plays += 1

    # Yearly tally (new criteria only)
    year = extract_year(row.get("ts") or "")
    song_counts_by_year[year][display_key] += 1

    # Old criteria: only count if lead artist matches one of OLD_CRITERIA_ARTIST_NAMES
    lead_artist = (row.get("master_metadata_album_artist_name") or "").strip()
    if lead_artist and any(lead_artist.lower() == name.lower() for name in OLD_CRITERIA_ARTIST_NAMES):
      # Use the same display key to keep comparisons aligned
      if display_key:
        song_counts_old[display_key] += 1

  total_old_plays = sum(song_counts_old.values())
  print(f"\nTotal qualifying plays (new criteria - playlist): {total_plays}")
  print(f"Total qualifying plays (old criteria - lead artist only): {total_old_plays}")

  # Top 100 songs
  top_items = list(song_counts.items())
  top_items.sort(key=lambda kv: (-kv[1], kv[0]))

  print("Top 100 Songs (Playlist-qualified):")
  for idx, (disp, cnt) in enumerate(top_items[:100], start=1):
    print(f"{idx}. {disp} - {cnt} plays")

  # Top 50 songs NOT in exclude playlist (match by normalized title||lead artist)
  not_in_exclude: List[Tuple[str, int]] = []
  for disp, cnt in song_counts.items():
    norm_key = display_to_key.get(disp)
    # If we lack a key, conservatively include (cannot prove it's in exclude)
    if norm_key and norm_key in exclude_keys:
      continue
    not_in_exclude.append((disp, cnt))
  not_in_exclude.sort(key=lambda kv: (-kv[1], kv[0]))
  print("\nTop 50 Songs NOT in exclude playlist:")
  for idx, (disp, cnt) in enumerate(not_in_exclude[:50], start=1):
    print(f"{idx}. {disp} - {cnt} plays")

  # Per-year top 20
  print("\nTop 20 Songs by Year (Playlist-qualified):")
  for year in sorted(song_counts_by_year.keys()):
    year_items = list(song_counts_by_year[year].items())
    year_items.sort(key=lambda kv: (-kv[1], kv[0]))
    total_year = sum(cnt for _, cnt in year_items)
    print(f"\nYear: {year} (Total plays: {total_year})")
    for idx, (disp, cnt) in enumerate(year_items[:20], start=1):
      print(f"{idx}. {disp} - {cnt} plays")

  # Breakdown: songs that would NOT have been included under old criteria
  # Compute counts that are "new only" (playlist-qualified plays minus old-criteria plays)
  diff_items = []
  for disp, new_cnt in song_counts.items():
    old_cnt = song_counts_old.get(disp, 0)
    diff = new_cnt - old_cnt
    if diff > 0:
      diff_items.append((disp, diff))

  diff_items.sort(key=lambda kv: (-kv[1], kv[0]))
  print("\nTop 50 Songs newly included vs old criteria:")
  for idx, (disp, diff_cnt) in enumerate(diff_items[:50], start=1):
    print(f"{idx}. {disp} - {diff_cnt} additional plays")


if __name__ == "__main__":
  main()


