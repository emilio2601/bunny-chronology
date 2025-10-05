import os
import sys
import json
import glob
import dotenv
import spotipy

from collections import defaultdict
from typing import Dict, List, Tuple, Set
from spotipy.oauth2 import SpotifyOAuth
from tqdm import tqdm


dotenv.load_dotenv()


# Hardcoded playlist id; can be overridden via CLI argument
HARDCODED_PLAYLIST_ID = "5gTwnL2iQATKAYeHaEoo0I"


def fetch_playlist_tracks(sp: spotipy.Spotify, playlist_id: str) -> Tuple[Dict[str, dict], Dict[str, List[dict]]]:
  """Return mapping of track_id -> track_obj, and uri-> track artists list, for a playlist."""
  track_id_to_track: Dict[str, dict] = {}
  uri_to_artists: Dict[str, List[dict]] = {}

  limit = 100
  offset = 0
  total = sp.playlist_items(playlist_id, limit=1).get("total", 0)
  pbar = tqdm(total=total or None, desc="Fetching playlist", unit="trk")
  try:
    while True:
      page = sp.playlist_items(playlist_id, limit=limit, offset=offset)
      items = page.get("items", [])
      for it in items:
        track = it.get("track")
        if not track:
          continue
        track_id = track.get("id")
        uri = track.get("uri")
        if not track_id or not uri:
          continue
        track_id_to_track[track_id] = track
        uri_to_artists[uri] = track.get("artists", [])
      if items:
        pbar.update(len(items))
      if page.get("next") is None:
        break
      offset += limit
  finally:
    pbar.close()
  return track_id_to_track, uri_to_artists


def iter_history_records(folder: str):
  """Yield history records from all JSON files under folder."""
  # supports both single large JSON arrays and line-delimited JSON
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


def main():
  if len(sys.argv) < 2:
    print("Usage: python history_top_from_folder.py <folder> [playlist_id]")
    sys.exit(1)

  folder = sys.argv[1]
  playlist_id = sys.argv[2] if len(sys.argv) > 2 else HARDCODED_PLAYLIST_ID

  auth_manager = SpotifyOAuth(scope='playlist-read-private,playlist-read-collaborative', redirect_uri='http://localhost:8080/callback')
  sp = spotipy.Spotify(auth_manager=auth_manager)

  print(f"Loading playlist {playlist_id}...")
  track_id_to_track, uri_to_artists = fetch_playlist_tracks(sp, playlist_id)

  playlist_track_uris: Set[str] = set(uri_to_artists.keys())

  song_to_ms: Dict[str, int] = defaultdict(int)  # by track uri
  song_to_count: Dict[str, int] = defaultdict(int)
  artist_to_ms: Dict[str, int] = defaultdict(int)  # by artist id
  artist_to_count: Dict[str, int] = defaultdict(int)

  print(f"Scanning history files in {folder}...")
  for row in tqdm(iter_history_records(folder), desc="History records"):
    uri = row.get("spotify_track_uri")
    ms_played = int(row.get("ms_played") or 0)
    if not uri or uri not in playlist_track_uris:
      continue

    # Tally song metrics
    song_to_ms[uri] += ms_played
    song_to_count[uri] += 1

    # Tally artist metrics (expand all artists from playlist snapshot)
    artists = uri_to_artists.get(uri, [])
    for artist in artists:
      artist_id = artist.get("id") or f"name:{artist.get('name') or 'Unknown'}"
      artist_to_ms[artist_id] += ms_played
      artist_to_count[artist_id] += 1

  # Prepare top 50 songs and artists
  # Fetch names for songs and artists
  uri_to_name: Dict[str, str] = {}
  for uri in song_to_ms.keys():
    # attempt to get name from cached playlist track info
    # If missing, fallback to ID lookup is intentionally skipped to avoid extra API calls
    uri_to_name[uri] = next((t.get("name") for t in track_id_to_track.values() if t.get("uri") == uri), uri)

  artist_id_to_name: Dict[str, str] = {}
  for uri, artists in uri_to_artists.items():
    for artist in artists:
      artist_id = artist.get("id") or f"name:{artist.get('name') or 'Unknown'}"
      artist_id_to_name[artist_id] = artist.get("name") or artist_id

  song_sorted: List[Tuple[str, int]] = sorted(song_to_ms.items(), key=lambda x: -x[1])[:50]
  artist_sorted: List[Tuple[str, int]] = sorted(artist_to_ms.items(), key=lambda x: -x[1])[:50]

  print("Top 50 songs by ms_played (playlist-only):")
  for idx, (uri, ms) in enumerate(song_sorted, start=1):
    name = uri_to_name.get(uri, uri)
    plays = song_to_count.get(uri, 0)
    minutes = ms / 60000.0
    print(f"{idx}. {name} - {minutes:.1f} min across {plays} plays")

  print("\nTop 50 artists by ms_played (playlist-only):")
  for idx, (aid, ms) in enumerate(artist_sorted, start=1):
    name = artist_id_to_name.get(aid, aid)
    plays = artist_to_count.get(aid, 0)
    minutes = ms / 60000.0
    print(f"{idx}. {name} - {minutes:.1f} min across {plays} appearances")


if __name__ == "__main__":
  main()


