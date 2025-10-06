import os
import sys
import dotenv
import spotipy

from datetime import datetime
from typing import Dict, List, Optional, Tuple
from spotipy.oauth2 import SpotifyOAuth
from tqdm import tqdm


dotenv.load_dotenv()



# Hardcoded playlist id; can be overridden via CLI argument
HARDCODED_PLAYLIST_ID = "5gTwnL2iQATKAYeHaEoo0I"


# Optional date overrides for specific tracks.
# Keyed by track id -> ISO date string (YYYY-MM-DD or YYYY-MM or YYYY)
DATE_OVERRIDES: Dict[str, str] = {
  "2Eg6dOam7cAe5turf2bnCg": "2001-12-04",
  "3xKOScU4dJYq30uDzbpG2j": "2004-06-08",
  "1DdrejuwM8C3ExsXaPAgF8": "2004-10-12",
  "57zpFPybSWc4aNwDHV0kBo": "2006-06-12",
  "7Fmf6fTY42XwGIgQQR69CU": "2006-01-01",
  "59L5lxOJNIfcp8INaT9vkV": "1992-12-01",
  "2pr7niU3YfbVMQZxzsXubr": "2005-05-10",
}


def normalize_release_date(date_str: str) -> str:
  """Normalize Spotify release_date strings to YYYY-MM-DD for comparison.

  Spotify release_date_precision can be: year, month, day.
  We pad missing month/day with 01 to make ordering deterministic.
  """
  if not date_str:
    return "0000-01-01"

  parts = date_str.split("-")
  if len(parts) == 1:
    return f"{parts[0]}-01-01"
  if len(parts) == 2:
    return f"{parts[0]}-{parts[1]}-01"
  return date_str


def chunker(seq: List[str], size: int):
  return (seq[pos:pos + size] for pos in range(0, len(seq), size))


def fetch_all_playlist_tracks(sp: spotipy.Spotify, playlist_id: str) -> List[dict]:
  """Fetch all playlist items handling pagination, with progress bar."""
  items: List[dict] = []
  limit = 100
  offset = 0
  try:
    total = sp.playlist_items(playlist_id, limit=1).get("total", 0)
  except Exception:
    total = 0
  pbar = tqdm(total=total or None, desc="Fetching tracks", unit="trk")
  try:
    while True:
      page = sp.playlist_items(playlist_id, limit=limit, offset=offset)
      page_items = page.get("items", [])
      items.extend(page_items)
      if page_items:
        pbar.update(len(page_items))
      if page.get("next") is None:
        break
      offset += limit
  finally:
    pbar.close()
  return items


def wipe_playlist(sp: spotipy.Spotify, playlist_id: str) -> None:
  """Remove all items from a playlist safely in batches."""
  while True:
    page = sp.playlist_items(playlist_id, fields="items(track(id)),total,limit,offset,next", limit=100)
    track_ids = [it["track"]["id"] for it in page.get("items", []) if it.get("track") and it["track"].get("id")]
    if not track_ids:
      break
    sp.playlist_remove_all_occurrences_of_items(playlist_id, track_ids)
    if page.get("next") is None:
      # double-check to ensure emptiness
      check = sp.playlist_items(playlist_id, limit=1)
      if not check.get("items"):
        break


def compute_track_release_date(sp: spotipy.Spotify, track_obj: dict) -> Optional[str]:
  """Return normalized release date for a track, considering DATE_OVERRIDES."""
  if not track_obj:
    return None
  track_id = track_obj.get("id")
  if not track_id:
    return None

  if track_id in DATE_OVERRIDES:
    return normalize_release_date(DATE_OVERRIDES[track_id])

  album = track_obj.get("album") or {}
  date_str = album.get("release_date")
  if not date_str:
    # fetch full album as fallback
    album_id = album.get("id")
    if album_id:
      album_full = sp.album(album_id)
      date_str = album_full.get("release_date")
  if not date_str:
    return None
  return normalize_release_date(date_str)


def main():
  # Optional CLI override for playlist id
  playlist_id = HARDCODED_PLAYLIST_ID
  if len(sys.argv) > 1 and sys.argv[1]:
    playlist_id = sys.argv[1]

  auth_manager = SpotifyOAuth(scope='playlist-modify-public,playlist-modify-private,playlist-read-private', redirect_uri='http://localhost:8080/callback')
  sp = spotipy.Spotify(auth_manager=auth_manager)

  print(f"Sorting playlist {playlist_id} by release date...")

  # Fetch tracks
  items = fetch_all_playlist_tracks(sp, playlist_id)

  print("Computing dates...")
  # (normalized_date, album_id, disc_number, track_number, original_index, track_id)
  tracks_with_meta: List[Tuple[str, str, int, int, int, str]] = []
  for idx, it in enumerate(items):
    track = it.get("track")
    if not track:
      continue
    track_id = track.get("id")
    if not track_id:
      continue
    normalized_date = compute_track_release_date(sp, track)
    if normalized_date is None:
      # Put unknowns at the very end but keep deterministic order
      normalized_date = "9999-12-31"
    album = track.get("album") or {}
    album_id = album.get("id") or ""
    disc_number = track.get("disc_number") or 0
    track_number = track.get("track_number") or 0
    tracks_with_meta.append((normalized_date, album_id, int(disc_number), int(track_number), idx, track_id))

  # Sort by release date, then album, then disc and track number to preserve album order
  tracks_with_meta.sort(key=lambda x: (x[0], x[1], x[2], x[3], x[4]))

  # Clear playlist
  print("Clearing playlist...")
  wipe_playlist(sp, playlist_id)

  # Re-add in release order from oldest to newest; within album, in album order
  ordered_track_ids = [t[-1] for t in tracks_with_meta]
  print("Re-adding tracks...")
  for chunk in chunker(ordered_track_ids, 100):
    sp.playlist_add_items(playlist_id, chunk)

  print("Done.")


if __name__ == "__main__":
  main()


