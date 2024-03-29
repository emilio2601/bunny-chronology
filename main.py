import sys
import dotenv
import spotipy

from spotipy.oauth2 import SpotifyOAuth

dotenv.load_dotenv()

artist_info = {
  "bunny": {
    "uri": "4q3ewBCX7sLwd24euuV69X",
    "playlist_uri": "3cwfW1Gn2qABuaD6ryiSZS",
    "song_blacklist": [
      '3pNgxsIiiFPokFmf0xQ6QJ', 
      '44w3jlijhm6VAkWHrHCuLS', 
      '2GPCE96x9Go6acdDt8ex6p', 
      '3YOFaS3tpXEBEGBlLKpzJc', 
      '43aPjI5XPpK5X9lNpSqIfX', 
      '6WK7h2WfMmnX7zOZnfzoYo', 
      '0lj1MuedLy7ZNo3AWcLIHp', 
      '2WI0AMgzaEdKs2hcoN21vF', 
      '3URT2JrBkTjzueCl7c8VAc', 
      '7gc33UVszqP31CRJczTcMv', 
      '0jPkVCHNaLJhhtq70OingB',
      '13DJ7BruwToSrQeQyUQgIT',
      '0WdWNUBp44Bx30rILtT7Cg',
      '3fvb9N0z7RK4Bjznxidly4',
      '66x45Yh9x397PWpxMX0ZDT',
      '6wxoNLTYHJaDnNnER92WRZ',
      '2ZOKsLsf7vqZtd2QKNwI9p',
    ],
    "album_blacklist": ['3qjsecGpiaOlfUbFZ8ZKJs', '0CDLQ6cxLj0UydmFX394VL'],
    "album_whitelist": ['287ZdmXv5M5YH5xxdGLhbY'],
    "song_whitelist": ['5NhLA2P7AiV3cloVmwtwLS', '4eMKD8MRroxCqugpsxCCNb', '0afpbPmRHldjP59YRslQz9', '7zqbzmtpvJ9yqfnykmGYW6']
  },

  "ferxxo": {
    "uri": "2LRoIwlKmHjgvigdNGBHNo",
    "playlist_uri": "6FaAPTVUTJrYQM0LXEf0FW",
    "song_blacklist": [
      '5mmX8AXuQ55bmiKHicnlNJ',
      '1SCYfmGM5ueb6YiiQbLXVn'
    ],
    "album_blacklist": [
      '4smxiuT4f7lMI4j2TDmhzr'
    ],
    "album_whitelist": []
  }
}

auth_manager = SpotifyOAuth(scope='playlist-modify-public', redirect_uri='http://localhost:8080/callback')
sp = spotipy.Spotify(auth_manager=auth_manager)

def artist_in_track(artist_uri, track):
  for artist in track['artists']:
    if artist['id'] == artist_uri:
      return True
  return False

def chunker(seq, size):
    return (seq[pos:pos + size] for pos in range(0, len(seq), size))

def artist_name_in_album(artist_name, album):
  for artist in album['artists']:
    if artist['name'] == artist_name:
      return True
  return False

def wipe_playlist(playlist_uri):
  playlist_tracks_uris = [track['track']['id'] for track in sp.playlist_items(playlist_uri)['items']]
  sp.playlist_remove_all_occurrences_of_items(playlist_uri, playlist_tracks_uris)

  if sp.playlist_items(playlist_uri)['total'] > 0:
    wipe_playlist(playlist_uri)


def main():
  if len(sys.argv) < 2:
    raise Exception("Please specify an artist name")
  
  selected_artist = artist_info[sys.argv[1]]

  name_uri_dict = {}

  album_count = sp.artist_albums(selected_artist["uri"], country="MX", limit=1)['total']
  wipe_playlist(selected_artist["playlist_uri"])

  for i in range(0, album_count, 50):
    albums = sp.artist_albums(selected_artist["uri"], country="MX", limit=50, offset=i)

    for album in albums['items']:
      if (album['album_type'] == 'compilation' or artist_name_in_album('Various Artists', album) or album['id'] in selected_artist["album_blacklist"]) and (album['id'] not in selected_artist["album_whitelist"]): 
        continue
      
      print(f"{album['name']} ({album['album_type']} released on {album['release_date']})")
      album_full = sp.album(album['id'])
      
      for track in album_full['tracks']['items']:
        if not artist_in_track(selected_artist["uri"], track):
          continue

        artist_names = ", ".join([artist['name'] for artist in track['artists']])

        if not track['name'] in name_uri_dict.keys() and track['id'] not in selected_artist["song_blacklist"]:
          name_uri_dict[track['name']] = [album['release_date'], track['id']]
          print(f"-- {track['name']} ({track['id']}) - {artist_names}")

  if selected_artist['song_whitelist']:
    for song in selected_artist['song_whitelist']:
      track = sp.track(song)
      artist_names = ", ".join([artist['name'] for artist in track['artists']])
      name_uri_dict[track['name']] = [track['album']['release_date'], track['id']]
      print(f"-- {track['name']} ({track['id']}) - {artist_names}")
  
  tracks = list(name_uri_dict.values())
  tracks.sort(key=lambda x: x[0])

  track_chunks = chunker([track[1] for track in reversed(tracks)], 100)

  for chunk in track_chunks:
    sp.playlist_add_items(selected_artist["playlist_uri"], chunk)

if __name__ == "__main__":
  main()