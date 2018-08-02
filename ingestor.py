# Spotipy
from spotipy import Spotify
from spotipy.oauth2 import SpotifyClientCredentials  
from spotipy.client import SpotifyException

# Local
from DBConnector import DBConnector

# Third party
from tqdm import tqdm

# debug
from IPython.core.debugger import Pdb

CLIENT_ID = '42f875ddda804fdbbf596da0e0c112d9'
CLIENT_SECRET = '9df4f102a4a94ababae489826b2f4bcf'

NUM_REL_ARTISTS = 20

class Ingestor():

    def __init__(self, client_id = None, client_secret = None, dbURl = None,
        db_user = None, db_password = None):

        # spotipy Client flow
        if client_id is None:
            client_id = CLIENT_ID
        if client_secret is None:
            client_secret = CLIENT_SECRET

        self.client_id = client_id
        self.client_secret = client_secret
        self.client_creds = SpotifyClientCredentials(
            client_id = self.client_id, client_secret = self.client_secret)

        # creeate spotipy client
        self.spotipy = Spotify(auth = self.client_creds.get_access_token())

        self.connector = DBConnector()

    def ingest(self):

        self.ingest_categories()

        for c in tqdm(self.connector.get_node_label('Category'), desc = 'intgesting category playlists'):
            self.ingest_category_playlist(c['id'])

        # TODO: ingest tracks
        for p in tqdm(self.connector.get_node_label('Playlist'), desc = 'ingesting playlist tracks'):
            self.ingest_tracklist(p['tracks'], playlist= p['id'])


    def get_artist_by_name(self, name):
        """Retrives a list of artist obecjts based on searching for the artist
        name"""

        q = 'artist:' + name
        return self.spotipy.search(q, limit=10, offset=0, type='artist',
            market=None)['artists']['items']

    def ingest_related_artist(self, artist_id, limit = NUM_REL_ARTISTS,
            _depth = 0):
        """Recursively ingest artist related to a specific artist.
            Artist must already be in DB."""

        # TODO: check if artist DNE and insert if so

        self.connector.update_artist(artist_id, 'ingested', 'true')

        ara = self.spotipy.artist_related_artists(artist_id)

        for i, a in enumerate(ara['artists']):
            if i > limit:
                break

            self.connector.insert_artist(a)
            self.connector.insert_related_relation(artist_id, a['id'])

            if _depth > 0:
                self.ingest_related_artist(a['id'], _depth - 1)

    def ingest_by_name(self, name):
        """Ingests the first artist from the search of on artist by name into
        the database"""

        res = self.get_artist_by_name(name)[0]
        self.connector.insert_artist(res)

    def ingest_by_id(self, artist_id):
        """Ingest artist by id"""

        res = self.spotipy.artist(artist_id)
        self.connector.insert_artist(res)

    def ingest_categories(self):
        """Ingest categories from spotifies list of categories"""

        print("Pull categories from API" )
        categories = self.spotipy.categories()['categories']['items']

        for c in tqdm(categories, desc = 'Inserting categories'):
            
            self.connector.insert_category(c)

    def ingest_category_playlist(self, category_id):
        # This is a list of playlists

        try:
            play_lists = self.spotipy.category_playlists(category_id)['playlists']['items']
        except SpotifyException as e:
            print('Category %s play list ingestion failed' % category_id)
        else:
            for p in tqdm(play_lists, desc = 'Insertiing %s playlists' % category_id):

                self.connector.insert_playlist(p)
                self.connector.insert_cat_pl_relation(category_id, p['id'])
    
    def ingest_tracklist(self, tracks_href, playlist = None):
        
        tracks = self.spotipy._get(tracks_href)
        for t in tqdm(tracks['items'], desc = 'Ingesting Tracks'):

            self.connector.insert_track(t['track'])

            if playlist is not None:
                self.connector.insert_pl_tr_relation(playlist, t['track']['id'])
                # TODO: Add relation attributes

    def clear_database(self):
        """Clears the database this ingerstor is connected to."""

        # TODO: add warning

        self.connector.clear_database()

if __name__  == '__main__':

    ing = Ingestor()

    kanye = '5K4W6rqBFWDnAN6FQUkS6x'
