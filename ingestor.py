from spotipy import Spotify
from spotipy.oauth2 import SpotifyClientCredentials
from DBConnector import DBConnector

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
                self.ingest_related_artist(a['id'], depth - 1)

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

        categories = self.spotipy.categories()['categories']['items']

        for c in categories:
            self.connector.insert_category(c)
            # TODO: ingest Category playlist

    def ingest_category_playlists(self, category_id):
        # This is a list of playlists

        p = spotipy.category_playlists(category_id)['playlists']['items']

        # TODO: insert_playlist
        pass

if __name__  == '__main__':

    ing = Ingestor()

    kanye = '5K4W6rqBFWDnAN6FQUkS6x'
