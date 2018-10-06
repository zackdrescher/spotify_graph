# Spotipy
from spotipy import Spotify
from spotipy.oauth2 import SpotifyClientCredentials  
from spotipy.client import SpotifyException

# Local
from DBConnector import DBConnector

# Third party
from tqdm import tqdm
import pandas as pd
import numpy as np

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

    def ingest(self, limit = None):

        # ingest categories
        self.ingest_categories()

        # from categories ingest playlists
        for c in tqdm(self.connector.get_node_label('Category')[:limit], 
                desc = 'intgesting category playlists'):
            self.ingest_category_playlist(c['id'])

        # from playlists ingest tracks
        for p in tqdm(self.connector.get_node_label('Playlist')[:limit], 
                desc = 'ingesting playlist tracks'):
            self.ingest_tracklist(p['tracks'], playlist= p['id'])

        # ingest artists from tracks
        artist = self.connector.get_artist_track_dataframe(
            'artist_href', 'Track').groupby('artist_href')

        for a, t in tqdm(artist,
                desc="Ingest track's artists"):
            self.ingest_artist(a, track=t['id'])

        # TODO: ingest ARA
        self.ingest_related_artists()

        # TODO: ingest artist genres

    def get_artist_by_name(self, name):
        """Retrives a list of artist obecjts based on searching for the artist
        name"""

        q = 'artist:' + name
        return self.spotipy.search(q, limit=10, offset=0, type='artist',
            market=None)['artists']['items']

    def ingest_related_artists(self, limit = NUM_REL_ARTISTS, iterations = None):

        # TODO: outer loop

        # pull artists
        artists = self.connector.get_node_label('Artist', {'ARA' : False})

        # get their ARA
        ara = []
        for a in tqdm(artists, desc = 'Getting related artists'):

            related = pd.DataFrame(
                pd.Series(
                    self.spotipy.artist_related_artists(a['id'])['artists']),
                columns = ['payload'])
            
            related['rel_id'] = related['payload'].apply(lambda x: x['id'])
            related['id']  = a['id']

            ara.append(related)

        ara = pd.concat(ara, ignore_index= True)

        # TODO: check for and add new artists
        a_ids = ara['id'].unique()
        rel_ids = ara['rel_id'].unique()

        # remove artists in the input list
        insert_ids = np.setdiff1d(rel_ids, a_ids, assume_unique=True)

        # Pull insert IDs that are in DB 
        db_ids = np.array(self.connector.get_prop_in('id', 'Artist', insert_ids.tolist()))
        
        #disjoint those off of insert ids
        insert_ids = np.setdiff1d(insert_ids, db_ids, assume_unique=True)

        # get the payloads and add the remove duplicates
        insert = ara.loc[ara['rel_id'].isin(insert_ids), ['rel_id','payload']]
        insert = insert.drop_duplicates(subset = 'rel_id')['payload']

        # add the new artists
        for res in tqdm(insert, desc = "Inserting new artists"):
            self.connector.insert_artist(res)

        # TODO: add the realtions
        for i, row in tqdm(ara.iterrows(), desc='Inserting relations'):
            self.connector.insert_related_relation(row['id'], row['rel_id'])

        # Update flag
        self.connector.update_prop_in(
            'Artist', 'ARA', True, [a['id'] for a in artists], 'id')
    
    def ingest_by_name(self, name):
        """Ingests the first artist from the search of on artist by name into
        the database"""

        res = self.get_artist_by_name(name)[0]
        self.connector.insert_artist(res)

    def ingest_artist(self, artist_href, track = None):
        """Ingest artist by href
        Track is optional track ID for plays relation"""

        res = self.spotipy._get(artist_href)
        self.connector.insert_artist(res)

        if isinstance(track, str):
            # If there is only one value (as a string)
            self.connector.insert_a_t_plays_relation(res['id'], track)
        elif track is not None:
            # if there is a group of values (as a list or series most likely)
            for t in track:
                self.connector.insert_a_t_plays_relation(res['id'], t)
        
    def ingest_artist_by_id(self, artist_id):
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
        except SpotifyException:
            print('Category %s play list ingestion failed' % category_id)
        else:
            for p in tqdm(play_lists, desc = 'Insertiing %s playlists' % category_id):

                self.connector.insert_playlist(p)
                self.connector.insert_cat_pl_relation(category_id, p['id'])
    
    def ingest_tracklist(self, tracks_href, playlist = None):
        """Inserts track by href.
        optional playlist argument is plalist id for HAS relation"""
        
        tracks = self.spotipy._get(tracks_href)
        for t in tqdm(tracks['items'], desc = 'Ingesting Tracks'):

            self.connector.insert_track(t['track'])

            try:
                self.connector.insert_pl_tr_relation(playlist, t['track']['id'])
                # TODO: Add relation attributes
            except TypeError:
                pass

    def clear_database(self):
        """Clears the database this ingerstor is connected to."""

        # TODO: add warning

        self.connector.clear_database()

if __name__  == '__main__':

    ing = Ingestor()

    kanye = '5K4W6rqBFWDnAN6FQUkS6x'
