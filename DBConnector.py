from neo4j.v1 import GraphDatabase, basic_auth, CypherError, ConstraintError

# debug
from IPython.core.debugger import Pdb

URL = "bolt://localhost:7687"
USER = 'neo4j'
PASSWORD = 'password'

class DBConnector(object):
    """docstring for DBConnector."""

    def __init__(self, dbUrl = URL, db_user = USER, db_password = PASSWORD):

        self.driver = GraphDatabase.driver(dbUrl,
            auth=basic_auth(db_user, db_password))

    # Get data
    ############################################################################        
    def get_node_label(self, label):

        with self.driver.session() as session:
            return [record['n'] for record in session.run(
                "MATCH (n:%s) RETURN n" % label)]

    def get_distinct_property(self, prop, label): 

        with self.driver.session() as session:
            i = 'n.%s' % prop
            return [record for record in session.run(
                "MATCH (n:%s) RETURN distinct n.id as id, n.%s as %s" % (
                    label, prop, prop))]

    # Update data
    ############################################################################
    def update_artist(self, artist_id, prop, val):
        """Sets the property of the artist to the the value"""

        with self.driver.session() as session:
            return [record['n'] for record in session.run(
                "MATCH (n:Artist { id : '%s' }) "
                "SET n.%s = %s "
                "RETURN n" % (artist_id, prop, val)
                )]

    # insert Nodes
    ############################################################################
    def insert_artist(self, res, genre = False):
        """inserrts API response res as artist into db. Ingests artist generes
        as well."""

        res, genres = self.parse_artist(res)

        with self.driver.session() as session:
            try:
                result = session.run(
                    statement="MERGE (a:Artist %s) return a;" % self.merge_param(res)
                    )
            except ConstraintError:
                print(res['name'] +' already stored')

        # ingest artist genres
        if genre:
            for g in genres:
                self.insert_genre(g)
                self.insert_a_g_plays_realtion(res['id'], g)

    def insert_category(self, res):
        """inserts API respose res as category in database."""

        del res['icons']

        with self.driver.session() as session:
            try:
                result = session.run(
                    statement="MERGE (a:Category %s) return a;" % self.merge_param(res)
                    )
            except ConstraintError:
                print(res['name'] +' already stored')

    def insert_genre(self, name):
        """Inserts genre into database"""
        d = {'name' : name}
        with self.driver.session() as session:
            try:
                result = session.run(
                    statement="MERGE (a:Genre 'name' : %s) return a;" % self.merge_param(name)
                    )
            except ConstraintError:
                print(d['name'] +' already stored') 

    def insert_playlist(self, res):
        # parse_parse playlist

        with self.driver.session() as session:
            # create feilds
            res['num_tracks'] = res['tracks']['total']
            res['tracks'] = res['tracks']['href']
            # Select feilds
            feilds = ['collaborative', 'href', 'id', 'name', 'type', 'tracks', 'num_tracks']
            d3 = {k : v for k,v in res.items() if k in feilds}
            try:

                result = session.run(
                    statement="MERGE (a:Playlist %s) return a;" % self.merge_param(res)
                    )
            except ConstraintError:
                print('Playlist %s already stored' % res['name'])

    def insert_track(self, res):
        
        if res is None:
            return

        # process feilds
        artist = res.pop('artists')[0]
        res['artist'] = artist['name']
        res['artist_href'] = artist['href']
        album = res.pop('album')
        res['album'] = album['name']
        res['album_href'] = album['href']
        res.pop('available_markets')
        res.pop('external_urls')
        res.pop('external_ids')

        with self.driver.session() as session:
            try:
                result = session.run(
                    statement="MERGE (a:Track %s) return a;" % self.merge_param(res)
                    )

            except ConstraintError:
                print('Track %s already stored' % res['name'])

    # Insert Relations
    ############################################################################
    def insert_related_relation(self, artist1, artist2):
        """Insersts related relation between given artists. a1 -> a2."""

        with self.driver.session() as session:
            result = session.run(
                statement="MATCH (a1:Artist), (a2:Artist) "
                          "WHERE a1.id = {artist1} AND a2.id = {artist2} "
                          "CREATE UNIQUE (a1)-[:RELATED]->(a2) ",
                parameters = {'artist1' : artist1, 'artist2' : artist2})

    def insert_a_g_plays_realtion(self, artist, genre):
        """Insert artist plays genre relation into database"""

        with self.driver.session() as session:
            #try:
            result = session.run(
                statement="MATCH (a:Artist), (g:Genre) "
                          "WHERE a.id = {artist} AND g.name = {genre} "
                          "CREATE UNIQUE (a)-[:PLAYS]->(g) ",
                parameters = {'artist' : artist, 'genre' : genre})

    def insert_a_t_plays_relation(self, artist, track):
        with self.driver.session() as session:
            #try:
            result = session.run(
                statement="MATCH (a:Artist), (t:Track) "
                          "WHERE a.id = {artist} AND t.id = {track} "
                          "CREATE UNIQUE (a)-[:PLAYS]->(t) ",
                parameters = {'artist' : artist, 'track' : track})

    def insert_cat_pl_relation(self, category, playlist):
        with self.driver.session() as session:
            #try:
            result = session.run(
                statement="MATCH (c:Category), (p:Playlist) "
                          "WHERE c.id = {category} AND p.id = {playlist} "
                          "CREATE UNIQUE (c)-[:HAS]->(p) ",
                parameters = {'category' : category, 'playlist' : playlist})

    def insert_pl_tr_relation(self, playlist, track):
        with self.driver.session() as session:
            #try:
            result = session.run(
                statement="MATCH (p:Playlist), (t:Track) "
                          "WHERE p.id = {playlist} AND t.id = {track} "
                          "CREATE UNIQUE (p)-[:HAS]->(t) ",
                parameters = {'playlist' : playlist, 'track' : track})

    # Remove
    ############################################################################
    def clear_database(self):
        """Removes all the objects in the data base"""

        with self.driver.session() as session:
            session.run("MATCH ()-[r]-() DELETE r")
            session.run("MATCH (n) DELETE n ")

    # Statics
    ############################################################################
    @staticmethod
    def parse_artist(obj):
        """Parses the artist json returned by the api into the property format
        for the DB"""

        del obj['images']
        del obj['external_urls']
        obj['followers'] = obj['followers']['total']
        genres = obj.pop('genres')
        return obj, genres

    @staticmethod
    def merge_param(d):
        """Converts a dictionary into a string suitable for Cypher Merge 
        attributes"""
        s = '{'
        for k, v in d.items():
            if type(v) is str:
                v = repr(v)
            elif type(v) is not int: continue
            s += ' %s : %s,' % (k, v)
        return s[:-1] + '}'

if __name__ == '__main__':
     conn = DBConnector()
