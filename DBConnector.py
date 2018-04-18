from neo4j.v1 import GraphDatabase, basic_auth, CypherError

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
    def get_categories(self):
        """Returns a list of the cagegories in the playlist"""

        with self.driver.session() as session:
            return [record['n'] for record in session.run(
                "MATCH (n:Category) RETURN n")]

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
    def insert_artist(self, res):
        """inserrts API response res as artist into db. Ingests artist generes
        as well."""

        res, genres = self.parse_artist(res)

        with self.driver.session() as session:
            try:
                result = session.run(
                    statement="CREATE (a:Artist) SET a = {dict_param}",
                    parameters={'dict_param': res}
                    )
            except CypherError:
                print(res['name'] +' already stored')

        # ingest artist genres
        for g in genres:
            self.insert_genre(g)
            self.insert_plays_realtion(res['id'], g)

    def insert_category(self, res):
        """inserts API respose res as category in database."""

        del res['icons']

        with self.driver.session() as session:
            try:
                result = session.run(
                    statement="CREATE (a:Category) SET a = {dict_param}",
                    parameters={'dict_param': res}
                    )
            except CypherError:
                print(res['name'] +' already stored')

    def insert_genre(self, name):
        """Inserts genre into database"""
        d = {'name' : name}
        with self.driver.session() as session:
            try:
                result = session.run(
                    statement="CREATE (a:Genre) SET a = {dict_param}",
                    parameters={'dict_param': d}
                    )
            except CypherError:
                print(d['name'] +' already stored')

    def insert_playlist(self, res):
        # parse_parse playlist
        pass

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

    def insert_plays_realtion(self, artist, genre):
        """Insert artist plays genre relation into database"""

        with self.driver.session() as session:
            #try:
            result = session.run(
                statement="MATCH (a:Artist), (g:Genre) "
                          "WHERE a.id = {artist} AND g.name = {genre} "
                          "CREATE UNIQUE (a)-[:PLAYS]->(g) ",
                parameters = {'artist' : artist, 'genre' : genre})

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

if __name__ == '__main__':
     conn = DBConnector()
