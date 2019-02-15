from ingestor import Ingestor

if __name__ == '__main__':

    # WIPE AND RELOAD OFF OF kanye

    ing = Ingestor()

    kanye = '5K4W6rqBFWDnAN6FQUkS6x'

    ing.clear_database()

    ing.ingest_by_id(kanye)

    ing.ingest_related_artist(kanye, _depth = 2)
