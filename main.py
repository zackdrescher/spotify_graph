from ingestor import Ingestor

if __name__ == '__main__':

    # WIPE AND RELOAD OFF OF kanye

    ing = Ingestor()

    ing.clear_database()

    ing.ingest()