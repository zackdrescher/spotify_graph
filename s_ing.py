from ingestor import Ingestor

ing = Ingestor()

ing.clear_database()

ing.ingest(limit=20)