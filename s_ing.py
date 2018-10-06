from ingestor import Ingestor
from datetime import datetime

ing = Ingestor()

start = datetime.now()

ing.clear_database()

ing.ingest(limit=5)

end = datetime.now()
t = end - start
print("Ingestion ended at ", end)
print("Ingestion ended in ", t)