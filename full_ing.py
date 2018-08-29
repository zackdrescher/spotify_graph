from ingestor import Ingestor
from datetime import datetime

ing = Ingestor()

ing.clear_database()

start = datetime.now()

ing.ingest()

end = datetime.now()
t = end - start
print("Ingestion ended at ", end)
print("Ingestion ended in ", end)