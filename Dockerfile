FROM python:3.6-stretch
# Debian9

# Install odbc
RUN mkdir /util/

COPY /util/odbc.sh /util/odbc.sh
RUN chmod +x /util/odbc.sh
RUN /util/odbc.sh

RUN apt-get -y update && apt-get install -y libzbar-dev

# python setup
RUN pip install spotipy
RUN pip install pyodbc

ADD /ingestor/*.py /app/ingestor/ 
ADD *.py /app/

WORKDIR /files

ENTRYPOINT [ "/bin/bash" ]