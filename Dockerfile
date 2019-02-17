FROM continuumio/miniconda

RUN pip install spotipy
RUN conda install pyodbc

ADD /ingestor/*.py /app/ingestor/ 
ADD *.py /app/

WORKDIR /files

#COPY requirements.txt /files
#RUN pip install -r /files/requirements.txt

ADD odbc.sh /files

RUN chmod +x /files/odbc.sh
RUN /files/odbc.sh

ENTRYPOINT [ "/bin/bash" ]