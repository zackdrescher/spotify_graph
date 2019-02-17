#! \bin\bash

# install docker
sudo apt install docker.io

git clone https://github.com/zackdrescher/spotify_ingestor.git

docker build -t ingestor .

