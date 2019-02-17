#!/bin/bash

docker run -it \
    -e USER=$USER \
    -e PASSWORD=$PASSWORD \
    -e HOST=$HOST \
    -e DATABASE=$DATABASE ingestor