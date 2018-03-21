# spotify_graph

This repository contains tooling for scraping data off of the spotify API and building a graph database with it using neo4j. Uses spotipy python library to pull information from spotify API

## Setup

### Requirements
- python 3.6
- neo4j

### Dependencies

Install python package dependencies using `pip install -r requirements.txt`

### Running
- Start neo4j server
- run ingestion script

## API Reference

### ingestor
- orchestrates ETL from spotify API to neo4j
