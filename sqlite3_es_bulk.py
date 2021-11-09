import json
import sqlite3

from elasticsearch import Elasticsearch
from elasticsearch.helpers import streaming_bulk
from typing import List
from collections import namedtuple

from const import BODY_SETTINGS, DB_PATH, FIELD_NAMES, INDEX_NAME, URL

SQL = """
    WITH x as (
    SELECT m.id, group_concat(a.id) as actors_ids, group_concat(a.name) as actors_names FROM movies m
    LEFT JOIN movie_actors ma on m.id = ma.movie_id
    LEFT JOIN actors a on ma.actor_id = a.id
    GROUP BY m.id
    )
    SELECT m.id, genre, director, title, plot, imdb_rating, x.actors_ids, x.actors_names,
    CASE
    WHEN m.writers = '' THEN '[{"id": "' || m.writer || '"}]' ELSE m.writers END AS writers 
    FROM movies m LEFT JOIN x ON m.id = x.id
"""

conn = sqlite3.Connection(DB_PATH)


def create_index(client):
    client.indices.create(
        index=INDEX_NAME,
        body=BODY_SETTINGS,
        ignore=400,
    )


def load_from_sqldb() -> List[dict]:
    DataSQL = namedtuple("DataSQL", FIELD_NAMES)
    list_data: List[dict] = []

    for row in map(DataSQL._make, conn.execute(SQL)):
        data = {
            'id': row.id,
            'genre': row.genre,
            'director': row.director,
            'title': row.title,
            'plot': row.plot,
            'imdb_rating': row.imdb_rating,
            'actors_ids': row.actors_ids,
            'actors_names': row.actors_names,
            'writers': row.writers
        }
        list_data.append(data)
    return list_data


def load_writers_names() -> dict:
    writers = {}
    for writer in conn.execute("""SELECT DISTINCT id, name FROM writers"""):
        writers[writer[0]] = writer[1]
    return writers


def generate_actions() -> dict:
    list_data = load_from_sqldb()
    writers = load_writers_names()
    for i in list_data:
        movie_writers = []
        writers_set = set()
        for writer in json.loads(i['writers']):
            writer_id = writer['id']
            if writers[writer_id] != 'N/A' and writer_id not in writers_set:
                movie_writers.append(writers[writer_id])
                writers_set.add(writer_id)

        json_schema = {
              "id": i["id"],
              "imdb_rating": float(i["imdb_rating"]) if i['imdb_rating'] != 'N/A' else None,
              "genre": i["genre"].replace(' ', '').split(','),
              "title": i["title"],
              "description": i['plot'] if i['plot'] != 'N/A' else None,
              "director": [x.strip() for x in i["director"].split(',')] if i['director'] != 'N/A' else None,
              "actors_names": [x for x in i['actors_names'].split(',') if x != 'N/A'],
              "writers_names": movie_writers,
              "actors": [
                {"id": _id, "name": name} for _id, name in zip(i["actors_ids"].split(","), i['actors_names'].split(','))
                if name != "N/A"],
              "writers": movie_writers
        }
        json_data = json.dumps(json_schema)

        yield json_data


def load_to_es():
    es_client = Elasticsearch(URL)
    create_index(es_client)

    successes = 0
    failed = 0
    for ok, item in streaming_bulk(
            client=es_client, index="movies", actions=generate_actions(),
    ):
        if not ok:
            failed += 1
        else:
            successes += 1

    print("Indexed %d/%d documents" % (successes, failed))


if __name__ == "__main__":
    load_to_es()
