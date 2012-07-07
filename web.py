from flask import Flask, request
from data.es import ElasticSearch
from pyes import *
import json

app = Flask(__name__)


@app.route('/fts')
def search_fts():
    name = request.args.get('name')
    es = ElasticSearch('http://127.0.0.1:9200', 'places')
    q = FuzzyQuery('name', name)
    results = es.query(q)
    names = []
    for r in results:
        names.append(r.name)
    return json.dumps(names)

@app.route('/geo')
def search_geo():
    geohash = request.args.get('hash')
    distance = request.args.get('distance', '0.1km')
    es = ElasticSearch('http://127.0.0.1:9200', 'places')
    #gq = GeoBoundingBoxFilter("location", location_tl = geohash, location_br = geohash)
    gq = GeoDistanceFilter("location", geohash, distance)
    q = FilteredQuery(MatchAllQuery(), gq)
    results = es.query(q)
    names = []
    for r in results:
        names.append(r.name)
    return json.dumps(names)


if __name__ == '__main__':
    app.run(host='0.0.0.0', debug=True)
