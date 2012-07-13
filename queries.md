# On all documents,
# filter to documents distant to X km from a given lat/lon point
# sort by distance to this point
# include a field containing the distance in km to the given lat/lon point
# NOTE: _geo_distance takes lon/lat (instead of lon/lat) due to GeoJSON format
# see https://github.com/elasticsearch/elasticsearch/issues/1885

curl -XGET http://127.0.0.1:9200/places/_search?pretty=true -d '{
  "query": {
    "match_all": {}
  },
  "filter": {
    "geo_distance": {
      "distance": "0.2km",
      "distance_type": "arc",
      "location": { "lat": 51.7605390, "lon": -1.2601904 }
    }
  },
  "sort": [
    {
      "_geo_distance": {
        "location": [-1.2601904, 51.7605390],
        "order": "asc",
        "unit": "km"
      }
    }
  ],
  "fields": [
    "_source"
  ],
  "script_fields": {
    "distance": {
      "params": {
        "lat": 51.7605390,
        "lon": -1.2601904
      },
      "script": "doc[\u0027location\u0027].arcDistanceInKm(lat, lon)"
    }
  }
}'
