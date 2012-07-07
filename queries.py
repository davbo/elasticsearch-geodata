from pyes import *

INDICES = "places"

def search_dailyinfo_id(conn):
    wildcard = WildcardQuery("osm:dailyinfo:venue_id", "*")
    results = conn.search(wildcard, indices=INDICES)
    print len(results)

def count_documents_with_osm_and_naptan():
    pass

if __name__ == '__main__':
    conn = ES('127.0.0.1:9200')
    search_dailyinfo_id(conn)
