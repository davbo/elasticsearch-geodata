from data.es import ElasticSearch
import json


class OxpointsImporter(object):
    
    def __init__(self, config, mappings):
        self.es = ElasticSearch('http://127.0.0.1:9200', 'places', config, mappings)

    def import_data(self):
        dump = open('oxpoints.json')
        data = json.load(dump)

        for datum in data:
            name = datum.get('oxp_fullyQualifiedTitle', datum.get('dc_title', ''))
            oxpoints_id = datum['uri'].rsplit('/')[-1]
            
            if datum.get('type', '') == 'http://xmlns.com/foaf/0.1/Image' or oxpoints_id.endswith('.jpg'):
                continue

            index = dict()
            index['name'] = name
            
            ids = list()
            ids.append('oxpoints:{0}'.format(oxpoints_id))

            if 'oxp_hasOUCSCode' in datum:
                ids.append('oucs:{0}'.format(datum.pop('oxp_hasOUCSCode')))
            if 'oxp_hasOLISCode' in datum:
                ids.append('olis:{0}'.format(datum.pop('oxp_hasOLISCode')))
            if 'oxp_hasOLISAlephCode' in datum:
                ids.append('olis-aleph:{0}'.format(datum.pop('oxp_hasOLISAlephCode')))
            if 'oxp_hasOSMIdentifier' in datum:
                ids.append('osm:{0}'.format(datum.pop('oxp_hasOSMIdentifier').split('/')[1]))

            index['identifiers'] = ids

            if 'geo_lat' in datum and 'geo_long' in datum:
                index['location'] = {'lon': datum.pop('geo_long'), 'lat': datum.pop('geo_lat')}

            # Added raw_ for Solr...
            index.update([('raw_oxpoints:{0}'.format(k), v) for k, v in datum.items()])

            self.es.index(index, 'poi')
        print "Updated: {0}".format(self.es.count_updated)
        print "Created: {0}".format(self.es.count_created)


if __name__ == '__main__':
    config = {
            'index': {
                'analysis': {
                    'tokenizer': {
                        'tokenizer-categories': {
                            'type': 'path_hierarchy'
                            }
                        },
                    'analyzer': {
                        'analyzer-categories': {
                            'tokenizer': 'tokenizer-categories'
                            }
                        }
                    }
                }
            }
    mappings = {'poi': {
                'identifiers': {
                    'type': 'string',
                    'store': 'yes',
                    'index': 'not_analyzed',
                    'index_name': 'identifier',
                    },
                'categories': {
                    'type': 'object',
                    'analyzer': 'analyzer-categories',
                    },
                'location': {
                    'type': 'geo_point',
                    }
                }
            }
 
    importer = OxpointsImporter(config, mappings)
    importer.import_data()
