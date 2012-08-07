import bz2  # TODO: Import using a bzip compressed file
import logging

from xml.sax import handler, make_parser
from data.es import ElasticSearch
from data.solr import Solr

h = logging.StreamHandler()
logger = logging.getLogger(__name__)
logger.addHandler(h)
INDEX_NAME = "places"


class OSMHandler(handler.ContentHandler):

    def __init__(self, config, mappings):
        self.solr = Solr('')
        #self.es = ElasticSearch('http://127.0.0.1:9200', 'places', config, mappings)

    def startDocument(self):
        self.tags = {}
        self.valid_node = True
        self.create_count, self.modify_count = 0, 0
        self.delete_count, self.unchanged_count = 0, 0
        self.ignore_count = 0
        self.node_locations = {}

    def startElement(self, name, attrs):
        if name == 'node':
            lon, lat = float(attrs['lon']), float(attrs['lat'])
            id = attrs['id']
            self.node_location = lon, lat
            self.attrs = attrs
            self.id = id
            self.tags = {}
            self.node_locations[id] = lon, lat
        elif name == 'tag':
            self.tags[attrs['k']] = attrs['v']
        elif name == 'way':
            self.nodes = []
            self.tags = {}
            self.attrs = attrs
            self.id = attrs['id']
        elif name == 'nd':
            self.nodes.append(attrs['ref'])

    def endElement(self, element_type):
        if element_type == 'node':
            location = self.node_location
        elif element_type == 'way':
            min_, max_ = (float('inf'), float('inf')), (float('-inf'), float('-inf'))
            for lon, lat in [self.node_locations[n] for n in self.nodes]:
                min_ = min(min_[0], lon), min(min_[1], lat)
                max_ = max(max_[0], lon), max(max_[1], lat)
            location = (min_[0] + max_[0]) / 2, (min_[1] + max_[1]) / 2
        if element_type in ['way', 'node'] and any([x in self.tags for x in ['amenity', 'naptan:AtcoCode']]):
            result = dict([('raw_osm:%s' % k, v) for k, v in self.tags.items()])
            result['raw_osm:type'] = element_type
            result['raw_osm:version'] = self.attrs['version']
            result['identifiers'] = ['osm:%s' % self.id]
            atco = self.tags.get('naptan:AtcoCode', None)
            if atco:
                result['identifiers'].append('atco:%s' % atco)
            # Some ameneties do not have names, this is correct behaviour.
            # For example, post boxes and car parks.
            result['name'] = self.tags.get('name', self.tags.get('operator', None))
            result['location'] = location
            self.solr.index(result, 'poi')

    def endDocument(self):
        print "Updated: ", self.solr.count_updated
        print "Created: ", self.solr.count_created

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
    parser = make_parser(['xml.sax.xmlreader.IncrementalParser'])
    parser.setContentHandler(OSMHandler(config, mappings))
    # Parse in 8k chunks
    osm = open('oxfordshire.osm')
    buffer = osm.read(8192)
    # bunzip = bz2.BZ2Decompressor()
    while buffer:
        parser.feed(buffer)
        buffer = osm.read(8192)
    parser.close()
