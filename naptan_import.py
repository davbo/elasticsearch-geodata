from data.es import ElasticSearch
from xml.sax import handler, make_parser


class NaptanXMLHandler(handler.ContentHandler):
    def __init__(self, areas, config, mappings):
        self.es = ElasticSearch('http://127.0.0.1:9200', 'places', config, mappings)
        self.areas = areas
        self.prev_tag = None
        self.data = None
        self.capture_data = False
        self.results = []

    def startElement(self, name, attrs):
        self.prev_tag = name
        if name == 'StopPoint':
            self.data = dict()
            self.capture_data = True

    def endElement(self, name):
        if name == 'StopPoint':
            self.add_stop(self.data)
            self.data = None
            self.capture_data = False

    def characters(self, content):
        if self.capture_data:
            self.data[self.prev_tag] = content

    def add_stop(self, data):
        """If within our set of areas then store to be indexed"""
        area_code = data['AtcoCode'][:3]
        if area_code in self.areas:
            self.results.append(data)

    def endDocument(self):
        print "Result length: ", len(self.results)
        for result in self.results:
            name = result['CommonName']
            location = {'lon': result.pop('Longitude'), 'lat': result.pop('Latitude')}
            data = dict([('naptan:%s' % k, v) for k, v in result.items()])
            data['identifiers'] = ["atco:%s" % result['AtcoCode']]
            data['location'] = location
            data['name'] = name
            self.es.index(data, 'poi')
        print "Updated: ", self.es.count_updated
        print "Created: ", self.es.count_created



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
    parser.setContentHandler(NaptanXMLHandler(['340'], config, mappings))
    # Parse in 8k chunks
    naptan = open('NaPTAN.xml')
    buffer = naptan.read(8192)
    # bunzip = bz2.BZ2Decompressor()
    while buffer:
        parser.feed(buffer)
        buffer = naptan.read(8192)
    parser.close()
