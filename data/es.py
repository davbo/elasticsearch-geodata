from pyes import ES, TermsQuery
from pyes.exceptions import IndexAlreadyExistsException


class ElasticSearch(object):
    def __init__(self, es_uri, index_name, config=None, mappings=dict()):
        self.connection = ES(es_uri)
        self.index_name = index_name
        try:
            self.connection.create_index(index_name, settings=config)
        except IndexAlreadyExistsException:
            pass
        for mapping_name, mapping in mappings.iteritems():
            self.connection.put_mapping(mapping_name, {'properties': mapping}, [index_name])
        self.count_created = 0
        self.count_updated = 0


    def index(self, doc, doc_type, identifier_field_name='identifiers', *args, **kwargs):
        q = TermsQuery(identifier_field_name, doc[identifier_field_name])
        results = self.connection.search(q, indices=self.index_name)
        results = list(results)
        if len(results) == 0:
            self.connection.index(doc, self.index_name, doc_type, *args, **kwargs)
            self.count_created += 1
        elif len(results) == 1:
            old_doc = results[0]
            old_doc.update(doc)
            self.count_updated += 1
        elif len(results) > 1:
            raise Exception("Too many results! %s" % doc[identifier_field_name])
