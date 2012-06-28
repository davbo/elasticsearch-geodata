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


    def index(self, pending_document, doc_type, identifier_field_name='identifiers', *args, **kwargs):
        q = TermsQuery(identifier_field_name, pending_document[identifier_field_name])
        results = self.connection.search(q, indices=self.index_name)
        results = list(results)
        if len(results) == 0:
            self.connection.index(pending_document, self.index_name, doc_type, *args, **kwargs)
            self.count_created += 1
        elif len(results) == 1:
            current_doc = results[0]
            current_idents = current_doc[identifier_field_name]
            pending_idents = pending_document[identifier_field_name]
            merged_idents = pending_idents.extend(current_idents)
            current_doc.update(pending_document)
            current_doc[identifier_field_name] = merged_idents
            if merged_idents is None:
                print "Current: ", current_idents
                print "Pending: ", pending_idents
            self.connection.update(current_doc, self.index_name, doc_type, current_doc.get_id(), *args, **kwargs)
            self.count_updated += 1
        elif len(results) > 1:
            raise Exception("Too many results! %s" % pending_document[identifier_field_name])
