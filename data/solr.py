from sunburnt import Solr

class Solr(object):

    UNIQUE_ID_FIELD = "uniqueId"
    IDS_FIELD = "id"

    def __init__(self, uri, index_name="", config=None, mappings=dict()):
        self.connection = sunburnt.SolrInterface("http://localhost:8983/solr/{0}".format(index_name))
        self.count_created = 0
        self.count_updated = 0


    # this method signature exposes some logic that is proper to ES, should be clean to expose
    # only what we need from an app pov.
    # "identifiers" is what is used in providers atm
    def index(self, pending_document, doc_type, identifier_field_name='identifiers', *args, **kwargs):
        results = self.connection.query(id=pending_document[identifier_field_name]).execute()
        results = list(results)

        # prepare document for solr format

        # lat,lon e.g. <field name="geo">43.17614,-90.57341</field>
        pending_document['location'] = "{0},{1}".format(pending_document['location'].get('lat'),
                pending_document['location'].get('lon'))

        # named id instead of identifiers...
        pending_document[IDS_FIELD] = pending_document.pop(identifier_field_name)
        # adding unique ID field, this will be used to update document. Order of IDs *should NOT* change
        # over time, not entirely sure if we can trust that
        pending_document[UNIQUE_ID_FIELD] = pending_document[IDS_FIELD][0]

        if len(results) == 0:
            self.connection.add(pending_document)
            self.count_created += 1
        elif len(results) == 1:
            # TODO this IDs merging process should be extracted from this method, and tested
            current_doc = results[0]
            current_idents = current_doc[IDS_FIELD]
            pending_idents = pending_document[IDS_FIELD]
            pending_idents.extend(current_idents)
            merged_idents = list(set(pending_idents))
            current_doc.update(pending_document)
            current_doc[IDS_FIELD] = merged_idents
            if merged_idents is None:
                print "Current: ", current_idents
                print "Pending: ", pending_idents
            self.connection.add(current_doc) 
            self.count_updated += 1
        elif len(results) > 1:
            raise Exception("Too many results! %s" % pending_document[identifier_field_name])

    def query(self, q):
        return self.connection.query(q).execute()
