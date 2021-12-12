from unittest import TestCase

from rdflib.term import URIRef

from datasources import triples as triplessrc

def generate_synthetic_triples(start=0, number=100):
    triples = []
    for i in range(number):
        triples.append(
            (
                URIRef(f'thing{start + i}', base='http://localhost/'),
                URIRef('has_thing', base='http://localhost/'),
                URIRef(f'otherthing{start + i}', base='http://localhost/')
            )
        )
        
    return triples

class TestRDFLibSource(TestCase):
    def test_insert_triples(self):
        rdf = triplessrc.RDFLibSource()
        triples = generate_synthetic_triples()
        rdf.insert_triples(triples)

        query = '''
            select * where {?s ?p ?o}
        '''
        self.assertEqual(len(rdf.sparql_get(query)), 100)

    def test_multiple_inserts(self):
        rdf = triplessrc.RDFLibSource()

        triples = generate_synthetic_triples(0, 100)
        rdf.insert_triples(triples)

        triples = generate_synthetic_triples(50, 100)
        rdf.insert_triples(triples)

        query = '''
            select * where {?s ?p ?o}
        '''
        self.assertEqual(len(rdf.sparql_get(query)), 150)
