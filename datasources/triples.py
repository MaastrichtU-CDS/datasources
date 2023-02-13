from abc import abstractmethod
from typing import Tuple
from pathlib import Path
from datetime import datetime

import rdflib as rdf
from rdflib.term import Node, URIRef
from SPARQLWrapper import SPARQLWrapper, POST, GET, POSTDIRECTLY, JSON
import requests

from .util import AbstractSource


class AbstractTripleSource(AbstractSource):
    # TODO: add namespace
    def import_file(self, path: Path):
        """Generic method for importing files into a triples database. Should
        always work given a proper implementation of `self.insert_triples()`
        but should really be overriden with something more performant where
        possible.

        Args:
            path (Path): the path to the file to be imported
        """
        g = rdf.Graph()
        g.parse(path)

        i = 0
        triples = []
        for triple in g:
            i += 1
            triples.append(triple)
            if i >= 100:
                self.insert_triples(triples)
                i = 0
                triples = []

    def insert_triples(
        self,
        triples: list[Tuple[Node]],
        graphURI: URIRef = None
    ) -> None:
        """Generic method for inserting triples. Should work given a good
        implementation of `self.sparql_update()`, but should be implemented
        more efficiently where possible

        Args:
            triples (list[Tuple[Node]]): a list of triples to be inserted
            graph (URIRef): Optional, the graph to insert the triples into,
            inserts into the default graph... by default <_<

        Returns:
            None
        """
        triplestr = ''
        for triple in triples:
            triplestr += f'<{triple[0]}> <{triple[1]}> <{triple[2]}> .\n'

        if graphURI:
            query = """
                INSERT {
                    GRAPH <%s> {
                        %s
                    }
                } WHERE {}
            """ % (str(graphURI), triplestr)
        else:
            query = """
                INSERT {
                    %s
                } WHERE {}
            """ % (triplestr)

        return self.sparql_update(query)

    @abstractmethod
    def sparql_get(self, query: str):
        pass

    @abstractmethod
    def sparql_update(self, query: str):
        pass


class RDFLibSource(AbstractTripleSource):
    def __init__(self, path: Path = None, graph: rdf.Graph = None) -> None:
        if graph:
            self.graph = graph
        elif path:
            self.import_file(path)
        else:
            self.graph = rdf.Graph()

        super().__init__()

    def import_file(self, path: Path):
        self.graph.parse(path.absolute)

    def export_file(self, path: Path):
        if path.is_dir:
            path = \
                path / f'export_{datetime.now().strftime("%Y%m%dT%H%M%S")}.ttl'
        if path.suffix != '.ttl':
            path = path.with_suffix('.ttl')

        self.graph.serialize(path, format='turtle')

    def insert_triples(
        self,
        triples: list[Tuple[Node]],
        graphURI: URIRef = None
    ):
        # We have to turn these into quads to actually insert them
        context = graphURI if graphURI else self.graph
        quads = [(t[0], t[1], t[2], context) for t in triples]
        self.graph.addN(quads)

    def sparql_get(self, query: str):
        # TODO: figure out all the types returned by this
        ret = self.graph.query(query)

        return ret

    def sparql_update(self, query: str):
        return self.graph.update(query)

class SPARQLTripleStore(AbstractTripleSource):
    def __init__(
        self,
        endpoint: str,
        update_endpoint: str = None,
        gsp_endpoint: str = None,
        gsp_update_endpoint: str = None
    ) -> None:

        self.gsp_endpoint = gsp_endpoint if gsp_endpoint else endpoint
        self.gsp_update_endpoint = \
            gsp_update_endpoint if gsp_update_endpoint else gsp_endpoint

        self.sparql = SPARQLWrapper(
            endpoint,
            updateEndpoint=update_endpoint if update_endpoint else endpoint
        )

        super().__init__()

    def sparql_update(self, query: str):
        """Runs an update query against the endpoint

        Args:
            query (str): The query to be

        Returns:
            [type]: [description]
        """
        self.sparql.setQuery(query)
        self.sparql.setMethod(POST)
        self.sparql.setRequestMethod(POSTDIRECTLY)
        results = self.sparql.query()

        self.sparql.resetQuery()

        return results.response.read()

    def sparql_get(self, query: str):
        """Does a sparql get query on the database, anything like select and
        construct.

        Args:
            query (str): The query to run

        Returns:
            [type]: Type depends on query being run, usually a list of dicts
        """
        self.sparql.setQuery(query)
        self.sparql.setMethod(GET)
        self.sparql.setReturnFormat(JSON)

        results = self.sparql.query().convert()

        self.sparql.resetQuery()

        return results["results"]["bindings"]

    def export_file(self, path: Path, graph: URIRef = None) -> None:
        """Exports a particular graph to a turtle file.

        Args:
            path (Path): Path to where the file will be exported.
            graph (URIRef, optional): Graph to get. If no graph supplied,
            the default graph is returned instead. Defaults to None.
        """
        self.download_graph(graph).serialize(destination=path, format='turtle')

    def download_graph(self, graph: URIRef = None) -> rdf.Graph:
        """Helper(ish) function. downloads an entire graph through the graph
        store protocol, rather than through queries. Much faster as a result.

        Args:
            graph (URIRef, optional): Graph to get. If no graph supplied, the
            default graph is returned instead. Defaults to None.

        Returns:
            rdf.Graph: An RDFLib graph containing the graph in question.
        """
        ret = requests.get(
            self.gsp_endpoint +
            ('?default' if graph is None else f'?graph={graph}')
        )

        return rdf.Graph().parse(data=ret.text)


class GraphDBTripleStore(SPARQLTripleStore):
    def __init__(self, endpoint: str) -> None:
        super().__init__(
            endpoint,
            update_endpoint=endpoint + '/statements',
            gsp_endpoint=endpoint + '/rdf-graphs/service'
        )
