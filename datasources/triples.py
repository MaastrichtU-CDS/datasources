from abc import abstractmethod
from typing import Tuple
from pathlib import Path
from datetime import datetime

import rdflib as rdf
from rdflib.term import Node, URIRef

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
