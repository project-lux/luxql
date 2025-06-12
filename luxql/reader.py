from . import LuxAPI, LuxLeaf, LuxBoolean, LuxRelationship
from SPARQLBurger.SPARQLQueryBuilder import *


class Reader:
    def __init__(self, config):
        self.config = config

    def read(self, query, scope):
        """Parse query in JSON into luxql objects"""
        if not query:
            raise ValueError("Query is empty")
        if not isinstance(query, dict):
            raise ValueError("Query is not a dictionary")
        if not self.config:
            raise ValueError("Reader is missing config")
        if not scope in self.config.scopes:
            raise ValueError(f"Unknown query scope '{scope}'")

        api = LuxAPI(scope)
        return self.read_query(query, api)

    def read_query(self, query, parent):
        """What sort of node are we?"""
        for k, v in query.items():
            if k[0] != "_":
                # This is the main function
                if type(v) is list:
                    # we're a boolean
                    return self.make_boolean(k, query, parent)
                elif type(v) is dict:
                    # we're a relationship
                    return self.make_relationship(k, query, parent)
                elif type(v) in [str, int, float, bool]:
                    # we're a leaf
                    return self.make_leaf(k, query, parent)
        # If we reach here, the query is invalid
        raise ValueError("Invalid query")

    def make_boolean(self, k, query, parent):
        # we're a boolean
        bl = LuxBoolean(k, parent=parent)
        for v in query[k]:
            self.read_query(v, bl)
        return bl

    def make_relationship(self, k, query, parent):
        # we're a relationship
        rel = LuxRelationship(k, parent=parent)
        self.read_query(query[k], rel)
        return rel

    def make_leaf(self, k, query, parent):
        # we're a leaf
        cmpr = query.get("_comp", None)
        opts = query.get("_options", [])
        wgt = query.get("_weight", None)
        comp = query.get("_complete", None)
        leaf = LuxLeaf(k, value=query[k], parent=parent, comparitor=cmpr, options=opts, weight=wgt, complete=comp)
        return leaf


class SparqlTranslator:
    def __init__(self, config):
        self.config = config
        self.counter = 0

    def translate(self, query):
        # Implement translation logic here
        self.counter = 0
        sparql = SPARQLSelectQuery(distinct=True, limit=25)
        sparql.add_prefix(prefix=Prefix("rdf", namespace="http://www.w3.org/1999/02/22-rdf-syntax-ns#"))
        sparql.add_prefix(prefix=Prefix("xsd", namespace="http://www.w3.org/2001/XMLSchema#"))
        sparql.add_prefix(prefix=Prefix("dc", namespace="http://purl.org/dc/elements/1.1/"))
        sparql.add_prefix(prefix=Prefix("rdfs", namespace="http://www.w3.org/2000/01/rdf-schema#"))
        sparql.add_prefix(prefix=Prefix("crm", namespace="http://www.cidoc-crm.org/cidoc-crm/"))
        sparql.add_prefix(prefix=Prefix("la", namespace="https://linked.art/ns/terms/"))
        sparql.add_prefix(prefix=Prefix("skos", namespace="http://www.w3.org/2004/02/skos/core#"))
        sparql.add_prefix(prefix=Prefix("sci", namespace="http://www.ics.forth.gr/isl/CRMsci/"))
        sparql.add_prefix(prefix=Prefix("dig", namespace="http://www.ics.forth.gr/isl/CRMdig/"))
        sparql.add_prefix(prefix=Prefix("lux", namespace="https://lux.collections.yale.edu/ns/"))

        sparql.add_variables(variables=["?uri"])
        where = SPARQLGraphPattern()
        query.var = f"?uri"
        self.translate_query(query, where)
        sparql.set_where_pattern(graph_pattern=where)
        return sparql

    def translate_query(self, query, where):
        # print(f"translate query: {query.to_json()}")
        if isinstance(query, LuxBoolean):
            if query.field == "AND":
                self.translate_and(query, where)
            elif query.field == "OR":
                self.translate_or(query, where)
            elif query.field == "NOT":
                self.translate_not(query, where)
        elif isinstance(query, LuxRelationship):
            self.translate_relationship(query, where)
        elif isinstance(query, LuxLeaf):
            self.translate_leaf(query, where)
        else:
            print(f"Got {type(query)}")

    def translate_or(self, query, parent):
        # UNION a,b,c...
        x = 0
        for child in query.children:
            child.var = query.var
            if x == 0:
                clause = SPARQLGraphPattern()
            else:
                clause = SPARQLGraphPattern(union=True)
            x += 1
            self.translate_query(child, clause)
            parent.add_nested_graph_pattern(graph_pattern=clause)

    def translate_and(self, query, parent):
        # just add the patterns in
        for child in query.children:
            child.var = query.var
            self.translate_query(child, parent)

    def translate_not(self, query, parent):
        # FILTER NOT EXISTS { ...}
        pass

    def translate_relationship(self, query, parent):
        query.children[0].var = f"?var{self.counter}"
        self.counter += 1
        parent.add_triples(
            triples=[Triple(subject=query.var, predicate=f"lux:{query.field}", object=query.children[0].var)]
        )
        self.translate_query(query.children[0], parent)

    def translate_leaf(self, query, parent):
        # provides_scope is the 'relation' from advanced-search-config
        typ = query.provides_scope
        if typ == "text":
            parent.add_triples(triples=[Triple(subject=query.var, predicate="lux:text", object=f'"{query.value}"')])
        elif typ == "date":
            # do date query per qlever
            pass
        elif typ == "float":
            # do number query per qlever
            pass
        else:
            # Unknown
            raise ValueError(f"Unknown provides_scope: {typ}")
