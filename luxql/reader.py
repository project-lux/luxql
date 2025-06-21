from . import LuxAPI, LuxLeaf, LuxBoolean, LuxRelationship
from SPARQLBurger.SPARQLQueryBuilder import *


class JsonReader:
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
