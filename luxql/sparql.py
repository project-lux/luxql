from . import LuxLeaf, LuxBoolean, LuxRelationship
from SPARQLBurger.SPARQLQueryBuilder import *
from SPARQLBurger.SPARQLQueryBuilder import OrderBy

Pattern = SPARQLGraphPattern


class SparqlTranslator:
    def __init__(self, config):
        self.config = config
        self.counter = 0
        self.prefixes = {
            "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
            "xsd": "http://www.w3.org/2001/XMLSchema#",
            "dc": "http://purl.org/dc/elements/1.1/",
            "dct": "http://purl.org/dc/terms/",
            "rdfs": "http://www.w3.org/2000/01/rdf-schema#",
            "la": "https://linked.art/ns/terms/",
            "lux": "https://lux.collections.yale.edu/ns/",
        }

        # ^elt is inverse path (e.g. from object to subject)
        # elt* is zero or more
        # elt+ is one or more
        # elt? is zero or one

    def make_sparql_or(self, operands):
        x = 0
        parent = Pattern()
        for child in operands:
            if x == 0:
                clause = Pattern()
            else:
                clause = Pattern(union=True)
            x += 1
            clause.add_triples(triples=[child])
            parent.add_nested_graph_pattern(clause)
        return parent

    def translate(self, query, scope=None):
        # Implement translation logic here
        self.counter = 0
        ob = OrderBy(["?score"], True)

        sparql = SPARQLSelectQuery(distinct=True, limit=25, offset=0)
        for pfx, uri in self.prefixes.items():
            sparql.add_prefix(prefix=Prefix(pfx, uri))
        sparql.add_variables(variables=["?uri"])

        where = Pattern()
        if scope is not None and scope != "any":
            t = Triple("?uri", "a", f"lux:{scope.Title()}")
            where.add_triples([t])

        query.var = f"?uri"
        self.translate_query(query, where)
        bs = []
        for x in range(self.counter):
            bs.append(f"COALESCE(?score{x}, 0)")
        where.add_binding(Binding(" + ".join(bs), "?score"))

        sparql.add_order_by(ob)
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
                clause = Pattern()
            else:
                clause = Pattern(union=True)
            x += 1
            self.translate_query(child, clause)
            parent.add_nested_graph_pattern(graph_pattern=clause)

    def translate_and(self, query, parent):
        # just add the patterns in
        for child in query.children:
            child.var = query.var
            self.translate_query(child, parent)

    def translate_not(self, query, parent):
        # FILTER NOT EXISTS { ...} ?
        pass

    def translate_relationship(self, query, parent):
        query.children[0].var = f"?var{self.counter}"
        self.counter += 1
        pred = f"lux:{query.field}"
        parent.add_triples([Triple(query.var, pred, query.children[0].var)])
        self.translate_query(query.children[0], parent)

    def translate_leaf(self, query, parent):
        typ = query.provides_scope  # text / date / number etc.
        scope = query.parent.provides_scope  # item/work/etc

        if typ == "text":
            # extract words
            words = query.value.lower().split()

            # Test if we should make them prefixes or phrases
            # prefix = word*
            # phrase = search for words and then FILTER()

            if query.field == "name":
                value = " ".join(words)
                field = f"lux:{scope}Name"
                patt = Pattern()
                trips = self.make_sparql_word(query.var, field, 0, value, 0)
                patt.add_triples(trips)
                word_scores = []
                for word in words:
                    word_scores.append(f"(?ql_score_word_txt0{self.counter}0_{word} *2)")
                patt.add_binding(Binding(" + ".join(word_scores), f"?score{self.counter}"))
                parent.add_nested_graph_pattern(patt)

            elif query.field == "text":
                # If a phrase then filter() the result

                top = Pattern()
                wx = 0
                for w in words:
                    wpatt = Pattern()
                    p1 = Pattern()
                    opt1 = self.make_sparql_ref(query, scope, 1, w, wx, False)
                    p1.add_nested_graph_pattern(opt1)
                    opt2 = self.make_sparql_anywhere(query, scope, 2, w, wx, True)
                    p1.add_nested_graph_pattern(opt2)
                    p1.add_binding(
                        Binding(
                            f"COALESCE(?score_refs_{self.counter}{wx}, 0) + COALESCE(?score_text_{self.counter}{wx}, 0)",
                            f"?score_{self.counter}{wx}",
                        )
                    )
                    wpatt.add_nested_graph_pattern(p1)

                    p2 = Pattern(union=True)
                    opt2 = self.make_sparql_anywhere(query, scope, 2, w, wx, False)
                    p2.add_nested_graph_pattern(opt2)
                    opt1 = self.make_sparql_ref(query, scope, 1, w, wx, True)
                    p2.add_nested_graph_pattern(opt1)
                    p2.add_binding(
                        Binding(
                            f"COALESCE(?score_refs_{self.counter}{wx}, 0) + COALESCE(?score_text_{self.counter}{wx}, 0)",
                            f"?score_{self.counter}{wx}",
                        )
                    )
                    wpatt.add_nested_graph_pattern(p2)
                    top.add_nested_graph_pattern(wpatt)
                    wx += 1

                parent.add_nested_graph_pattern(top)
            elif query.field == "identifier":
                pass

        elif typ == "date":
            # do date query per qlever
            pass
        elif typ == "float":
            # do number query per qlever
            pass
        else:
            # Unknown
            raise ValueError(f"Unknown provides_scope: {typ}")
        self.counter += 1

    def make_sparql_word(self, var, pred, n, w, wx):
        fvar = f"?field{n}{self.counter}{wx}"
        wvar = f"?txt{n}{self.counter}{wx}"
        trips = [
            Triple(var, pred, fvar),
            Triple(wvar, "ql:contains-word", f'"{w}"'),
            Triple(wvar, "ql:contains-entity", fvar),
        ]
        return trips

    def make_sparql_ref(self, query, scope, n, w, wx, optional):
        opt1 = Pattern(optional=optional)
        trips = self.make_sparql_word(query.var, f"lux:{scope}Any/lux:primaryName", n, w, wx)
        opt1.add_triples(trips)
        opt1.add_binding(
            Binding(f"?ql_score_word_txt{n}{self.counter}{wx}_{w} * 6", f"?score_refs_{self.counter}{wx}")
        )
        return opt1

    def make_sparql_anywhere(self, query, scope, n, w, wx, optional):
        opt12 = Pattern(optional=optional)
        trips = self.make_sparql_word(query.var, "lux:recordText", n, w, wx)
        nvar = f"?name{self.counter}{wx}"
        trips.append(Triple(query.var, f"lux:{scope}PrimaryName", nvar))
        opt12.add_triples(trips)
        opt1n = Pattern(optional=True)
        wvar = f"?namet{self.counter}{wx}"
        trips = [Triple(wvar, "ql:contains-word", f'"{w}"'), Triple(wvar, "ql:contains-entity", nvar)]
        opt1n.add_triples(trips)
        opt12.add_nested_graph_pattern(opt1n)
        opt12.add_binding(
            Binding(
                f"?ql_score_word_txt{n}{self.counter}{wx}_{w} + (?ql_score_word_namet{self.counter}{wx}_{w} *4)",
                f"?score_text_{self.counter}{wx}",
            )
        )
        return opt12
