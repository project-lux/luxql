from . import LuxLeaf, LuxBoolean, LuxRelationship
from SPARQLBurger.SPARQLQueryBuilder import *

try:
    from SPARQLBurger.SPARQLQueryBuilder import OrderBy
except Exception:
    print("You need a version of SPARQLBurger with OrderBy and offset")

from SPARQLBurger.SPARQLQueryBuilder import GroupBy

Pattern = SPARQLGraphPattern  # noqa


class SparqlTranslator:
    def __init__(self, config):
        self.config = config
        self.counter = 0
        self.scored = []
        self.prefixes = {
            "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
            "xsd": "http://www.w3.org/2001/XMLSchema#",
            # "dc": "http://purl.org/dc/elements/1.1/",
            # "dct": "http://purl.org/dc/terms/",
            # "rdfs": "http://www.w3.org/2000/01/rdf-schema#",
            "la": "https://linked.art/ns/terms/",
            "lux": "https://lux.collections.yale.edu/ns/",
        }

        self.scope_fields = {
            "agent": {
                "startAt": "placeOfAgentBeginning",
                "endAt": "placeOfAgentEnding",
                "foundedBy": "agentOfAgentBeginning",
                "gender": "gender",
                "occupation": "occupation",
                "nationality": "nationality",
                "professionalActivity": "typeOfAgentActivity",
                "activeAt": "placeOfAgentActivity",
                "createdSet": "^agentOfSetBeginning",
                "produced": "^agentOfItemBeginning",
                "created": "^agentOfWorkBeginning",
                "carriedOut": "^eventCarriedOutBy",
                "curated": "^setCuratedBy",
                "encountered": "^agentOfItemEncounter",
                "founded": "^agentOfAgentBeginning",
                "memberOfInverse": "^agentMemberOfGroup",
                "influencedProduction": "^agentInfluenceOfItemBeginning",
                "influencedCreation": "^agentInfluenceOfWorkBeginning",
                "publishedSet": "^agentOfSetPublication",
                "published": "^agentOfWorkPublication",
                "subjectOfSet": "^setAboutAgent",
                "subjectOfWork": "^workAboutAgent",
            },
            "item": {
                "producedAt": "placeOfItemBeginning",
                "producedBy": "agentOfItemBeginning",
                "producedUsing": "typeOfItemBeginning",
                "productionInfluencedBy": "agentInfluenceOfItemBeginning",
                "encounteredAt": "placeOfItemEncounter",
                "encounteredBy": "agentOfItemEncounter",
                "carries": "carries",
                "material": "material",
                "subjectOfSet": "^setAboutItem",
                "subjectOfWork": "^workAboutItem",
            },
            "concept": {
                "broader": "broader",
                "classificationOfSet": "^setClassification",
                "classificationOfConcept": "^conceptClassification",
                "classificationOfEvent": "^eventClassification",
                "classificationOfItem": "^itemClassification",
                "classificationOfAgent": "^agentClassification",
                "classificationOfPlace": "^placeClassification",
                "classificationOfWork": "^workClassification",
                "genderOf": "^gender",
                "languageOf": "^workLanguage",
                "languageOfSet": "^setLanguage",
                "materialOfItem": "^material",
                "narrower": "^broader",
                "nationalityOf": "^nationality",
                "occupationOf": "^occupation",
                "professionalActivityOf": "^typeOfAgentActivity",
                "subjectOfSet": "^setAboutConcept",
                "subjectOfWork": "^workAboutConcept",
                "usedToProduce": "^typeOfItemBeginning",
            },
            "event": {
                "carriedOutBy": "agentOfEvent",
                "tookPlaceAt": "placeOfEvent",
                "used": "eventUsedSet",
                "causeOfEvent": "causeOfEvent",
                "causedCreationOf": "^causeOfWorkBeginning",
                "subjectOfSet": "^setAboutEvent",
                "subjectOfWork": "^workAboutEvent",
            },
            "place": {
                "partOf": "placePartOf",
                "activePlaceOfAgent": "^placeOfAgentActivity",
                "startPlaceOfAgent": "^placeOfAgentBeginning",
                "producedHere": "^placeOfItemBeginning",
                "createdHere": "^placeOfWorkBeginning",
                "endPlaceOfAgent": "^placeOfAgentEnding",
                "encounteredHere": "^placeOfItemEncounter",
                "placeOfEvent": "^placeOfEvent",
                "setPublishedHere": "^placeOfSetPublication",
                "publishedHere": "^placeOfWorkPublication",
                "subjectOfSet": "^setAboutPlace",
                "subjectOfWork": "^workAboutPlace",
            },
            "set": {
                "aboutConcept": "setAboutConcept",
                "aboutEvent": "setAboutEvent",
                "aboutItem": "setAboutItem",
                "aboutAgent": "setAboutAgent",
                "aboutPlace": "setAboutPlace",
                "aboutWork": "setAboutWork",
                "createdAt": "placeOfSetBeginning",
                "createdBy": "agentOfSetBeginning",
                "creationCausedBy": "causeOfSetBeginning",
                "curatedBy": "setCuratedBy",
                "publishedAt": "placeOfSetPublication",
                "publishedBy": "agentOfSetPublication",
                "containingSet": "^setMemberOfSet",
                "containingItem": "^itemMemberOfSet",
                "usedForEvent": "^eventUsedSet",
            },
            "work": {
                "aboutConcept": "workAboutConcept",
                "aboutEvent": "workAboutEvent",
                "aboutItem": "workAboutItem",
                "aboutAgent": "workAboutAgent",
                "aboutPlace": "workAboutPlace",
                "aboutWork": "workAboutWork",
                "createdAt": "placeOfWorkBeginning",
                "createdBy": "agentOfWorkBeginning",
                "creationCausedBy": "causeOfWorkBeginning",
                "creationInfluencedBy": "agentInfluenceOfWorkBeginning",
                "publishedAt": "placeOfWorkPublication",
                "publishedBy": "agentOfWorkPublication",
                "language": "workLanguage",
                "partOfWork": "workPartOf",
                "subjectOfSet": "^setAboutWork",
                "subjectOfWork": "^workAboutWork",
                "carriedBy": "^carries",
                "containsWork": "^partOfWork",
            },
        }
        # Property Path Notation:
        # ^elt is inverse path (e.g. from object to subject)
        # elt* is zero or more
        # elt+ is one or more
        # elt? is zero or one

    def translate_search(self, query, scope=None, limit=25, offset=0):
        # Implement translation logic here
        self.counter = 0
        self.scored = []
        ob = OrderBy(["?score"], True)

        sparql = SPARQLSelectQuery(distinct=True, limit=limit, offset=offset)
        for pfx, uri in self.prefixes.items():
            sparql.add_prefix(Prefix(pfx, uri))
        sparql.add_variables(["?uri"])

        where = Pattern()
        if scope is not None and scope != "any":
            t = Triple("?uri", "a", f"lux:{scope.title()}")
            where.add_triples([t])

        query.var = f"?uri"
        self.translate_query(query, where)
        bs = []
        for x in self.scored:
            bs.append(f"COALESCE(?score{x}, 0)")
        where.add_binding(Binding(" + ".join(bs), "?score"))

        sparql.add_order_by(ob)
        sparql.set_where_pattern(where)
        return sparql

    def translate_facet(self, query, facet, scope=None, limit=25, offset=0):
        self.counter = 0
        gb = GroupBy(["?facet"])
        ob = OrderBy(["?facetCount"], True)

        sparql = SPARQLSelectQuery(limit=limit, offset=offset)
        for pfx, uri in self.prefixes.items():
            sparql.add_prefix(prefix=Prefix(pfx, uri))
        sparql.add_variables(["?facet", "(COUNT(?facet) AS ?facetCount)"])

        inner = SPARQLSelectQuery(distinct=True)
        inner.add_variables(["?uri"])
        where = Pattern()
        if scope is not None and scope != "any":
            t = Triple("?uri", "a", f"lux:{scope.Title()}")
            where.add_triples([t])
        query.var = "?uri"
        self.translate_query(query, where)
        inner.set_where_pattern(where)

        outer = Pattern()
        outer.add_nested_select_query(inner)
        outer.add_triples([Triple("?uri", facet, "?facet")])

        sparql.add_group_by(gb)
        sparql.add_order_by(ob)
        sparql.set_where_pattern(outer)
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

    def get_predicate(self, rel, scope):
        # only relationships
        if rel in ["classification", "memberOf"]:
            return f"lux:{scope}{rel.title()}"
        else:
            p = self.scope_fields[scope].get(rel, "missed")
            if p[0] == "^":
                return f"^lux:{p[1:]}"
            else:
                return f"lux:{p}"

    def translate_relationship(self, query, parent):
        query.children[0].var = f"?var{self.counter}"
        self.counter += 1
        pred = self.get_predicate(query.field, query.parent.provides_scope)
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
                self.scored.append(self.counter)

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
                binds = []
                for x in range(wx):
                    binds.append(f"COALESCE(?score_{self.counter}{x}, 0)")
                parent.add_binding(Binding(" + ".join(binds), f"?score_{self.counter}"))
                self.scored.append(self.counter)

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
