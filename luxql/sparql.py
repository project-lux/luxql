from . import LuxLeaf, LuxBoolean, LuxRelationship
from .SPARQLQueryBuilder import *
import shlex

Pattern = GraphPattern  # noqa


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

        self.anywhere_field = "text"
        self.id_field = "id"
        self.name_field = "name"
        self.record_name_weight = 10
        self.record_text_weight = 3
        self.reference_name_weight = 1

        self.scope_leaf_fields = {
            "agent": {},
            "concept": {},
            "event": {},
            "place": {},
            "set": {},
            "work": {},
            "item": {},
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

    def translate_search(self, query, scope=None, limit=25, offset=0, sort="", order="", sortDefault="ZZZZZZZZZZ"):
        # Implement translation logic here
        self.counter = 0
        self.scored = []
        self.calculate_scores = False
        sparql = SelectQuery(limit=limit, offset=offset)
        for pfx, uri in self.prefixes.items():
            sparql.add_prefix(Prefix(pfx, uri))
        if sort and sort != "relevance":
            sparql.add_variables(["?uri", "(MIN(?sortWithDefault) AS ?sort)"])
        else:
            sparql.add_variables(["?uri", "(SUM(?score) AS ?sscore)"])
            self.calculate_scores = True

        where = Pattern()
        if scope is not None and scope != "any":
            t = Triple("?uri", "a", f"lux:{scope.title()}")
            where.add_triples([t])

        query.var = f"?uri"
        self.translate_query(query, where)

        gby = GroupBy(["?uri"])
        sparql.add_group_by(gby)

        if sort == "relevance":
            bs = []
            for x in self.scored:
                bs.append(f"COALESCE(?score_{x}, 0)")
            if bs:
                # ?uri lux:recordText ?rectxt .
                # BIND (COALESCE(?score_0, 0) / STRLEN(?rectxt) AS ?score)
                # where.add_triples(Triple("?uri", "lux:recordText", "?rectxt"))
                # This ranks archive components too highly
                where.add_binding(Binding(" + ".join(bs), "?score"))
                ob = OrderBy(["?sscore"], True)
                sparql.add_order_by(ob)
        elif sort:
            spatt = Pattern(optional=True)
            spatt.add_triples([Triple("?uri", sort, "?sortValue")])
            if "SortName" in sort:
                spatt.add_filter(Filter("!isNumeric(?sortValue)"))
            where.add_nested_graph_pattern(spatt)
            where.add_binding(Binding(f'COALESCE(?sortValue, "{sortDefault}")', "?sortWithDefault"))
            ob = OrderBy(["?sort"], order == "DESC")
            sparql.add_order_by(ob)

        sparql.set_where_pattern(where)
        return sparql

    def translate_search_count(self, query, scope=None):
        # Implement translation logic here
        self.counter = 0
        self.scored = []
        self.calculate_scores = False
        sparql = SelectQuery()
        for pfx, uri in self.prefixes.items():
            sparql.add_prefix(Prefix(pfx, uri))
        sparql.add_variables(["(COUNT(DISTINCT ?uri) AS ?count)"])

        where = Pattern()
        if scope is not None and scope != "any":
            t = Triple("?uri", "a", f"lux:{scope.title()}")
            where.add_triples([t])

        query.var = f"?uri"
        self.translate_query(query, where)
        sparql.set_where_pattern(where)
        return sparql

    def translate_search_related(self, query, scope=None):
        self.counter = 0
        self.scored = []
        self.calculate_scores = False
        sparql = SelectQuery(limit=100)
        for pfx, uri in self.prefixes.items():
            sparql.add_prefix(Prefix(pfx, uri))
        sparql.add_variables(["?uri", "(COUNT(?uri) AS ?count)"])

        where = Pattern()
        query.var = f"?uri"
        self.translate_query(query, where)
        where.add_filter(Filter("?uri != <URI-HERE>"))

        sparql.set_where_pattern(where)
        gby = GroupBy(["?uri"])
        sparql.add_group_by(gby)
        ob = OrderBy(["?count"], True)
        sparql.add_order_by(ob)

        return sparql

    def translate_facet(self, query, facet, scope=None, limit=25, offset=0):
        self.counter = 0
        gb = GroupBy(["?facet"])
        ob = OrderBy(["?facetCount"], True)

        sparql = SelectQuery(limit=limit, offset=offset)
        for pfx, uri in self.prefixes.items():
            sparql.add_prefix(prefix=Prefix(pfx, uri))
        sparql.add_variables(["?facet", "(COUNT(?facet) AS ?facetCount)"])

        inner = SelectQuery(distinct=True)
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

    def translate_facet_count(self, query, facet):
        """
        PREFIX lux: <https://lux.collections.yale.edu/ns/>
        SELECT (COUNT(?facet) AS ?count) WHERE {
          {
            SELECT ?facet WHERE {
              {
                SELECT DISTINCT ?uri WHERE {
                  ?uri lux:placeOfItemBeginning <https://lux.collections.yale.edu/data/place/02cff2e2-4285-4f82-bc5a-8d3b33596c9c> .
                }
              }
              ?uri lux:itemClassification ?facet .
            }
            GROUP BY ?facet
          }
        }
        """
        self.counter = 0

        sparql = SelectQuery()
        for pfx, uri in self.prefixes.items():
            sparql.add_prefix(prefix=Prefix(pfx, uri))
        sparql.add_variables(["(COUNT(?facet) AS ?count)"])

        inner = SelectQuery()
        gb = GroupBy(["?facet"])
        inner.add_variables(["?facet"])

        inner2 = SelectQuery(distinct=True)
        inner2.add_variables(["?uri"])
        where = Pattern()
        query.var = "?uri"
        self.translate_query(query, where)
        inner2.set_where_pattern(where)

        outer = Pattern()
        outer.add_nested_select_query(inner2)
        outer.add_triples([Triple("?uri", facet, "?facet")])
        inner.add_group_by(gb)
        inner.set_where_pattern(outer)

        swhere = Pattern()
        swhere.add_nested_select_query(inner)
        sparql.set_where_pattern(swhere)
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
            parent.add_nested_graph_pattern(clause)

    def translate_and(self, query, parent):
        # just add the patterns in
        for child in query.children:
            child.var = query.var
            self.translate_query(child, parent)

    def translate_not(self, query, parent):
        # FILTER NOT EXISTS { ...}
        clause = Pattern(not_exists=True)
        query.children[0].var = query.var
        self.translate_query(query.children[0], clause)
        parent.add_nested_graph_pattern(clause)

    def get_predicate(self, rel, scope):
        # only relationships
        if rel == "classification":
            return f"lux:{scope}{rel[0].upper()}{rel[1:]}"
        elif rel == "memberOf":
            typ = "Group" if scope == "agent" else "Set"
            return f"lux:{scope}{rel[0].upper()}{rel[1:]}{typ}"
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
        # test if only leaf is id:<uri>
        lf = query.children[0]
        if type(lf) is LuxLeaf and lf.field == "id":
            parent.add_triples([Triple(query.var, pred, f"<{lf.value}>")])
        else:
            parent.add_triples([Triple(query.var, pred, query.children[0].var)])
            self.translate_query(query.children[0], parent)

    def get_leaf_predicate(self, field, scope):
        if field in ["height", "width", "depth", "weight", "dimension"]:
            return f"lux:{field}"

        if field in ["startDate", "producedDate", "createdDate"]:
            return [f"lux:startOf{scope.title()}Beginning", f"lux:endOf{scope.title()}Beginning"]
        elif field == "endDate":
            return [f"lux:startOf{scope.title()}Ending", f"lux:endOf{scope.title()}Ending"]
        elif field == "activeDate":
            return [f"lux:startOf{scope.title()}Activity", f"lux:endOf{scope.title()}Activity"]
        elif field == "publishedDate":
            return [f"lux:startOf{scope.title()}Publication", f"lux:endOf{scope.title()}Publication"]
        elif field == "encounteredDate":
            return [f"lux:startOf{scope.title()}Encounter", f"lux:endOf{scope.title()}Encounter"]

        if field == "hasDigitalImage":
            return f"lux:{scope}{field[0].upper()}{field[1:]}"
        elif field == f"{scope}HasDigitalImage":
            return f"lux:{field}"

        pred = self.scope_leaf_fields[scope].get(field, "missed")
        return pred

    def translate_leaf(self, query, parent):
        typ = query.provides_scope  # text / date / number etc.
        scope = query.parent.provides_scope  # item/work/etc

        if typ == "text":
            # extract words

            # extract quoted phrases first
            val = query.value.lower()
            try:
                shwords = shlex.split(val)
            except:
                raise
            phrases = [w for w in shwords if " " in w]
            words = val.replace('"', "").split()

            # Test if we should make them prefixes or phrases
            # prefix = word*

            # FIXME:
            # invalid characters in sparql variable names are replaced with "_{ord(chr)}_"
            # eg o'malley --> o_39_malley --> ?ql_score_word_txt110_o_39_malley

            if query.field == self.name_field:
                value = " ".join(words)
                field = f"lux:{scope}Name"
                patt = Pattern()
                trips = self.make_sparql_word(query.var, field, 0, value, 0)
                patt.add_triples(trips)
                if self.calculate_scores:
                    word_scores = []
                    for word in words:
                        word_scores.append(f"(?ql_score_word_txt0{self.counter}0_{word} *2)")
                    patt.add_binding(Binding(" + ".join(word_scores), f"?score_{self.counter}"))
                if phrases:
                    fvar = f"?field0{self.counter}0"
                    for p in phrases:
                        patt.add_filter(Filter(f'CONTAINS(LCASE({fvar}), "{p.lower()}")'))
                parent.add_nested_graph_pattern(patt)
                self.scored.append(self.counter)

            elif query.field == self.anywhere_field:
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
                    if self.calculate_scores:
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

                    if self.calculate_scores:
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
                if self.calculate_scores:
                    binds = []
                    for x in range(wx):
                        binds.append(f"COALESCE(?score_{self.counter}{x}, 0)")
                    parent.add_binding(Binding(" + ".join(binds), f"?score_{self.counter}"))
                    self.scored.append(self.counter)
                if phrases:
                    fvar = f"?field2{self.counter}0"
                    nvar = f"?field1{self.counter}0"
                    ### How to also test OR in name text?
                    for p in phrases:
                        top.add_filter(Filter(f'CONTAINS(LCASE({fvar}), "{p}")'))

            elif query.field == self.id_field:
                v = Values([f"<{query.value}>"], query.var)
                parent.add_value(v)

            elif query.field == "identifier":
                # do exact match on the string
                pred = f"lux:{scope}Identifier"
                parent.add_triples([Triple(query.var, pred, f'"{query.value}"')])
            elif query.field == "recordType":
                parent.add_triples([Triple(query.var, "a", f"lux:{query.value}")])

        elif typ == "date":
            # do date query per qlever
            dt = query.value
            comp = query.comparitor
            field = query.field
            # botb, eote
            preds = self.get_leaf_predicate(field, scope)
            qvar = query.var
            bvar = f"?date1{self.counter}"
            evar = f"?date2{self.counter}"

            # This is insufficient -- it needs to turn the query into a range, and then compare
            #
            p = Pattern()
            trips = [Triple(qvar, preds[0], bvar), Triple(qvar, preds[1], evar)]
            p.add_triples(trips)
            p.add_filter(Filter(f'{bvar} {comp} "{dt}"^^xsd:dateTime'))
            parent.add_nested_graph_pattern(p)

        elif typ == "float":
            # do number query per qlever
            dt = query.value
            comp = query.comparitor
            field = query.field
            pred = self.get_leaf_predicate(field, scope)
            qvar = query.var
            fvar = f"?float{self.counter}"

            p = Pattern()
            trips = [Triple(qvar, pred, fvar)]
            p.add_triples(trips)
            p.add_filter(Filter(f'{fvar} {comp} "{dt}"^^xsd:float'))
            parent.add_nested_graph_pattern(p)

        elif typ == "boolean":
            dt = query.value
            field = query.field
            pred = self.get_leaf_predicate(field, scope)
            qvar = query.var

            p = Pattern()
            trips = [Triple(qvar, pred, f'"{dt}"^^xsd:decimal')]
            p.add_triples(trips)
            parent.add_nested_graph_pattern(p)

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
        if self.calculate_scores:
            opt1.add_binding(
                Binding(
                    f"?ql_score_word_txt{n}{self.counter}{wx}_{w} * {self.reference_name_weight}",
                    f"?score_refs_{self.counter}{wx}",
                )
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
        if self.calculate_scores:
            opt12.add_binding(
                Binding(
                    f"?ql_score_word_txt{n}{self.counter}{wx}_{w} * {self.record_text_weight} + (?ql_score_word_namet{self.counter}{wx}_{w} * {self.record_name_weight})",
                    f"?score_text_{self.counter}{wx}",
                )
            )
        return opt12
