from luxql import JsonReader, LuxConfig
from luxql.sparql import SparqlTranslator

from SPARQLWrapper import SPARQLWrapper, JSON

cfg = LuxConfig()
r = JsonReader(cfg)
st = SparqlTranslator(cfg)
q = r.read(
    {
        "AND": [
            {"classification": {"name": "painting"}},
            {"carries": {"aboutAgent": {"encountered": {"classification": {"name": "fossil"}}}}},
        ]
    },
    "item",
)

q = r.read(
    {"AND": [{"aboutItem": {"producedBy": {"name": "turner"}}}, {"createdBy": {"name": "tate britain"}}]}, "work"
)


# q = r.read(
#    {"AND": [{"OR": [{"name": "John"}, {"name": "Jane"}]}, {"OR": [{"name": "Trumbull"}, {"name": "West"}]}]}, "agent"
# )

# q = r.read({"text": "froissart robinson"}, "work")

spq = st.translate_search(q)
# spq = st.translate_facet(q, "lux:workLanguage")
q = spq.get_text()


endpoint = SPARQLWrapper("http://localhost:7010")
endpoint.setReturnFormat(JSON)
endpoint.setQuery(q)
try:
    ret = endpoint.queryAndConvert()
    for r in ret["results"]["bindings"]:
        print(r)
except Exception as e:
    print(e)
