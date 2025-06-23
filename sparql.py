from luxql import JsonReader, LuxConfig
from luxql.sparql import SparqlTranslator

from SPARQLWrapper import SPARQLWrapper, JSON

cfg = LuxConfig()
rdr = JsonReader(cfg)
st = SparqlTranslator(cfg)
q = rdr.read(
    {
        "AND": [
            {"classification": {"name": "painting"}},
            {
                "carries": {
                    "aboutAgent": {
                        "encountered": {
                            "classification": {
                                "id": "https://lux.collections.yale.edu/data/concept/05a41429-8a18-4911-854e-eae804b7d46f"
                            }
                        }
                    }
                }
            },
        ]
    },
    "item",
)

# q = rdr.read(
#    {"AND": [{"aboutItem": {"producedBy": {"name": "turner"}}}, {"createdBy": {"name": "tate britain"}}]}, "work"
# )


# q = r.read(
#    {"AND": [{"OR": [{"name": "John"}, {"name": "Jane"}]}, {"OR": [{"name": "Trumbull"}, {"name": "West"}]}]}, "agent"
# )

# q = r.read({"text": "froissart robinson"}, "work")

spq = st.translate_search(q)
# spq = st.translate_facet(q, "lux:workLanguage")
qt = spq.get_text()
endpoint = SPARQLWrapper("http://localhost:7010")
endpoint.setReturnFormat(JSON)
endpoint.setQuery(qt)
try:
    ret = endpoint.queryAndConvert()
    for r in ret["results"]["bindings"]:
        print(r)
except Exception as e:
    print(e)
