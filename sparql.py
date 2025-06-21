from luxql import JsonReader, LuxConfig
from luxql.sparql import SparqlTranslator

cfg = LuxConfig()
r = JsonReader(cfg)
st = SparqlTranslator(cfg)
q = r.read(
    {
        "OR": [
            {"text": "squirrel"},
            {"carries": {"aboutAgent": {"startAt": {"name": "amsterdam"}}}},
        ]
    },
    "item",
)

q = r.read(
    {"AND": [{"OR": [{"name": "John"}, {"name": "Jane"}]}, {"OR": [{"name": "Trumbull"}, {"name": "West"}]}]}, "agent"
)

# q = r.read({"text": "froissart robinson"}, "work")

spq = st.translate(q)
print(spq.get_text())
