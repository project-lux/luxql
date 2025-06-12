from luxql import Reader, LuxConfig
from luxql.reader import SparqlTranslator

cfg = LuxConfig()
r = Reader(cfg)
st = SparqlTranslator(cfg)
q = r.read(
    {
        "OR": [
            {"text": "squirrel"},
            {"carries": {"aboutAgent": {"occupation": {"name": "artist", "_options": ["stemmed"]}}}},
        ]
    },
    "item",
)

q = r.read(
    {"AND": [{"OR": [{"name": "John"}, {"name": "Jane"}]}, {"OR": [{"name": "Trumbull"}, {"name": "West"}]}]}, "agent"
)

spq = st.translate(q)
print(spq.get_text())
