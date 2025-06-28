from fastapi import FastAPI
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware

import requests
import json
import uvicorn
import os
import copy
import aiohttp
import urllib

import psycopg2
from psycopg2.extras import RealDictCursor

from middletier_config import cfg, rdr, st
from middletier_config import hal_link_templates, hal_queries, sparql_hal_queries
from middletier_config import sorts, facets
from middletier_config import related_list_names, related_list_queries, related_list_sparql

conn = psycopg2.connect(user="rs2668", dbname="rs2668")
table = "merged_data_cache"

app = FastAPI()
origins = ["http://localhost:3000", "*"]
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


async def fetch_sparql(spq):
    if type(spq) is str:
        q = spq
    else:
        q = spq.get_text()
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                "http://localhost:7010/sparql",
                data={"query": q},
                headers={"Accept": "application/sparql-results+json"},
            ) as response:
                ret = await response.json()
                if "results" in ret:
                    results = [r for r in ret["results"]["bindings"]]
                else:
                    print(q)
                    print(json.dumps(ret, indent=2))
                    results = []
    except Exception as e:
        print(q)
        print(e)
        raise
        results = []
    return results


@app.get("/api/advanced-search-config")
async def do_get_config():
    return JSONResponse(content=cfg.lux_config)


@app.get("/api/search/{scope}")
async def do_search(scope, q={}, page=1, pageLength=20, sort=""):
    if scope == "multi":
        # Handle multi scope search for archiveSort
        # FIXME -- figure this out
        print(q)
        return JSONResponse({})

    page = int(page)
    pageLength = int(pageLength)
    offset = (page - 1) * pageLength
    sort = sort.strip()
    if sort:
        try:
            sort, ascdesc = sort.split(":")
            ascdesc = ascdesc.upper().strip()
            sort = sort.strip()
        except Exception:
            ascdesc = "ASC"
    else:
        sort = "relevance"
        ascdesc = "DESC"
    pred = sorts[scope].get(sort, "relevance")

    q = q.replace("http://localhost:5001/", "https://lux.collections.yale.edu/")
    jq = json.loads(q)
    parsed = rdr.read(jq, scope)
    spq = st.translate_search(parsed, limit=pageLength, offset=offset, sort=pred, order=ascdesc)
    qt = spq.get_text()
    res = await fetch_sparql(qt)

    spq2 = st.translate_search_count(parsed)
    qt2 = spq2.get_text()
    ttl_res = await fetch_sparql(qt2)
    ttl = ttl_res[0]["count"]["value"]
    uq = urllib.parse.quote(q)

    js = {
        "@context": "https://linked.art/ns/v1/search.json",
        "id": f"http://localhost:5001/api/search/{scope}?q={uq}&page=1",
        "type": "OrderedCollectionPage",
        "partOf": {
            "id": f"http://localhost:5001/api/search-estimate/{scope}?q={uq}",
            "type": "OrderedCollection",
            "label": {"en": ["Search Results"]},
            "summary": {"en": ["Description of Search Results"]},
            "totalItems": ttl,
        },
        "orderedItems": [],
    }
    # do next and prev

    for r in res:
        js["orderedItems"].append(
            {
                "id": r["uri"]["value"].replace(
                    "https://lux.collections.yale.edu/data/", "http://localhost:5001/data/"
                ),
                "type": "Object",
            }
        )
    return JSONResponse(content=js)


@app.get("/api/search-estimate/{scope}")
async def do_search_estimate(scope, q={}, page=1):
    q = q.replace("http://localhost:5001/", "https://lux.collections.yale.edu/")
    jq = json.loads(q)
    uq = urllib.parse.quote(q)
    js = {
        "@context": "https://linked.art/ns/v1/search.json",
        "id": f"http://localhost:5001/api/search/{scope}?q={uq}",
        "type": "OrderedCollection",
        "label": {"en": ["Search Results"]},
        "summary": {"en": ["Description of Search Results"]},
        "totalItems": 0,
    }
    try:
        parsed = rdr.read(jq, scope)
    except ValueError as e:
        return JSONResponse(content=js)
    spq2 = st.translate_search_count(parsed)
    qt2 = spq2.get_text()
    ttl_res = await fetch_sparql(qt2)
    ttl = ttl_res[0]["count"]["value"]
    js["totalItems"] = int(ttl)
    return JSONResponse(content=js)


@app.get("/api/search-will-match")
async def do_search_match(q={}):
    scope = q["_scope"]
    del q["_scope"]

    q = q.replace("http://localhost:5001/", "https://lux.collections.yale.edu/")
    jq = json.loads(q)
    parsed = rdr.read(jq, scope)
    spq2 = st.translate_search_count(parsed)
    qt2 = spq2.get_text()
    ttl_res = await fetch_sparql(qt2)
    ttl = ttl_res[0]["count"]["value"]
    js = {
        "unnamed": {
            "hasOneOrMoreResult": 1 if ttl > 0 else 0,
            "isRelatedList": False,
        }
    }
    return JSONResponse(content=js)


@app.get("/api/facets/{scope}")
async def do_facet(scope, q={}, name="", page=1):
    q = q.replace("http://localhost:5001/", "https://lux.collections.yale.edu/")
    jq = json.loads(q)
    parsed = rdr.read(jq, scope)

    uq = urllib.parse.quote(q)
    js = {
        "@context": "https://linked.art/ns/v1/search.json",
        "id": f"http://localhost:5001/api/facets/{scope}?q={uq}&name={name}&page={page}",
        "type": "OrderedCollectionPage",
        "partOf": {"type": "OrderedCollection", "totalItems": 1000},
        "orderedItems": [],
    }

    pname = None
    pname2 = None
    if name.endswith("RecordType"):
        pred = "a"
    elif name.endswith("IsOnline"):
        return JSONResponse(js)
    elif name == "responsibleCollections":
        pred = "lux:itemMemberOfSet/lux:setCuratedBy"
    elif name == "responsibleUnits":
        pred = "lux:itemMemberOfSet/lux:setCuratedBy/lux:agentMemberOfGroup"
    else:
        pname = facets.get(name, None)
        pname2 = pname["searchTermName"]
        pred = st.get_predicate(pname2, scope)
        if pred == "lux:missed":
            pred = st.get_leaf_predicate(pname2, scope)
            if type(pred) is list:
                pred = pred[0]
            if pred == "missed":
                pred = pname2
    if ":" not in pred and pred != "a":
        pred = f"lux:{pred}"
    # print(f"{name} {pname} {pname2} {pred}")

    spq = st.translate_facet(parsed, pred)
    res = await fetch_sparql(spq)
    if res:
        spq2 = st.translate_facet_count(parsed, pred)
        res2 = await fetch_sparql(spq2)
        ttl = int(res2[0]["count"]["value"])
        js["partOf"]["totalItems"] = ttl

    for r in res:
        # Need to know type of facet (per datatype below)
        # and what query to AND based on the predicate
        # e.g:
        # AND: [(query), {"rel": {"id": "val"}}]

        if r["facet"]["type"] == "uri":
            clause = {pname2: {"id": r["facet"]["value"]}}
            val = (
                r["facet"]["value"]
                .replace("https://lux.collections.yale.edu/data/", "http://localhost:5001/data/")
                .replace("https://lux.collections.yale.edu/ns/", "")
                .replace("https://linked.art/ns/terms/", "")
            )

        elif r["facet"]["datatype"].endswith("int") or r["facet"]["datatype"].endswith("decimal"):
            val = int(r["facet"]["value"])
            clause = {pname2: val}
        elif r["facet"]["datatype"].endswith("float"):
            val = float(r["facet"]["value"])
            clause = {pname2: val}

        elif r["facet"]["datatype"].endswith("dateTime"):
            val = r["facet"]["value"]
            clause = {pname2: val}
        else:
            raise ValueError(r)

        nq = {"AND": [clause, jq]}
        qstr = urllib.parse.quote(json.dumps(nq, separators=(",", ":")))
        js["orderedItems"].append(
            {
                "id": f"http://localhost:5001/api/search-estimate/{scope}?q={qstr}",
                "type": "OrderedCollection",
                "value": val,
                "totalItems": int(r["facetCount"]["value"]),
            }
        )

    return JSONResponse(content=js)


@app.get("/api/related-list/{scope}")
async def do_related_list(scope, name, uri, page=1):
    """?name=relatedToAgent&uri=(uri-of-record)"""
    uuri = urllib.parse.quote(uri)
    js = {
        "@context": "https://linked.art/ns/v1/search.json",
        "id": f"http://localhost:5001/api/related-list/{scope}?name={name}&uri={uuri}&page={page}",
        "type": "OrderedCollectionPage",
        "orderedItems": [],
    }
    entry = {
        "id": f"http://localhost:5001/api/search-estimate/{scope}?q=QUERY-HERE",
        "type": "OrderedCollection",
        "totalItems": 0,
        "first": {
            "id": f"http://localhost:5001/api/search/{scope}?q=QUERY-HERE",
            "type": "OrderedCollectionPage",
        },
        "value": "",
        "name": "",
    }
    # scope is the type of records to find
    # name gives related list type (relatedToAgent)
    # uri is the anchoring entity

    all_res = {}
    cts = {}
    for name, spq in related_list_sparql[scope].items():
        qry = spq.replace("URI-HERE", uri)
        res = await fetch_sparql(qry)
        for row in res:
            what = row["uri"]["value"]
            ct = int(row["count"]["value"])
            try:
                cts[what] += ct
            except KeyError:
                cts[what] = ct
            sqry = related_list_queries[scope][name].replace("URI-HERE", uri)
            try:
                all_res[what].append((name, ct, sqry))
            except Exception:
                all_res[what] = [(name, ct, sqry)]

    # FIXME: These queries aren't complete
    # https://lux.collections.yale.edu/api/related-list/concept?&name=relatedToAgent&uri=https%3A%2F%2Flux.collections.yale.edu%2Fdata%2Fperson%2F66049111-383e-4526-9632-2e9b6b6302dd
    # vs
    # http://localhost:5001/api/related-list/concept?name=relatedToAgent&uri=https%3A//lux.collections.yale.edu/data/person/66049111-383e-4526-9632-2e9b6b6302dd
    # Need to include `what` in the query, as per facets

    all_sort = sorted(cts, key=cts.get, reverse=True)
    for what in all_sort[:25]:
        es = sorted(all_res[what], key=lambda x: x[1], reverse=True)
        for rel, ct, sqry in es:
            usqry = urllib.parse.quote(sqry)
            e = copy.deepcopy(entry)
            e["id"] = e["id"].replace("QUERY-HERE", usqry)
            e["value"] = what.replace("https://lux.collections.yale.edu/", "http://localhost:5001/")
            e["totalItems"] = ct
            e["name"] = related_list_names[rel]
            e["first"]["id"] = e["first"]["id"].replace("QUERY-HERE", usqry)
            js["orderedItems"].append(e)
    js["orderedItems"].sort(
        key=lambda x: cts[x["value"].replace("http://localhost:5001/", "https://lux.collections.yale.edu/")],
        reverse=True,
    )
    return JSONResponse(content=js)


@app.get("/api/translate/{scope}")
async def do_translate(scope, q={}):
    # take simple search in text and return json query equivalent
    js = {"_scope": scope, "AND": [{"text": q}]}
    return JSONResponse(content=js)


async def do_hal_links(scope, identifier):
    if os.path.exists(f"hal_cache/{identifier}.json"):
        with open(f"hal_cache/{identifier}.json", "r") as f:
            links = json.load(f)
        return links

    uri = f"https://lux.collections.yale.edu/data/{scope}/{identifier}"
    links = {}
    if scope in ["person", "group"]:
        hscope = "agent"
    elif scope in ["object", "digital"]:
        hscope = "item"
    elif scope in ["place", "set", "event", "concept"]:
        hscope = scope
    elif scope in ["period", "activity"]:
        hscope = "event"
    elif scope in ["text", "visual", "image"]:
        hscope = "work"
    else:
        print(f"MISSED SCOPE IN HAL: {scope}")
        hscope = scope
    uuri = urllib.parse.quote(uri)
    for hal, spq in sparql_hal_queries[hscope].items():
        if type(spq) is str:
            # related-list ... just add it
            href = hal_link_templates[hal].replace("{id}", uuri)
            links[hal] = {"href": href, "_estimate": 1}
            continue
        qt = spq.get_text()
        qt = qt.replace("URI-HERE", uri)
        res = await fetch_sparql(qt)
        ttl = int(res[0]["count"]["value"])
        if ttl > 0:
            jq = hal_queries[hscope][hal]
            jqs = json.dumps(jq, separators=(",", ":"))
            jqs = jqs.replace("URI-HERE", uri)
            jqs = urllib.parse.quote(jqs)
            href = hal_link_templates[hal].replace("{q}", jqs)
            links[hal] = {"href": href, "_estimate": 1}

    with open(f"hal_cache/{identifier}.json", "w") as f:
        json.dump(links, f)
    return links


@app.get("/data/{scope}/{identifier}")
async def do_get_record(scope, identifier, profile=None):
    # Check postgres cache

    cursor = conn.cursor(cursor_factory=RealDictCursor)
    qry = f"SELECT * FROM {table} WHERE identifier = %s"
    params = (identifier,)
    cursor.execute(qry, params)
    row = cursor.fetchone()
    if row:
        js = row["data"]
        jstr = json.dumps(js)

        links = {
            "curies": [
                {"name": "lux", "href": "http://localhost:5001/api/rels/{rel}", "templated": True},
                {"name": "la", "href": "https://linked.art/api/1.0/rels/{rel}", "templated": True},
            ],
            "self": {"href": f"http://localhost:5001/data/{scope}/{identifier}"},
        }
        if not profile:
            # Calculate _links here
            more_links = await do_hal_links(scope, identifier)
            links.update(more_links)

        jstr = jstr.replace("https://lux.collections.yale.edu/data/", "http://localhost:5001/data/")
        js2 = json.loads(jstr)

        js2["_links"] = links
        return JSONResponse(content=js2)
    else:
        return JSONResponse(content={}, status_code=404)

    # Check filesystem cache
    fn = f"cache/{scope}/{identifier}"
    if os.path.exists(fn):
        with open(fn) as fh:
            return JSONResponse(content=json.load(fh))

    # Nope, fetch from MarkLogic
    url = f"https://lux.collections.yale.edu/data/{scope}/{identifier}"
    r = requests.get(url)
    txt = r.text
    txt = txt.replace("https://lux.collections.yale.edu/data/", "http://localhost:5001/data/")
    txt = txt.replace("https://lux.collections.yale.edu/api/", "http://localhost:5001/api/")
    with open(fn, "w") as fh:
        fh.write(txt)

    js = json.loads(txt)
    return JSONResponse(content=js)


@app.get("/api/stats")
async def do_stats():
    """Fetch counts of each class"""
    spq = "SELECT ?class (COUNT(?class) as ?count) {?what a ?class}  GROUP  BY  ?class"
    res = await fetch_sparql(spq)
    vals = {}
    for r in res:
        vals[r["class"]["value"].rsplit("/")[-1].lower()] = int(r["count"]["value"])
    cts = {}
    for s in cfg.scopes:
        cts[s] = vals[s]
    js = {"estimates": {"searchScopes": cts}}
    return JSONResponse(content=js)


# --- Main Execution ---
if __name__ == "__main__":
    print("Starting uvicorn server...")
    uvicorn.run(app, host="0.0.0.0", port=5001, log_level="warning")
