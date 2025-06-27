from fastapi import FastAPI
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from luxql import JsonReader, LuxConfig, LuxBoolean, LuxRelationship, LuxLeaf
from luxql.sparql import SparqlTranslator
from SPARQLWrapper import SPARQLWrapper, JSON
import requests
import json
import uvicorn
import os
import copy
import time
import aiohttp
import asyncio

import psycopg2
from psycopg2.extras import RealDictCursor, Json

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

cfg = LuxConfig()
rdr = JsonReader(cfg)
st = SparqlTranslator(cfg)
endpoint = SPARQLWrapper("http://localhost:7010")
endpoint.setReturnFormat(JSON)

facets = {
    "agentActiveDate": {"searchTermName": "agentActiveDate", "idFacet": False},
    "agentActivePlaceId": {"searchTermName": "activeAt", "idFacet": True},
    "agentEndDate": {"searchTermName": "agentEndDate", "idFacet": False},
    "agentEndPlaceId": {"searchTermName": "endAt", "idFacet": True},
    "agentGenderId": {"searchTermName": "agentGender", "idFacet": True},
    "agentHasDigitalImage": {"searchTermName": "agentHasDigitalImage", "idFacet": False},
    "agentIdentifier": {"searchTermName": "agentIdentifier", "idFacet": False},
    "agentMemberOfId": {"searchTermName": "agentMemberOf", "idFacet": True},
    "agentNationalityId": {"searchTermName": "agentNationality", "idFacet": True},
    "agentOccupationId": {"searchTermName": "agentOccupation", "idFacet": True},
    "agentProfessionalActivityId": {"searchTermName": "agentProfessionalActivity", "idFacet": True},
    "agentRecordType": {"searchTermName": "agentRecordType", "idFacet": False},
    "agentStartDate": {"searchTermName": "agentStartDate", "idFacet": False},
    "agentStartPlaceId": {"searchTermName": "startAt", "idFacet": True},
    "agentTypeId": {"searchTermName": "classification", "idFacet": True},
    "conceptHasDigitalImage": {"searchTermName": "conceptHasDigitalImage", "idFacet": False},
    "conceptIdentifier": {"searchTermName": "conceptIdentifier", "idFacet": False},
    "conceptInfluencedByAgentId": {"searchTermName": "conceptInfluencedByAgent", "idFacet": True},
    "conceptInfluencedByConceptId": {"searchTermName": "conceptInfluencedByConcept", "idFacet": True},
    "conceptInfluencedByEventId": {"searchTermName": "conceptInfluencedByEvent", "idFacet": True},
    "conceptInfluencedByPlaceId": {"searchTermName": "conceptInfluencedByPlace", "idFacet": True},
    "conceptPartOfId": {"searchTermName": "broader", "idFacet": True},
    "conceptRecordType": {"searchTermName": "conceptRecordType", "idFacet": False},
    "conceptTypeId": {"searchTermName": "classification", "idFacet": True},
    "eventAgentId": {"searchTermName": "carriedOutBy", "idFacet": True},
    "eventEndDate": {"searchTermName": "eventEndDate", "idFacet": False},
    "eventIdentifier": {"searchTermName": "eventIdentifier", "idFacet": False},
    "eventPlaceId": {"searchTermName": "tookPlaceAt", "idFacet": True},
    "eventRecordType": {"searchTermName": "eventRecordType", "idFacet": False},
    "eventStartDate": {"searchTermName": "eventStartDate", "idFacet": False},
    "eventTypeId": {"searchTermName": "classification", "idFacet": True},
    "itemCarriedById": {"searchTermName": "carries", "idFacet": True},
    "itemDepthDimensionValue": {"searchTermName": "depth", "idFacet": False},
    "itemDimensionValue": {"searchTermName": "dimension", "idFacet": False},
    "itemEncounteredAgentId": {"searchTermName": "encounteredBy", "idFacet": True},
    "itemEncounteredDate": {"searchTermName": "encounteredDate", "idFacet": False},
    "itemEncounteredPlaceId": {"searchTermName": "encounteredAt", "idFacet": True},
    "itemHasDigitalImage": {"searchTermName": "itemHasDigitalImage", "idFacet": False},
    "itemHeightDimensionValue": {"searchTermName": "height", "idFacet": False},
    "itemIdentifier": {"searchTermName": "itemIdentifier", "idFacet": False},
    "itemIsOnline": {"searchTermName": "itemIsOnline", "idFacet": False},
    "itemMaterialId": {"searchTermName": "material", "idFacet": True},
    "itemMemberOfId": {"searchTermName": "itemMemberOf", "idFacet": True},
    "itemProductionAgentId": {"searchTermName": "producedBy", "idFacet": True},
    "itemProductionDate": {"searchTermName": "producedDate", "idFacet": False},
    "itemProductionPlaceId": {"searchTermName": "producedAt", "idFacet": True},
    "itemProductionTechniqueId": {"searchTermName": "producedUsing", "idFacet": False},
    "itemRecordType": {"searchTermName": "lux:recordType", "idFacet": False},
    "itemShownById": {"searchTermName": "carries", "idFacet": True},
    "itemTypeId": {"searchTermName": "classification", "idFacet": True},
    "itemWidthDimensionValue": {"searchTermName": "width", "idFacet": False},
    "placeHasDigitalImage": {"searchTermName": "placeHasDigitalImage", "idFacet": False},
    "placeIdentifier": {"searchTermName": "placeIdentifier", "idFacet": False},
    "placePartOfId": {"searchTermName": "placePartOf", "idFacet": True},
    "placeTypeId": {"searchTermName": "classification", "idFacet": True},
    "setAboutAgentId": {"searchTermName": "setAboutAgent", "idFacet": True},
    "setAboutConceptId": {"searchTermName": "setAboutConcept", "idFacet": True},
    "setAboutEventId": {"searchTermName": "setAboutEvent", "idFacet": True},
    "setAboutItemId": {"searchTermName": "setAboutItem", "idFacet": True},
    "setAboutPlaceId": {"searchTermName": "setAboutPlace", "idFacet": True},
    "setAboutSetId": {"searchTermName": "setAboutSet", "idFacet": True},
    "setAboutWorkId": {"searchTermName": "setAboutWork", "idFacet": True},
    "setCreationAgentId": {"searchTermName": "createdBy", "idFacet": True},
    "setCreationDate": {"searchTermName": "createdDate", "idFacet": False},
    "setCreationOrPublicationDate": {"searchTermName": "setCreationOrPublicationDate", "idFacet": False},
    "setCreationPlaceId": {"searchTermName": "createdAt", "idFacet": True},
    "setCurationAgentId": {"searchTermName": "setCurationAgent", "idFacet": True},
    "setHasDigitalImage": {"searchTermName": "setHasDigitalImage", "idFacet": False},
    "setIdentifier": {"searchTermName": "setIdentifier", "idFacet": False},
    "setIsOnline": {"searchTermName": "setIsOnline", "idFacet": False},
    "setPartOfId": {"searchTermName": "setPartOf", "idFacet": True},
    "setPublicationAgentId": {"searchTermName": "publishedBy", "idFacet": True},
    "setPublicationDate": {"searchTermName": "publishedDate", "idFacet": False},
    "setPublicationPlaceId": {"searchTermName": "publishedAt", "idFacet": True},
    "setTypeId": {"searchTermName": "classification", "idFacet": True},
    "workAboutAgentId": {"searchTermName": "workAboutAgent", "idFacet": True},
    "workAboutConceptId": {"searchTermName": "workAboutConcept", "idFacet": True},
    "workAboutEventId": {"searchTermName": "workAboutEvent", "idFacet": True},
    "workAboutItemId": {"searchTermName": "workAboutItem", "idFacet": True},
    "workAboutPlaceId": {"searchTermName": "workAboutPlace", "idFacet": True},
    "workAboutSetId": {"searchTermName": "workAboutSet", "idFacet": True},
    "workAboutWorkId": {"searchTermName": "workAboutWork", "idFacet": True},
    "workCreationAgentId": {"searchTermName": "createdBy", "idFacet": True},
    "workCreationDate": {"searchTermName": "createdDate", "idFacet": False},
    "workCreationOrPublicationDate": {"searchTermName": "workCreationOrPublicationDate", "idFacet": False},
    "workCreationPlaceId": {"searchTermName": "createdAt", "idFacet": True},
    "workHasDigitalImage": {"searchTermName": "workHasDigitalImage", "idFacet": False},
    "workIdentifier": {"searchTermName": "workIdentifier", "idFacet": False},
    "workIsOnline": {"searchTermName": "workIsOnline", "idFacet": False},
    "workLanguageId": {"searchTermName": "workLanguage", "idFacet": True},
    "workPartOfId": {"searchTermName": "workPartOf", "idFacet": True},
    "workPublicationAgentId": {"searchTermName": "publishedBy", "idFacet": True},
    "workPublicationDate": {"searchTermName": "publishedDate", "idFacet": False},
    "workPublicationPlaceId": {"searchTermName": "publishedAt", "idFacet": True},
    "workRecordType": {"searchTermName": "workRecordType", "idFacet": False},
    "workTypeId": {"searchTermName": "classification", "idFacet": False},
}

qs = os.listdir("queries")
queries = {}
for q in qs:
    if q.endswith(".json"):
        with open(f"queries/{q}", "r") as f:
            queries[q[:-5]] = json.load(f)

hal_queries = {
    "agent": {
        "lux:agentAgentMemberOf": queries["agentsMemberOfGroup"],
        "lux:agentCreatedPublishedInfluencedWork": queries["worksCreatedPublishedInfluencedByAgent"],
        "lux:agentEventsCarriedOut": queries["eventsCarriedOutByAgent"],
        "lux:agentEventsUsingProducedObjects": queries["eventsUsingAgentsProducedObjects"],
        "lux:agentFoundedByAgent": queries["agentsFoundedByAgent"],
        "lux:agentInfluencedConcepts": queries["conceptsInfluencedByAgent"],
        "lux:agentItemEncounteredTime": queries["itemsEncounteredByAgent"],
        "lux:agentItemMadeTime": queries["itemsProducedByAgent"],
        "lux:agentMadeDiscoveredInfluencedItem": queries["itemsProducedEncounteredInfluencedByAgent"],
        # "lux:agentRelatedAgents": queries["agentsRelatedToAgent"],
        # "lux:agentRelatedConcepts": queries["conceptsRelatedToAgent"],
        # "lux:agentRelatedItemTypes": queries["itemsProducedByAgent"],
        # "lux:agentRelatedMaterials": queries["itemsProducedByAgent"],
        # "lux:agentRelatedPlaces": queries["placesRelatedToAgent"],
        "lux:agentRelatedSubjects": queries["worksCreatedByAgent"],
        "lux:agentRelatedWorkTypes": queries["worksCreatedByAgent"],
        "lux:agentWorkAbout": queries["worksAboutAgent"],
        "lux:agentWorkCreatedTime": queries["worksCreatedPublishedInfluencedByAgent"],
        "lux:agentWorkPublishedTime": queries["worksCreatedPublishedInfluencedByAgent"],
        "lux:departmentItems": queries["itemsForDepartment"],
    },
    "concept": {
        "lux:conceptChildren": queries["childrenOfConcept"],
        "lux:conceptInfluencedConcepts": queries["conceptsInfluencedByConcept"],
        "lux:conceptItemEncounteredTime": queries["itemsOfTypeOrMaterial"],
        "lux:conceptItemMadeTime": queries["itemsOfTypeOrMaterial"],
        "lux:conceptItemTypes": queries["itemsOfTypeOrMaterial"],
        # "lux:conceptRelatedAgents": queries["agentsRelatedToConcept"],
        # "lux:conceptRelatedConcepts": queries["conceptsRelatedToConcept"],
        "lux:conceptRelatedItems": queries["itemsOfTypeOrMaterial"],
        # "lux:conceptRelatedPlaces": queries["placesRelatedToConcept"],
        "lux:conceptRelatedWorks": queries["worksRelatedToConcept"],
        "lux:conceptWorkCreatedTime": queries["worksRelatedToConcept"],
        "lux:conceptWorkPublishedTime": queries["worksRelatedToConcept"],
        "lux:conceptWorkTypes": queries["worksRelatedToConcept"],
        "lux:genderForAgent": queries["agentsWithGender"],
        "lux:nationalityForAgent": queries["agentsWithNationality"],
        "lux:occupationForAgent": queries["agentsWithOccupation"],
        "lux:typeForAgent": queries["agentsClassifiedAs"],
        "lux:typeForEvent": queries["eventsClassifiedAs"],
        "lux:typeForPlace": queries["placesClassifiedAs"],
    },
    "event": {
        "lux:eventConceptsInfluencedBy": queries["conceptsSubjectsForPeriod"],
        "lux:eventIncludedItems": queries["itemsForEvent"],
        "lux:eventItemMaterials": queries["itemsForEvent"],
        "lux:eventObjectTypesUsed": queries["itemsForEvent"],
        "lux:eventObjectTypesAbout": queries["itemsAboutEvent"],
        # "lux:eventRelatedAgents": queries["agentsRelatedToEvent"],
        # "lux:eventRelatedConcepts": queries["conceptsRelatedToEvent"],
        # "lux:eventRelatedPlaces": queries["placesRelatedToEvent"],
        "lux:eventWorksAbout": queries["worksAboutEvent"],
        "lux:eventWorkTypesUsed": queries["worksForEvent"],
        "lux:eventWorkTypesAbout": queries["worksAboutEvent"],
        "lux:eventCausedWorks": queries["worksCausedByEvent"],
    },
    "item": {
        "lux:itemArchive": queries["archivesWithItem"],
        "lux:itemEvents": queries["eventsWithItem"],
        "lux:itemDepartment": queries["departmentsWithItem"],
        "lux:itemUnit": queries["itemById"],
        "lux:itemWorksAbout": queries["worksAboutItem"],
    },
    "place": {
        "lux:placeActiveAgent": queries["agentsActiveAtPlace"],
        "lux:placeBornAgent": queries["agentsBornAtPlace"],
        "lux:placeCreatedWork": queries["worksCreatedAtPlace"],
        "lux:placeDepictingWork": queries["worksCreatedAtPlace"],
        "lux:placeDepictedAgentsFromRelatedWorks": queries["worksRelatedToPlace"],
        "lux:placeDiedAgent": queries["agentsDiedAtPlace"],
        "lux:placeEvents": queries["eventsHappenedAtPlace"],
        "lux:placeInfluencedConcepts": queries["conceptsInfluencedByPlace"],
        "lux:placeItemTypes": queries["itemsProducedAtPlace"],
        "lux:placeMadeDiscoveredItem": queries["itemsProducedEncounteredAtPlace"],
        "lux:placeParts": queries["partsOfPlace"],
        "lux:placePublishedWork": queries["worksPublishedAtPlace"],
        # "lux:placeRelatedAgents": queries["agentsRelatedToPlace"],
        # "lux:placeRelatedConcepts": queries["conceptsRelatedToPlace"],
        # "lux:placeRelatedPlaces": queries["placesRelatedToPlace"],
        "lux:placeWorkAbout": queries["worksAboutPlace"],
        "lux:placeWorkTypes": queries["worksRelatedToPlace"],
    },
    "set": {
        "lux:setDepartment": queries["departmentsCuratedSet"],
        "lux:setEvents": queries["eventsWithSet"],
        "lux:setIncludedItems": queries["itemsInSet"],
        # "lux:setIncludedWorks": queries["worksInSet"],
        "lux:setItemEncounteredTime": queries["itemsInSet"],
        "lux:setItemMadeTime": queries["itemsInSet"],
        "lux:setItemTypes": queries["itemsInSet"],
        "lux:setUnit": queries["workById"],
        "lux:setItemsWithImages": queries["itemsInSetWithImages"],
        "lux:workCarriedBy": queries["itemsCarryingWork"],
        "lux:workWorksAbout": queries["worksAboutWork"],
    },
    "work": {
        "lux:workContainedWorks": queries["worksContainingWork"],
        "lux:workIncludedWorks": queries["worksInWork"],
        "lux:workCarriedBy": queries["itemsCarryingWork"],
        "lux:workWorksAbout": queries["worksAboutWork"],
    },
}

sparql_hal_queries = {}
sparql_hal_singles = {}

for scope in hal_queries:
    sparql_hal_queries[scope] = {}
    for hal, query in hal_queries[scope].items():
        try:
            qscope = query["_scope"]
        except KeyError:
            # Already been processed
            continue
        del query["_scope"]
        try:
            parsed = rdr.read(query, qscope)
        except Exception as e:
            print(f"Error parsing query for {hal}: {e}\n{query}")
            continue
        spq = st.translate_search(parsed, qscope)
        sparql_hal_queries[scope][hal] = spq
        ts = spq.where.graph
        if len(ts) == 2 and ts[0].predicate == "a":
            try:
                sparql_hal_singles[ts[1].predicate].append((hal, scope))
            except KeyError:
                sparql_hal_singles[ts[1].predicate] = [(hal, scope)]


sorts = {
    "item": {
        "anySortName": "lux:itemSortName",
        "itemProductionDate": "lux:startOfItemBeginning",
        "itemEncounteredDate": "lux:startOfItemEncounter",
        "itemDimensionValue": "lux:dimension",
        "itemDepthDimensionValue": "lux:depth",
        "itemHeightDimensionValue": "lux:height",
        "itemWidthDimensionValue": "lux:width",
        "itemClassificationConceptName": "lux:itemClassification/lux:conceptSortName",
        "itemEncounterPlaceName": "lux:placeOfItemEncounter/lux:placeSortName",
        "itemEncounterAgentName": "lux:agentOfItemEncounter/lux:agentSortName",
        "itemProductionInfluencedByAgentName": "lux:agentInfluenceOfItem/lux:agentSortName",
        "itemMaterialConceptName": "lux:material/lux:conceptSortName",
        "itemProductionPlaceName": "lux:placeOfItemProduction/lux:placeSortName",
        "itemProductionAgentName": "lux:agentOfItemProduction/lux:agentSortName",
        "itemTechniqueConceptName": "lux:typeOfItemProduction/lux:conceptSortName",
        "itemHasDigitalImage": "lux:itemHasDigitalImage",
        "itemRecordType": "lux:recordType",
    },
    "work": {
        "anySortName": "lux:workSortName",
        "workCreationDate": "lux:startOfWorkBeginning",
        "workPublicationDate": "lux:startOfItemBeginning",
        "workClassificationConceptName": "lux:workClassification/lux:conceptSortName",
        "workCreationAgentName": "lux:agentOfWorkBeginning/lux:agentSortName",
        "workPublicationPlaceName": "lux:placeOfWorkPublication/lux:placeSortName",
        "workPublicationAgentName": "lux:agentOfWorkPublication/lux:agentSortName",
        "workHasDigitalImage": "lux:workHasDigitalImage",
        "workRecordType": "lux:recordType",
    },
    "set": {
        "anySortName": "lux:setSortName",
        "setClassificationConceptName": "lux:setClassification/lux:conceptSortName",
        "setCreationAgentName": "lux:agentOfSetBeginning/lux:agentSortName",
        "setCreationPlaceName": "lux:placeOfSetBeginning/lux:placeSortName",
        "setCreationDate": "lux:startOfSetBeginning",
        "setCurationAgentName": "lux:setCuratedBy/lux:agentSortName",
        "setHasDigitalImage": "lux:setHasDigitalImage",
        "setPublicationAgentName": "lux:agentOfSetPublication/lux:agentSortName",
        "setPublicationPlaceName": "lux:placeOfSetPublication/lux:placeSortName",
        "setPublicationDate": "lux:startOfSetPublication",
    },
    "agent": {
        "anySortName": "lux:agentSortName",
        "agentEndDate": "lux:startOfAgentEnding",
        "agentStartDate": "lux:startOfAgentBeginning",
        "agentHasDigitalImage": "lux:agentHasDigitalImage",
        "agentRecordType": "lux:recordType",
        "agentStartPlaceName": "lux:placeOfAgentBeginning/lux:placeSortName",
        "agentClassificationConceptName": "lux:agentClassification/lux:conceptSortName",
        "agentEndPlaceName": "lux:placeOfAgentEnding/lux:placeSortName",
        "agentGenderConceptName": "lux:gender/lux:conceptSortName",
        "agentNationalityConceptName": "lux:nationality/lux:conceptSortName",
        "agentOccupationConceptName": "lux:occupation/lux:conceptSortName",
    },
    "place": {
        "anySortName": "lux:placeSortName",
        "placeHasDigitalImage": "lux:placeHasDigitalImage",
        "placeClassificationConceptName": "lux:placeClassification/lux:conceptSortName",
    },
    "concept": {
        "anySortName": "lux:conceptSortName",
        "conceptRecordType": "lux:recordType",
        "conceptClassificationConceptName": "lux:conceptClassification/lux:conceptSortName",
    },
    "event": {
        "anySortName": "lux:eventSortName",
        "eventStartDate": "lux:startOfEvent",
        "eventEndDate": "lux:endOfEvent",
        "eventRecordType": "lux:recordType",
        "eventCarriedOutByAgentName": "lux:agentOfEvent/lux:agentSortName",
        "eventClassificationConceptName": "lux:eventClassification/lux:conceptSortName",
        "eventTookPlaceAtPlaceName": "lux:placeOfEvent/lux:placeSortName",
    },
}


async def fetch_sparql(spq):
    if type(spq) is str:
        q = spq
    else:
        q = spq.get_text()
    try:
        async with aiohttp.ClientSession() as session:
            # Assuming your SPARQL endpoint accepts POST requests
            async with session.post(
                "http://localhost:7010/sparql",
                data={"query": q},
                headers={"Accept": "application/sparql-results+json"}
            ) as response:
                ret = await response.json()
                results = [r for r in ret["results"]["bindings"]]
    except Exception as e:
        print(q)
        print(e)
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
        except:
            ascdesc = "ASC"
    else:
        sort = "relevance"
        ascdesc = "DESC"
    pred = sorts[scope].get(sort, "relevance")

    # print(f"SORT: {pred}, {ascdesc}")

    jq = json.loads(q)
    parsed = rdr.read(jq, scope)
    spq = st.translate_search(parsed, limit=pageLength, offset=offset, sort=pred, order=ascdesc)
    qt = spq.get_text()
    res = await fetch_sparql(qt)

    spq2 = st.translate_search_count(parsed)
    qt2 = spq2.get_text()
    ttl_res = await fetch_sparql(qt2)
    ttl = ttl_res[0]["count"]["value"]

    js = {
        "@context": "https://linked.art/ns/v1/search.json",
        "id": f"https://localhost:5001/api/search/{scope}?q={q}&page=1",
        "type": "OrderedCollectionPage",
        "partOf": {
            "id": "",
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
    jq = json.loads(q)
    js = {
        "@context": "https://linked.art/ns/v1/search.json",
        "id": f"http://localhost:5001/api/search/{scope}?q={q}",
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
    print("FACET REQUEST")
    await asyncio.sleep(5)  # Non-blocking sleep
    jq = json.loads(q)
    parsed = rdr.read(jq, scope)

    js = {
        "@context": "https://linked.art/ns/v1/search.json",
        "id": f"http://localhost:5001/api/facets/{scope}?q={q}&name={name}&page={page}",
        "type": "OrderedCollectionPage",
        "partOf": {"type": "OrderedCollection", "totalItems": 1000},
        "orderedItems": [],
    }

    if name.endswith("RecordType"):
        # special handling needed
        pred = "a"
    elif name.endswith("IsOnline"):
        return JSONResponse(js)
    elif name == "responsibleCollections":
        pred = "lux:itemMemberOfSet/lux:setCuratedBy"
    elif name == "responsibleUnits":
        pred = "lux:itemMemberOfSet/lux:setCuratedBy/lux:groupMemberOfGroup"
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
    spq = st.translate_facet(parsed, pred)
    res = await fetch_sparql(spq)
    print(f"FACET: {name} / {pred}: {len(res)}")

    # fq = LuxBoolean("AND")
    # fq.provides_scope = scope
    # fq.add(parsed)
    # print(res)
    for r in res:
        # lr = LuxRelationship(pname2, parent=fq)
        # LuxLeaf(
        #    "id",
        #    value=r["facet"]["value"].replace(
        #        "http://localhost:5001/data/", "https://lux.collections.yale.edu/data/"
        #    ),
        #    parent=lr,
        # )
        if r["facet"]["type"] == "uri":
            val = (
                r["facet"]["value"]
                .replace("https://lux.collections.yale.edu/data/", "http://localhost:5001/data/")
                .replace("https://lux.collections.yale.edu/ns/", "")
                .replace("https://linked.art/ns/terms/", "")
            )
        elif r["facet"]["datatype"].endswith("int") or r["facet"]["datatype"].endswith("decimal"):
            val = int(r["facet"]["value"])
        elif r["facet"]["datatype"].endswith("float"):
            val = float(r["facet"]["value"])
        elif r["facet"]["datatype"].endswith("dateTime"):
            val = r["facet"]["value"]
        else:
            raise ValueError(r)

        js["orderedItems"].append(
            {
                "id": f"http://localhost:5001/api/search-estimate/{scope}?q=",
                "type": "OrderedCollection",
                "value": val,
                "totalItems": int(r["facetCount"]["value"]),
            }
        )
        # fq.children.pop()

    return JSONResponse(content=js)


@app.get("/api/related-list/{scope}")
async def do_related_list(scope, name, uri):
    """?name=relatedToAgent&uri=(uri-of-record)"""
    js = {
        "@context": "https://linked.art/ns/v1/search.json",
        "id": "",
        "type": "OrderedCollectionPage",
        "orderedItems": [],
    }
    entry = {
        "id": "",
        "type": "OrderedCollection",
        "totalItems": 0,
        "first": {
            "id": "",
            "type": "OrderedCollectionPage",
        },
        "value": "",
        "name": "",
    }
    # scope is the type of records to find
    # name gives related list type (relatedToAgent)
    # uri is the anchoring entity

    qry = f"""PREFIX lux: <https://lux.collections.yale.edu/ns/>
SELECT DISTINCT ?what ?prep ?prep2 (COUNT(?objwk) AS ?ct) WHERE {{
    ?what a lux:{scope[0].upper()}{scope[1:]} .
    {{
      ?objwk a lux:Item ;
            ?prep ?what ;
            ?prep2 <{uri}> .
      FILTER (?prep2 != lux:itemAny)
      FILTER (?prep != lux:itemAny)
    }}
    UNION {{
      ?objwk a lux:Work ;
            ?prep ?what ;
      	    ?prep2 <{uri}> .
      FILTER (?prep != lux:workAny)
	  FILTER (?prep2 != lux:workAny)
      FILTER (?prep2 != lux:placeOfWorkPublication)
   	  FILTER (?prep != lux:placeOfWorkPublication)
    }}
}} GROUP BY ?what ?prep ?prep2 ORDER BY ?what
    """
    res = await fetch_sparql(qry)
    cts = {}

    for row in res:
        what = row["what"]["value"]
        prep = row["prep"]["value"].rsplit("/", 1)[-1]
        prep2 = row["prep2"]["value"].rsplit("/", 1)[-1]
        ct = int(row["ct"]["value"])
        try:
            cts[what] += ct
        except KeyError:
            cts[what] = ct
        e = copy.deepcopy(entry)
        e["value"] = what
        e["totalItems"] = ct
        e["name"] = f"{prep} -> {prep2}"
        js["orderedItems"].append(e)

    print(cts)
    js["orderedItems"].sort(key=lambda x: cts[x["value"]], reverse=True)

    return JSONResponse(content=js)


@app.get("/api/translate/{scope}")
async def do_translate(scope, q={}):
    # take simple search in text and return json query equivalent
    js = {"_scope": scope, "AND": [{"text": q}]}
    return JSONResponse(content=js)


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
        if profile is None:
            # Calculate _links here
            sqry = f"SELECT DISTINCT ?pred WHERE {{ ?what ?pred <https://lux.collections.yale.edu/data/{scope}/{identifier}> . }}"
            res = await fetch_sparql(sqry)
            for r in res:
                pred = r["pred"]["value"].rsplit("/", 1)[-1]
                print(f"profile: {profile} / {pred}")
                # hal, halQuery = make_single_hal(pred, scope, identifier)
                # links[hal] = halQuery

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
