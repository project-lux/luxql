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
import aiohttp
import urllib

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
        "lux:agentRelatedAgents": "lux:agentRelatedAgents",
        "lux:agentRelatedConcepts": "lux:agentRelatedConcepts",
        "lux:agentRelatedPlaces": "lux:agentRelatedPlaces",
        "lux:agentAgentMemberOf": queries["agentsMemberOfGroup"],
        "lux:agentCreatedPublishedInfluencedWork": queries["worksCreatedPublishedInfluencedByAgent"],
        "lux:agentEventsCarriedOut": queries["eventsCarriedOutByAgent"],
        "lux:agentEventsUsingProducedObjects": queries["eventsUsingAgentsProducedObjects"],
        "lux:agentFoundedByAgent": queries["agentsFoundedByAgent"],
        "lux:agentInfluencedConcepts": queries["conceptsInfluencedByAgent"],
        "lux:agentItemEncounteredTime": queries["itemsEncounteredByAgent"],
        "lux:agentItemMadeTime": queries["itemsProducedByAgent"],
        "lux:agentMadeDiscoveredInfluencedItem": queries["itemsProducedEncounteredInfluencedByAgent"],
        "lux:agentRelatedItemTypes": queries["itemsProducedByAgent"],
        "lux:agentRelatedMaterials": queries["itemsProducedByAgent"],
        "lux:agentRelatedSubjects": queries["worksCreatedByAgent"],
        "lux:agentRelatedWorkTypes": queries["worksCreatedByAgent"],
        "lux:agentWorkAbout": queries["worksAboutAgent"],
        "lux:agentWorkCreatedTime": queries["worksCreatedPublishedInfluencedByAgent"],
        "lux:agentWorkPublishedTime": queries["worksCreatedPublishedInfluencedByAgent"],
        "lux:departmentItems": queries["itemsForDepartment"],
    },
    "concept": {
        "lux:conceptRelatedAgents": "lux:conceptRelatedAgents",
        "lux:conceptRelatedConcepts": "lux:conceptRelatedConcepts",
        "lux:conceptRelatedPlaces": "lux:conceptRelatedPlaces",
        "lux:conceptChildren": queries["childrenOfConcept"],
        "lux:conceptInfluencedConcepts": queries["conceptsInfluencedByConcept"],
        "lux:conceptItemEncounteredTime": queries["itemsOfTypeOrMaterial"],
        "lux:conceptItemMadeTime": queries["itemsOfTypeOrMaterial"],
        "lux:conceptItemTypes": queries["itemsOfTypeOrMaterial"],
        "lux:conceptRelatedItems": queries["itemsOfTypeOrMaterial"],
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
        "lux:eventRelatedAgents": "lux:eventRelatedAgents",
        "lux:eventRelatedConcepts": "lux:eventRelatedConcepts",
        "lux:eventRelatedPlaces": "lux:eventRelatedPlaces",
        "lux:eventConceptsInfluencedBy": queries["conceptsSubjectsForPeriod"],
        "lux:eventIncludedItems": queries["itemsForEvent"],
        "lux:eventItemMaterials": queries["itemsForEvent"],
        "lux:eventObjectTypesUsed": queries["itemsForEvent"],
        "lux:eventObjectTypesAbout": queries["itemsAboutEvent"],
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
        "lux:placeRelatedAgents": "lux:placeRelatedAgents",
        "lux:placeRelatedConcepts": "lux:placeRelatedConcepts",
        "lux:placeRelatedPlaces": "lux:placeRelatedPlaces",
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

searchUriHost = "http://localhost:5001"
hal_link_templates = {
    "lux:agentAgentMemberOf": f"{searchUriHost}/api/search/agent?q=<<$q>>",
    "lux:agentCreatedPublishedInfluencedWork": f"{searchUriHost}/api/search/work?q=<<$q>>",
    "lux:agentEventsCarriedOut": f"{searchUriHost}/api/search/event?q=<<$q>>&sort=eventStartDate:asc",
    "lux:agentEventsUsingProducedObjects": f"{searchUriHost}/api/search/event?q=<<$q>>&sort=eventStartDate:asc",
    "lux:agentFoundedByAgent": f"{searchUriHost}/api/search/agent?q=<<$q>>&sort=anySortName:asc",
    "lux:agentInfluencedConcepts": f"{searchUriHost}/api/search/concept?q=<<$q>>",
    "lux:agentItemEncounteredTime": f"{searchUriHost}/api/facets/item?q=<<$q>>&name=itemEncounteredDate",
    "lux:agentItemMadeTime": f"{searchUriHost}/api/facets/item?q=<<$q>>&name=itemProductionDate",
    "lux:agentMadeDiscoveredInfluencedItem": f"{searchUriHost}/api/search/item?q=<<$q>>",
    "lux:agentRelatedAgents": f"{searchUriHost}/api/related-list/agent?&name=relatedToAgent&uri=<<$id>>",
    "lux:agentRelatedConcepts": f"{searchUriHost}/api/related-list/concept?&name=relatedToAgent&uri=<<$id>>",
    "lux:agentRelatedItemTypes": f"{searchUriHost}/api/facets/item?q=<<$q>>&name=itemTypeId",
    "lux:agentRelatedMaterials": f"{searchUriHost}/api/facets/item?q=<<$q>>&name=itemMaterialId",
    "lux:agentRelatedPlaces": f"{searchUriHost}/api/related-list/place?&name=relatedToAgent&uri=<<$id>>",
    "lux:agentRelatedSubjects": f"{searchUriHost}/api/facets/work?q=<<$q>>&name=workAboutConceptId",
    "lux:agentRelatedWorkTypes": f"{searchUriHost}/api/facets/work?q=<<$q>>&name=workTypeId",
    "lux:agentWorkAbout": f"{searchUriHost}/api/search/work?q=<<$q>>",
    "lux:agentWorkCreatedTime": f"{searchUriHost}/api/facets/work?q=<<$q>>&name=workCreationDate",
    "lux:agentWorkPublishedTime": f"{searchUriHost}/api/facets/work?q=<<$q>>&name=workPublicationDate",
    "lux:conceptChildren": f"{searchUriHost}/api/search/concept?q=<<$q>>",
    "lux:conceptInfluencedConcepts": f"{searchUriHost}/api/search/concept?q=<<$q>>",
    "lux:conceptItemEncounteredTime": f"{searchUriHost}/api/facets/item?q=<<$q>>&name=itemEncounteredDate",
    "lux:conceptItemMadeTime": f"{searchUriHost}/api/facets/item?q=<<$q>>&name=itemProductionDate",
    "lux:conceptItemTypes": f"{searchUriHost}/api/facets/item?q=<<$q>>&name=itemTypeId",
    "lux:conceptRelatedAgents": f"{searchUriHost}/api/related-list/agent?&name=relatedToConcept&uri=<<$id>>",
    "lux:conceptRelatedConcepts": f"{searchUriHost}/api/related-list/concept?&name=relatedToConcept&uri=<<$id>>",
    "lux:conceptRelatedItems": f"{searchUriHost}/api/search/item?q=<<$q>>",
    "lux:conceptRelatedPlaces": f"{searchUriHost}/api/related-list/place?name=relatedToConcept&uri=<<$id>>",
    "lux:conceptRelatedWorks": f"{searchUriHost}/api/search/work?q=<<$q>>",
    "lux:conceptWorkCreatedTime": f"{searchUriHost}/api/facets/work?q=<<$q>>&name=workCreationDate",
    "lux:conceptWorkPublishedTime": f"{searchUriHost}/api/facets/work?q=<<$q>>&name=workPublicationDate",
    "lux:conceptWorkTypes": f"{searchUriHost}/api/facets/work?q=<<$q>>&name=workTypeId",
    "lux:departmentItems": f"{searchUriHost}/api/search/item?q=<<$q>>",
    "lux:eventCausedWorks": f"{searchUriHost}/api/search/work?q=<<$q>>",
    "lux:eventConceptsInfluencedBy": f"{searchUriHost}/api/search/concept?q=<<$q>>",
    "lux:eventIncludedItems": f"{searchUriHost}/api/search/item?q=<<$q>>",
    "lux:eventItemMaterials": f"{searchUriHost}/api/facets/item?q=<<$q>>&name=itemMaterialId",
    "lux:eventObjectTypesUsed": f"{searchUriHost}/api/facets/item?q=<<$q>>&name=itemTypeId",
    "lux:eventObjectTypesAbout": f"{searchUriHost}/api/facets/item?q=<<$q>>&name=itemTypeId",
    "lux:eventRelatedAgents": f"{searchUriHost}/api/related-list/agent?name=relatedToEvent&uri=<<$id>>",
    "lux:eventRelatedConcepts": f"{searchUriHost}/api/related-list/concept?name=relatedToEvent&uri=<<$id>>",
    "lux:eventRelatedPlaces": f"{searchUriHost}/api/related-list/place?name=relatedToEvent&uri=<<$id>>",
    "lux:eventWorksAbout": f"{searchUriHost}/api/search/work?q=<<$q>>",
    "lux:eventWorkTypesUsed": f"{searchUriHost}/api/facets/work?q=<<$q>>&name=workTypeId",
    "lux:eventWorkTypesAbout": f"{searchUriHost}/api/facets/work?q=<<$q>>&name=workTypeId",
    "lux:genderForAgent": f"{searchUriHost}/api/search/agent?q=<<$q>>",
    "lux:itemArchive": f"{searchUriHost}/api/search/set?q=<<$q>>",
    "lux:itemEvents": f"{searchUriHost}/api/search/event?q=<<$q>>&sort=eventStartDate:asc",
    "lux:itemDepartment": f"{searchUriHost}/api/search/agent?q=<<$q>>",
    "lux:itemUnit": f"{searchUriHost}/api/facets/item?q=<<$q>>&name=responsibleUnits",
    "lux:itemWorksAbout": f"{searchUriHost}/api/search/work?q=<<$q>>",
    "lux:nationalityForAgent": f"{searchUriHost}/api/search/agent?q=<<$q>>",
    "lux:occupationForAgent": f"{searchUriHost}/api/search/agent?q=<<$q>>",
    "lux:placeActiveAgent": f"{searchUriHost}/api/search/agent?q=<<$q>>",
    "lux:placeBornAgent": f"{searchUriHost}/api/search/agent?q=<<$q>>",
    "lux:placeCreatedWork": f"{searchUriHost}/api/search/work?q=<<$q>>",
    "lux:placeDepictedAgentsFromRelatedWorks": f"{searchUriHost}/api/facets/work?q=<<$q>>&name=workAboutAgentId",
    "lux:placeDepictingWork": f"{searchUriHost}/api/facets/work?q=<<$q>>&name=workAboutConceptId",
    "lux:placeDiedAgent": f"{searchUriHost}/api/search/agent?q=<<$q>>",
    "lux:placeEvents": f"{searchUriHost}/api/search/event?q=<<$q>>",
    "lux:placeInfluencedConcepts": f"{searchUriHost}/api/search/concept?q=<<$q>>",
    "lux:placeItemTypes": f"{searchUriHost}/api/facets/item?q=<<$q>>&name=itemTypeId",
    "lux:placeMadeDiscoveredItem": f"{searchUriHost}/api/search/item?q=<<$q>>",
    "lux:placeParts": f"{searchUriHost}/api/search/place?q=<<$q>>",
    "lux:placePublishedWork": f"{searchUriHost}/api/search/work?q=<<$q>>",
    "lux:placeRelatedAgents": f"{searchUriHost}/api/related-list/agent?name=relatedToPlace&uri=<<$id>>",
    "lux:placeRelatedConcepts": f"{searchUriHost}/api/related-list/concept?name=relatedToPlace&uri=<<$id>>",
    "lux:placeRelatedPlaces": f"{searchUriHost}/api/related-list/place?name=relatedToPlace&uri=<<$id>>",
    "lux:placeWorkAbout": f"{searchUriHost}/api/search/work?q=<<$q>>",
    "lux:placeWorkTypes": f"{searchUriHost}/api/facets/work?q=<<$q>>&name=workTypeId",
    "lux:setDepartment": f"{searchUriHost}/api/search/agent?q=<<$q>>",
    "lux:setEvents": f"{searchUriHost}/api/search/event?q=<<$q>>",
    "lux:setIncludedItems": f"{searchUriHost}/api/search/item?q=<<$q>>&sort=itemArchiveSortId:asc",
    "lux:setIncludedWorks": f"{searchUriHost}/api/search/work?q=<<$q>>&sort=workArchiveSortId:asc",
    "lux:setItemEncounteredTime": f"{searchUriHost}/api/facets/item?q=<<$q>>&name=itemEncounteredDate",
    "lux:setItemMadeTime": f"{searchUriHost}/api/facets/item?q=<<$q>>&name=itemProductionDate",
    "lux:setItemTypes": f"{searchUriHost}/api/facets/item?q=<<$q>>&name=itemTypeId",
    "lux:setUnit": f"{searchUriHost}/api/facets/work?q=<<$q>>&name=responsibleUnits",
    "lux:setItemsWithImages": f"{searchUriHost}/api/search/item?q=<<$q>>",
    "lux:typeForAgent": f"{searchUriHost}/api/search/agent?q=<<$q>>",
    "lux:typeForEvent": f"{searchUriHost}/api/search/event?q=<<$q>>",
    "lux:typeForPlace": f"{searchUriHost}/api/search/place?q=<<$q>>",
    "lux:workCarriedBy": f"{searchUriHost}/api/search/item?q=<<$q>>",
    "lux:workContainedWorks": f"{searchUriHost}/api/search/work?q=<<$q>>",
    "lux:workIncludedWorks": f"{searchUriHost}/api/search/work?q=<<$q>>",
    "lux:workWorksAbout": f"{searchUriHost}/api/search/work?q=<<$q>>",
}

# TODO: Test if a single query for ?uri ?pred <uri> and then looking for which hal
# links match the returned predicates would be faster or not

sparql_hal_queries = {}
for scope in hal_queries:
    sparql_hal_queries[scope] = {}
    for hal, query in hal_queries[scope].items():
        if type(query) is str:
            sparql_hal_queries[scope][hal] = ""
            continue
        try:
            qscope = query["_scope"]
        except KeyError:
            # Already been processed
            continue
        try:
            parsed = rdr.read(query, qscope)
        except Exception as e:
            print(f"Error parsing query for {hal}: {e}\n{query}")
            continue
        spq = st.translate_search_count(parsed, qscope)
        sparql_hal_queries[scope][hal] = spq

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


related_list_names = {
    "classificationOfItem-classification": "Is the Category of Objects Categorized As",
    "classificationOfItem-encounteredAt": "Is the Category of Objects Encountered At",
    "classificationOfItem-encounteredBy": "Is the Category of Objects Encountered By",
    "classificationOfItem-material": "Is the Category of Objects Made Of",
    "classificationOfItem-memberOf-usedForEvent": "Is the Category of Objects Used At",
    "classificationOfItem-producedAt": "Is the Category of Objects Created At",
    "classificationOfItem-producedBy": "Is the Category of Objects Created By",
    "classificationOfItem-producedUsing": "Is the Category of Objects Created Using",
    "classificationOfWork-aboutAgent": "Is the Category of Works About",
    "classificationOfWork-aboutConcept": "Is the Category of Works About",
    "classificationOfWork-aboutPlace": "Is the Category of Works About",
    "classificationOfWork-carriedBy-memberOf-usedForEvent": "Is the Category of Works Carried By Objects Used At",
    "classificationOfWork-classification": "Is the Category of Works Categorized As",
    "classificationOfWork-createdAt": "Is the Category of Works Created At",
    "classificationOfWork-createdBy": "Is the Category of Works Created By",
    "classificationOfWork-language": "Is the Category of Works In",
    "classificationOfWork-publishedBy": "Is the Category of Works Published By",
    "classificationOfSet-aboutAgent": "Is the Category of Collections About",
    "classificationOfSet-aboutConcept": "Is the Category of Collections About",
    "classificationOfSet-aboutPlace": "Is the Category of Collections About",
    "classificationOfSet-classification": "Is the Category of Collections Categorized As",
    "classificationOfSet-createdAt": "Is the Category of Collections Created At",
    "classificationOfSet-createdBy": "Is the Category of Collections Created By",
    "classificationOfSet-publishedBy": "Is the Category of Collections Published By",
    "created-aboutAgent": "Created Works About",
    "created-aboutItem-memberOf-usedForEvent": "Created Works About Objects Used At",
    "created-aboutConcept": "Created Works About",
    "created-aboutPlace": "Created Works About",
    "createdSet-aboutAgent": "Created Collections About",
    "createdSet-aboutConcept": "Created Collections About",
    "createdSet-aboutPlace": "Created Collections About",
    "created-carriedBy-memberOf-usedForEvent": "Created Works Carried by Objects Used At",
    "created-classification": "Created Works Categorized As",
    "created-createdAt": "Created Works Created At",
    "created-createdBy": "Co-created Works With",
    "created-creationInfluencedBy": "Created Works Influenced By",
    "created-language": "Created Works In",
    "created-publishedBy": "Created Works Published By",
    "createdSet-classification": "Created Collections Categorized As",
    "createdSet-createdAt": "Created Collections Created At",
    "createdSet-createdBy": "Co-created Collections With",
    "createdSet-publishedBy": "Created Collections Published By",
    "createdHere-aboutAgent": "Is the Place of Creation of Works About",
    "createdHere-aboutConcept": "Is the Place of Creation of Works About",
    "createdHere-aboutPlace": "Is the Place of Creation of Works About",
    "createdHere-carriedBy-memberOf-usedForEvent": "Is the Place of Creation of Works Carried By Items Used At",
    "createdHere-classification": "Is the Place of Creation of Works Categorized As",
    "createdHere-createdAt": "Is the Place of Creation of Works Created At",
    "createdHere-createdBy": "Is the Place of Creation of Works Created By",
    "createdHere-language": "Is the Place of Creation of Works In",
    "createdHere-publishedBy": "Is the Place of Creation of Works Published By",
    "encountered-classification": "Encountered Objects Categorized As",
    "encountered-encounteredAt": "Encountered Objects Encountered At",
    "encountered-encounteredBy": "Co-encountered Objects With",
    "encountered-material": "Encountered Objects Made Of",
    "encountered-producedAt": "Encountered Objects Created At",
    "encountered-producedBy": "Encountered Objects Created By",
    "encountered-producedUsing": "Encountered Objects Created Using",
    "encountered-productionInfluencedBy": "Encountered Objects Influenced By",
    "encounteredHere-classification": "Is the Place of Encounter of Objects Categorized As",
    "encounteredHere-encounteredAt": "Is the Place of Encounter of Objects Encountered At",
    "encounteredHere-encounteredBy": "Is the Place of Encounter of Objects Encountered By",
    "encounteredHere-material": "Is the Place of Encounter of Objects Made Of",
    "encounteredHere-memberOf-usedForEvent": "Is the Place of Encounter of Objects Used At",
    "encounteredHere-producedAt": "Is the Place of Encounter of Objects Created At",
    "encounteredHere-producedBy": "Is the Place of Encounter of Objects Created By",
    "encounteredHere-producedUsing": "Is the Place of Encounter of Objects Created Using",
    "encountered-memberOf-usedForEvent": "Encountered Objects Used At",
    "influencedCreation-aboutAgent": "Influenced Creation of Works About",
    "influencedCreation-aboutConcept": "Influenced Creation of Works About",
    "influencedCreation-aboutEvent": "Influenced Creation of Works About",
    "influencedCreation-aboutItem-carries-aboutEvent": "Influenced Creation of Works About",
    "influencedCreation-aboutItem-carries-creationCausedBy": "Influenced Creation of Works About",
    "influencedCreation-createdBy": "Influenced Creation of Works Created By",
    "influencedCreation-creationInfluencedBy": "Influenced Creation of Works Influenced By",
    "influencedCreation-classification": "Influenced Creation of Works Categorized As",
    "influencedCreation-language": "Influenced Creation of Works In",
    "influencedCreation-publishedBy": "Influenced Creation of Works Published By",
    "influencedProduction-classification": "Influenced Creation of Objects Categorized As",
    "influencedProduction-encounteredBy": "Influenced Creation of Objects Encountered By",
    "influencedProduction-material": "Influenced Creation of Objects Made Of",
    "influencedProduction-producedBy": "Influenced Creation of Objects Created By",
    "influencedProduction-producedUsing": "Influenced Creation of Objects Created Using",
    "influencedProduction-productionInfluencedBy": "Influenced Creation of Objects Influenced By",
    "languageOf-aboutAgent": "Is the Language of Works About",
    "languageOf-aboutConcept": "Is the Language of Works About",
    "languageOf-aboutPlace": "Is the Language of Works About",
    "languageOf-carriedBy-memberOf-usedForEvent": "Is the Language of Works Carried by Objects Used At",
    "languageOf-classification": "Is the Language of Works Categorized As",
    "languageOf-createdAt": "Is the Language of Works Created At",
    "languageOf-createdBy": "Is the Language of Works Created By",
    "languageOf-language": "Is the Language of Works In",
    "languageOf-publishedBy": "Is the Language of Works Published By",
    "materialOfItem-classification": "Is the Material of Objects Categorized As",
    "materialOfItem-encounteredAt": "Is the Material of Objects Encountered At",
    "materialOfItem-encounteredBy": "Is the Material of Objects Encountered By",
    "materialOfItem-material": "Is the Material of Objects Made Of",
    "materialOfItem-memberOf-usedForEvent": "Is the Material of Objects Used At",
    "materialOfItem-producedAt": "Is the Material of Objects Created At",
    "materialOfItem-producedBy": "Is the Material of Objects Created By",
    "materialOfItem-producedUsing": "Is the Material of Objects Created Using",
    "produced-classification": "Created Objects Categorized As",
    "produced-encounteredAt": "Created Objects Encountered At",
    "produced-encounteredBy": "Created Objects Encountered By",
    "produced-material": "Created Objects Made Of",
    "produced-memberOf-usedForEvent": "Produced Objects Used At",
    "produced-producedAt": "Created Objects Created At",
    "produced-producedBy": "Co-created Objects With",
    "produced-producedUsing": "Created Objects Using",
    "produced-productionInfluencedBy": "Created Objects Influenced By",
    "producedHere-classification": "Is the Place of Creation of Objects Categorized As",
    "producedHere-encounteredAt": "Is the Place of Creation of Objects Encountered At",
    "producedHere-encounteredBy": "Is the Place of Creation of Objects Encountered By",
    "producedHere-material": "Is the Place of Creation of Objects Made Of",
    "producedHere-memberOf-usedForEvent": "Is the Place of Creation of Objects Used At",
    "producedHere-producedAt": "Is the Place of Creation of Objects Created At",
    "producedHere-producedBy": "Is the Place of Creation of Objects Created By",
    "producedHere-producedUsing": "Is the Place of Creation of Objects Created Using",
    "published-aboutAgent": "Published Works About",
    "published-aboutConcept": "Published Works About",
    "published-aboutPlace": "Published Works About",
    "published-carriedBy-memberOf-usedForEvent": "Published Works Carried by Objects Used At",
    "published-classification": "Published Works Categorized As",
    "published-createdAt": "Published Works Created At",
    "published-createdBy": "Published Works Created By",
    "published-creationInfluencedBy": "Published Works Influenced By",
    "published-language": "Published Works In",
    "published-publishedBy": "Published Works With",
    # "setCreatedHere-aboutAgent": "Is the Place of Creation of Collections About",
    # "setCreatedHere-aboutConcept": "Is the Place of Creation of Collections About",
    # "setCreatedHere-aboutPlace": "Is the Place of Creation of Collections About",
    # "setCreatedHere-carriedBy-memberOf-usedForEvent": "Is the Place of Creation of Collections Carried By Items Used At",
    # "setCreatedHere-classification": "Is the Place of Creation of Collections Categorized As",
    # "setCreatedHere-createdAt": "Is the Place of Creation of Collections Created At",
    # "setCreatedHere-createdBy": "Is the Place of Creation of Collections Created By",
    # "setCreatedHere-publishedBy": "Is the Place of Creation of Collections Published By",
    "subjectOfWork-aboutAgent": "Is the Subject of Works About",
    "subjectOfWork-aboutConcept": "Is the Subject of Works About",
    "subjectOfWork-aboutPlace": "Is the Subject of Works About",
    "subjectOfWork-carriedBy-memberOf-usedForEvent": "Is the Subject of Works Carried by Objects Used At",
    "subjectOfWork-classification": "Is the Subject of Works Categorized As",
    "subjectOfWork-createdAt": "Is the Subject of Works Created At",
    "subjectOfWork-createdBy": "Is the Subject of Works Created By",
    "subjectOfWork-creationInfluencedBy": "Is the Subject of Works Influenced By",
    "subjectOfWork-language": "Is the Subject of Works In",
    "subjectOfWork-publishedBy": "Is the Subject of Works Published By",
    "subjectOfSet-aboutAgent": "Is the Subject of Collections About",
    "subjectOfSet-aboutConcept": "Is the Subject of Collections About",
    "subjectOfSet-aboutPlace": "Is the Subject of Collections About",
    "subjectOfSet-classification": "Is the Subject of Collections Categorized As",
    "subjectOfSet-createdAt": "Is the Subject of Collections Created At",
    "subjectOfSet-createdBy": "Is the Subject of Collections Created By",
    "subjectOfSet-publishedBy": "Is the Subject of Collections Published By",
    "usedToProduce-classification": "Is the Technique of Objects Categorized As",
    "usedToProduce-encounteredAt": "Is the Technique of Objects Encountered At",
    "usedToProduce-encounteredBy": "Is the Technique of Objects Encountered By",
    "usedToProduce-material": "Is the Technique of Objects Made Of",
    "usedToProduce-memberOf-usedForEvent": "Is the Technique of Objects Used At",
    "usedToProduce-producedAt": "Is the Technique of Objects Created At",
    "usedToProduce-producedBy": "Is the Technique of Objects Created By",
    "usedToProduce-producedUsing": "Is the Technique of Objects Created Using",
}

related_list_queries = {}
related_list_sparql = {}
for name in related_list_names.keys():
    bits = name.split("-")
    bits.append("padding-for-id")
    q = {}
    top_q = q
    for b in bits[:-1]:
        q[b] = {}
        q = q[b]
    q["id"] = "URI-HERE"
    scope = None
    for s in cfg.lux_config["terms"]:
        if bits[0] in cfg.lux_config["terms"][s]:
            scope = s
            break
    if scope is None:
        print(f"Couldn't find scope for {name}")
        continue
    lq = rdr.read(top_q, scope)
    spq = st.translate_search_related(lq)
    try:
        related_list_queries[scope][name] = json.dumps(top_q, separators=(",", ":"))
        related_list_sparql[scope][name] = spq.get_text()
    except Exception:
        related_list_queries[scope] = {name: json.dumps(top_q, separators=(",", ":"))}
        related_list_sparql[scope] = {name: spq.get_text()}


async def fetch_sparql_wrapper(spq):
    if type(spq) is str:
        q = spq
    else:
        q = spq.get_text()
    try:
        endpoint.setQuery(q)
        ret = endpoint.queryAndConvert()
        results = [r for r in ret["results"]["bindings"]]
    except Exception as e:
        print(q)
        print(e)
        results = []
    return results


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
                results = [r for r in ret["results"]["bindings"]]
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
        except:
            ascdesc = "ASC"
    else:
        sort = "relevance"
        ascdesc = "DESC"
    pred = sorts[scope].get(sort, "relevance")

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
            e["value"] = what
            e["totalItems"] = ct
            e["name"] = related_list_names[rel]
            e["first"]["id"] = e["first"]["id"].replace("QUERY-HERE", usqry)
            js["orderedItems"].append(e)
    js["orderedItems"].sort(key=lambda x: cts[x["value"]], reverse=True)
    return JSONResponse(content=js)


@app.get("/api/translate/{scope}")
async def do_translate(scope, q={}):
    # take simple search in text and return json query equivalent
    js = {"_scope": scope, "AND": [{"text": q}]}
    return JSONResponse(content=js)


async def do_hal_links(scope, identifier):
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
            href = hal_link_templates[hal].replace("<<$id>>", uuri)
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
            href = hal_link_templates[hal].replace("<<$q>>", jqs)
            links[hal] = {"href": href, "_estimate": 1}
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
