
from luxql import *

api = LuxAPI('item')
bl = LuxBoolean('AND', parent=api)
r = LuxRelationship("carries", parent=bl)
l = LuxLeaf("name", value="visual", parent=r)
l2 = LuxLeaf("name", value="painting", parent=bl)
q = api.to_json()

assert(q == {'AND': [{'carries': {'name': 'visual'}}, {'name': 'painting'}]})
