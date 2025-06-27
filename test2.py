from luxql import *

api = LuxAPI('item')
bl = LuxBoolean('AND', parent=api)
l = LuxRelationship("date", parent=bl)
l2 = LuxLeaf("date", value="1800", comparitor="<", parent=l)
print(api.to_json())
