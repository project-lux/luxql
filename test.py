import unittest
from luxql import *


class TestLuxQL(unittest.TestCase):
    def test_config(self):
        config = dict(
            lux_config="https://lux.collections.yale.edu/api/advanced-search-config",
            booleans=["AND", "OR", "NOT"],
            comparitors=[">", "<", ">=", "<=", "==", "!="],
        )
        try:
            cfg = LuxConfig(config)
        except:
            raise
        self.assertTrue(type(cfg.lux_config) == dict)
        self.assertTrue("terms" in cfg.lux_config)
        self.assertTrue("item" in cfg.scopes)
        self.assertTrue("name" in cfg.inverted)

    def test_config_bad_url(self):
        config = dict(
            lux_config="https://lux.collections.yale.edu/",
            booleans=["AND", "OR", "NOT"],
            comparitors=[">", "<", ">=", "<=", "==", "!="],
        )
        self.assertRaises(Exception, LuxConfig, config)

        config = dict(
            lux_config="https://lux.collections.yale.edu/api/not-okay",
            booleans=["AND", "OR", "NOT"],
            comparitors=[">", "<", ">=", "<=", "==", "!="],
        )
        self.assertRaises(ValueError, LuxConfig, config)

    def test_api(self):
        api = LuxAPI("item")
        self.assertTrue(api.provides_scope == "item")
        self.assertTrue(api.config != None)
        self.assertRaises(ValueError, api.to_json)

    def test_api_bad_scope(self):
        self.assertRaises(ValueError, LuxAPI, "fish")

    def test_api_no_scope(self):
        self.assertRaises(TypeError, LuxAPI)

    def test_api_two_queries(self):
        api = LuxAPI("item")
        bl = LuxBoolean("AND")
        api.add(bl)
        self.assertTrue(bl in api.children)
        self.assertRaises(ValueError, api.add, LuxBoolean("OR"))

    def test_add_api(self):
        api = LuxAPI("item")
        bl = LuxBoolean("AND")
        self.assertRaises(ValueError, bl.add, api)

    def test_api_json(self):
        api = LuxAPI("item")
        LuxLeaf("name", value="fish", parent=api)
        js = api.to_json()
        self.assertEqual(js, {"name": "fish"})

    def test_boolean(self):
        # Test basic instantiation
        bl = LuxBoolean("AND")
        self.assertRaises(ValueError, bl.to_json)

    def test_boolean_parent(self):
        # Test adding to a scope
        api = LuxAPI("item")
        bl = LuxBoolean("AND")
        api.add(bl)
        self.assertTrue(bl in api.children)

        # Test adding to a scope via parent arg
        api2 = LuxAPI("item")
        bl2 = LuxBoolean("OR", parent=api2)
        self.assertTrue(bl2 in api2.children)

        # Test scope is set properly
        self.assertEqual(bl2.provides_scope, "item")

    def test_boolean_boolean(self):
        api = LuxAPI("item")
        bl = LuxBoolean("AND", parent=api)
        bl2 = LuxBoolean("OR", parent=bl)
        self.assertEqual(bl2.provides_scope, "item")

    def test_boolean_add_bad_rel(self):
        api = LuxAPI("item")
        bl = LuxBoolean("AND", parent=api)
        rel = LuxRelationship("foundedBy")
        self.assertRaises(ValueError, bl.add, rel)

    def test_add_boolean_bad_rel(self):
        api = LuxAPI("item")
        bl = LuxBoolean("AND")
        rel = LuxRelationship("foundedBy")
        bl.add(rel)
        self.assertRaises(ValueError, api.add, bl)

    def test_boolean_rel(self):
        api = LuxAPI("item")
        bl = LuxBoolean("AND", parent=api)
        rel = LuxRelationship("carries", parent=bl)
        self.assertTrue(rel in bl.children)

    def test_boolean_no_field(self):
        self.assertRaises(TypeError, LuxBoolean)

    def test_boolean_bad_field(self):
        self.assertRaises(ValueError, LuxBoolean, "fish")

    def test_boolean_json(self):
        api = LuxAPI("item")
        bl = LuxBoolean("AND", parent=api)
        leaf = LuxLeaf("name", value="fish", parent=bl)
        js = api.to_json()
        self.assertEqual(js, {"AND": [{"name": "fish"}]})

    def test_leaf(self):
        leaf = LuxLeaf("name")
        self.assertRaises(ValueError, leaf.to_json)
        leaf = LuxLeaf("name", value="okay")
        self.assertEqual(leaf.to_json(), {"name": "okay"})

    def test_leaf_not_leaf(self):
        self.assertRaises(ValueError, LuxLeaf, "carries", value="fish")

    def test_leaf_bool(self):
        leaf = LuxLeaf("isOnline", value=True)
        self.assertEqual(leaf.to_json(), {"isOnline": "1"})

    def test_leaf_not_bool(self):
        self.assertRaises(ValueError, LuxLeaf, "name", value=True)

    def test_leaf_text_options(self):
        leaf = LuxLeaf("name", value="okay", options=["punctuation-sensitive"])

        leaf = LuxLeaf("name", value="okay", weight=3)

        leaf = LuxLeaf("name", value="okay", complete=True)

    # FAILING
    def test_leaf_bad_option(self):
        self.assertRaises(ValueError, LuxLeaf, "name", value="okay", options=["fish"])

    def test_leaf_date(self):
        l = LuxLeaf("startDate", value="2000-01-01T00:00:00", comparitor=">")

    def test_leaf_bad_date(self):
        self.assertRaises(ValueError, LuxLeaf, "startDate", value="fish", comparitor=">")

    def test_leaf_no_field(self):
        self.assertRaises(TypeError, LuxLeaf)

    def test_leaf_bad_field(self):
        self.assertRaises(ValueError, LuxLeaf, "fish")

    def test_relationship(self):
        rel = LuxRelationship("carries")
        self.assertRaises(ValueError, rel.to_json)

    def test_relationship_no_field(self):
        self.assertRaises(TypeError, LuxRelationship)

    def test_relationship_bad_field(self):
        self.assertRaises(ValueError, LuxRelationship, "fish")

    def test_relationship_not_relationship(self):
        self.assertRaises(ValueError, LuxRelationship, "name")


# api = LuxAPI('item')
# bl = LuxBoolean('AND')
# carries= LuxRelationship("carries")
# name = LuxLeaf("name", value="visual", options=["punctuation-sensitive", "unwildcarded"], weight=3)
# name2 = LuxLeaf("name", value="painting")

# about = LuxRelationship("aboutConcept")
# broader = LuxRelationship("broader")


# q = api.to_json()

if __name__ == "__main__":
    unittest.main()
