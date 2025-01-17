import re
import json
import requests

config = dict(
    lux_config="https://lux.collections.yale.edu/api/advanced-search-config",
    lux_url="https://lux.collections.yale.edu/api",
    default="item",
    objects="item",
    works="work",
    people="agent",
    places="place",
    concepts="concept",
    events="event",
    set="set",
    booleans = ["AND", "OR", "NOT"],
    comparitors = [">", "<", ">=", "<=", "==", "!="],
    auto_add = True
)

class LuxConfig(object):
    """Handler for retrieving and processing the LUX search configuration"""
    def __init__(self, config):
        self.module_config = config
        url = config['lux_config']
        resp = requests.get(url)
        if resp.status_code == 200:
            self.lux_config = resp.json()
        else:
            raise ValueError(f"Couldn't retrieve configuration from {url}")
        self.scopes = list(self.lux_config['terms'].keys())

        # The format is 'YYYY-MM-DDThh:mm:ss.000Z' or '-YYYYYY-MM-DDThh:mm:ss.000Z'
        self.valid_date_re = re.compile(r"((-[0-9][0-9])?[0-9]{4})(-[0-1][0-9]-[0-3][0-9](T[0-2][0-9]:[0-5][0-9]:[0-5][0-9])?)?")

_cached_lux_config = LuxConfig(config)


### To do
# Deal with options
# Allow grafting between query trees
# Consider refactor so that validation happens on add() so you can construct a branch
#     and then try to add it to a tree. Otherwise always add?


class LuxScope(object):
    """Abstract base class for both the API and the Query language, as the API also needs a scope and children"""
    def __init__(self, scope):
        self.config = _cached_lux_config
        if scope and not scope in self.config.scopes:
            raise ValueError(f"Unknown scope {scope}; valid scopes are {', '.join(self.config.scopes)}")
        self.provides_scope = scope
        self.children = []

    def accepts(self, field):
        """is the field acceptable within the current query node's scope? If so, return details, if not return False"""
        okay = self.config.lux_config['terms'][self.provides_scope]
        return okay.get(field, False)

    def add(self, what):
        # Test scope if not being called from __init__?
        self.children.append(what)


class LuxAPI(LuxScope):
    """Minimal API instance that downstream applications should inherit"""
    def __init__(self, scope):
        super().__init__(scope)

    def add(self, what):
        if self.children:
            raise ValueError("Already have a top level query")
        super().add(what)

    def to_json(self):
        if not self.children:
            raise ValueError("No query has been defined")
        return self.children[0].to_json()

    def get(self):
        """This is where wrappers would make the URL, retrieve it and process"""
        q = json.dumps(self.to_json())
        print(q)


class LuxQuery(LuxScope):
    """Abstract base class for the different parts of the LUX query language"""

    def __init__(self, field, parent=None):
        super().__init__(None)
        self.class_name = "Query Component"
        self.field = field
        self.parent = parent
        self.requires_scope = None

    def test_parent_scope(self):
        if self.parent is not None:
            self.requires_scope = self.parent.provides_scope
            okay = self.parent.accepts(self.field)
            if okay is False:
                raise ValueError(f"Cannot add a new {self.class_name} '{self.field}' to a scope of {self.parent.provides_scope}")
            else:
                return okay
        return None

    def test_my_scope(self, info):
        pass

    def add_to_parent(self):
        if self.parent is not None and self.config.module_config['auto_add']:
            self.parent.add(self)

    def to_json(self):
        return {}

class LuxBoolean(LuxQuery):
    """Boolean operators AND, OR and NOT"""

    def __init__(self, field, parent=None):
        super().__init__(field, parent=parent)
        self.class_name = "Boolean"
        if not field in self.config.module_config['booleans']:
            raise ValueError(f"Tried to construct unknown boolean {field}; known: {self.config.module_config['booleans']}")
        # Booleans are currently accepted everywhere other than leaves, so parent scope doesn't need testing
        if parent is not None:
            self.provides_scope = parent.provides_scope
        self.add_to_parent()

    def to_json(self):
        if not self.children:
            raise ValueError(f"Boolean {self.field} is missing children")
        return {self.field: [x.to_json() for x in self.children]}

class LuxLeaf(LuxQuery):
    """A Leaf node in the query, where the field + (comparitor +) term (+ options) sits"""

    def __init__(self, field, parent=None, value=None, comparitor=None, options=[], weight=0, complete=False):
        super().__init__(field, parent=parent)
        # Can field exist within current scope?
        self.class_name = "Leaf"
        if isinstance(value, bool):
            value = "1" if value else "0"
        elif not isinstance(value, str):
            value = str(value)
        self.value = value
        self.comparitor = comparitor
        self.options = options
        self.children = None
        self.weight = weight
        self.complete = complete
        info = self.test_parent_scope()
        self.test_my_scope(info)
        self.add_to_parent()

    def test_my_scope(self, info):
        # test self.value against info['relation']
        # Also test comparitors and options are acceptable for scope

        if info['relation'] in self.config.scopes:
            # This isn't a leaf
            raise ValueError(f"Cannot create a {self.class_name} calls {self.field} as it is a Relationship")
        elif info['relation'] == 'text':
            # value must be a string
            if "allowedOptionsName" in info:
                optName = info['allowedOptionsName']
                okay_opts = self.config.lux_config['options'][optName]['allowed']
                for o in self.options:
                    if not o in okay_opts:
                        raise ValueError(f"Unknown option specified: {o}\nAllowed: {', '.join(okay_opts)}")
        elif info['relation'] == 'date':
            # test value is a datestring
            if not self.config.valid_date_re.match(self.value):
                raise ValueError(f"Dates require a specific format: 'YYYY-MM-DDThh:mm:ss.000Z' or '-YYYYYY-MM-DDThh:mm:ss.000Z'")
            # Test there's a comparitor
            if not self.comparitor:
                raise ValueError(f"Dates require a comparitor")
            elif not self.comparitor in self.config.module_config['comparitors']:
                raise ValueError(f"{self.comparitor} is not a valid comparitor")
        elif info['relation'] == 'float':
            # test value is a number
            try:
                f = float(self.value)
            except ValueError:
                raise ValueError(f"Numbers must be expressed using only numbers and .")
            if not self.comparitor:
                raise ValueError(f"Numbers require a comparitor")
            elif not self.comparitor in self.config.module_config['comparitors']:
                raise ValueError(f"{self.comparitor} is not a valid comparitor")
        elif info['relation'] == 'boolean':
            # test is bool
            if self.value not in ["0", "1"]:
                raise ValueError(f"Booleans must be expressed as either '1' or '0'")
        else:
            # broken??
            raise ValueError(f"Unknown scope: {info['relation']}")

        self.provides_scope = info['relation']

    def add(self, what):
        raise ValueError(f"You cannot add further query components to a Leaf")

    def to_json(self):
        js = {self.field: self.value}
        if self.comparitor:
            js['_comp'] = self.comparitor
        if self.options:
            js['_options'] = self.options
        if self.weight:
            js['_weight'] = self.weight
        if self.complete:
            js['_complete'] = True if self.complete else False
        return js

class LuxRelationship(LuxQuery):
    """A relationship node in the query"""

    def __init__(self, field, parent=None):
        super().__init__(field, parent=parent)
        self.class_name = "Relationship"
        # Can field exist within current scope?
        info = self.test_parent_scope()
        self.test_my_scope(info)
        self.add_to_parent()

    def test_my_scope(self, info):
        if info['relation'] not in self.config.scopes:
            raise ValueError(f"Cannot create a {self.class_name} called {self.field} as it is a Leaf")
        self.provides_scope = info['relation']

    def add(self, what):
        if self.children:
            raise ValueError(f"Relationship already has a child")
        super().add(what)

    def to_json(self):
        if not self.children:
            raise ValueError(f"Relationship {self.field} is missing children")
        return {self.field: self.children[0].to_json()}
