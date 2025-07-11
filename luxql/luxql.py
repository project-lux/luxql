import os
import re
import json
import requests


config = dict(
    lux_config="https://lux.collections.yale.edu/api/advanced-search-config",
    booleans=["AND", "OR", "NOT"],
    comparitors=[">", "<", ">=", "<=", "==", "!="],
    leaf_scopes=["text", "date", "float", "boolean"],
)


class LuxConfig(object):
    """Handler for retrieving and processing the LUX search configuration"""

    def __init__(self, config=config):
        self.module_config = config
        url = config["lux_config"]

        # read from disk
        fn = os.path.join(os.path.dirname(__file__), "advanced-search-config.json")
        with open(fn) as fh:
            js = json.load(fh)
        self.lux_config = js

        # try:
        #    resp = requests.get(url, timeout=10)
        #    if resp.status_code == 200:
        #        self.lux_config = resp.json()
        #        # recache it
        #        fn = os.path.join(os.path.dirname(__file__), "advanced-search-config.json")
        #        with open(fn, "w") as fh:
        #            fh.write(json.dumps(self.lux_config, indent=2))
        #    else:
        #        raise ValueError(f"Couldn't retrieve configuration from {url}")
        # except Exception:
        #    raise

        self.scopes = list(self.lux_config["terms"].keys())

        # The format is 'YYYY-MM-DDThh:mm:ss.000Z' or '-YYYYYY-MM-DDThh:mm:ss.000Z'
        self.valid_date_re = re.compile(
            r"((-[0-9][0-9])?[0-9]{4})(-[0-1][0-9]-[0-3][0-9](T[0-2][0-9]:[0-5][0-9]:[0-5][0-9])?)?"
        )

        self.inverted = {}
        for scope, terms in self.lux_config["terms"].items():
            for t in terms.keys():
                try:
                    self.inverted[t].append(scope)
                except Exception:
                    self.inverted[t] = [scope]

        self.possible_options = {}
        for k in self.lux_config["options"].values():
            for o in k["allowed"]:
                self.possible_options[o] = 1

        self.possible_comparitors = config["comparitors"]


_cached_lux_config = LuxConfig(config)


class LuxScope(object):
    """Abstract base class for both the API and the Query language, as the API also needs a scope and children"""

    def __init__(self, scope):
        self.config = _cached_lux_config
        if scope and scope not in self.config.scopes:
            raise ValueError(f"Unknown scope {scope}; valid scopes are {', '.join(self.config.scopes)}")
        self.provides_scope = scope
        self.children = []

    def add(self, what):
        # Actually add and do a callback
        self.children.append(what)
        what.added_to(self)

    def test_child_scope(self, what):
        # Can I accept what as a child?
        if self.provides_scope in what.possible_parent_scopes:
            if isinstance(what, LuxBoolean):
                return None
            else:
                info = self.config.lux_config["terms"][self.provides_scope][what.field]
                what.set_info(info)
                return info
        elif not self.provides_scope:
            # if we don't have a scope, we can't test (e.g. unanchored bool)
            return None
        else:
            raise ValueError(
                f"Cannot add a new {what.class_name} of {what.field} to a scope of {self.provides_scope}"
            )


class LuxAPI(LuxScope):
    """Minimal API instance that downstream applications should inherit"""

    def add(self, what):
        # No parent scope, we're the root of the scope tree
        if self.children:
            raise ValueError("Already have a top level query")
        info = self.test_child_scope(what)
        if info is not None:
            what.test_my_value(info)
        super().add(what)

    def to_json(self):
        if not self.children:
            raise ValueError("No query has been defined")
        return self.children[0].to_json()


class LuxQuery(LuxScope):
    """Abstract base class for the different parts of the LUX query language"""

    def __init__(self, field, parent=None):
        super().__init__(None)
        self.class_name = "Query Component"
        self.field = field
        self.parent = parent
        self.requires_scope = None
        self.possible_parent_scopes = []
        self.possible_provides_scopes = []

    def calculate_scopes(self):
        self.possible_parent_scopes = self.config.inverted.get(self.field, [])
        for s in self.possible_parent_scopes:
            prov = self.config.lux_config["terms"][s][self.field]["relation"]
            if prov not in self.possible_provides_scopes:
                self.possible_provides_scopes.append(prov)
        if len(self.possible_provides_scopes) == 1:
            self.provides_scope = self.possible_provides_scopes[0]
        elif not self.possible_provides_scopes:
            raise ValueError(f"No possible scope for {self.class_name} component '{self.field}'")
        elif not self.possible_parent_scopes:
            raise ValueError(f"No possible parent scope for {self.class_name} component '{self.field}'")
        if self.parent is not None:
            self.add_to_parent()

    def add(self, what):
        # Test if we can add
        if isinstance(what, LuxAPI):
            # Nope!
            raise ValueError("Cannot add an API instance into a query")
        info = self.test_child_scope(what)
        if info is not None:
            what.test_my_value(info)
        # If above hasn't raised, then add
        super().add(what)

    def test_my_value(self, info):
        pass

    def add_to_parent(self):
        if self.parent is not None:
            self.parent.add(self)

    def added_to(self, parent):
        pass

    def set_info(self, info):
        self.provides_scope = info["relation"]


class LuxBoolean(LuxQuery):
    """Boolean operators AND, OR and NOT"""

    def __init__(self, field, parent=None):
        super().__init__(field, parent=parent)
        self.class_name = "Boolean"
        if field not in self.config.module_config["booleans"]:
            raise ValueError(
                f"Tried to construct unknown boolean {field}; known: {self.config.module_config['booleans']}"
            )
        # Booleans are currently accepted everywhere other than leaves, so parent scope doesn't need testing
        self.possible_parent_scopes = self.config.scopes
        if parent is not None:
            self.add_to_parent()

    def to_json(self):
        if not self.children:
            raise ValueError(f"Boolean {self.field} is missing children")
        return {self.field: [x.to_json() for x in self.children]}

    def added_to(self, parent):
        self.provides_scope = parent.provides_scope

    def add(self, what):
        super().add(what)
        self.possible_parent_scopes = what.possible_parent_scopes


class LuxLeaf(LuxQuery):
    """A Leaf node in the query, where the field + (comparitor +) term (+ options) sits"""

    def __init__(self, field, parent=None, value=None, comparitor=None, options=[], weight=0, complete=False):
        super().__init__(field, parent=parent)
        # Can field exist within current scope?
        self.class_name = "Leaf"
        self.value = value
        self.comparitor = comparitor
        if self.comparitor and self.comparitor not in self.config.possible_comparitors:
            raise ValueError(f"{self.comparitor} is not a known comparitor")
        self.options = options
        for o in self.options:
            if o not in self.config.possible_options:
                raise ValueError(f"{o} is not a known option")
        self.children = None
        self.weight = weight
        self.complete = complete
        self.calculate_scopes()

    def calculate_scopes(self):
        super().calculate_scopes()
        for s in self.possible_provides_scopes:
            if s not in self.config.module_config["leaf_scopes"]:
                raise ValueError(f"Unknown leaf scope '{s}' in {self.field}")
            if self.value is not None:
                self.test_my_value({"relation": s})

    def test_my_value(self, info):
        if info["relation"] in self.config.scopes:
            # This isn't a leaf
            raise ValueError(f"Cannot create a {self.class_name} called {self.field} as it is a Relationship")
        elif info["relation"] == "text":
            # value must be a string
            if type(self.value) is not str:
                raise ValueError(f"Text values must be strings; '{self.field}' received {self.value})")
            if "allowedOptionsName" in info:
                optName = info["allowedOptionsName"]
                okay_opts = self.config.lux_config["options"][optName]["allowed"]
                for o in self.options:
                    if o not in okay_opts:
                        raise ValueError(f"Unknown option specified: {o}\nAllowed: {', '.join(okay_opts)}")
        elif self.options:
            raise ValueError("Only 'text' leaf nodes can have options")
        elif info["relation"] == "date":
            # test value is a datestring
            if not self.config.valid_date_re.match(self.value):
                raise ValueError(
                    "Dates require a specific format: 'YYYY-MM-DDThh:mm:ss.000Z' or '-YYYYYY-MM-DDThh:mm:ss.000Z'"
                )
            # Test there's a comparitor
            if not self.comparitor:
                raise ValueError("Dates require a comparitor")
            elif self.comparitor not in self.config.module_config["comparitors"]:
                raise ValueError(f"{self.comparitor} is not a valid comparitor")
        elif info["relation"] == "float":
            # test value is a number
            try:
                f = float(self.value)
            except ValueError:
                raise ValueError("Numbers must be expressed using only numbers and .")
            if not self.comparitor:
                raise ValueError("Numbers require a comparitor")
            elif not self.comparitor in self.config.module_config["comparitors"]:
                raise ValueError(f"{self.comparitor} is not a valid comparitor")
        elif info["relation"] == "boolean":
            # test is bool
            if self.value not in ["0", "1", True, False]:
                raise ValueError(f"Booleans must be expressed as either '1' or '0' or a native boolean")
        else:
            # broken??
            raise ValueError(f"Unknown scope: {info['relation']}")

    def add(self, what):
        raise ValueError(f"You cannot add further query components to a Leaf")

    def to_json(self):
        if self.value is None:
            raise ValueError(f"Leaf node '{self.field}' does not have a value set")
        elif isinstance(self.value, bool):
            value = "1" if self.value else "0"
        elif not isinstance(self.value, str):
            value = str(self.value)
        else:
            value = self.value

        js = {self.field: value}
        if self.comparitor:
            js["_comp"] = self.comparitor
        if self.options:
            js["_options"] = self.options
        if self.weight:
            js["_weight"] = self.weight
        if self.complete:
            js["_complete"] = True if self.complete else False
        return js


class LuxRelationship(LuxQuery):
    """A relationship node in the query"""

    def __init__(self, field, parent=None):
        super().__init__(field, parent=parent)
        self.class_name = "Relationship"
        self.calculate_scopes()

    def calculate_scopes(self):
        super().calculate_scopes()
        for s in self.possible_provides_scopes:
            if not s in self.config.scopes:
                raise ValueError(f"Unknown relationship scope '{s}' in {self.field}")

    def test_my_value(self, info):
        if info["relation"] not in self.config.scopes:
            raise ValueError(f"Cannot create a {self.class_name} called {self.field} as it is a Leaf")

    def add(self, what):
        if self.children:
            raise ValueError(f"Relationship already has a child")
        super().add(what)

    def to_json(self):
        if not self.children:
            raise ValueError(f"Relationship {self.field} is missing children")
        return {self.field: self.children[0].to_json()}
