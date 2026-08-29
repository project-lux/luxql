import json
import os
import re

import requests

config = dict(
    lux_base="https://lux.collections.yale.edu/api/",
    lux_config="advanced-search-config",
    lux_stats="stats",
    booleans=["AND", "OR", "NOT"],
    comparitors=[">", "<", ">=", "<=", "==", "!="],
    leaf_scopes=["text", "date", "float", "boolean"],
    cache_remote_config=True,
    cache_remote_stats=False,
)


class LuxConfig(object):
    """Handler for retrieving and processing the LUX search configuration"""

    def __init__(self, config=config, lux_config=""):
        self.module_config = config
        if not lux_config:
            lux_config = os.path.join(
                os.path.dirname(__file__), f"{config['lux_config']}.json"
            )
        if not os.path.exists(lux_config):
            lux_config = ""

        self.remote_lux_config = f"{config['lux_base']}{config['lux_config']}"
        self.remote_lux_stats = f"{config['lux_base']}{config['lux_stats']}"

        self.lux_stats = {}

        # read from disk
        if lux_config:
            self.using_local_config = True
            with open(lux_config) as fh:
                js = json.load(fh)
            self.lux_config = js
        elif self.remote_lux_config:
            # Read from remote LUX instance
            try:
                resp = requests.get(self.remote_lux_config, timeout=10)
                if resp.status_code == 200:
                    self.lux_config = resp.json()
                    if config["cache_remote_config"]:
                        fn = os.path.join(
                            os.path.dirname(__file__), "advanced-search-config.json"
                        )
                        with open(fn, "w") as fh:
                            fh.write(json.dumps(self.lux_config, indent=2))
                else:
                    raise ValueError(
                        f"Couldn't retrieve configuration from {self.remote_lux_config}"
                    )
            except Exception:
                raise
            if self.remote_lux_stats:
                try:
                    resp = requests.get(self.remote_lux_stats, timeout=10)
                    if resp.status_code == 200:
                        self.lux_stats = resp.json()
                        if config["cache_remote_stats"]:
                            fn = os.path.join(os.path.dirname(__file__), "stats.json")
                            with open(fn, "w") as fh:
                                fh.write(json.dumps(self.lux_stats, indent=2))
                    else:
                        raise ValueError(
                            f"Couldn't retrieve statistics from {self.remote_lux_stats}"
                        )
                except Exception:
                    raise
        else:
            # No configuration provided, fail
            raise ValueError("No data statistics provided or available")

        self.scopes = list(self.lux_config["terms"].keys())

        # The format is 'YYYY-MM-DDThh:mm:ss.000Z' or '-YYYYYY-MM-DDThh:mm:ss.000Z'
        self.valid_date_re = re.compile(
            r"((-[0-9][0-9])?[0-9]{4})(-[0-1][0-9]-[0-3][0-9](T[0-2][0-9]:[0-5][0-9]:[0-5][0-9])?)?"
        )

        self.inverted = {}
        self.terms = {"leaf": set([]), "rel": set([])}
        for scope, terms in self.lux_config["terms"].items():
            for t in terms.keys():
                try:
                    self.inverted[t].append(scope)
                except Exception:
                    self.inverted[t] = [scope]
                relt = terms[t]["relation"]
                if relt in self.scopes:
                    self.terms["rel"].add(t)
                else:
                    self.terms["leaf"].add(t)

        self.possible_options = {}
        for k in self.lux_config["options"].values():
            for o in k["allowed"]:
                self.possible_options[o] = 1

        self.possible_comparitors = config["comparitors"]


_cached_lux_config = LuxConfig(config)
