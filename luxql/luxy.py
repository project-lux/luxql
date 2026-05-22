from luxql import LuxBoolean, LuxLeaf, LuxRelationship


def AND(*args, **kw):
    b = LuxBoolean("AND")
    for a in args:
        b.add(a)
    return b


def OR(*args, **kw):
    b = LuxBoolean("OR")
    for a in args:
        b.add(a)
    return b


def NOT(*args, **kw):
    b = LuxBoolean("NOT")
    for a in args:
        b.add(a)
    return b


_temp = LuxBoolean("AND")
rels = list(_temp.config.terms["rel"])
leaves = list(_temp.config.terms["leaf"])
del _temp

__all__ = ["AND", "OR", "NOT"] + rels + leaves


def __dir__():
    return __all__


def __getattr__(name):
    if name in rels:

        def magic_rel(*args, **kw):
            r = LuxRelationship(name)
            r.add(args[0])
            return r

        return magic_rel

    elif name in leaves:

        def magic(*args, **kw):
            return LuxLeaf(name, value=args[0], **kw)

        return magic
    else:
        raise AttributeError(f"Unknown attribute: {name}")
