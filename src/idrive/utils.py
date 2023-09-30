def map_tuples_to_dict(keys, iterator):
    return map(lambda values: dict(zip(keys, values)), iterator)

def dict_include(d, keys):
    return {k:v for k, v in d.items() if k in keys}

def dict_exclude(d, keys):
    return {k:v for k, v in d.items() if k not in keys}
