def map_tuples_to_dict(keys, iterator):
    return map(lambda values: dict(zip(keys, values)), iterator)
