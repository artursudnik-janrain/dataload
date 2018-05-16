import copy

def merge_dicts(a, b):
    """
    Recursively merge 2 dictionaries. This does not use deepcopy (performance is
    too slow for large data) and thus it will modify the dict in place.

    Args:
        a - The primary dictionary
        b - The dictionary to merge into a

    Returns:
        A new dictonary with b merged into a

    Example:
        >>> merge_dicts(_, [])
        []

        >>> a, b = {'a': 1}, {'b': 2}
        >>> c = merge_dicts(a, b)
        >>> c == {'a': 1, 'b': 2} and (a is c) and (a is not b) and (b is not c)
        True

        >>> merge_dicts({'a':{'b':2, 'c':3}}, {'a':{'b':4, 'd':5}})
        {'a': {'c': 3, 'b': 4, 'd': 5}}
    """
    if not isinstance(b, dict):
        return b

    result = a
    for k, v in b.items():
        if k in result and isinstance(result[k], dict):
                result[k] = merge_dicts(result[k], v)
        else:
            result[k] = v
    return result

def expand_objects(record):
    """
    Expand attributes expressed in dot-notation and merge back into dictionary.

    Args:
        a - The primary dictionary
        b - The dictionary to merge into a

    Returns:
        A new dictonary with b merged into a

    Example:
        >>> a = {"foo.bar": "hello", "foo.baz": "world!"}
        >>> b = expand_objects(a)
        >>> b == {"foo": {"bar": "hello", "baz": "world!"}}
        True
    """
    new_record = copy.deepcopy(record)
    for key, value in record.items():
        parts = key.split(".")
        if len(parts) > 1:
            parts.reverse()
            current = {parts[0]: value}
            for part in parts[1:]:
                current = {part: current}
            del new_record[key]
            new_record = merge_dicts(new_record, current)

    return new_record

