import itertools

global_counter: itertools.count = itertools.count()


def default_name(obj: object) -> str:
    return f'{obj.__class__.__name__}-{next(global_counter)}'
