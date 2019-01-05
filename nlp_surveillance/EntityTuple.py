from functools import wraps
from typing import NamedTuple


class Entity(NamedTuple):
    """To have a name for the returned entity tuples"""
    entity: str
    resolved: list = []


def entity_tuple(entity_extractor):
    """Wraps the return value of and entity extractor into a Entity tuple"""
    @wraps(entity_extractor)
    def decorate(doc, **kwargs):
        resolved = entity_extractor(doc, **kwargs)
        entity = entity_extractor.__name__
        if type(resolved) != list:
            resolved = [resolved]
        return Entity(entity, resolved)
    return decorate
