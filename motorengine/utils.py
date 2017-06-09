#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys


try:
    from ujson import loads, dumps

    def serialize(value):
        return dumps(value)

    def deserialize(value):
        return loads(value)
except ImportError:
    from json import loads, dumps
    from bson import json_util

    def serialize(value):
        return dumps(value, default=json_util.default)

    def deserialize(value):
        return loads(value, object_hook=json_util.object_hook)


def get_class(module_name, klass=None):
    if '.' not in module_name and klass is None:
        raise ImportError("Can't find class %s." % module_name)

    try:
        module_parts = module_name.split('.')

        if klass is None:
            module_name = '.'.join(module_parts[:-1])
            klass_name = module_parts[-1]
        else:
            klass_name = klass

        module = __import__(module_name)

        if '.' in module_name:
            for part in module_name.split('.')[1:]:
                module = getattr(module, part)

        return getattr(module, klass_name)
    except AttributeError:
        err = sys.exc_info()
        raise ImportError("Can't find class %s (%s)." % (module_name, str(err)))

__json_able = (str, unicode, int, long, float, bool)

def son_to_json_inplace(son):
    for key, value in son.items():
        if isinstance(value, dict):
            son_to_json_inplace(value)

        elif isinstance(value, (list, tuple)):
            for i, v in enumerate(value):
                if isinstance(v, dict):
                    son_to_json_inplace(v)

                elif v is not None and not isinstance(v, __json_able):
                    value[i] = str(v)

        elif value is not None and not isinstance(value, __json_able):
            son[key] = str(value)

class attrdict(dict):
    def __init__(self, d=None, **kwargs):
        if d is None:
            d = kwargs

        elif kwargs:
            d.update(kwargs)

        for k, v in d.items():
            setattr(self, k, v)

    def __setattr__(self, name, value):
        if isinstance(value, (list, tuple)):
            value = [self.__class__(x)
                     if isinstance(x, dict) else x for x in value]

        else:
            value = self.__class__(value) if isinstance(value, dict) else value

        super(attrdict, self).__setattr__(name, value)
        super(attrdict, self).__setitem__(name, value)

    __setitem__ = __setattr__

    def to_json(self):
        son_to_json_inplace(self)
        return self

