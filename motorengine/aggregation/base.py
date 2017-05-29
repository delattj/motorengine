#!/usr/bin/env python
# -*- coding: utf-8 -*-

import six
from bson import ObjectId
from easydict import EasyDict as edict
from tornado.concurrent import return_future

from motorengine import DESCENDING, ASCENDING
from motorengine.query_builder.transform import update
from motorengine.query_builder.node import Q, QCombination, QNot
from motorengine.fields import BaseField

class BaseAggregation(object):
    def __init__(self, field, alias):
        self._field = field
        self.alias = alias

    @property
    def field(self):
        return self._field


class PipelineOperation(object):
    def __init__(self, aggregation):
        self.aggregation = aggregation

    def to_query(self):
        return {}


class GroupBy(PipelineOperation):
    def __init__(self, aggregation, first_group_by, *groups):
        super(GroupBy, self).__init__(aggregation)
        self.first_group_by = first_group_by
        self.groups = groups

    def to_query(self):
        group_obj = {'$group': {'_id': {}}}

        for group in self.groups:
            if isinstance(group, BaseAggregation):
                group_obj['$group'].update(group.to_query(self.aggregation))
                continue

            if isinstance(group, six.string_types):
                field_name = group
            else:
                field_name = self.aggregation.get_field(group).db_field

            if self.first_group_by:
                group_obj['$group']['_id'][field_name] = "$%s" % field_name
            else:
                group_obj['$group']['_id'][field_name] = "$_id.%s" % field_name

        return group_obj


class Match(PipelineOperation):
    def __init__(self, aggregation, *args, **filters):
        super(Match, self).__init__(aggregation)
        if args and len(args) == 1 and isinstance(args[0], (Q, QNot, QCombination)):
            self.filters = args[0]
        
        else:
            self.filters = Q(**filters)

    def to_query(self):
        match = {}

        query = self.aggregation.queryset.get_query_from_filters(self.filters)

        update(match, query)

        return {'$match': match}


class Unwind(PipelineOperation):
    def __init__(self, aggregation, field):
        super(Unwind, self).__init__(aggregation)
        self.field = self.aggregation.get_field(field)

    def to_query(self):
        field = self.field
        if isinstance(field, (str, unicode)):
            field_name = '.'.join(field.split('__'))

        else:
            field_name = self.field.db_field

        return {'$unwind': '$%s' % field_name}


class OrderBy(PipelineOperation):
    def __init__(self, aggregation, *fields):
        super(OrderBy, self).__init__(aggregation)
        self.fields = fields

    def to_query(self):
        sort = {}

        for field in self.fields:
            flag = ASCENDING
            if field[0] == '-':
                flag = DESCENDING
                field = field[1:]

            sort[field] = flag

        return {'$sort': sort}


class BaseOp(object):
    def to_query(self, aggregation):
        return {}


class Op(BaseOp):
    def __init__(self, op, *args):
        self.op = op
        self.args = args

    def to_query(self, aggregation):
        args = [(isinstance(arg, BaseField) and '$'+ arg.db_field) or arg for arg in self.args]
        if len(args) == 1:
            args = args[0]

        return { '$'+ self.op: args }


class _NoDefault:
    pass

class Switch(BaseOp):
    def __init__(self, default=_NoDefault):
        self.default = default
        self.exprs = []

    def case(self, expr, then):
        self.exprs.append((expr, then))
        return self
        
    def to_query(self, aggregation):
        switch_obj = {}
        if self.default is not _NoDefault:
            switch_obj['default'] = self.default

        branches = [{
            'case': expr.to_query(aggregation),
            'then': (isinstance(value, BaseOp) and value.to_query(aggregation)) or value
        } for expr, value in self.exprs]
        switch_obj['branches'] = branches

        return { '$switch': switch_obj }


class Last(BaseAggregation):
    def __init__(self, field, alias=None):
        super(Last, self).__init__(field, alias)
        
    def to_query(self, aggregation):
        alias = self.alias
        field_name = aggregation.get_field_name(self.field)

        if alias is None:
            alias = field_name

        return {
            alias: {"$last": ("$%s" % field_name)}
        }


class First(BaseAggregation):
    def __init__(self, field, alias=None):
        super(First, self).__init__(field, alias)
        
    def to_query(self, aggregation):
        alias = self.alias
        field_name = aggregation.get_field_name(self.field)

        if alias is None:
            alias = field_name

        return {
            alias: {"$first": ("$%s" % field_name)}
        }


class Push(BaseAggregation):
    def __init__(self, alias, *fields):
        super(Push, self).__init__(fields, alias)
        
    def to_query(self, aggregation):
        alias = self.alias
        fields = [aggregation.get_field_name(f) for f in self.field]

        return {
            alias: {"$push": dict((f, '$'+f) for f in fields)}
        }


class Fields(PipelineOperation):
    def __init__(self, aggregation, *fields, **kfields):
        super(Fields, self).__init__(aggregation)
        self.fields = fields
        self.kfields = kfields

    def to_query(self):
        project = {}

        for field in self.fields:
            if isinstance(field, (str, unicode)):
                field_name = '.'.join(field.split('__'))

            else:
                field_name = self.aggregation.get_field(field).db_field

            project[field_name] = 1

        for field, flag in self.kfields.items():
            if isinstance(flag, BaseOp):
                flag = flag.to_query(self.aggregation)

            elif isinstance(flag, (BaseField, str, unicode)):
                # Remapping
                flag = '$'+ self.aggregation.get_field_name(flag)

            project['.'.join(field.split('__'))] = flag

        return {'$project': project}


class GraphLookup(PipelineOperation):
    def __init__(self, aggregation, _from, start_with, connect_from_field,
            connect_to_field, _as, max_depth=None, depth_field=None, restrict_search_with_match=None):
        super(GraphLookup, self).__init__(aggregation)
        self._from = _from
        self.start_with = start_with
        self.connect_from_field = connect_from_field
        self.connect_to_field = connect_to_field
        self._as = _as
        self.max_depth = max_depth
        self.depth_field = depth_field
        self.restrict_search_with_match = restrict_search_with_match

    def to_query(self):
        lookup = {
                'from': self._from,
                'startWith': '$'+ ((isinstance(self.start_with, BaseField) and self.start_with.db_field) or self.start_with),
                'connectFromField': (isinstance(self.connect_from_field, BaseField) and self.connect_from_field.db_field) or self.connect_from_field,
                'connectToField': (isinstance(self.connect_to_field, BaseField) and self.connect_to_field.db_field) or self.connect_to_field,
                'as': self._as,
        }

        if self.max_depth is not None:
            lookup['maxDepth'] = self.max_depth

        if self.depth_field is not None:
            lookup['depthField'] = self.depth_field

        if self.restrict_search_with_match is not None:
            lookup['restrictSearchWithMatch'] = self.restrict_search_with_match.to_query(self.aggregation.queryset.__kclass__)

        return { '$graphLookup': lookup }


class Aggregation(object):
    def __init__(self, queryset):
        self.first_group_by = True
        self.queryset = queryset
        self.pipeline = []
        self.ids = []
        self.raw_query = None

    def get_field_name(self, field):
        if isinstance(field, six.string_types):
            return field

        return field.db_field

    def get_field(self, field):
        return field

    def raw(self, steps):
        self.raw_query = steps
        return self

    def group_by(self, *args):
        self.pipeline.append(GroupBy(self, self.first_group_by, *args))
        self.first_group_by = False
        return self

    def match(self, *args, **filters):
        self.pipeline.append(Match(self, *args, **filters))
        return self

    def unwind(self, field):
        self.pipeline.append(Unwind(self, field))
        return self

    def order_by(self, *fields):
        self.pipeline.append(OrderBy(self, *fields))
        return self

    def fields(self, *fields, **kfields):
        self.pipeline.append(Fields(self, *fields, **kfields))
        return self

    def graph_lookup(self, *args, **kwargs):
        self.pipeline.append(GraphLookup(self, *args, **kwargs))
        return self

    def fill_ids(self, item):
        if not '_id' in item:
            return

        if isinstance(item['_id'], (dict,)):
            for id_name, id_value in list(item['_id'].items()):
                item[id_name] = id_value

    def get_instance(self, item):
        return self.queryset.__klass__.from_son(item)

    def handle_aggregation(self, callback):
        def handle(*arguments, **kw):
            if arguments[1]:
                raise RuntimeError('Aggregation failed due to: %s' % str(arguments[1]))

            results = []
            for item in arguments[0]:
                self.fill_ids(item)
                results.append(edict(item))

            callback(results)

        return handle

    @return_future
    def fetch(self, callback=None, alias=None):
        coll = self.queryset.coll(alias)
        cursor = coll.aggregate(self.to_query())
        cursor.to_list(None, callback=self.handle_aggregation(callback))

    @classmethod
    def avg(cls, field, alias=None):
        from motorengine.aggregation.avg import AverageAggregation
        return AverageAggregation(field, alias)

    @classmethod
    def sum(cls, field, alias=None):
        from motorengine.aggregation.sum import SumAggregation
        return SumAggregation(field, alias)

    def to_query(self):
        if self.raw_query is not None:
            return self.raw_query

        query = []

        for pipeline_step in self.pipeline:
            query_steps = pipeline_step.to_query()
            if isinstance(query_steps, (tuple, set, list)):
                for step in query_steps:
                    query.append(step)
            else:
                query.append(query_steps)

        return query
