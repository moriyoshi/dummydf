# coding: utf-8
#
# Copyright 2018 Moriyoshi Koizumi
# 
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
# 
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
# 
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

import math
import operator
from .column import _Function, _Literal, _GenericExpr, FunctionSpec, infer_data_type, _PythonCompilableFunctionSpec
from .types import DoubleType, BooleanType


class SimpleFunctionSpec(FunctionSpec):
    def __init__(self, name):
        super(SimpleFunctionSpec, self).__init__(name, 1)

    def infer_data_type(self, cols):
        assert len(cols) > 0 and cols[0] is not None
        return infer_data_type(cols[0])


class SimpleAggregationFunctionSpec(SimpleFunctionSpec):
    def __init__(self, name, fn):
        super(SimpleAggregationFunctionSpec, self).__init__(name)
        self.fn = fn


fn_spec_min = SimpleAggregationFunctionSpec('min', 'min')
fn_spec_max = SimpleAggregationFunctionSpec('max', 'max')
fn_spec_mean = SimpleAggregationFunctionSpec('mean', 'mean')
fn_spec_floor = _PythonCompilableFunctionSpec('floor', 1, math.floor, DoubleType())
fn_spec_add = _PythonCompilableFunctionSpec('+', 2, operator.add, DoubleType())
fn_spec_sub = _PythonCompilableFunctionSpec('-', 2, operator.sub, DoubleType())
fn_spec_mul = _PythonCompilableFunctionSpec('*', 2, operator.mul, DoubleType())
fn_spec_div = _PythonCompilableFunctionSpec('/', 2, operator.div, DoubleType())
fn_spec_mod = _PythonCompilableFunctionSpec('%', 2, operator.mod, DoubleType())
fn_spec_eq = _PythonCompilableFunctionSpec('==', 2, operator.eq, BooleanType())
fn_spec_ne = _PythonCompilableFunctionSpec('!=', 2, operator.ne, BooleanType())
fn_spec_gt = _PythonCompilableFunctionSpec('>', 2, operator.gt, BooleanType())
fn_spec_ge = _PythonCompilableFunctionSpec('>=', 2, operator.ge, BooleanType())
fn_spec_lt = _PythonCompilableFunctionSpec('<', 2, operator.lt, BooleanType())
fn_spec_le = _PythonCompilableFunctionSpec('<=', 2, operator.le, BooleanType())
fn_spec_and = _PythonCompilableFunctionSpec('and', 2, operator.and_, BooleanType())
fn_spec_or = _PythonCompilableFunctionSpec('or', 2, operator.or_, BooleanType())


def lit(pyvalue):
    return _Literal(pyvalue)

def min(c):
    return _Function(fn_spec_min, [c])

def max(c):
    return _Function(fn_spec_max, [c])

def mean(c):
    return _Function(fn_spec_mean, [c])

def expr(expr):
    return _GenericExpr('expr', expr)

def floor(c):
    return _Function(fn_spec_floor, [c])
