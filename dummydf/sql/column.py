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

import ast
import numpy
from .types import infer_data_type_from_python_value


class Column(object):
    def __add__(self, that):
        from .functions import fn_spec_add
        return _Function(fn_spec_add, [self, promote_if_necessary(that)])

    def __sub__(self, that):
        from .functions import fn_spec_sub
        return _Function(fn_spec_sub, [self, promote_if_necessary(that)])

    def __mul__(self, that):
        from .functions import fn_spec_mul
        return _Function(fn_spec_mul, [self, promote_if_necessary(that)])

    def __div__(self, that):
        from .functions import fn_spec_div
        return _Function(fn_spec_div, [self, promote_if_necessary(that)])

    def __mod__(self, that):
        from .functions import fn_spec_mod
        return _Function(fn_spec_mod, [self, promote_if_necessary(that)])

    def __eq__(self, that):
        from .functions import fn_spec_eq
        return _Function(fn_spec_eq, [self, promote_if_necessary(that)])

    def __ne__(self, that):
        from .functions import fn_spec_ne
        return _Function(fn_spec_ne, [self, promote_if_necessary(that)])

    def __gt__(self, that):
        from .functions import fn_spec_gt
        return _Function(fn_spec_gt, [self, promote_if_necessary(that)])

    def __ge__(self, that):
        from .functions import fn_spec_ge
        return _Function(fn_spec_ge, [self, promote_if_necessary(that)])

    def __lt__(self, that):
        from .functions import fn_spec_lt
        return _Function(fn_spec_lt, [self, promote_if_necessary(that)])

    def __le__(self, that):
        from .functions import fn_spec_le
        return _Function(fn_spec_le, [self, promote_if_necessary(that)])

    def __and__(self, that):
        from .functions import fn_spec_and
        return _Function(fn_spec_and, [self, promote_if_necessary(that)])

    def __or__(self, that):
        from .functions import fn_spec_or
        return _Function(fn_spec_or, [self, promote_if_necessary(that)])


    def alias(self, name):
        return _Alias(self, name)


class _Alias(Column):
    def __init__(self, inner, name):
        self.inner = inner
        self.name = name

    def __str__(self):
        return self.name


class _DataFrameColumn(Column):
    def __init__(self, df, field, index):
        self.df = df
        self.field = field
        self.index = index

    def __str__(self):
        return self.field.name


class _AggregationStarColumn(Column):
    def __init__(self, dataframe):
        self.dataframe = dataframe
    
    def __str__(self):
        return '*'


class _Literal(Column):
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return self.value


class _GenericExpr(Column):
    def __init__(self, expr_str):
        self.expr_str = expr_str

    def __str__(self):
        return self.expr_str


class FunctionSpec(object):
    def __init__(self, name, arity):
        self.name = name
        self.arity = arity

    def infer_data_type(self, cols):
        raise NotImplementedError()


class _Function(Column):
    def __init__(self, spec, operands):
        assert spec.arity == len(operands)
        self.spec = spec
        self.operands = operands

    def _infer_data_type(self):
        return self.spec.infer_data_type(self.operands)

    def __str__(self):
        return '{0}({1})'.format(
            self.spec.name,
            ','.join(str(oper) for oper in self.operands),
        )


class _PythonCompilableFunctionSpec(FunctionSpec):

    def __init__(self, name, arity, callable, dataType):
        super(_PythonCompilableFunctionSpec, self).__init__(name, arity)
        self.callable = callable
        self.dataType = dataType

    def infer_data_type(self, cols):
        return self.dataType


def resolve_alias(col):
    while isinstance(col, _Alias):
        col = col.inner
    return col


def infer_data_type(col):
    if isinstance(col, _DataFrameColumn):
        return col.field.dataType
    elif isinstance(col, _Literal):
        return infer_data_type_from_python_value(col.value)
    elif isinstance(col, _Function):
        return col._infer_data_type()
    elif isinstance(col, _Alias):
        return infer_data_type(col.inner)
    else:
        raise ValueError()


def create_literal_node(value):
    dt = numpy.dtype(value.__class__)
    if dt.char in 'US':
        return ast.Str(value, lineno=0, col_offset=0)
    elif dt.char in 'lqdf':
        return ast.Num(value, lineno=0, col_offset=0)
    elif dt.name == 'bool':
        return ast.Name('True' if value else 'False', ctx=Load(), lineno=0, col_offset=0)
    else:
        raise ValueError()


def compile_col_to_ast(col):
    if isinstance(col, _Function):
        if isinstance(col.spec, _PythonCompilableFunctionSpec):
            return ast.Call(
                ast.Num(col.spec.callable, col_offset=0, lineno=0),
                [
                    compile_col_to_ast(oper) 
                    for oper in col.operands
                ],
                [], None, None,
                col_offset=0,
                lineno=0
            )
        else:
            raise TypeError()
    elif isinstance(col, _Literal):
        return create_literal_node(col.value)
    elif isinstance(col, _DataFrameColumn):
        nctx = ast.Load()
        return ast.Subscript(
            ast.Name('row', nctx, col_offset=0, lineno=0),
            ast.Index(
                ast.Attribute(
                    ast.Subscript(
                        ast.Name('df', nctx, col_offset=0, lineno=0),
                        ast.Index(
                            ast.Str(col.field.name, col_offset=0, lineno=0),
                            col_offset=0, lineno=0
                        ),
                        nctx,
                        col_offset=0, lineno=0
                    ),
                    'index',
                    nctx,
                    col_offset=0, lineno=0
                ),
                col_offset=0, lineno=0
            ),
            nctx,
            col_offset=0, lineno=0
        )
    elif isinstance(col, _Alias):
        return compile_col_to_ast(col.inner)
    else:
        raise TypeError(col.__class__.__name__)


def compile_to_raf(df, col):
    code = compile(
        ast.Expression(
            compile_col_to_ast(col),
            col_offset=0,
            lineno=0
        ),
        'compiled_from_dummy_spark_column',
        'eval'
    )
    return lambda row: eval(code, dict(df=df, row=row))


def eval_column(df, pdf, col):
    return pdf.apply(compile_to_raf(df, col), axis=1)


def promote_if_necessary(value):
    if isinstance(value, Column):
        return value
    else:
        return _Literal(value)        
