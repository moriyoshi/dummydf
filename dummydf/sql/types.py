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

import six
import numpy


class DataType(object):
    _numpy_dtype = numpy.dtype(object)

    def __repr__(self):
        return '{0}()'.format(self.__class__.__name__)


class AtomicType(DataType):
    pass


class NumericType(AtomicType):
    pass


class FractionalType(NumericType):
    pass


class IntegralType(NumericType):
    pass


class DoubleType(FractionalType):
    _numpy_dtype = numpy.dtype(numpy.float64)


class LongType(IntegralType):
    _numpy_dtype = numpy.dtype(numpy.int64)


class BooleanType(AtomicType):
    pass


class StringType(AtomicType):
    _numpy_dtype = numpy.dtype(str)


class StructType(DataType):
    def __init__(self, fields=None):
        self.fields = fields

    @property
    def _numpy_dtype(self):
        return numpy.dtype([
            (field.name, as_numpy_dtype(field.dataType))
            for field in fields
        ])

    def __repr__(self):
        return '{0}({1!r})'.format(self.__class__.__name__, self.fields)


class StructField(DataType):
    def __init__(self, name, dataType, nullable=True, metadata=None):
        self.name = name
        self.dataType = dataType
        self.nullable = nullable
        self.metadata = metadata

    def __repr__(self):
        return '{0}(name={1.name!r}, dataType={1.dataType!r}, nullable={1.nullable!r}, metadata={1.metadata!r})'.format(self.__class__.__name__, self)


def infer_data_type_from_numpy_dtype(dtype):
    if dtype.name == 'int64':
        return LongType()
    elif dtype.name == 'float64':
        return DoubleType()
    elif dtype.name == 'S' or dtype.name == '<U':
        return StringType()
    else:
        raise ValueError()

def infer_data_type_from_python_value(value):
    if isinstance(value, (int, long)):
        return LongType()
    elif isinstance(value, float):
        return DoubleType()
    elif isinstance(value, six.text_types):
        return StringType()
    else:
        raise ValueError()

def as_numpy_dtype(data_type):
    return data_type._numpy_dtype
