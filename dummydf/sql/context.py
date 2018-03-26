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

from .dataframe import DataFrame, _Raw
from .types import StructType, StructField, infer_data_type_from_numpy_dtype, as_numpy_dtype
import pandas

class SQLContext(object):
    def createDataFrame(self, data=None, schema=None, samplingRatio=None):
        if schema is None:
            assert data is not None
            pdf = pandas.DataFrame(data)
            assert pdf.columns > 0
            schema = StructType([
                StructField(
                    name=name,
                    dataType=infer_data_type_from_numpy_dtype(type_)
                )
                for name, type_ in zip(pdf.columns, pdf.dtypes)
            ])
        else:
            pdf = pandas.DataFrame(data or [], columns=range(0, len(schema.fields)))
            if len(pdf) > 0:
                assert len(pdf.columns) == len(schema.fields)
                pdf = pdf.astype({
                    k: as_numpy_dtype(schema.fields[i].dataType)
                    for i, k in enumerate(pdf.columns)
                })

        return DataFrame(self, schema, _Raw(pdf))
