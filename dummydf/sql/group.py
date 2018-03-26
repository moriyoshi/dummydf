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

from .column import _Function, _AggregationStarColumn, infer_data_type
from .types import StructType, StructField

class GroupedData(object):
    def __init__(self, df, cols):
        self.df = df
        self.cols = cols

    def agg(self, *exprs):
        if len(exprs) == 1 and isinstance(exprs[0], dict):
            exprs = [
                _Function(
                    fn_name, [
                        _DataFrameColumn(
                            self.df,
                            self.df.schema[col_name] if col_name != '*'
                            else _AggregationStarColumn(df)
                        )
                    ]
                )
                for col_name, fn_name in exprs[0].items()
            ]
        from .dataframe import _Aggregation, DataFrame
        return DataFrame(
            self.df.sql_ctx,
            schema=StructType([
                StructField(
                    name=str(expr),
                    dataType=infer_data_type(expr)
                )
                for expr in self.cols + exprs
            ]),
            modifier=_Aggregation(
                self,
                exprs
            )
        )
