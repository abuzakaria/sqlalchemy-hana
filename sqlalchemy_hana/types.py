# Copyright 2015 SAP SE.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http: //www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
# either express or implied. See the License for the specific
# language governing permissions and limitations under the License.

from sqlalchemy import types as sqltypes
from sqlalchemy.sql import expression, default_comparator, operators


class TINYINT(sqltypes.TypeEngine):

    __visit_name__ = "TINYINT"


class DOUBLE(sqltypes.Float):

    __visit_name__ = "DOUBLE"


class BOOLEAN(sqltypes.Boolean):

    def get_dbapi_type(self, dbapi):
        return dbapi.NUMBER


class DATE(sqltypes.Date):

    def literal_processor(self, dialect):
        self.bind_processor(dialect)

        def process(value):
            return "to_date('%s')" % value
        return process


class TIME(sqltypes.Time):

    def literal_processor(self, dialect):
        self.bind_processor(dialect)

        def process(value):
            return "to_time('%s')" % value
        return process


class TIMESTAMP(sqltypes.DateTime):

    def literal_processor(self, dialect):
        self.bind_processor(dialect)

        def process(value):
            return "to_timestamp('%s')" % value
        return process


# LOB stuff setup matches lob code in sqlalchemy/dialects/oracle/cx_oracle.py

class _LOBMixin(object):
    def result_processor(self, dialect, coltype):
        if not dialect.auto_convert_lobs:
            # return the cx_oracle.LOB directly.
            return None

        def process(value):
            if value is not None:
                return value.read()
            else:
                return value
        return process


class HanaText(_LOBMixin, sqltypes.Text):
    def get_dbapi_type(self, dbapi):
        return dbapi.CLOB


class HanaUnicodeText(_LOBMixin, sqltypes.UnicodeText):
    def get_dbapi_type(self, dbapi):
        return dbapi.NCLOB

    def result_processor(self, dialect, coltype):
        lob_processor = _LOBMixin.result_processor(self, dialect, coltype)
        if lob_processor is None:
            return None

        string_processor = sqltypes.UnicodeText.result_processor(
            self, dialect, coltype)

        if string_processor is None:
            return lob_processor
        else:
            def process(value):
                return string_processor(lob_processor(value))
            return process


class HanaBinary(_LOBMixin, sqltypes.LargeBinary):
    def get_dbapi_type(self, dbapi):
        return dbapi.BLOB

    def bind_processor(self, dialect):
        return None


class NCLOB(sqltypes.Text):
    __visit_name__ = 'NCLOB'


class ARRAY(sqltypes.Concatenable, sqltypes.TypeEngine):

    """Postgresql ARRAY type.

    Represents values as Python lists.

    An :class:`.ARRAY` type is constructed given the "type"
    of element::

        mytable = Table("mytable", metadata,
                Column("data", ARRAY(Integer))
            )

    The above type represents an N-dimensional array,
    meaning Postgresql will interpret values with any number
    of dimensions automatically.   To produce an INSERT
    construct that passes in a 1-dimensional array of integers::

        connection.execute(
                mytable.insert(),
                data=[1,2,3]
        )

    The :class:`.ARRAY` type can be constructed given a fixed number
    of dimensions::

        mytable = Table("mytable", metadata,
                Column("data", ARRAY(Integer, dimensions=2))
            )

    This has the effect of the :class:`.ARRAY` type
    specifying that number of bracketed blocks when a :class:`.Table`
    is used in a CREATE TABLE statement, or when the type is used
    within a :func:`.expression.cast` construct; it also causes
    the bind parameter and result set processing of the type
    to optimize itself to expect exactly that number of dimensions.
    Note that Postgresql itself still allows N dimensions with such a type.

    SQL expressions of type :class:`.ARRAY` have support for "index" and
    "slice" behavior.  The Python ``[]`` operator works normally here, given
    integer indexes or slices.  Note that Postgresql arrays default
    to 1-based indexing.  The operator produces binary expression
    constructs which will produce the appropriate SQL, both for
    SELECT statements::

        select([mytable.c.data[5], mytable.c.data[2:7]])

    as well as UPDATE statements when the :meth:`.Update.values` method
    is used::

        mytable.update().values({
            mytable.c.data[5]: 7,
            mytable.c.data[2:7]: [1, 2, 3]
        })

    .. note::

        Multi-dimensional support for the ``[]`` operator is not supported
        in SQLAlchemy 1.0.  Please use the :func:`.type_coerce` function
        to cast an intermediary expression to ARRAY again as a workaround::

            expr = type_coerce(my_array_column[5], ARRAY(Integer))[6]

        Multi-dimensional support will be provided in a future release.

    :class:`.ARRAY` provides special methods for containment operations,
    e.g.::

        mytable.c.data.contains([1, 2])

    For a full list of special methods see :class:`.ARRAY.Comparator`.

    .. versionadded:: 0.8 Added support for index and slice operations
       to the :class:`.ARRAY` type, including support for UPDATE
       statements, and special array containment operations.

    The :class:`.ARRAY` type may not be supported on all DBAPIs.
    It is known to work on psycopg2 and not pg8000.

    Additionally, the :class:`.ARRAY` type does not work directly in
    conjunction with the :class:`.ENUM` type.  For a workaround, see the
    special type at :ref:`postgresql_array_of_enum`.

    See also:

    :class:`.postgresql.array` - produce a literal array value.

    """
    __visit_name__ = 'ARRAY'

    class Comparator(sqltypes.Concatenable.Comparator):

        """Define comparison operations for :class:`.ARRAY`."""

        def __getitem__(self, index):
            shift_indexes = 1 if self.expr.type.zero_indexes else 0
            if isinstance(index, slice):
                if shift_indexes:
                    index = slice(
                        index.start + shift_indexes,
                        index.stop + shift_indexes,
                        index.step
                    )
                index = _Slice(index, self)
                return_type = self.type
            else:
                index += shift_indexes
                return_type = self.type.item_type

            return default_comparator._binary_operate(
                self.expr, operators.getitem, index,
                result_type=return_type)

        def any(self, other, operator=operators.eq):
            """Return ``other operator ANY (array)`` clause.

            Argument places are switched, because ANY requires array
            expression to be on the right hand-side.

            E.g.::

                from sqlalchemy.sql import operators

                conn.execute(
                    select([table.c.data]).where(
                            table.c.data.any(7, operator=operators.lt)
                        )
                )

            :param other: expression to be compared
            :param operator: an operator object from the
             :mod:`sqlalchemy.sql.operators`
             package, defaults to :func:`.operators.eq`.

            .. seealso::

                :class:`.postgresql.Any`

                :meth:`.postgresql.ARRAY.Comparator.all`

            """
            return Any(other, self.expr, operator=operator)

        def all(self, other, operator=operators.eq):
            """Return ``other operator ALL (array)`` clause.

            Argument places are switched, because ALL requires array
            expression to be on the right hand-side.

            E.g.::

                from sqlalchemy.sql import operators

                conn.execute(
                    select([table.c.data]).where(
                            table.c.data.all(7, operator=operators.lt)
                        )
                )

            :param other: expression to be compared
            :param operator: an operator object from the
             :mod:`sqlalchemy.sql.operators`
             package, defaults to :func:`.operators.eq`.

            .. seealso::

                :class:`.postgresql.All`

                :meth:`.postgresql.ARRAY.Comparator.any`

            """
            return All(other, self.expr, operator=operator)

        def contains(self, other, **kwargs):
            """Boolean expression.  Test if elements are a superset of the
            elements of the argument array expression.
            """
            return self.expr.op('@>')(other)

        def contained_by(self, other):
            """Boolean expression.  Test if elements are a proper subset of the
            elements of the argument array expression.
            """
            return self.expr.op('<@')(other)

        def overlap(self, other):
            """Boolean expression.  Test if array has elements in common with
            an argument array expression.
            """
            return self.expr.op('&&')(other)

        def _adapt_expression(self, op, other_comparator):
            if isinstance(op, operators.custom_op):
                if op.opstring in ['@>', '<@', '&&']:
                    return op, sqltypes.Boolean
            return sqltypes.Concatenable.Comparator.\
                _adapt_expression(self, op, other_comparator)

    comparator_factory = Comparator

    def __init__(self, item_type, as_tuple=False, dimensions=None,
                 zero_indexes=False):
        """Construct an ARRAY.

        E.g.::

          Column('myarray', ARRAY(Integer))

        Arguments are:

        :param item_type: The data type of items of this array. Note that
          dimensionality is irrelevant here, so multi-dimensional arrays like
          ``INTEGER[][]``, are constructed as ``ARRAY(Integer)``, not as
          ``ARRAY(ARRAY(Integer))`` or such.

        :param as_tuple=False: Specify whether return results
          should be converted to tuples from lists. DBAPIs such
          as psycopg2 return lists by default. When tuples are
          returned, the results are hashable.

        :param dimensions: if non-None, the ARRAY will assume a fixed
         number of dimensions.  This will cause the DDL emitted for this
         ARRAY to include the exact number of bracket clauses ``[]``,
         and will also optimize the performance of the type overall.
         Note that PG arrays are always implicitly "non-dimensioned",
         meaning they can store any number of dimensions no matter how
         they were declared.

        :param zero_indexes=False: when True, index values will be converted
         between Python zero-based and Postgresql one-based indexes, e.g.
         a value of one will be added to all index values before passing
         to the database.

         .. versionadded:: 0.9.5

        """
        if isinstance(item_type, ARRAY):
            raise ValueError("Do not nest ARRAY types; ARRAY(basetype) "
                             "handles multi-dimensional arrays of basetype")
        if isinstance(item_type, type):
            item_type = item_type()
        self.item_type = item_type
        self.as_tuple = as_tuple
        self.dimensions = dimensions
        self.zero_indexes = zero_indexes

    @property
    def python_type(self):
        return list

    def compare_values(self, x, y):
        return x == y

    def _proc_array(self, arr, itemproc, dim, collection):
        if dim is None:
            arr = list(arr)
        if dim == 1 or dim is None and (
                # this has to be (list, tuple), or at least
                # not hasattr('__iter__'), since Py3K strings
                # etc. have __iter__
                not arr or not isinstance(arr[0], (list, tuple))):
            if itemproc:
                return collection(itemproc(x) for x in arr)
            else:
                return collection(arr)
        else:
            return collection(
                self._proc_array(
                    x, itemproc,
                    dim - 1 if dim is not None else None,
                    collection)
                for x in arr
            )

    def bind_processor(self, dialect):
        item_proc = self.item_type.\
            dialect_impl(dialect).\
            bind_processor(dialect)

        def process(value):
            if value is None:
                return value
            else:
                return self._proc_array(
                    value,
                    item_proc,
                    self.dimensions,
                    list)
        return process

    def result_processor(self, dialect, coltype):
        item_proc = self.item_type.\
            dialect_impl(dialect).\
            result_processor(dialect, coltype)

        def process(value):
            if value is None:
                return value
            else:
                return self._proc_array(
                    value,
                    item_proc,
                    self.dimensions,
                    tuple if self.as_tuple else list)
        return process


class _Slice(expression.ColumnElement):
    __visit_name__ = 'slice'
    type = sqltypes.NULLTYPE

    def __init__(self, slice_, source_comparator):
        self.start = default_comparator._check_literal(
            source_comparator.expr,
            operators.getitem, slice_.start)
        self.stop = default_comparator._check_literal(
            source_comparator.expr,
            operators.getitem, slice_.stop)


class All(expression.ColumnElement):

    """Represent the clause ``left operator ALL (right)``.  ``right`` must be
    an array expression.

    .. seealso::

        :class:`.postgresql.ARRAY`

        :meth:`.postgresql.ARRAY.Comparator.all` - ARRAY-bound method

    """
    __visit_name__ = 'all'

    def __init__(self, left, right, operator=operators.eq):
        self.type = sqltypes.Boolean()
        self.left = expression._literal_as_binds(left)
        self.right = right
        self.operator = operator


class Any(expression.ColumnElement):

    """Represent the clause ``left operator ANY (right)``.  ``right`` must be
    an array expression.

    .. seealso::

        :class:`.postgresql.ARRAY`

        :meth:`.postgresql.ARRAY.Comparator.any` - ARRAY-bound method

    """
    __visit_name__ = 'any'

    def __init__(self, left, right, operator=operators.eq):
        self.type = sqltypes.Boolean()
        self.left = expression._literal_as_binds(left)
        self.right = right
        self.operator = operator