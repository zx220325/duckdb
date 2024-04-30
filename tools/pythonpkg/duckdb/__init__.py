_exported_symbols = []

# Modules
import duckdb.functional as functional
import duckdb.typing as typing
import functools

_exported_symbols.extend([
    "typing",
    "functional"
])

# Classes
from .duckdb import (
    DuckDBPyRelation,
    DuckDBPyConnection,
    Statement,
    ExplainType,
    StatementType,
    ExpectedResultType,
    PythonExceptionHandling,
    RenderMode,
    Expression,
    ConstantExpression,
    ColumnExpression,
    StarExpression,
    FunctionExpression,
    CaseExpression,
)
_exported_symbols.extend([
    "DuckDBPyRelation",
    "DuckDBPyConnection",
    "ExplainType",
    "PythonExceptionHandling",
    "Expression",
    "ConstantExpression",
    "ColumnExpression",
    "StarExpression",
    "FunctionExpression",
    "CaseExpression",
])

# These are overloaded twice, we define them inside of C++ so pybind can deal with it
_exported_symbols.extend([
    'df',
    'arrow'
])
from .duckdb import (
    df,
    arrow
)

# NOTE: this section is generated by tools/pythonpkg/scripts/generate_connection_wrapper_methods.py.
# Do not edit this section manually, your changes will be overwritten!

# START OF CONNECTION WRAPPER

def cursor(**kwargs):
    if 'connection' in kwargs:
        conn = kwargs.pop('connection')
    else:
        conn = duckdb.connect(":default:")
    return conn.cursor(**kwargs)
_exported_symbols.append('cursor')

def register_filesystem(filesystem, **kwargs):
    if 'connection' in kwargs:
        conn = kwargs.pop('connection')
    else:
        conn = duckdb.connect(":default:")
    return conn.register_filesystem(filesystem, **kwargs)
_exported_symbols.append('register_filesystem')

def unregister_filesystem(name, **kwargs):
    if 'connection' in kwargs:
        conn = kwargs.pop('connection')
    else:
        conn = duckdb.connect(":default:")
    return conn.unregister_filesystem(name, **kwargs)
_exported_symbols.append('unregister_filesystem')

def list_filesystems(**kwargs):
    if 'connection' in kwargs:
        conn = kwargs.pop('connection')
    else:
        conn = duckdb.connect(":default:")
    return conn.list_filesystems(**kwargs)
_exported_symbols.append('list_filesystems')

def filesystem_is_registered(name, **kwargs):
    if 'connection' in kwargs:
        conn = kwargs.pop('connection')
    else:
        conn = duckdb.connect(":default:")
    return conn.filesystem_is_registered(name, **kwargs)
_exported_symbols.append('filesystem_is_registered')

def create_function(name, function, parameters = None, return_type = None, **kwargs):
    if 'connection' in kwargs:
        conn = kwargs.pop('connection')
    else:
        conn = duckdb.connect(":default:")
    return conn.create_function(name, function, parameters, return_type, **kwargs)
_exported_symbols.append('create_function')

def remove_function(name, **kwargs):
    if 'connection' in kwargs:
        conn = kwargs.pop('connection')
    else:
        conn = duckdb.connect(":default:")
    return conn.remove_function(name, **kwargs)
_exported_symbols.append('remove_function')

def sqltype(type_str, **kwargs):
    if 'connection' in kwargs:
        conn = kwargs.pop('connection')
    else:
        conn = duckdb.connect(":default:")
    return conn.sqltype(type_str, **kwargs)
_exported_symbols.append('sqltype')

def dtype(type_str, **kwargs):
    if 'connection' in kwargs:
        conn = kwargs.pop('connection')
    else:
        conn = duckdb.connect(":default:")
    return conn.dtype(type_str, **kwargs)
_exported_symbols.append('dtype')

def type(type_str, **kwargs):
    if 'connection' in kwargs:
        conn = kwargs.pop('connection')
    else:
        conn = duckdb.connect(":default:")
    return conn.type(type_str, **kwargs)
_exported_symbols.append('type')

def array_type(type, size, **kwargs):
    if 'connection' in kwargs:
        conn = kwargs.pop('connection')
    else:
        conn = duckdb.connect(":default:")
    return conn.array_type(type, size, **kwargs)
_exported_symbols.append('array_type')

def list_type(type, **kwargs):
    if 'connection' in kwargs:
        conn = kwargs.pop('connection')
    else:
        conn = duckdb.connect(":default:")
    return conn.list_type(type, **kwargs)
_exported_symbols.append('list_type')

def union_type(members, **kwargs):
    if 'connection' in kwargs:
        conn = kwargs.pop('connection')
    else:
        conn = duckdb.connect(":default:")
    return conn.union_type(members, **kwargs)
_exported_symbols.append('union_type')

def string_type(collation = "", **kwargs):
    if 'connection' in kwargs:
        conn = kwargs.pop('connection')
    else:
        conn = duckdb.connect(":default:")
    return conn.string_type(collation, **kwargs)
_exported_symbols.append('string_type')

def enum_type(name, type, values, **kwargs):
    if 'connection' in kwargs:
        conn = kwargs.pop('connection')
    else:
        conn = duckdb.connect(":default:")
    return conn.enum_type(name, type, values, **kwargs)
_exported_symbols.append('enum_type')

def decimal_type(width, scale, **kwargs):
    if 'connection' in kwargs:
        conn = kwargs.pop('connection')
    else:
        conn = duckdb.connect(":default:")
    return conn.decimal_type(width, scale, **kwargs)
_exported_symbols.append('decimal_type')

def struct_type(fields, **kwargs):
    if 'connection' in kwargs:
        conn = kwargs.pop('connection')
    else:
        conn = duckdb.connect(":default:")
    return conn.struct_type(fields, **kwargs)
_exported_symbols.append('struct_type')

def row_type(fields, **kwargs):
    if 'connection' in kwargs:
        conn = kwargs.pop('connection')
    else:
        conn = duckdb.connect(":default:")
    return conn.row_type(fields, **kwargs)
_exported_symbols.append('row_type')

def map_type(key, value, **kwargs):
    if 'connection' in kwargs:
        conn = kwargs.pop('connection')
    else:
        conn = duckdb.connect(":default:")
    return conn.map_type(key, value, **kwargs)
_exported_symbols.append('map_type')

def duplicate(**kwargs):
    if 'connection' in kwargs:
        conn = kwargs.pop('connection')
    else:
        conn = duckdb.connect(":default:")
    return conn.duplicate(**kwargs)
_exported_symbols.append('duplicate')

def execute(query, parameters = None, multiple_parameter_sets = False, **kwargs):
    if 'connection' in kwargs:
        conn = kwargs.pop('connection')
    else:
        conn = duckdb.connect(":default:")
    return conn.execute(query, parameters, multiple_parameter_sets, **kwargs)
_exported_symbols.append('execute')

def executemany(query, parameters = None, **kwargs):
    if 'connection' in kwargs:
        conn = kwargs.pop('connection')
    else:
        conn = duckdb.connect(":default:")
    return conn.executemany(query, parameters, **kwargs)
_exported_symbols.append('executemany')

def close(**kwargs):
    if 'connection' in kwargs:
        conn = kwargs.pop('connection')
    else:
        conn = duckdb.connect(":default:")
    return conn.close(**kwargs)
_exported_symbols.append('close')

def interrupt(**kwargs):
    if 'connection' in kwargs:
        conn = kwargs.pop('connection')
    else:
        conn = duckdb.connect(":default:")
    return conn.interrupt(**kwargs)
_exported_symbols.append('interrupt')

def fetchone(**kwargs):
    if 'connection' in kwargs:
        conn = kwargs.pop('connection')
    else:
        conn = duckdb.connect(":default:")
    return conn.fetchone(**kwargs)
_exported_symbols.append('fetchone')

def fetchmany(size = 1, **kwargs):
    if 'connection' in kwargs:
        conn = kwargs.pop('connection')
    else:
        conn = duckdb.connect(":default:")
    return conn.fetchmany(size, **kwargs)
_exported_symbols.append('fetchmany')

def fetchall(**kwargs):
    if 'connection' in kwargs:
        conn = kwargs.pop('connection')
    else:
        conn = duckdb.connect(":default:")
    return conn.fetchall(**kwargs)
_exported_symbols.append('fetchall')

def fetchnumpy(**kwargs):
    if 'connection' in kwargs:
        conn = kwargs.pop('connection')
    else:
        conn = duckdb.connect(":default:")
    return conn.fetchnumpy(**kwargs)
_exported_symbols.append('fetchnumpy')

def fetchdf(**kwargs):
    if 'connection' in kwargs:
        conn = kwargs.pop('connection')
    else:
        conn = duckdb.connect(":default:")
    return conn.fetchdf(**kwargs)
_exported_symbols.append('fetchdf')

def fetch_df(**kwargs):
    if 'connection' in kwargs:
        conn = kwargs.pop('connection')
    else:
        conn = duckdb.connect(":default:")
    return conn.fetch_df(**kwargs)
_exported_symbols.append('fetch_df')

def fetch_df_chunk(vectors_per_chunk = 1, **kwargs):
    if 'connection' in kwargs:
        conn = kwargs.pop('connection')
    else:
        conn = duckdb.connect(":default:")
    return conn.fetch_df_chunk(vectors_per_chunk, **kwargs)
_exported_symbols.append('fetch_df_chunk')

def pl(rows_per_batch = 1000000, **kwargs):
    if 'connection' in kwargs:
        conn = kwargs.pop('connection')
    else:
        conn = duckdb.connect(":default:")
    return conn.pl(rows_per_batch, **kwargs)
_exported_symbols.append('pl')

def fetch_arrow_table(rows_per_batch = 1000000, **kwargs):
    if 'connection' in kwargs:
        conn = kwargs.pop('connection')
    else:
        conn = duckdb.connect(":default:")
    return conn.fetch_arrow_table(rows_per_batch, **kwargs)
_exported_symbols.append('fetch_arrow_table')

def fetch_record_batch(rows_per_batch = 1000000, **kwargs):
    if 'connection' in kwargs:
        conn = kwargs.pop('connection')
    else:
        conn = duckdb.connect(":default:")
    return conn.fetch_record_batch(rows_per_batch, **kwargs)
_exported_symbols.append('fetch_record_batch')

def torch(**kwargs):
    if 'connection' in kwargs:
        conn = kwargs.pop('connection')
    else:
        conn = duckdb.connect(":default:")
    return conn.torch(**kwargs)
_exported_symbols.append('torch')

def tf(**kwargs):
    if 'connection' in kwargs:
        conn = kwargs.pop('connection')
    else:
        conn = duckdb.connect(":default:")
    return conn.tf(**kwargs)
_exported_symbols.append('tf')

def begin(**kwargs):
    if 'connection' in kwargs:
        conn = kwargs.pop('connection')
    else:
        conn = duckdb.connect(":default:")
    return conn.begin(**kwargs)
_exported_symbols.append('begin')

def commit(**kwargs):
    if 'connection' in kwargs:
        conn = kwargs.pop('connection')
    else:
        conn = duckdb.connect(":default:")
    return conn.commit(**kwargs)
_exported_symbols.append('commit')

def rollback(**kwargs):
    if 'connection' in kwargs:
        conn = kwargs.pop('connection')
    else:
        conn = duckdb.connect(":default:")
    return conn.rollback(**kwargs)
_exported_symbols.append('rollback')

def checkpoint(**kwargs):
    if 'connection' in kwargs:
        conn = kwargs.pop('connection')
    else:
        conn = duckdb.connect(":default:")
    return conn.checkpoint(**kwargs)
_exported_symbols.append('checkpoint')

def append(table_name, df, **kwargs):
    if 'connection' in kwargs:
        conn = kwargs.pop('connection')
    else:
        conn = duckdb.connect(":default:")
    return conn.append(table_name, df, **kwargs)
_exported_symbols.append('append')

def register(view_name, python_object, **kwargs):
    if 'connection' in kwargs:
        conn = kwargs.pop('connection')
    else:
        conn = duckdb.connect(":default:")
    return conn.register(view_name, python_object, **kwargs)
_exported_symbols.append('register')

def unregister(view_name, **kwargs):
    if 'connection' in kwargs:
        conn = kwargs.pop('connection')
    else:
        conn = duckdb.connect(":default:")
    return conn.unregister(view_name, **kwargs)
_exported_symbols.append('unregister')

def table(table_name, **kwargs):
    if 'connection' in kwargs:
        conn = kwargs.pop('connection')
    else:
        conn = duckdb.connect(":default:")
    return conn.table(table_name, **kwargs)
_exported_symbols.append('table')

def view(view_name, **kwargs):
    if 'connection' in kwargs:
        conn = kwargs.pop('connection')
    else:
        conn = duckdb.connect(":default:")
    return conn.view(view_name, **kwargs)
_exported_symbols.append('view')

def values(values, **kwargs):
    if 'connection' in kwargs:
        conn = kwargs.pop('connection')
    else:
        conn = duckdb.connect(":default:")
    return conn.values(values, **kwargs)
_exported_symbols.append('values')

def table_function(name, parameters = None, **kwargs):
    if 'connection' in kwargs:
        conn = kwargs.pop('connection')
    else:
        conn = duckdb.connect(":default:")
    return conn.table_function(name, parameters, **kwargs)
_exported_symbols.append('table_function')

def read_json(name, **kwargs):
    if 'connection' in kwargs:
        conn = kwargs.pop('connection')
    else:
        conn = duckdb.connect(":default:")
    return conn.read_json(name, **kwargs)
_exported_symbols.append('read_json')

def extract_statements(query, **kwargs):
    if 'connection' in kwargs:
        conn = kwargs.pop('connection')
    else:
        conn = duckdb.connect(":default:")
    return conn.extract_statements(query, **kwargs)
_exported_symbols.append('extract_statements')

def sql(query, **kwargs):
    if 'connection' in kwargs:
        conn = kwargs.pop('connection')
    else:
        conn = duckdb.connect(":default:")
    return conn.sql(query, **kwargs)
_exported_symbols.append('sql')

def query(query, **kwargs):
    if 'connection' in kwargs:
        conn = kwargs.pop('connection')
    else:
        conn = duckdb.connect(":default:")
    return conn.query(query, **kwargs)
_exported_symbols.append('query')

def from_query(query, **kwargs):
    if 'connection' in kwargs:
        conn = kwargs.pop('connection')
    else:
        conn = duckdb.connect(":default:")
    return conn.from_query(query, **kwargs)
_exported_symbols.append('from_query')

def read_csv(path_or_buffer, **kwargs):
    if 'connection' in kwargs:
        conn = kwargs.pop('connection')
    else:
        conn = duckdb.connect(":default:")
    return conn.read_csv(path_or_buffer, **kwargs)
_exported_symbols.append('read_csv')

def from_csv_auto(path_or_buffer, **kwargs):
    if 'connection' in kwargs:
        conn = kwargs.pop('connection')
    else:
        conn = duckdb.connect(":default:")
    return conn.from_csv_auto(path_or_buffer, **kwargs)
_exported_symbols.append('from_csv_auto')

def from_df(df, **kwargs):
    if 'connection' in kwargs:
        conn = kwargs.pop('connection')
    else:
        conn = duckdb.connect(":default:")
    return conn.from_df(df, **kwargs)
_exported_symbols.append('from_df')

def from_arrow(arrow_object, **kwargs):
    if 'connection' in kwargs:
        conn = kwargs.pop('connection')
    else:
        conn = duckdb.connect(":default:")
    return conn.from_arrow(arrow_object, **kwargs)
_exported_symbols.append('from_arrow')

def from_parquet(file_glob, binary_as_string = False, **kwargs):
    if 'connection' in kwargs:
        conn = kwargs.pop('connection')
    else:
        conn = duckdb.connect(":default:")
    return conn.from_parquet(file_glob, binary_as_string, **kwargs)
_exported_symbols.append('from_parquet')

def read_parquet(file_glob, binary_as_string = False, **kwargs):
    if 'connection' in kwargs:
        conn = kwargs.pop('connection')
    else:
        conn = duckdb.connect(":default:")
    return conn.read_parquet(file_glob, binary_as_string, **kwargs)
_exported_symbols.append('read_parquet')

def from_substrait(proto, **kwargs):
    if 'connection' in kwargs:
        conn = kwargs.pop('connection')
    else:
        conn = duckdb.connect(":default:")
    return conn.from_substrait(proto, **kwargs)
_exported_symbols.append('from_substrait')

def get_substrait(query, **kwargs):
    if 'connection' in kwargs:
        conn = kwargs.pop('connection')
    else:
        conn = duckdb.connect(":default:")
    return conn.get_substrait(query, **kwargs)
_exported_symbols.append('get_substrait')

def get_substrait_json(query, **kwargs):
    if 'connection' in kwargs:
        conn = kwargs.pop('connection')
    else:
        conn = duckdb.connect(":default:")
    return conn.get_substrait_json(query, **kwargs)
_exported_symbols.append('get_substrait_json')

def from_substrait_json(json, **kwargs):
    if 'connection' in kwargs:
        conn = kwargs.pop('connection')
    else:
        conn = duckdb.connect(":default:")
    return conn.from_substrait_json(json, **kwargs)
_exported_symbols.append('from_substrait_json')

def get_table_names(query, **kwargs):
    if 'connection' in kwargs:
        conn = kwargs.pop('connection')
    else:
        conn = duckdb.connect(":default:")
    return conn.get_table_names(query, **kwargs)
_exported_symbols.append('get_table_names')

def install_extension(extension, **kwargs):
    if 'connection' in kwargs:
        conn = kwargs.pop('connection')
    else:
        conn = duckdb.connect(":default:")
    return conn.install_extension(extension, **kwargs)
_exported_symbols.append('install_extension')

def load_extension(extension, **kwargs):
    if 'connection' in kwargs:
        conn = kwargs.pop('connection')
    else:
        conn = duckdb.connect(":default:")
    return conn.load_extension(extension, **kwargs)
_exported_symbols.append('load_extension')

def project(df, project_expr, **kwargs):
    if 'connection' in kwargs:
        conn = kwargs.pop('connection')
    else:
        conn = duckdb.connect(":default:")
    return conn.from_df(df).project(project_expr, **kwargs)
_exported_symbols.append('project')

def distinct(df, **kwargs):
    if 'connection' in kwargs:
        conn = kwargs.pop('connection')
    else:
        conn = duckdb.connect(":default:")
    return conn.from_df(df).distinct(**kwargs)
_exported_symbols.append('distinct')

def write_csv(df, *args, **kwargs):
    if 'connection' in kwargs:
        conn = kwargs.pop('connection')
    else:
        conn = duckdb.connect(":default:")
    return conn.from_df(df).write_csv(*args, **kwargs)
_exported_symbols.append('write_csv')

def aggregate(df, aggr_expr, group_expr = "", **kwargs):
    if 'connection' in kwargs:
        conn = kwargs.pop('connection')
    else:
        conn = duckdb.connect(":default:")
    return conn.from_df(df).aggregate(aggr_expr, group_expr, **kwargs)
_exported_symbols.append('aggregate')

def alias(df, alias, **kwargs):
    if 'connection' in kwargs:
        conn = kwargs.pop('connection')
    else:
        conn = duckdb.connect(":default:")
    return conn.from_df(df).set_alias(alias, **kwargs)
_exported_symbols.append('alias')

def filter(df, filter_expr, **kwargs):
    if 'connection' in kwargs:
        conn = kwargs.pop('connection')
    else:
        conn = duckdb.connect(":default:")
    return conn.from_df(df).filter(filter_expr, **kwargs)
_exported_symbols.append('filter')

def limit(df, n, offset = 0, **kwargs):
    if 'connection' in kwargs:
        conn = kwargs.pop('connection')
    else:
        conn = duckdb.connect(":default:")
    return conn.from_df(df).limit(n, offset, **kwargs)
_exported_symbols.append('limit')

def order(df, order_expr, **kwargs):
    if 'connection' in kwargs:
        conn = kwargs.pop('connection')
    else:
        conn = duckdb.connect(":default:")
    return conn.from_df(df).order(order_expr, **kwargs)
_exported_symbols.append('order')

def query_df(df, virtual_table_name, sql_query, **kwargs):
    if 'connection' in kwargs:
        conn = kwargs.pop('connection')
    else:
        conn = duckdb.connect(":default:")
    return conn.from_df(df).query(virtual_table_name, sql_query, **kwargs)
_exported_symbols.append('query_df')

def description(**kwargs):
    if 'connection' in kwargs:
        conn = kwargs.pop('connection')
    else:
        conn = duckdb.connect(":default:")
    return conn.description
_exported_symbols.append('description')

def rowcount(**kwargs):
    if 'connection' in kwargs:
        conn = kwargs.pop('connection')
    else:
        conn = duckdb.connect(":default:")
    return conn.rowcount
_exported_symbols.append('rowcount')
# END OF CONNECTION WRAPPER

# Enums
from .duckdb import (
    ANALYZE,
    DEFAULT,
    RETURN_NULL,
    STANDARD,
    COLUMNS,
    ROWS
)
_exported_symbols.extend([
    "ANALYZE",
    "DEFAULT",
    "RETURN_NULL",
    "STANDARD"
])


# read-only properties
from .duckdb import (
    __standard_vector_size__,
    __interactive__,
    __jupyter__,
    __version__,
    apilevel,
    comment,
    default_connection,
    identifier,
    keyword,
    numeric_const,
    operator,
    paramstyle,
    string_const,
    threadsafety,
    token_type,
    tokenize
)
_exported_symbols.extend([
    "__standard_vector_size__",
    "__interactive__",
    "__jupyter__",
    "__version__",
    "apilevel",
    "comment",
    "default_connection",
    "identifier",
    "keyword",
    "numeric_const",
    "operator",
    "paramstyle",
    "string_const",
    "threadsafety",
    "token_type",
    "tokenize"
])


from .duckdb import (
    connect
)

_exported_symbols.extend([
    "connect"
])

# Exceptions
from .duckdb import (
    Error,
    DataError,
    ConversionException,
    OutOfRangeException,
    TypeMismatchException,
    FatalException,
    IntegrityError,
    ConstraintException,
    InternalError,
    InternalException,
    InterruptException,
    NotSupportedError,
    NotImplementedException,
    OperationalError,
    ConnectionException,
    IOException,
    HTTPException,
    OutOfMemoryException,
    SerializationException,
    TransactionException,
    PermissionException,
    ProgrammingError,
    BinderException,
    CatalogException,
    InvalidInputException,
    InvalidTypeException,
    ParserException,
    SyntaxException,
    SequenceException,
    Warning
)
_exported_symbols.extend([
    "Error",
    "DataError",
    "ConversionException",
    "OutOfRangeException",
    "TypeMismatchException",
    "FatalException",
    "IntegrityError",
    "ConstraintException",
    "InternalError",
    "InternalException",
    "InterruptException",
    "NotSupportedError",
    "NotImplementedException",
    "OperationalError",
    "ConnectionException",
    "IOException",
    "HTTPException",
    "OutOfMemoryException",
    "SerializationException",
    "TransactionException",
    "PermissionException",
    "ProgrammingError",
    "BinderException",
    "CatalogException",
    "InvalidInputException",
    "InvalidTypeException",
    "ParserException",
    "SyntaxException",
    "SequenceException",
    "Warning"
])

# Value
from .value.constant import (
    Value,
    NullValue,
    BooleanValue,
    UnsignedBinaryValue,
    UnsignedShortValue,
    UnsignedIntegerValue,
    UnsignedLongValue,
    BinaryValue,
    ShortValue,
    IntegerValue,
    LongValue,
    HugeIntegerValue,
    FloatValue,
    DoubleValue,
    DecimalValue,
    StringValue,
    UUIDValue,
    BitValue,
    BlobValue,
    DateValue,
    IntervalValue,
    TimestampValue,
    TimestampSecondValue,
    TimestampMilisecondValue,
    TimestampNanosecondValue,
    TimestampTimeZoneValue,
    TimeValue,
    TimeTimeZoneValue,
)

_exported_symbols.extend([
    "Value",
    "NullValue",
    "BooleanValue",
    "UnsignedBinaryValue",
    "UnsignedShortValue",
    "UnsignedIntegerValue",
    "UnsignedLongValue",
    "BinaryValue",
    "ShortValue",
    "IntegerValue",
    "LongValue",
    "HugeIntegerValue",
    "FloatValue",
    "DoubleValue",
    "DecimalValue",
    "StringValue",
    "UUIDValue",
    "BitValue",
    "BlobValue",
    "DateValue",
    "IntervalValue",
    "TimestampValue",
    "TimestampSecondValue",
    "TimestampMilisecondValue",
    "TimestampNanosecondValue",
    "TimestampTimeZoneValue",
    "TimeValue",
    "TimeTimeZoneValue",
])

__all__ = _exported_symbols
