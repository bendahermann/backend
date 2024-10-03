from functools import reduce
import hashlib
from delta.tables import DeltaTable as dt
from operator import itemgetter
from pyspark.sql import (functions as F, types as T, 
    Row, Window as W, DataFrame as spk_DF, Column) 
from typing import Union
import re
import os
from collections import OrderedDict
from uuid import uuid4
from pyspark.sql.utils import AnalysisException

from config import TABLE_MAPPER, PATH_BUILDER, HIGH_ENVS, HOURS_DELTA

from datetime import timedelta, datetime
import pytz

def upsert_delta(spark, new_tbl: spk_DF, base_path, how, 
                 on_cols:Union[list, dict]): 
    if how == 'simple': 
        if not isinstance(on_cols, list): 
            raise Exception("ON_COLS must be of type LIST.")
        on_str = " AND ".join(f"(t1.{col} = t0.{col})" for col in on_cols)
        
        (dt.forPath(spark, base_path).alias('t0')
            .merge(new_tbl.alias('t1'), on_str)
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute())
        
    elif how == 'delete': 
        if not isinstance(on_cols, dict): 
            raise Exception("ON_COLS must be of type DICT.")
        ids_cc, cmp_cc = map(on_cols.get, ['ids', 'cmp'])
        s_delete = (spark.read
            .load(base_path)
            .join(new_tbl, how='leftsemi', on=ids_cc)
            .join(new_tbl, how='leftanti', on=ids_cc + cmp_cc)
            .withColumn('to_delete', F.lit(True)))
        t_merge = (new_tbl
            .withColumn('to_delete', F.lit(False))
            .unionByName(s_delete))
        mrg_condtn = ' AND '.join(f"(t1.{cc} = t0.{cc})" for cc in ids_cc+cmp_cc) 
        new_values = {cc: F.col(f"t1.{cc}") for cc in new_tbl.columns}
        
        (dt.forPath(spark, base_path).alias('t0')
            .merge(t_merge.alias('t1'), mrg_condtn)
            .whenMatchedDelete('to_delete')
            .whenMatchedUpdate(set=new_values)
            .whenNotMatchedInsert(values=new_values)
            .execute())
    return 

    
def with_columns(a_df: spk_DF, cols_dict: dict) -> spk_DF: 
    ff = lambda x_df, kk_vv: x_df.withColumn(*kk_vv)
    b_df = reduce(ff, cols_dict.items(), a_df)
    return b_df


def struct_typer(the_cols: Union[dict, list], spk_type='str') -> T.StructType: 
    if isinstance(the_cols, dict): 
        the_fields = [T.StructField(kk, set_type(vv), True)
                for kk, vv in the_cols.items()]
    elif isinstance(the_cols, (list, set)): 
        the_fields = [T.StructField(xx, set_type(spk_type), True)
                for xx in the_cols]
    return T.StructType(the_fields)


def set_type(key: Union[str, tuple]): 
    base_types = {
        'date'  : T.DateType,      'ts'  : T.TimestampType, 'bool': T.BooleanType, 'str': T.StringType, 
        'date_2': T.StringType,    'int' : T.StringType,    'long': T.LongType,
        'date_3': T.TimestampType, 'map' : T.StringType,    'hex' : T.StringType}
    # date_2: 'yyyymmdd', date_1: yyyy-mm-dd. 
    arg_types = {
        'dbl':   (T.DecimalType, (20, 2)), 
        'map_t': (T.MapType, (T.StringType(), T.StringType()))}
    
    if isinstance(key, str): 
        (key, args) = key, None
    elif isinstance(key, tuple) and (len(key) == 2): 
        (key, args) = key
    else: 
        raise Exception(f"KEY {key} must be STR or TUPLE(2)")
    
    if key in base_types: 
        the_type = base_types[key]()
    elif key in arg_types: 
        a_type, dflt = arg_types[key]
        args = args if args is not None else dflt
        the_type = a_type(*args)
    else: 
        raise Exception(f"Key {key} is not recognized.")
    return the_type

def camel_to_snake(str):
    try:
        new_str = re.sub(r'(?<!^)(?=[A-Z])', '_', str).lower()
        if "ID" in str:
            new_str = new_str.replace('i_d','id')
        if "TS" in str:
            new_str = new_str.replace('t_s','ts')
        return new_str
    except:
        return str

def schema_fields_dict(df):
    sch_names = df.schema.names
    sch_dtypes = [str(f.dataType).replace("Type","").upper() for f in df.schema.fields]

    dict_fields = dict(zip(sch_names,sch_dtypes))
    for k,v in dict_fields.items():
        print(f"{k} {v},")


def extract_date_time(a_col: Union[str, Column], a_format):
    if a_format == r'\Date': 
        b_col = F.to_timestamp(F.regexp_extract(a_col, r'\d+', 0)/1000)
    elif a_format == r'M/d/y H:m:S': 
        b_col = F.to_timestamp(a_col, r'Mdy hmS a')
    else: 
        raise r'Allowed formats are: [\Date, Mdy hmS a]'
    return b_col




def post_format(a_df, col_types) -> spk_DF:

    def one_col(_col, _type): 
        if  _type == 'int': 
            _with = F.col(_col).cast(T.IntegerType())
        elif _type == 'long': 
            _with = F.col(_col).cast(T.LongType())
        elif _type == 'date_2': 
            _with = F.to_date(_col, 'yyyyMMdd')
        elif _type == 'date_3': 
            _with = F.to_date(_col)
        else: 
            _with = None
        return _with

    withers = {kk: one_col(kk, vv) 
        for kk, vv in col_types.items()
        if one_col(kk, vv) is not None}
    return with_columns(a_df, withers)


def rename_keys(a_dict, keys): 
    b_dict = a_dict.copy()
    for ka, kb in keys.items(): 
        b_dict[kb] = b_dict.pop(ka)
    return b_dict

## Modificado por otros desarrolladores.  

def get_staged_updates(dt, df):
    """
    """
    recent_registries = df.where(F.col("end_date").isNull())
    insert_registries = df.where(F.col("end_date").isNotNull())
    
    new_to_insert = (recent_registries.alias("updates")
        .join(dt.toDF().alias("satellite"), on="hash_id")
        .where("satellite.end_date is NULL AND updates.hash_diff <> satellite.hash_diff"))
    
    staged_updates = (new_to_insert
        .selectExpr("NULL as merge_key", "updates.*") 
        .unionByName(recent_registries
        .selectExpr("hash_id AS merge_key", "*")) 
        .unionByName(insert_registries
        .selectExpr("NULL as merge_key", "*"))) 
    return staged_updates
    
    
def merge_satellite_into_delta(df, dt):
    """
    """
    staged_updates = get_staged_updates(dt, df)
    
    (dt.alias("clients")
        .merge(staged_updates.alias("staged_updates"),
            condition="clients.hash_id = staged_updates.merge_key")
         .whenMatchedUpdate(
            condition="clients.end_date is NULL AND clients.hash_diff <> staged_updates.hash_diff", 
             # el end_date debería estar vacio solo para UN registro con su hash_id unico
            set={"end_date": "current_date()"})
         .whenNotMatchedInsertAll().execute())
    
    
def batch_recent_retrieval(df):
    """
    """
    windowSpec = (W.partitionBy("hash_id")
        .orderBy(F.desc("event_datetime")))
    retrievals = (df
        .withColumn("row_number", F.row_number().over(windowSpec))
        .withColumn("end_date", F.when(F.col("row_number") > 1, F.current_timestamp()))
        .drop("row_number"))
    return retrievals
    
    
def deduplicate_id_diff_rows(df: spk_DF) -> spk_DF:
    """
    """
    window_id_diff = (W.partitionBy("hash_id", "hash_diff")
                      .orderBy(F.desc("event_datetime")))
    deduplicated = (df
                    .withColumn("row_number", F.row_number().over(window_id_diff))
                    .filter("row_number = 1")
                    .drop("row_number"))
    
    return deduplicated


def retrieve_delta_table(spark, dt_path):
    """
    """
    return dt.forPath(spark, dt_path)


def add_hashdiff(df):
    """
    creates a new column which is the computation of the hash code of all the dataframe columns with exception of
    column names included in the exclude_cols variable
    """
    def create_hashdiff(row, 
            exclude_cols=["hash_id", "load_date", "end_date", 'event_datetime', 'created_datetime']):
        row_dict = row.asDict()                                                  
        concat_str = ''
        allowed_cols = sorted(set(row_dict.keys()) - set(exclude_cols))
        for v in allowed_cols:
            concat_str = concat_str + '||' + str(row_dict[v])         # concatenate values
        concat_str = concat_str[2:] 
        row_dict["hash_raw"] = concat_str                 # preserve concatenated value for testing (this can be removed later)
        row_dict["hash_diff"] = hashlib.sha256(concat_str.encode("utf-8")).hexdigest()
        return Row(**row_dict)

    _schema = df.alias("df2").schema # get a cloned schema from dataframe
    (_schema
        .add("hash_raw", T.StringType(), False)
        .add("hash_diff", T.StringType(), False)) # modified inplace
    the_hashdiff = df.rdd.map(create_hashdiff).toDF(_schema)
    return the_hashdiff


def concat_column_values(df, 
    exclude_cols=["hash_id", "load_date", "end_date", 'event_datetime', 'created_datetime']):
    prefix = "casted"
    original_columns = df.columns
    set_original_columns = set(original_columns)
    set_exclude_cols = set(exclude_cols)
    allowed_cols = sorted(set_original_columns - set_exclude_cols)
    prefix_allowed_cols = [f"{prefix}_{c}" for c in allowed_cols]
    print(f"Using columns: {allowed_cols}")
    
    intermediate_df = (df.select(df.columns 
        + [F.col(c).cast("string").alias(f"{prefix}_{c}") for c in allowed_cols])
        .fillna("", subset=prefix_allowed_cols))
    
    subset_df = (intermediate_df
        .withColumn("hash_raw", F.concat_ws("||", *prefix_allowed_cols))
        .withColumn("hash_diff", F.sha2(F.concat_ws("||", *prefix_allowed_cols), 256))
        .drop(*prefix_allowed_cols))
    return subset_df


# def compute_hashdiff(df, separator='|', offset=8, num_bits=256, take_hash_id=True):
#     """
#     """
#     og_cols = df.columns[offset:]
    
#     if not take_hash_id:
#         out = df.withColumn("hash_diff", F.sha2(F.concat_ws(separator, *og_cols), num_bits))
#     else:
#         og_cols = ['hash_id'] + og_cols
#         out = df.withColumn("hash_diff", F.sha2(F.concat_ws(separator, *og_cols), num_bits))

#     return out

def table_builder(deltaTable, container, entity) -> str:
    """Returns the absolute path of the save location within the delta lake

    :param str deltaTable: Name of the delta table associated with the entity refered.
    :param str container: corresponding multi-hop architecture. valid values: ´raw´, ´brz´, ´slv´, ´gld´.
    :param str entity: Named entity for the desired corresponding path.

    :returns: Full valid path for delta table entity to be saved.
    :rtype: string
    :raises ValueError: If not valid container value is sent.
    """
    env = os.getenv('ENV_TYPE')
    if container == 'raw': container = 'brz'
    deltaTablePath = '/'.join(['', entity, TABLE_MAPPER[container][entity][deltaTable]])
    lakes = PATH_BUILDER['envs']
    endpoints = PATH_BUILDER['endpoints']
    if env == 'drp': env = 'prd'
    if env == 'dev':
        lake = lakes[env]
    elif env in HIGH_ENVS:
        lake = lakes['others'] + env
    else:
        raise ValueError(f'Invalid environment, please choose one of dev, qas, stg, prd, drp')
    return endpoints['prefix'] + endpoints[container] + '@' + lake + endpoints['posfix'] + deltaTablePath

def checkpoint_builder(deltaTable, entity, input: bool) -> str:
    """Returns the absolute path of the save location for checkpoints within the delta lake

    :param str deltaTable: Name of the delta table associated with the entity refered.
    :param str container: corresponding multi-hop architecture. valid values: ´raw´, ´brz´, ´slv´, ´gld´.
    :param str entity: Named entity for the desired corresponding path.
    :param bool input: Validates if it's for the input or output process of checkpoint.

    :returns: Full valid path for checkpoint to be saved.
    :rtype: string
    :raises ValueError: If not valid container value is sent.
    """
    env = os.getenv('ENV_TYPE')
    container = 'brz'
    if input: check = '/in'
    else: check = '/out'
    deltaTablePath = '/'.join(['', 'checkpoints', entity, TABLE_MAPPER[container][entity][deltaTable]])
    lakes = PATH_BUILDER['envs']
    endpoints = PATH_BUILDER['endpoints']
    if env == 'drp': env = 'prd'
    if env == 'dev':
        lake = lakes[env]
    elif env in HIGH_ENVS:
        lake = lakes['others'] + env
    else:
        raise ValueError(f'Invalid environment, please choose one of dev, qas, stg, prd, drp')
    return endpoints['prefix'] + 'raw' + '@' + lake + endpoints['posfix'] + deltaTablePath + check

def lnk_hash(df, business_keys: list, separator='|', num_bits=256):
    """
    """
    return df.withColumn("hash_id", F.sha2(F.concat_ws(separator, *business_keys), num_bits))
    
def compute_hashdiff_no_offset(df, og_cols: list, separator='|', num_bits=256, take_hash_id=True):
    """Computes a new column ´hash_diff´ on the passed dataframe which is the hash calculation of
    all the given columns subset passed to calculate the hash. For 2 dataframes that are passed to
    this function, given the same parameter values for both calls, if the value of some of the columns
    mismatch (even just one), the hash difference column returned will be different for that registry.

    Args:
        :param DataFrame df: The original dataframe to compute the hash difference
        :param List og_cols: The original columns list; this list must be a subset of the existing dataframe columns
        :param str separator: Custom string character that works as a separator between column values
        :param int num_bits: The number of bits used to make the hashing
        :param boolean take_hash_id: An indicator to check if the hash_id should be considered in the hash computation

        :returns: A new DataFrame with a ´hash_diff´ generated column
        :rtype: DataFrame
    """
    if take_hash_id:
        og_cols = ['hash_id', *og_cols]
        
    return df.withColumn("hash_diff", F.sha2(F.concat_ws(separator, *og_cols), num_bits))
    
def create_unity_table(spark, tableName, schema, tablePath):
    """Creates a delta table following a pre-defined path structure
    Args:
        :param SparkSession spark: Spark session to access spark context.
        :param str tableName: Name of the table registered.
        :param str schema: Name of the schema accessed within the catalog.
        :param str tablePath: Absolute path to the delta file location in the data lake.

        :returns: A table corresponding the path for the created table using the sql instruction. 
        :rtype: Spark DataFrame.
    """
    return spark.sql(f"""
                     CREATE TABLE IF NOT EXISTS {os.getenv('ENV_TYPE')}.{schema}.{tableName}
                     USING delta
                     LOCATION '{tablePath}'
                     """)
    
def gen_df_cols_fixed_wdth(df, mapping: OrderedDict):
    """Function specialized on parsing raw files sent by Fiserv. Creates the corresponding
    columns given a dictionary that maps column names and the respective index range where 
    that column values are contained. Atidionally removes head an tail information.
    Args:
        :param DataFrame df: A spark DataFrame result of reading the raw Fiserv file.
        Usually only contains the column 'value'; An only string column where all values
        are contained and must be parsed.
        :param OrderedDict mapping: An ordered dictionary which contains the column names of
        the desired resulting dataframe as keys and a tuple containing index and length of
        the substring that contains the corresponding column value.

        :returns: The resulting parsed dataframe with columns created using the ´mapping´ specs.
        :rtype: DataFrame
    """
    # columns with length less that 100 correspond to head/tail info. Intended to be discarded
    df = (df.withColumn("value", F.rtrim("value"))
            .filter(F.length("value") > 100)
    )
    for f in mapping.keys():
        df = df.withColumn(f, F.substring('value', mapping[f][0], mapping[f][1]))
    return df.drop("value")

def iso_nulls_to_blanks(df):
    for c in df.columns:
        df = (df.withColumn(c, F.regexp_replace(c, "^[\x00]+$", '').alias(c))
                .withColumn(c, F.regexp_replace(c, "^[ ]+$", '').alias(c)))
    return df

def blanks_to_nulls(df):
    return df.select([F.when(F.col(c) == "", None).otherwise(F.col(c)).alias(c) for c in df.columns])

def format_date(df, cols: list, dt_format: str):
    for c in cols:
        df = df.withColumn(c, F.to_date(c, dt_format))
    return df

def format_decimals(df, cols: list):
    for c in cols:
        df = df.withColumn(c, F.col(c).cast(T.DoubleType())/F.lit(100))
    return df

def format_chars(df, cols: list, leng: int):
    for c in cols:
        df = df.withColumn(c, F.col(c).cast(T.CharType(leng)))
    return df

def general_type_formater(df, cols, col_type):
    for c in cols:
        df = df.withColumn(c, F.col(c).cast(col_type()))
    return df

def column_subset_renamer(df: spk_DF, mapper: dict):
    """Given a dataframe and a dictionary with old_name: new_name mapping, returns a subset with all renamed columns.
    columns not contained in the dictionary are discarded.

    Args:
        :param DataFrame df: The spark DataFrame to be affected.
        :param dict mapper: The dictionary containing the column rename mapping. The columns should follow the ´old_name´: ´new_name´
        key-value pairing.
    """
    old_names = mapper.keys()
    new_names = mapper.values()

    df_subset = df.select(*old_names)
    df_subset_renamed = df_subset.toDF(*new_names)

    return df_subset_renamed

def read_cdf(spark, deltaTable, schema, params: dict, insert_only: bool = True):
    try:
        df = (spark.read.format('delta')
                .option("readChangeFeed", True)
                .option("startingTimestamp", params["init_ts"]
            )
        )
        if params["end_ts"]:
            df.option("endingTimestamp", params["end_ts"])
        df = df.table(f"{os.getenv('ENV_TYPE')}.{schema}.{deltaTable}")
        if insert_only:
            return df.filter("_change_type = 'insert'")
        else:
            return df.filter("_change_type in ('insert', 'update_postimage')")
    except AnalysisException:
        try:
            return (spark.createDataFrame(
                [],
                (spark.table(f"{os.getenv('ENV_TYPE')}.{schema}.{deltaTable}")).schema)
            )
        except AnalysisException:
            raise ValueError("Flow is inactive")
    
def generate_uuid4():
    return str(uuid4())

def cdf_delta_exists(spark, deltaTable, schema):
    version = (spark.sql(f"""
        with cte_hist as (
            desc history {os.getenv('ENV_TYPE')}.{schema}.{deltaTable}
        ) 
        select max(version) as version
        from cte_hist
        """)
    )
    if version.count() > 0:
        version = version.collect()[0]['version']
    else:
        version = 0
    return version

def cdf_not_exists(spark, deltaTable, schema):
    lookup_value = '{"delta.enableChangeDataFeed":"true"}'
    version = (spark.sql(f"""
        with cte_hist as (
            desc history {os.getenv('ENV_TYPE')}.{schema}.{deltaTable}
        ) 
        select version
        from cte_hist
        where 1=1
            and operation = 'SET TBLPROPERTIES'
            and operationParameters.properties = '{lookup_value}'
        """)
    )
    if version.count() > 0:
        version = version.collect()[0]['version']
    else:
        version = 0
    return version

def sdf_json_extractor(base_df, root, mapper):
    base_str = f"{base_df} = ({base_df}"
    for k,v in mapper.items():
        base_str += f"""
        .withColumn("{v}", F.col("{root}").{k})"""
    base_str += f"""
    .drop("{root}")
    )"""
    return base_str

def psdf_mass_renamer(base_df, mapper):
    base_str = f"{base_df} = ({base_df}"
    for k,v in mapper.items():
        base_str += f"""
        .withColumnRenamed("{k}", "{v}")"""
    base_str += ")"
    return base_str

def psdf_string_cleaner(base_df, col_except= list()):
    cols = list()
    blanks = list()
    for c in base_df.dtypes:
        if c[1] == 'string' and c[0] not in col_except:
            cols.append(c[0])
        elif c[1] == 'string':
            blanks.append(c[0])
    for c in blanks:
        base_df = (base_df
                    .withColumn(c,
                        F.when(F.col(c) == "" , None)
                        .otherwise(F.col(c))
            )
        )
    for c in cols:
        base_df = (base_df
                    .withColumn(c,
                            F.when(F.col(c) == "" , None)
                            .otherwise(F.col(c))
                    )
                    .withColumn(c, F.translate(c,
                                            'ãäöüẞáäčďéěíĺľñňóôŕšťúůýžÄÖÜẞÁÄČĎÉĚÍĹĽÑŇÓÔŔŠŤÚŮÝŽ',
                                            'aaousaacdeeillnnoorstuuyzAOUSAACDEEILLNNOORSTUUYZ'))
                    .withColumn(c, F.upper(c))
        )
    return base_df

def core_timestamp_converter(df, column):
    return (df.withColumn(column, 
                    F.regexp_extract(column, r"^\/Date\((\d+)\)\/$", 1)
                )
                .withColumn(column, 
                    F.to_utc_timestamp(
                        F.from_unixtime(F.col(column).cast('float')/1000), "UTC")
                )
    )

def set_cdf_params(dbutils):
    dbutils.widgets.text("init_timestamp", "")
    dbutils.widgets.text("end_timestamp", "")
    dbutils.widgets.text("hours_delta", "")

def get_cdf_params(dbutils):
    global HOURS_DELTA
    HOURS_DELTA = float(dbutils.widgets.get("hours_delta")) if dbutils.widgets.get("hours_delta") else HOURS_DELTA
    current_date_utc = datetime.now(pytz.utc)
    load_date_utc = current_date_utc - timedelta(hours=HOURS_DELTA)
    load_date_utc = dbutils.widgets.get("init_timestamp") if dbutils.widgets.get("init_timestamp") else load_date_utc
    end_date_utc = dbutils.widgets.get("end_timestamp") if dbutils.widgets.get("end_timestamp") else None
    return {
        "hours_delta": HOURS_DELTA,
        "init_ts": load_date_utc,
        "end_ts": end_date_utc
    }

def flatten_structs(nested_df):
    """ Flattens a dataframe with columns of type "struct"
        Example, before:
        root
        |-- x: string (nullable = true)
        |-- y: string (nullable = true)
        |-- foo: struct (nullable = true)
        |    |-- a: float (nullable = true)
        |    |-- b: float (nullable = true)
        |-- bar: struct (nullable = true)
        |    |-- a: float (nullable = true)
        After:

        root
        |-- x: string (nullable = true)
        |-- y: string (nullable = true)
        |-- foo_a: float (nullable = true)
        |-- foo_b: float (nullable = true)
        |-- bar_a: float (nullable = true)
    """
    stack = [((), nested_df)]
    columns = []

    while len(stack) > 0:
        parents, df = stack.pop()
        
        flat_cols = [
            F.col(".".join(parents + (c[0],))).alias("_".join(parents + (c[0],)))
            for c in df.dtypes
            if c[1][:6] != "struct"
        ]

        nested_cols = [
            c[0]
            for c in df.dtypes
            if c[1][:6] == "struct"
        ]
        
        columns.extend(flat_cols)
        for nested_col in nested_cols:
            projected_df = df.select(nested_col + ".*")
            stack.append((parents + (nested_col,), projected_df))
    return nested_df.select(columns)

def flatten_array_struct_df(df):
    """ Flattens a dataframe with columns of type "struct" containg an array of "structs"
        exploding the array into multiple rows
        
        Example, before:
        root
        |-- x: string (nullable = true)
        |-- y: string (nullable = true)
        |-- foo: struct (nullable = true)[
        |    |-- a: float (nullable = true)
        |    |-- b: float (nullable = true)
        |    |-- c: integer (nullable = true)]
        After:

        root
        |-- x: string (nullable = true)
        |-- y: string (nullable = true)
        |-- foo_a: float (nullable = true)
        |-- foo_b: float (nullable = true)
        |-- foo_c: integer (nullable = true)

        returns flattened dataframe with arrays exploded
    """
    array_cols = [
            c[0]
            for c in df.dtypes
            if c[1][:5] == "array"
        ]
    while len(array_cols) > 0:
        for array_col in array_cols:
            cols_to_select = [x for x in df.columns if x != array_col ]
            df = df.withColumn(array_col, F.explode(F.col(array_col)))
        # Calls flatten_structs function
        df = flatten_structs(df)
        array_cols = [
            c[0]
            for c in df.dtypes
            if c[1][:5] == "array"
        ]
    return df

def format_timestamps(df, cols: list):
    """
    Converts df columns in cols list from string to timestamp, testing several possible formats
    """
    for c in cols:
        df = df.withColumn(c, F.coalesce(F.to_timestamp(c), 
                                         F.to_timestamp(c, "yyyy/M[M]/d[d][ HH:mm[:ss]]"),
                                         F.to_timestamp(c, "yyyy.M[M].d[d][ HH:mm[:ss]]"),
                                         F.to_timestamp(c, "d[d]-M[M]-yyyy[ HH:mm[:ss]]"), 
                                         F.to_timestamp(c, "M[M]-d[d]-yyyy[ HH:mm[:ss]]"), 
                                         F.to_timestamp(c, "d[d]/M[M]/yyyy[ HH:mm[:ss]]"),
                                         F.to_timestamp(c, "M[M]/d[d]/yyyy[ HH:mm[:ss]]"),
                                         F.to_timestamp(c, 'd[d].M[M].yyyy[ HH:mm[:ss]]'),
                                         F.to_timestamp(c, 'M[M].d[d].yyyy[ HH:mm[:ss]]'),
                                         F.to_timestamp(c, "d[d]-M[M]-yy[ HH:mm[:ss]]"), 
                                         F.to_timestamp(c, "M[M]-d[d]-yy[ HH:mm[:ss]]"), 
                                         F.to_timestamp(c, "d[d]/M[M]/yy[ HH:mm[:ss]]"),
                                         F.to_timestamp(c, "M[M]/d[d]/yy[ HH:mm[:ss]]"),
                                         F.to_timestamp(c, 'd[d].M[M].yy[ HH:mm[:ss]]'),
                                         F.to_timestamp(c, 'M[M].d[d].yy[ HH:mm[:ss]]'),
                                         F.to_timestamp(c, 'yyyyMMdd[ HH:mm[:ss]]')))
    return df

def format_booleans(df, cols: list):
    """
    Converts df columns in cols list from string to boolean values
    """    
    for c in cols:
        df = df.withColumn(c, F.when(F.col(c) == 'true', True).otherwise(False))
    return df

def send_col_last(df, last_col:str = None):
    """
    Moves the given column to the last column position
    """
    if last_col in df.columns:
        cols_except_last = [c for c in df.columns if c != last_col]
        df = df.select(*cols_except_last, last_col)
    else:
        raise Exception("LAST_COL must be a column name of the DataFrame")
    return df

def move_col_to_end(df, columns_to_move) :
    """
    Move columns in the list "columns_to_move" to the end of the dataframe columns
    """
    original = df.columns
    # Considers only columns that exist originaly
    columns_to_move = [c for c in columns_to_move if c in original]
    first_columns = [c for c in original if c not in columns_to_move]
    df = df.select(*first_columns, *columns_to_move)
    return df

def null_literals_to_nulls(df):
    """
    Converts the string literal "null" (case insensitive) to None
    """
    return df.select([F.when(F.lower(F.col(c)) == "null", None).otherwise(F.col(c)).alias(c) for c in df.columns])    
    
def format_doubles(df, cols: list):
    for c in cols:
        df = df.withColumn(c, F.col(c).cast(T.DoubleType()))
    return df
