#!/usr/bin/env python3

"""
Data Validation Utility on PySpark.

Effulge means "to shine".

Analogy:
  To visually compare any two identical real world objects,
      we need a light source to equip our eyes.
  Similarly, to compare identical pyspark dataframes,
      Effulge utility can equip us to validate with ease.
"""


from pyspark.sql import SparkSession                        # pylint: disable=import-error
from pyspark.sql import Row                                 # pylint: disable=import-error
from pyspark.sql import DataFrame                           # pylint: disable=import-error
from pyspark.sql.types import ArrayType                     # pylint: disable=import-error
from pyspark.sql.types import StringType                    # pylint: disable=import-error
from pyspark.sql.functions import col                       # pylint: disable=import-error
from pyspark.sql.functions import round as pyspark_round    # pylint: disable=import-error
from .scatter_tree_compare import compare_two_collections


def spot_variance(expected_dataframe, available_dataframe, primary_key):
    """
    Function to find column level variance between two dataframes.
    # Assumption:
    #  1. All columns in first dataframe should be present in second dataframe
    #  2. Such common columns must have same column names on both dataframes
    #  3. Second dataframe could contain extra columns or records, they will be ignored

    Parameters:
        Name: expected_dataframe
        Type:    pyspark.sql.dataframe.DataFrame
        Name: available_dataframe
        Type:    pyspark.sql.dataframe.DataFrame
        Name: primary_key
        Type:    tuple or list

    Return Type:
        pyspark.sql.dataframe.DataFrame
        Format:

    +------------------------+---------+------------------------+-----------------------------+
    |  <<primary_column_1>>  |  . . .  |  <<primary_column_N>>  |  EFFULGE_VARIANCE_PROVOKER  |
    +------------------------+---------+------------------------+-----------------------------+
    |                        |         |                        |                             |
    |                        |         |                        |                             |
    +------------------------+---------+------------------------+-----------------------------+

        EFFULGE_VARIANCE_PROVOKER
            Type:    List of Strings
            Values:  Column Names, Constant Message
                Constant Message can be -
                    "MISSING_PRIMARY_KEY"
                        => when 'expected_dataframe' contains a primary key value
                        => but 'available_dataframe' does not have corresponding match
                    "DUPLICATE_PRIMARY_KEY"
                        => when 'expected_dataframe' contains a unique primary key value
                        => but 'available_dataframe' has many matches for the same primary key value

    Throws Exception:
        - if any input parameter value is not of the acceptable type
        - if 'primary_key' parameter is an empty tuple
        - if any column present in 'expected_dataframe' is not present in 'available_dataframe'
        - if 'expected_dataframe' is empty
        - if 'primary_key' does not prove uniqueness for 'expected_dataframe'
    """
    # To Do list:
    #
    # validate parameter types                 :: Done
    # handle empty parameter values            :: Done
    # check all columns exist                  :: Done
    # strict check given primary key is valid  :: Done
    # spot & report missing primary_key        :: Done
    # spot & report duplicated primary_key     :: Done
    # spot & report mismatching attributes     :: Done
    # cache if necessary                       :: Done
    # repartition if necessary                 :: Done
    # have a way to clean up resource          :: Done

    # validate input parameter types
    _check_instance_type(expected_dataframe, DataFrame, 'expected_dataframe')
    _check_instance_type(available_dataframe, DataFrame, 'available_dataframe')
    _check_instance_type(primary_key, (tuple,list), 'primary_key')

    # handle empty parameter
    if not primary_key:
        raise Exception("Please provide valid non-empty tuple as Primary Key")

    # list down primary and non primary attributes
    primary_attributes = [ a.lower() for a in primary_key ]
    non_primary_attributes = [ col.lower()
                                   for col in expected_dataframe.columns
                                       if col.lower() not in primary_attributes
                             ]

    # check all columns exist
    # all columns on expected_dataframe, must be present in available_dataframe
    missing_attributes = set((*primary_attributes, *non_primary_attributes)).difference(
                                 { c.lower() for c in available_dataframe.columns }
                             )
    if len(missing_attributes) > 0:
        raise Exception("Few attributes are missing in second data frame : {}"\
                            .format(missing_attributes))

    # repartition both data frames on primary keys
    expected_dataframe = _repartition_df_with_keys(expected_dataframe, primary_attributes)
    available_dataframe = _repartition_df_with_keys(available_dataframe, primary_attributes)

    # cache the data frames, if it is not cached before
    uncache_list = []  # to clear only those data frames that were cached by this function
    for d_f in (expected_dataframe, available_dataframe):
        if not d_f.is_cached:
            d_f.persist()
            uncache_list.append(d_f)

    # handle empty parameter
    if not expected_dataframe.count():
        raise Exception("Input 'expected_dataframe' can not be empty")

    # strict check given primary key is valid
    if expected_dataframe.groupBy(*primary_attributes).count().where("count > 1").count():
        raise Exception("Given primary key is not unique for the first data frame. \
                         Please retry with valid primary key")

    # spot & report missing primary key
    df_missing_primary_key = _spot_missing_primary_key(
                                     expected_dataframe,
                                     available_dataframe,
                                     primary_attributes
                                 )

    # spot & report duplicated primary key
    df_duplicated_primary_key = _spot_duplicated_primary_key(
                                        expected_dataframe,
                                        available_dataframe,
                                        primary_attributes
                                    )

    temp_view_list = [] # to clear only the temp views that were created by this function

    # detect variance
    if len(non_primary_attributes) > 0 :
        # when few non primary columns exists, then explicitly identify their variances
        #
        # create temporary views
        expected_dataframe.createOrReplaceTempView("effulge_expected_view")
        temp_view_list.append("effulge_expected_view")
        available_dataframe.createOrReplaceTempView("effulge_available_view")
        temp_view_list.append("effulge_available_view")

        # spot & report mismatching attributes
        df_variance = _spot_mismatch_variance(
                              "effulge_expected_view",
                              "effulge_available_view",
                              primary_attributes,
                              non_primary_attributes
                          )
    else:
        # when non primary columns do not exists,
        # i.e, we only have primary columns,
        # then the variances are caught implicity with MISSING_PRIMARY_KEY check
        #
        df_variance = _get_empty_result_df(expected_dataframe, primary_attributes)
    #
    #
    # merge all variances
    effulge_variance_dataframe = df_variance.union(df_missing_primary_key)\
                                             .union(df_duplicated_primary_key)\
                                             .repartition(*primary_attributes)

    # persist output dataframe
    effulge_variance_dataframe.persist()
    effulge_variance_dataframe.count()

    # clear cache and temp views
    _clean_cache_and_view(uncache_list, temp_view_list)

    # Return the output dataframe
    return effulge_variance_dataframe


#
def summarize_variance(variance_df):
    """
    Function to summarize the variance dataframe.
    It will provide count and percentage information for each variance attribute group.

    Parameters:
        Name: variance_df
        Type:    pyspark.sql.dataframe.DataFrame

    Return Type:
        pyspark.sql.dataframe.DataFrame
        Format:

    +-----------------------------+--------------------------+-------------------------------+
    |  EFFULGE_VARIANCE_PROVOKER  |  EFFULGE_VARIANCE_COUNT  |  EFFULGE_VARIANCE_PERCENTAGE  |
    +-----------------------------+--------------------------+-------------------------------+
    |                             |                          |                               |
    |                             |                          |                               |
    +-----------------------------+--------------------------+-------------------------------+

    Throws Exception:
        - If the input dataframe does not contain 'EFFULGE_VARIANCE_PROVOKER' column
    """
    all_columns = [ col_name.upper() for col_name in variance_df.columns ]
    if "EFFULGE_VARIANCE_PROVOKER" not in all_columns:
        raise Exception("Input Dataframe does not have column 'EFFULGE_VARIANCE_PROVOKER'")
    total_variance_count = variance_df.count()
    # create summary dataframe
    summary_df = variance_df.groupBy(
                                "EFFULGE_VARIANCE_PROVOKER"
                            ).count()\
                            .withColumnRenamed(
                                "count",
                                "EFFULGE_VARIANCE_COUNT"
                            ).withColumn(
                                "EFFULGE_VARIANCE_PERCENTAGE",
                                pyspark_round(
                                    col("EFFULGE_VARIANCE_COUNT") * 100 / total_variance_count,
                                    2
                                )
                            ).orderBy(
                                "EFFULGE_VARIANCE_COUNT",
                                ascending=False
                            )
    # persist summary dataframe
    summary_df.persist()
    summary_df.count()
    # return summary dataframe
    return summary_df


#
def _spot_corrupted_attributes(row_object, primary_column_list,
                               suspect_column_list, left_prefix, right_prefix):
    """
    Function to compare column values and identify mismatching column names
        Reads the 'row_object'
            which has 1 attribute for each column name in the 'primary_column_list'
        and has 2 attributes for each column name in the 'suspect_column_list'
        - one of the attribute will be prefixed with 'left_prefix' string
        - and the other attribute will be prefixed with 'right_prefix' string
        Then it compares corresponding attribute pairs
        If values of any attribute pair are different,
            then it tracks that attribute name in a buffer list
        At the end, it return a Row containing primary attributes
        and an additional column with contents of buffer list.

    Parameters:
        Name: row_object
        Type:    pyspark.sql.types.Row
        Name: primary_column_list
        Type:    list of String
        Name: suspect_column_list
        Type:    list of String
        Name: left_prefix
        Type:    String
        Name: right_prefix
        Type:    String

    Return Type:
        pyspark.sql.types.Row
    """
    varying_attributes = { c for c in suspect_column_list
                                 if not _are_equal(
                                            row_object[left_prefix + c],
                                            row_object[right_prefix + c]
                                        )
                         }
    output_dict = { p:row_object[p] for p in primary_column_list }
    output_dict['EFFULGE_VARIANCE_PROVOKER'] = sorted(varying_attributes)
    return Row(**output_dict)


#
def _are_equal(var_1, var_2):
    """
    Function to compare two variables for equality.

    Parameters:
        Name: var_1
        Type:    Any
        Name: var_2
        Type:    Any

    Return Type:
        Boolean
    """
    if isinstance(var_1, (tuple, list, dict)):
        is_equal = compare_two_collections(var_1, var_2)
    else:
        is_equal = var_1 == var_2
    return is_equal


#
def _repartition_df_with_keys(dataframe_object, partition_columns_tuple):
    """
    Function to check and repartition a dataframe based on given partition columns.
        Firstly this function reads the Physical Query Plan for 'dataframe_object'
        and searches to find partition columns
        If partitioning columns exist and if it is same as 'partition_columns_typle'
            then the input 'dataframe_object' is returned without change
        But if partitioning doesn't exist
          OR if partitioning columns are different from 'partition_columns_tuple'
            then a new dataframe is created
            by repartitioning 'dataframe_object' on 'partition_columns_typle'
            and this new repartitioned dataframe is returned

    Parameters:
        Name: dataframe_object
        Type:    pyspark.sql.dataframe.DataFrame
        Name: partition_columns_tuple
        Type:    tuple of String

    Return Type:
        pyspark.sql.dataframe.DataFrame
    """
    current_dataframe_partitioner = []
    # pylint: disable=protected-access
    for line in dataframe_object._jdf.queryExecution().simpleString().split("\n"):
    # pylint: enable=protected-access
        if 'hashpartitioning' in line.lower():
            #
            current_dataframe_partitioner = [ x.split('#')[0]
                                                  for x in line.lower()\
                                                               .split('hashpartitioning(', 1)[1]\
                                                               .split(')', 1)[0]\
                                                               .split(' ')[0:-1]
                                            ]
            break
    #
    s_1 = set(partition_columns_tuple)
    s_2 = set(current_dataframe_partitioner)
    #
    if s_1.difference(s_2) or s_2.difference(s_1):
        dataframe_object = dataframe_object.repartition(*partition_columns_tuple)
    #
    return dataframe_object


#
def _check_instance_type(given_object, acceptable_classes, param_name):
    """
    Function to verify if 'given_object' is an instance of 'acceptable_classes'

    Parameters:
        Name: given_object
        Type:    any type
        Name: acceptable_classes
        Type:    a Class Name or a collection of Class Names
        Name: param_name
        Type:    String

    Return Type:
        None

    Throws Exception:
        - if "given_object" is not an instance of "acceptable_classes"
    """
    if not isinstance(given_object, acceptable_classes):
        raise Exception("Given input '{0}' :: {1} is invalid. Expected types are {2}"\
	                    .format(param_name, type(given_object), acceptable_classes))


#
def _spot_missing_primary_key(reference_df, received_df, prime_columns):
    """
    Function to identify the primary keys,
        which have a record in "reference_df"
        but do not have a record in "received_df"

    Parameters:
        Name: reference_df
        Type:    pyspark.sql.dataframe.DataFrame
        Name: received_df
        Type:    pyspark.sql.dataframe.DataFrame
        Name: prime_columns
        Type:    list/tuple of Strings

    Return Type:
        pyspark.sql.dataframe.DataFrame
    """
    try:
        df_missing_primary_attributes = reference_df.select(*prime_columns)\
                                            .subtract(
                                                received_df.select(*prime_columns)
                                            )
        _schema = df_missing_primary_attributes.schema
        _schema.add("EFFULGE_VARIANCE_PROVOKER", ArrayType(StringType()) )
        df_missing_primary_key = df_missing_primary_attributes\
                                     .rdd\
                                     .map(
                                         lambda r:
                                             Row(
                                                 **r.asDict(),
                                                 EFFULGE_VARIANCE_PROVOKER=["MISSING_PRIMARY_KEY"]
                                             )
                                     ).toDF( schema = _schema )
    except ValueError as exp:
        if str(exp) == "RDD is empty":
            # create empty result set
            df_missing_primary_key = _get_empty_result_df(reference_df, prime_columns)
        else:
            # raise the same exception, when ValueError message is different
            raise exp
    return df_missing_primary_key


#
def _spot_duplicated_primary_key(reference_df, received_df, prime_columns):
    """
    Function to identify the primary keys,
        which have only 1 record in "reference_df"
        but have more than 1 record in "received_df"

    Parameters:
        Name: reference_df
        Type:    pyspark.sql.dataframe.DataFrame
        Name: received_df
        Type:    pyspark.sql.dataframe.DataFrame
        Name: prime_columns
        Type:    list/tuple of Strings

    Return Type:
        pyspark.sql.dataframe.DataFrame
    """
    df_duplicated_primary_attributes = reference_df.select(*prime_columns)\
                                           .join(
                                               received_df.select(*prime_columns)
                                               , prime_columns
                                               , "inner"
                                           ).groupBy(*prime_columns).count()\
                                           .where("count > 1").select(*prime_columns)
    try:
        _schema = df_duplicated_primary_attributes.schema
        _schema.add("EFFULGE_VARIANCE_PROVOKER", ArrayType(StringType()) )
        df_duplicated_primary_key = df_duplicated_primary_attributes\
                                        .rdd\
                                        .map(
                                            lambda r:
                                                Row(
                                                    **r.asDict(),
                                                    EFFULGE_VARIANCE_PROVOKER=[
                                                        "DUPLICATE_PRIMARY_KEY"
                                                    ]
                                                )
                                        ).toDF( schema = _schema )
    except ValueError as exp:
        if str(exp) == "RDD is empty":
            # create empty result set
            df_duplicated_primary_key = _get_empty_result_df(reference_df, prime_columns)
        else:
            # raise the same exception, when ValueError message is different
            raise exp
    return df_duplicated_primary_key


#
def _spot_mismatch_variance(reference_view_name, received_view_name,
                                prime_columns, non_prime_columns):
    """
    Function to identify the mismatching records
    and also to identify the columns responsible for mismatch

    Parameters:
        Name: reference_view_name
        Type:    String
        Name: received_view_name
        Type:    String
        Name: prime_columns
        Type:    list/tuple of Strings
        Name: non_prime_columns
        Type:    list/tuple of Strings

    Return Type:
        pyspark.sql.dataframe.DataFrame
    """
    df_mismatch = SparkSession.getActiveSession().sql(
            """
            select {0} from {1}
            MINUS
            select {0} from {2}
            """.format(
                   ", ".join((*prime_columns, *non_prime_columns)),
                   reference_view_name,
                   received_view_name
               )
        ).select(prime_columns)

    # retains same column names for primary attributes
    # but renames non primary attributes to have "e_" prefix
    df_expected_with_renamed_columns = SparkSession.getActiveSession().sql(
            """
            select
                -- primary columns
                {},
                -- non primary columns with "e_" prefix
                {}
            from {}
            """.format(
                   ", ".join( prime_columns ),
                   ", ".join( [ "{0} as e_{0}".format(c) for c in non_prime_columns ] ),
                   reference_view_name
               )
        )

    # retains same column names for primary attributes
    # but renames non primary attributes to have "a_" prefix
    df_available_with_renamed_columns = SparkSession.getActiveSession().sql(
            """
            select
                -- primary columns
                {},
                -- non primary columns with "a_" prefix
                {}
            from {}
            """.format(
                   ", ".join( prime_columns ),
                   ", ".join( [ "{0} as a_{0}".format(c) for c in non_prime_columns ] ),
                   received_view_name
               )
        )

    df_mismatch_join = df_mismatch.join(
                                       df_expected_with_renamed_columns,
                                       prime_columns,
                                       "inner"
                                 ).join(
                                       df_available_with_renamed_columns,
                                       prime_columns,
                                       "inner"
                                 )

    # for each mismatch record, compare and identify variance columns
    try:
        df_variance = df_mismatch_join\
                          .rdd\
                          .map(
                              lambda r:
                                  _spot_corrupted_attributes(r, prime_columns, non_prime_columns,
                                                             "e_", "a_")
                          ).toDF()
    except ValueError as exp:
        if str(exp) == "RDD is empty":
            # create empty result set
            df_variance = _get_empty_result_df(df_expected_with_renamed_columns, prime_columns)
        else:
            # raise the same exception, when ValueError message is different
            raise exp
    return df_variance


#
def _clean_cache_and_view(cached_dataframe_list, temporary_view_list):
    """
    Function to explicitly free dataframe cache and to remove temporary views

    Parameters:
        Name : cached_dataframe_list
        Type :    list of pyspark.sql.dataframe.DataFrame objects
        Name : temporary_view_list
        Type :    list of String

    Return Type:
        None
    """
    # clear previously cached list
    for d_f in cached_dataframe_list:
        d_f.unpersist(blocking=True)

    # clear temporary view
    for view in temporary_view_list:
        SparkSession.getActiveSession().catalog.dropTempView(view)


#
def _get_empty_result_df(reference_df, primary_column_list):
    """
    Function to create an empty dataframe containing Primary Key columns and an additional EFFULGE_VARIANCE_PROVOKER column
    Parameters:
        Name : reference_df
        Type :     pyspark.sql.dataframe.DataFrame object
        Name : primary_column_list
        Type :     list of String
    Return Type:
        pyspark.sql.dataframe.DataFrame object
    """
    _schema = reference_df.select( *primary_column_list ).schema
    _schema.add("EFFULGE_VARIANCE_PROVOKER", ArrayType(StringType()) )
    #
    empty_df = SparkSession.getActiveSession().createDataFrame(
        SparkSession.getActiveSession().sparkContext.emptyRDD(),
        _schema
    )
    #
    return empty_df

