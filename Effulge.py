#!/usr/bin/env python

from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import DataFrame


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
            ---------------------------------------------------------------------------------------------------------
           |    <<primary_column_1>>    |    . . .    |    <<primary_column_N>>    |    EFFULGE_VARIANCE_PROVOKER    |
            ---------------------------------------------------------------------------------------------------------
           |                            |             |                            |                                 |
           |                            |             |                            |                                 |
            ---------------------------------------------------------------------------------------------------------
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
    
    ## validate input parameter types
    _check_instance_type(expected_dataframe, DataFrame, 'expected_dataframe')
    _check_instance_type(available_dataframe, DataFrame, 'available_dataframe')
    _check_instance_type(primary_key, (tuple,list), 'primary_key')
    ##
    
    ## handle empty parameter
    if not primary_key:
        raise Exception("Please provide valid non-empty tuple as Primary Key")
    ##
    
    primary_attributes = [ a.lower() for a in primary_key ]
    non_primary_attributes = [ col.lower() for col in expected_dataframe.columns if col.lower() not in primary_attributes ]
    
    ## check all columns exist
    ## all columns on expected_dataframe, must be present in available_dataframe
    missing_attributes = set((*primary_attributes, *non_primary_attributes)).difference(
                                 { c.lower() for c in available_dataframe.columns }
                             )
    if len(missing_attributes):
        raise Exception("Few attributes are missing in second data frame : {}".format(missing_attributes))
    ##
    
    ## repartition both data frames on primary keys
    expected_dataframe = _repartition_df_with_keys(expected_dataframe, primary_attributes)
    available_dataframe = _repartition_df_with_keys(available_dataframe, primary_attributes)
    
    ## cache the data frames, if it is not cached before
    uncache_list = []  # to clear only those data frames that were cached by this function
    for df in (expected_dataframe, available_dataframe):
        if not df.is_cached:
            df.persist()
            uncache_list.append(df)
    ##
    
    ## handle empty parameter
    if not expected_dataframe.count():
        raise Exception("Input 'expected_dataframe' can not be empty")
    ##
    
    ## strict check given primary key is valid
    if expected_dataframe.groupBy(*primary_attributes).count().where("count > 1").count():
        raise Exception("Given primary key is not unique for the first data frame. Please retry with valid primary key")
    ##
    
    ## spot & report missing primary key || BEGIN
    df_missing_primary_attributes = expected_dataframe.select(*primary_attributes)\
                                        .subtract(
                                            available_dataframe.select(*primary_attributes)
                                        )
    rdd_missing_primary_key = df_missing_primary_attributes.rdd.map(
                                      lambda r: Row(**r.asDict(), EFFULGE_VARIANCE_PROVOKER=["MISSING_PRIMARY_KEY"])
                                  )
    ## spot & report missing primary key || END
    
    ## spot & report duplicated primary key || BEGIN
    df_duplicated_primary_attributes = expected_dataframe.select(*primary_attributes)\
                                           .join(
                                               available_dataframe.select(*primary_attributes)
                                               , primary_attributes
                                               , "inner"
                                           ).groupBy(*primary_attributes).count()\
                                           .where("count > 1").select(*primary_attributes)
    rdd_duplicated_primary_key = df_duplicated_primary_attributes.rdd.map(
                                         lambda r: Row(**r.asDict(), EFFULGE_VARIANCE_PROVOKER=["DUPLICATE_PRIMARY_KEY"])
                                     )
    ## spot & report duplicated primary key || END
    
    ## spot & report mismatching attributes || BEGIN
    df_mismatch = expected_dataframe.select(*primary_attributes, *non_primary_attributes)\
                      .subtract(
                          available_dataframe.select(*primary_attributes, *non_primary_attributes)
                      ).select(*primary_attributes)
    
    # create temporary views
    expected_dataframe.createOrReplaceTempView("effulge_expected_view")
    available_dataframe.createOrReplaceTempView("effulge_available_view")
    
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
               ", ".join( primary_attributes ),
               ", ".join( [ "{0} as e_{0}".format(c) for c in non_primary_attributes ] ),
               "effulge_expected_view"
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
               ", ".join( primary_attributes ),
               ", ".join( [ "{0} as a_{0}".format(c) for c in non_primary_attributes ] ),
               "effulge_available_view"
            )
    )
    
    df_mismatch_join = df_mismatch.join(
                                       df_expected_with_renamed_columns,
                                       primary_attributes,
                                       "inner"
                                 ).join(
                                       df_available_with_renamed_columns,
                                       primary_attributes,
                                       "inner"
                                 )
    
    # for each mismatch record, compare and identify variance columns
    rdd_variance = df_mismatch_join.rdd.map(
                           lambda r: _spot_corrupted_attributes(r, primary_attributes, non_primary_attributes, "e_", "a_")
                       )
    ## spot & report mismatching attributes || END
    
    # merge all variances
    effulge_variance_dataframe = rdd_variance.union(rdd_missing_primary_key)\
                                             .union(rdd_duplicated_primary_key)\
                                             .toDF().repartition(*primary_attributes)
    
    ## persist output dataframe
    effulge_variance_dataframe.persist()
    effulge_variance_dataframe.count()
    
    ## clear previously cached dataframes
    for d in uncache_list:
        d.unpersist(blocking=True)
    
    ## clear temporary view
    SparkSession.getActiveSession().catalog.dropTempView('effulge_expected_view')
    SparkSession.getActiveSession().catalog.dropTempView('effulge_available_view')
    
    ## Return the output dataframe
    return effulge_variance_dataframe


#
def _spot_corrupted_attributes(row_object, primary_column_list, suspect_column_list, left_prefix, right_prefix):
    """
    Function to compare column values and identify mismatching column names
        Reads the 'row_object' which has 1 attribute for each column name in the 'primary_column_list'
        and has 2 attributes for each column name in the 'suspect_column_list'
        - one of the attribute will be prefixed with 'left_prefix' string
        - and the other attribute will be prefixed with 'right_prefix' string
        Then it compares corresponding attribute pairs
        If values of any attribute pair are different, then it tracks that attribute name in a buffer list
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
    varying_attributes = { c for c in suspect_column_list if row_object[left_prefix + c] != row_object[right_prefix + c] }
    output_dict = { p:row_object[p] for p in primary_column_list }
    output_dict['EFFULGE_VARIANCE_PROVOKER'] = sorted(varying_attributes)
    return Row(**output_dict)


#
def _repartition_df_with_keys(dataframe_object, partition_columns_tuple):
    """
    Function to check and repartition a dataframe based on given partition columns.
        Firstly this function reads the Physical Query Plan for 'dataframe_object'
        and searches to find partition columns
        If partitioning columns exist and if it is same as 'partition_columns_typle'
            then the input 'dataframe_object' is returned without change
        But if partitioning doesn't exist or if partitioning columns are different from 'partition_columns_tuple'
            then a new dataframe is created by repartitioning 'dataframe_object' on 'partition_columns_typle'
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
    for line in dataframe_object._jdf.queryExecution().simpleString().split("\n"):
        if 'hashpartitioning' in line.lower():
            #
            current_dataframe_partitioner = [ x.split('#')[0] for x in line.lower().split('hashpartitioning(', 1)[1]\
                                                                                   .split(')', 1)[0]\
                                                                                   .split(' ')[0:-1]
                                            ]
            break
    #
    s1 = set(partition_columns_tuple)
    s2 = set(current_dataframe_partitioner)
    #
    if s1.difference(s2) or s2.difference(s1):
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
    """
    if not isinstance(given_object, acceptable_classes):
        raise Exception("Given input '{0}' :: {1} is invalid. Expected types are {2}".format(param_name, type(given_object), acceptable_classes))


if __name__ == '__main__' :
    spark = SparkSession.builder.appName("Effulge").getOrCreate()
    #
    df_expected = spark.read.option("header", True ).csv("/app/effulge/SampleData/table1.csv")
    df_available = spark.read.option("header", True ).csv("/app/effulge/SampleData/table2.csv")
    candidate_key = ("ProductID", "Colour")
    #
    result = spot_variance(df_expected, df_available, candidate_key)
    result.show(truncate=False)
