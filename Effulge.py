
from pyspark.sql import SparkSession
from pyspark.sql.types import Row

spark = SparkSession.builder.appName("Effulge").getOrCreate()

df_expected = spark.read.option("header", True ).csv("/app/effulge/SampleData/table1.csv")
df_available = spark.read.option("header", True ).csv("/app/effulge/SampleData/table2.csv")
candidate_key = ("ProductID", "Colour")

# Assumption:
#  1. Both DataFrames have same number of columns  ??
#  2. Both DataFrames have same column names
def spot_variance(expected_dataframe, available_dataframe, primary_key):
  # To Do list:
  #
  # validate parameter types
  # handle empty or invalid parameter values
  # check all columns exist                  :: Done
  # strict check given primary key is valid  :: Done
  # spot & report missing primary_key        :: Done
  # spot & report duplicated primary_key     :: Done
  # spot & report mismatching attributes     :: Done
  # cache if necessary
  # repartition if necessary                 :: Done
  # have a way to clean up resource
  
  primary_attributes = [ a.lower() for a in primary_key]
  non_primary_attributes = [ col.lower() for col in expected_dataframe.columns if col.lower() not in primary_attributes ]
  
  ## check all columns exist
  missing_attributes = [ c.lower() for c in available_dataframe.columns if((c.lower() not in primary_attributes) & (c.lower() not in non_primary_attributes)) ]
  if len(missing_attributes):
    raise Exception("Few attributes are missing in second data frame : {}".format(missing_attributes))
  ##
  
  
  ## repartition both data frames on primary keys
  expected_dataframe = repartition_df_with_keys(expected_dataframe, primary_attributes)
  available_dataframe = repartition_df_with_keys(available_dataframe, primary_attributes)
  
  
  ## cache the data frames, if it is not cached before
  uncache_list = []  # to clear cache only for the data frames that were cached by this library
  for df in (expected_dataframe, available_dataframe):
    if not df.is_cached:
      df.persist()
      uncache_list.append(df)
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
  
  
  ## spot * report mismatching attributes || BEGIN
  df_mismatch = expected_dataframe.select(*primary_attributes, *non_primary_attributes)\
                .subtract(
                  available_dataframe.select(*primary_attributes, *non_primary_attributes)
                ).select(*primary_attributes)
  
  ##
  expected_dataframe.createOrReplaceTempView("expected_view")
  available_dataframe.createOrReplaceTempView("available_view")
  
  df_expected_with_renamed_columns = spark.sql(
    "select " +\
      ", ".join(
             [c for c in primary_attributes]
          ) +\
      ", "  +\
      ", ".join(
             ["{0} as e_{0}".format(c) for c in non_primary_attributes]
          ) +\
      " from expected_view"
  )
  
  df_available_with_renamed_columns = spark.sql(
    "select " +\
      ", ".join(
             [c for c in primary_attributes]
          ) +\
      ", "  +\
      ", ".join(
             ["{0} as a_{0}".format(c) for c in non_primary_attributes]
          ) +\
      " from available_view"
  )
  
  df_mismatch_join = df_mismatch.join(df_expected_with_renamed_columns, primary_attributes, "inner").join(df_available_with_renamed_columns, primary_attributes, "inner")
  
  rdd_variance = df_mismatch_join.rdd.map( lambda r: spot_corrupted_attributes(r, primary_attributes, non_primary_attributes, "e_", "a_") )
  ##spot * report mismatching attributes || BEGIN
  
  return rdd_variance.union(rdd_missing_primary_key).union(rdd_duplicated_primary_key).toDF()

#
def spot_corrupted_attributes(row_object, primary_column_list, suspect_column_list, left_prefix, right_prefix):
  varying_attributes = { c for c in suspect_column_list if row_object[left_prefix + c] != row_object[right_prefix + c] }
  output_dict = { p:row_object[p] for p in primary_column_list }
  output_dict['EFFULGE_VARIANCE_PROVOKER'] = sorted(varying_attributes)
  return Row(**output_dict)


#
def repartition_df_with_keys(dataframe_object, partition_columns_tuple):
  current_dataframe_partitioner = []
  for line in dataframe_object._jdf.queryExecution().simpleString().split("\n"):
    if 'hashpartitioning' in line.lower():
      #
      current_dataframe_partitioner = [ x.split('#')[0] for x in line.lower().split('hashpartitioning(', 1)[1]\
                                                                             .split(')', 1)[0]\
                                                                             .split(' ')[0:-1]
                                      ]
  #
  s1 = set(partition_columns_tuple)
  s2 = set(current_dataframe_partitioner)
  #
  if (len(s2) == 0) or s1.difference(s2) or s2.difference(s1):
    dataframe_object = dataframe_object.repartition(*partition_columns_tuple)
  #
  return dataframe_object



result = spot_variance(df_expected, df_available, candidate_key)
result.show(truncate=False)

