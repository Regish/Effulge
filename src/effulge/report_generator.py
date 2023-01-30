#!/usr/bin/env python3

"""
Module to Generate Variance Report
  - uses Pandas library
  - generates Excel file

"""

from pyspark.sql.functions import sum     # pylint: disable=import-error, redefined-builtin
from pyspark.sql.functions import desc    # pylint: disable=import-error
from pandas import ExcelWriter            # pylint: disable=import-error
from .effulge import summarize_variance    # pylint: disable=no-name-in-module


def save_variance_report(variance_df, source_df, target_df, super_key, file_name, **kwargs):    # pylint: disable=too-many-locals
    """
    Function to create Excel report from Effulge Variance Dataframe

    Parameters:
        Name: variance_df
        Type:    pyspark.sql.dataframe.DataFrame
        Name: source_df
        Type:    pyspark.sql.dataframe.DataFrame
        Name: target_df
        Type:    pyspark.sql.dataframe.DataFrame
        Name: super_key
        Type:    tuple or list of String
        Name: file_name
        Type:    String
        Name: src_prefix    (OPTIONAL)
        Type:    String     (default is 'src')
        Name: tgt_prefix    (OPTIONAL)
        Type:    String     (default is 'tgt')

    Return Type:
        None
    """
    # assess impact of each attribute
    _r_summary = summarize_variance(variance_df)
    _r_impacted_attributes = _sort_by_most_impacted_attributes( _r_summary )
    # list out all attributes causing variance
    _list_of_rows = _r_impacted_attributes\
        .where("_attribute_name not in ('MISSING_PRIMARY_KEY','DUPLICATE_PRIMARY_KEY')")\
        .select("_attribute_name").collect()
    _list_of_attributes = [ x.asDict()['_attribute_name'] for x in _list_of_rows ]
    # set Excel configurations to avoid data conversion
    excel_options = {}
    excel_options['strings_to_formulas'] = False
    excel_options['strings_to_urls'] = False
    excel_engine_kwargs = {'options':excel_options}
    excel_writer = ExcelWriter(
        file_name + '.xlsx',
        engine='xlsxwriter',
        engine_kwargs=excel_engine_kwargs
    )
    # create Excel sheet for 'MISSING_PRIMARY_KEY'
    _missing_spark_df = variance_df\
        .where("ARRAY_CONTAINS( EFFULGE_VARIANCE_PROVOKER , 'MISSING_PRIMARY_KEY' )")\
        .select( *super_key )
    _missing_panda_df = _missing_spark_df.toPandas()
    _missing_panda_df.to_excel(excel_writer, sheet_name='MISSING_PRIMARY_KEY', index=False)
    #
    # create Excel sheet for 'DUPLICATE_PRIMARY_KEY'
    _duplicate_spark_df = variance_df\
        .where("ARRAY_CONTAINS( EFFULGE_VARIANCE_PROVOKER , 'DUPLICATE_PRIMARY_KEY' )")\
        .select( *super_key )
    _duplicate_panda_df = _duplicate_spark_df.toPandas()
    _duplicate_panda_df.to_excel(excel_writer, sheet_name='DUPLICATE_PRIMARY_KEY' , index=False)
    #
    # create Excel sheet for varying attributes
    for column in _list_of_attributes :
        _focused_spark_df = _focused_variance_for_each_attribute(variance_df, source_df, target_df,
                                                                 super_key, column, **kwargs)
        _focused_panda_df = _focused_spark_df.toPandas()
        _focused_panda_df.to_excel(excel_writer, sheet_name=column, index=False)
    # save Excel file
    excel_writer.save()


def _sort_by_most_impacted_attributes(variance_df):
    """
    Function to identify impact of each column
    and sort them based on their severity

    Parameters:
        Name: variance_df
        Type:    pyspark.sql.dataframe.DataFrame

    Return Type:
        pyspark.sql.dataframe.DataFrame
        Format:

    +-------------------+----------------+
    |  _attribute_name  |  _total_count  |
    +-------------------+----------------+
    |                   |                |
    |                   |                |
    +-------------------+----------------+

    """
    out_df = variance_df.selectExpr(
            "explode(EFFULGE_VARIANCE_PROVOKER) as _attribute_name",
            "EFFULGE_VARIANCE_COUNT as _count"
        )\
        .groupBy("_attribute_name")\
        .agg(sum("_count").alias("_total_count"))\
        .orderBy( desc("_total_count") )
    #
    return out_df


def _focused_variance_for_each_attribute(
            #pylint: disable=too-many-arguments
            variance_df, source_df, target_df, primary_key_list,
            attribute_name, src_prefix='src', tgt_prefix='tgt'):
    #pylint: disable=line-too-long
    """
    Function to fetch all variances for a given column.

    Parameters:
        Name: variance_df
        Type:    pyspark.sql.dataframe.DataFrame
        Name: source_df
        Type:    pyspark.sql.dataframe.DataFrame
        Name: target_df
        Type:    pyspark.sql.dataframe.DataFrame
        Name: primary_key_list
        Type:    tuple or list of String
        Name: attribute_name
        Type:    String
        Name: src_prefix    (OPTIONAL)
        Type:    String     (default is 'src')
        Name: tgt_prefix    (OPTIONAL)
        Type:    String     (default is 'tgt')

    Return Type:
        pyspark.sql.dataframe.DataFrame
        Format:

    +------------------------+---------+------------------------+--------------------------------------+--------------------------------------+
    |  <<primary_column_1>>  |  . . .  |  <<primary_column_N>>  |  _<<src_prefix>>_<<attribute_name>>  |  _<<tgt_prefix>>_<<attribute_name>>  |
    +------------------------+---------+------------------------+--------------------------------------+--------------------------------------+
    |                        |         |                        |                                      |                                      |
    |                        |         |                        |                                      |                                      |
    +------------------------+---------+------------------------+--------------------------------------+--------------------------------------+

    """
    _filtered_records = variance_df\
        .where(f"ARRAY_CONTAINS( EFFULGE_VARIANCE_PROVOKER, '{attribute_name}')")
    focused_df = _filtered_records\
        .join(
            source_df.selectExpr(*primary_key_list,
                                 f"{attribute_name} as _{src_prefix}_{attribute_name}")  ,
            [ *primary_key_list ] ,
            "left"
        ).join(
            target_df.selectExpr(*primary_key_list,
                                 f"{attribute_name} as _{tgt_prefix}_{attribute_name}")  ,
            [ *primary_key_list ] ,
            "left"
        ).selectExpr(
            *primary_key_list,
            f"_{src_prefix}_{attribute_name}",
            f"_{tgt_prefix}_{attribute_name}"
        )
    #
    return focused_df
