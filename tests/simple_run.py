
from pyspark.sql import SparkSession
from effulge.effulge import spot_variance

def simple_run():
    spark = SparkSession.builder.appName("Effulge").config("spark.sql.shuffle.partitions", 5).getOrCreate()
    #
    df_expected = spark.read.option("header", True ).csv("/app/effulge/SampleData/table1.csv")
    df_available = spark.read.option("header", True ).csv("/app/effulge/SampleData/table2.csv")
    candidate_key = ("ProductID", "Colour")
    #
    result = spot_variance(df_expected, df_available, candidate_key)
    result.show(truncate=False)

def test_simple():
    simple_run()
    assert True

if __name__ == '__main__':
    simple_run()
