import logging
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip


def setup_session():
    # configuracoes de extensao para funcionar
    builder = SparkSession.builder.appName("IngestaoCardio") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    return spark


def read_csv(spark, path=""):
    logging.info("Realizando leitura do arquivo")
    return spark.read.format("csv").option("header", "true").load(path)


def rename_column(df):
    logging.info("Renomeando coluna")
    return df.withColumnRenamed("Height_(cm)", "Height_cm").withColumnRenamed("Weight_(kg)", "Weight_kg")


def save_delta(df):
    logging.info("Armazenando dados")
    return df.write.format("delta").mode("overwrite").option("mergeSchema", True).partitionBy("General_Health").save("C:/PersonalProjects/DataopsPipeline/storage")


def main():
    spark = setup_session()
    df = read_csv(spark, "C:/PersonalProjects/DataopsPipeline/data_sources/cardiovascular-diseases-risk.csv")
    df.printSchema()
    df = rename_column(df)
    # df.printSchema()
    save_delta(df)

    spark.stop()


if __name__ == '__main__':
    main()
    