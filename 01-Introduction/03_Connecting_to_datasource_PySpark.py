# Databricks notebook source
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("reading sql database").getOrCreate()
jdbcHostname = "sql-yvco-datasource-prod-001.database.windows.net"
jdbcDatabase = "sqldb-yco-adventureworks-prod-001"
jdbcPort = 1433
jdbcUrl = "jdbc:sqlserver://{0}:{1};database={2}".format(jdbcHostname, jdbcPort, jdbcDatabase)
connectionProperties = {
    "user" : "yco-admin",
    "password" : "cas-ia2025",
    "driver" : "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

# COMMAND ----------

df = spark.read.jdbc(url=jdbcUrl, table="SalesLT.Customer", properties=connectionProperties)
display(df)
