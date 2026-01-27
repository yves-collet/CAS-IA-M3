-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Ingestion in the Bronze layer
-- MAGIC
-- MAGIC ## Connecting to the bronze layer (Target)

-- COMMAND ----------

USE CATALOG yvescollet_lakehouse;
USE DATABASE bronze;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Load data into bronze layer of the Lakehouse

-- COMMAND ----------

CREATE OR REPLACE TABLE address 
AS SELECT * FROM yco_adventureworks.saleslt.address;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Ingestion of SalesOrderDetail

-- COMMAND ----------

CREATE OR REPLACE TABLE SalesOrderDetail
AS SELECT * FROM yco_adventureworks.saleslt.SalesOrderDetail;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Ingestion of SalesOrderHeader

-- COMMAND ----------

CREATE OR REPLACE TABLE SalesOrderHeader
AS SELECT * FROM yco_adventureworks.saleslt.SalesOrderHeader;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Ingestion of Product

-- COMMAND ----------

CREATE OR REPLACE TABLE Product
AS SELECT * FROM yco_adventureworks.saleslt.Product;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Ingestion of ProductCategory

-- COMMAND ----------

CREATE OR REPLACE TABLE ProductCategory
AS SELECT * FROM yco_adventureworks.saleslt.ProductCategory;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Ingestion of Address
-- MAGIC
-- MAGIC

-- COMMAND ----------

CREATE OR REPLACE TABLE Address 
AS SELECT * FROM yco_adventureworks.saleslt.Address;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Ingestion of Customer

-- COMMAND ----------

CREATE OR REPLACE TABLE Customer 
AS SELECT * FROM yco_adventureworks.saleslt.Customer;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Ingestion of CustomerAddress

-- COMMAND ----------

CREATE OR REPLACE TABLE CustomerAddress 
AS SELECT * FROM yco_adventureworks.saleslt.customeraddress;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Ingestion of ProductDescription

-- COMMAND ----------

CREATE OR REPLACE TABLE ProductDescription 
AS SELECT * FROM yco_adventureworks.saleslt.productdescription;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Ingestion of ProductModel

-- COMMAND ----------

CREATE OR REPLACE TABLE ProductModel 
AS SELECT * FROM yco_adventureworks.saleslt.productmodel;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Ingestion of ProductModelProductDescription

-- COMMAND ----------

CREATE OR REPLACE TABLE ProductModelProductDescription 
AS SELECT * FROM yco_adventureworks.saleslt.productmodelproductdescription;
