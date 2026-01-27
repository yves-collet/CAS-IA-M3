-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 1. Compléter la couche Bronze
-- MAGIC Récupérez dans Databricks toutes les
-- MAGIC tables de la base de données
-- MAGIC AdventureWorksLT, sauf les tables de
-- MAGIC metadata (i.e. ErrorLog, BuildVersion).

-- COMMAND ----------

USE CATALOG yvescollet_lakehouse;
USE DATABASE bronze;

-- COMMAND ----------

CREATE OR REPLACE TABLE address AS 
SELECT * FROM yco_adventureworks.saleslt.address;

CREATE OR REPLACE TABLE customer AS 
SELECT * FROM yco_adventureworks.saleslt.customer;

CREATE OR REPLACE TABLE customer_address AS 
SELECT * FROM yco_adventureworks.saleslt.customeraddress;

CREATE OR REPLACE TABLE product AS 
SELECT * FROM yco_adventureworks.saleslt.product;

CREATE OR REPLACE TABLE product_category AS 
SELECT * FROM yco_adventureworks.saleslt.productcategory;

CREATE OR REPLACE TABLE product_description AS 
SELECT * FROM yco_adventureworks.saleslt.productdescription;

CREATE OR REPLACE TABLE product_model AS 
SELECT * FROM yco_adventureworks.saleslt.productmodel;

CREATE OR REPLACE TABLE product_model_product_description AS 
SELECT * FROM yco_adventureworks.saleslt.productmodelproductdescription;

CREATE OR REPLACE TABLE sales_order_detail AS 
SELECT * FROM yco_adventureworks.saleslt.salesorderdetail;

CREATE OR REPLACE TABLE sales_order_header AS 
SELECT * FROM yco_adventureworks.saleslt.salesorderheader;

-- COMMAND ----------


SELECT count(*) as table_count FROM information_schema.tables WHERE table_schema = 'bronze';
