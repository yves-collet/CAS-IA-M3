-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 9. Créer la dim_product
-- MAGIC Ecrivez le DDL et chargez la dimension
-- MAGIC produit (dim_product) basé sur les
-- MAGIC tables product, product_model. Liez la
-- MAGIC fact_sales avec la dim_product.

-- COMMAND ----------

USE CATALOG yvescollet_lakehouse;
USE DATABASE gold;

-- COMMAND ----------

CREATE OR REPLACE TABLE dim_product AS
SELECT 
    p.product_id,
    p.name AS product_name,
    p.product_number,
    p.color,
    p.standard_cost,
    p.list_price,
    p.size,
    p.weight,
    p.sell_start_date,
    p.sell_end_date,
    p.discontinued_date,
    pm.name AS model_name,
    pm.catalog_description,
    p.product_category_id,
    pc.category_name,
    pc.parent_category_name,
    pc.level
FROM silver.product p
LEFT JOIN silver.product_model pm 
    ON p.product_model_id = pm.product_model_id 
    AND pm._tf_valid_to IS NULL
LEFT JOIN gold.dim_product_category pc 
    ON p.product_category_id = pc.product_category_id
WHERE p._tf_valid_to IS NULL;

-- COMMAND ----------

SELECT 
    f.sales_order_id,
    f.product_id,
    p.product_name
FROM gold.fact_sales f
LEFT JOIN gold.dim_product p ON f.product_id = p.product_id
WHERE p.product_id IS NULL
LIMIT 10;
