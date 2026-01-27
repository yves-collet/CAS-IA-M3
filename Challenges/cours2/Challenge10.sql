-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 10. Créer la dim_product_category
-- MAGIC Ecrivez le DDL et chargez la dimension
-- MAGIC catégorie de produit
-- MAGIC (dim_product_category) en créant
-- MAGIC autant de niveau de colonnes que
-- MAGIC nécessaire.
-- MAGIC Ajouter la clé de la
-- MAGIC dim_product_category dans la
-- MAGIC dim_product.

-- COMMAND ----------

USE CATALOG yvescollet_lakehouse;
USE DATABASE gold;

-- COMMAND ----------

CREATE OR REPLACE TABLE dim_product_category AS
SELECT 
    cat.product_category_id,
    cat.name AS category_name,
    parent.product_category_id AS parent_category_id,
    parent.name AS parent_category_name,
    CASE 
        WHEN parent.product_category_id IS NULL THEN 1 
        ELSE 2 
    END AS level,
    COALESCE(parent.name, cat.name) AS root_category_name
FROM silver.product_category cat
LEFT JOIN silver.product_category parent 
    ON cat.parent_product_category_id = parent.product_category_id
    AND parent._tf_valid_to IS NULL
WHERE cat._tf_valid_to IS NULL;

-- COMMAND ----------

SELECT * FROM dim_product_category ORDER BY level, root_category_name, category_name;
