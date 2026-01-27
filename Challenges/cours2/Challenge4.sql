-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 4. Ecrire des notebooks de test
-- MAGIC Créez un notebook afin de tester la
-- MAGIC bonne historisation de quelques tables
-- MAGIC de votre pipeline, notamment
-- MAGIC sales_order_detail.

-- COMMAND ----------

USE CATALOG yvescollet_lakehouse;
USE DATABASE silver;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Test 1: Unicité des enregistrements actifs pour sales_order_detail
-- MAGIC Vérifier qu'il n'y a pas de doublons actifs pour la clé métier (sales_order_id, sales_order_detail_id).
-- MAGIC Le résultat doit être vide.

-- COMMAND ----------

SELECT 
    sales_order_id,
    sales_order_detail_id,
    COUNT(*) as count
FROM silver.sales_order_detail
WHERE _tf_valid_to IS NULL
GROUP BY sales_order_id, sales_order_detail_id
HAVING count > 1;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Test 2: Cohérence temporelle pour sales_order_detail
-- MAGIC Vérifier que la date de fin de validité est strictement supérieure à la date de début de validité (pour les enregistrements historisés).
-- MAGIC Le résultat doit être vide.

-- COMMAND ----------

SELECT *
FROM silver.sales_order_detail
WHERE _tf_valid_to IS NOT NULL 
  AND _tf_valid_to <= _tf_valid_from;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Test 3: Chevauchement des historiques pour sales_order_detail
-- MAGIC Vérifier qu'il n'y a pas de chevauchement de périodes de validité pour une même clé métier.
-- MAGIC Le résultat doit être vide.

-- COMMAND ----------

SELECT 
    t1.sales_order_id,
    t1.sales_order_detail_id,
    t1._tf_valid_from AS t1_start,
    t1._tf_valid_to AS t1_end,
    t2._tf_valid_from AS t2_start,
    t2._tf_valid_to AS t2_end
FROM silver.sales_order_detail t1
JOIN silver.sales_order_detail t2 ON 
    t1.sales_order_id = t2.sales_order_id 
    AND t1.sales_order_detail_id = t2.sales_order_detail_id
    AND t1._tf_valid_from < t2._tf_valid_from
WHERE 
    COALESCE(t1._tf_valid_to, current_timestamp()) > t2._tf_valid_from;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Test 4: Vérification générique sur d'autres tables (Address, Customer)
-- MAGIC Vérification rapide de l'unicité des enregistrements actifs.

-- COMMAND ----------

SELECT 
    address_id, 
    count(*) as count
FROM silver.address 
WHERE _tf_valid_to IS NULL 
GROUP BY address_id 
HAVING count(*) > 1;

-- COMMAND ----------

SELECT 
    customer_id, 
    count(*) as count
FROM silver.customer 
WHERE _tf_valid_to IS NULL 
GROUP BY customer_id 
HAVING count(*) > 1;
