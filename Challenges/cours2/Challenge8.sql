-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 8. Configurer la sécurité
-- MAGIC Ajoutez des restrictions de lecture sur
-- MAGIC les données personnelles dans la
-- MAGIC dim_customer.

-- COMMAND ----------

USE CATALOG yvescollet_lakehouse;
USE DATABASE gold;

-- COMMAND ----------

CREATE OR REPLACE VIEW gold.v_dim_customer_secure AS
SELECT
    customer_id,
    title,
    first_name,
    middle_name,
    last_name,
    suffix,
    company_name,
    sales_person,
    CASE 
      WHEN is_account_group_member('admin') THEN email_address
      ELSE '***@***.com'
    END AS email_address,
    CASE 
      WHEN is_account_group_member('admin') THEN phone
      ELSE '***-***-****'
    END AS phone
FROM gold.dim_customer;

-- COMMAND ----------

CREATE OR REPLACE VIEW gold.v_dim_customer_rls AS
SELECT *
FROM gold.dim_customer
WHERE 
  is_account_group_member('admin') 
  OR 
  (is_account_group_member('europe_sales') AND phone LIKE '1%'); -- Exemple arbitraire

-- COMMAND ----------

SELECT * FROM gold.v_dim_customer_secure LIMIT 10;
