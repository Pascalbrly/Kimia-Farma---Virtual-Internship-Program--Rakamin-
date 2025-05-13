CREATE OR REPLACE TABLE `kimia-farma-pbi-458804.kimia_farma_pbi.join_data_kimia_farma` AS
with

-- CTE Tabel Inventory
table_inv AS (
  SELECT * FROM `kimia-farma-pbi-458804.kimia_farma_pbi.Inventory`
),
-- CTE Tabel Transaction
table_ft AS (
  SELECT * FROM `kimia-farma-pbi-458804.kimia_farma_pbi.final_transaction`
),
-- CTE Tabel Kantor Cabang
table_kc AS (
  SELECT * FROM `kimia-farma-pbi-458804.kimia_farma_pbi.kantor_cabang`
),
-- CTE Tabel Product
table_pro AS (
  SELECT * FROM `kimia-farma-pbi-458804.kimia_farma_pbi.product`
),

-- -- Cek Missing Value Final Transaction
-- missing_ft as(
--   select  
--   count(*) as total_rows, -- Jumlah Row
--   countif(transaction_id is null) as missing_transaction, -- Jumlah missing transaction
--   countif(date is null) as missing_date, -- Jumlah missing date
--   countif(branch_id is null) as missing_branch, -- Jumlah missing branch id
--   countif(customer_name is null) as missing_cust_name, -- Jumlah missing customer name
--   countif(product_id is null) as missing_product_id, -- Jumlah missing product id
--   countif(price is null) as missing_price, -- Jumlah missing price
--   countif(discount_percentage is null) as missing_discount_percentage, -- Jumlah missing discount
--   countif(rating is null) as missing_rating -- Jumlah missing rating
--   from table_ft
-- ),

-- -- Cek Missing Value Inventory
-- missing_inv as(
--   select  
--   count(*) as total_rows,
--   countif(Inventory_id is null) as missing_inventory,
--   countif(branch_id is null) as missing_branch,
--   countif(product_id is null) as missing_product_id,
--   countif(product_name is null) as missing_product_name,
--   countif(opname_stock is null) as missing_opname_stock
--   from table_inv
-- ),

-- -- Cek Missing Value Kantor Cabang
-- missing_kc as(
--   select  
--   count(*) as total_rows,
--   countif(branch_id is null) as missing_branch,
--   countif(branch_category is null) as missing_branch_category,
--   countif(branch_name is null) as missing_branch_name,
--   countif(kota is null) as missing_kota,
--   countif(provinsi is null) as missing_provinsi,
--   countif(rating is null) as missing_rating
--   from table_kc
-- ),

-- -- Cek Missing Value Product
-- missing_pro as(
--   select  
--   count(*) as total_rows,
--   countif(product_id is null) as missing_pro,
--   countif(product_category is null) as missing_product_category,
--   countif(product_name is null) as missing_product_name,
--   countif(price is null) as missing_price
--   from table_pro
-- ),

-- CTE Tabel Inventory: Bersihkan missing values
cleaned_inventory AS (
  SELECT * 
  FROM (
    SELECT *,
    ROW_NUMBER() OVER(PARTITION BY inventory_id, branch_id, product_id) AS nRow
    from table_inv
    WHERE Inventory_id IS NOT NULL
    AND branch_id IS NOT NULL
    AND product_id IS NOT NULL
    AND product_name IS NOT NULL
    AND opname_stock IS NOT NULL
  ) AS sub
  WHERE nRow = 1
),

-- CTE Tabel Final Transaction: Bersihkan missing values
cleaned_transaction AS (
  SELECT * 
  FROM (
    SELECT *,
    ROW_NUMBER() OVER(PARTITION BY transaction_id, date, branch_id) AS nRow
  from table_ft
  WHERE transaction_id IS NOT NULL
    AND date IS NOT NULL
    AND branch_id IS NOT NULL
    AND customer_name IS NOT NULL
    AND product_id IS NOT NULL
    AND price IS NOT NULL
    AND discount_percentage IS NOT NULL
    AND rating IS NOT NULL
  ) AS sub
  WHERE nRow = 1
),

-- CTE Tabel Kantor Cabang: Bersihkan missing values
cleaned_branch AS (
  SELECT * 
  FROM (
    SELECT *,
    ROW_NUMBER() OVER(PARTITION BY branch_id, branch_name, kota) AS nRow
  from table_kc
  WHERE branch_id IS NOT NULL
    AND branch_category IS NOT NULL
    AND branch_name IS NOT NULL
    AND kota IS NOT NULL
    AND provinsi IS NOT NULL
    AND rating IS NOT NULL
  ) AS sub
  WHERE nRow = 1
),

-- CTE Tabel Product: Bersihkan missing values
cleaned_product AS (
  SELECT * 
  FROM (
    SELECT *,
    ROW_NUMBER() OVER(PARTITION BY product_id, product_name, product_category) AS nRow
  from table_pro
  WHERE product_id IS NOT NULL
    AND product_category IS NOT NULL
    AND product_name IS NOT NULL
    AND price IS NOT NULL
  ) AS sub
  WHERE nRow = 1
),

-- Gabungkan semua tabel dengan INNER JOIN
joined_data AS (
  SELECT
    ft.transaction_id,
    ft.date,
    ft.branch_id,
    kc.branch_category,
    kc.branch_name,
    kc.kota,
    kc.provinsi,
    ft.customer_name,
    ft.product_id,
    pro.product_name,
    pro.product_category,
    ft.price AS total_price,
    ft.discount_percentage,
    pro.price AS item_price,
    -- Kalkulasi nett_sales
    ft.price * (1 - ft.discount_percentage / 100.0) AS nett_sales,

    -- Kalkulasi persentase_gross_laba
    CASE
      WHEN ft.price <= 50000 THEN 0.10
      WHEN ft.price <= 100000 THEN 0.15
      WHEN ft.price <= 300000 THEN 0.20
      WHEN ft.price <= 500000 THEN 0.25
      ELSE 0.30
    END AS persentase_gross_laba,

    -- Kalkulasi nett_profit
    (ft.price * (1 - ft.discount_percentage / 100.0)) * 
    CASE
      WHEN ft.price <= 50000 THEN 0.10
      WHEN ft.price <= 100000 THEN 0.15
      WHEN ft.price <= 300000 THEN 0.20
      WHEN ft.price <= 500000 THEN 0.25
      ELSE 0.30
    END AS nett_profit,
    ft.rating as rating_transaction
    
  FROM cleaned_transaction AS ft
  JOIN cleaned_branch AS kc
    ON ft.branch_id = kc.branch_id
  JOIN cleaned_product AS pro
    ON ft.product_id = pro.product_id
)

-- select * from remove_duplicate
-- where nRow = 1


-- Penggunaan salah satu CTE
-- SELECT 
--   SUM(dupe_count) AS total_duplicate_rows
-- FROM (
--   SELECT 
--     COUNT(*) AS dupe_count
--   FROM table_ft
--   GROUP BY transaction_id
--   HAVING COUNT(*) > 1
-- )

SELECT * FROM joined_data;

-- select * from  `kimia-farma-pbi-458804.kimia_farma_pbi.join_data_kimia_farma`
