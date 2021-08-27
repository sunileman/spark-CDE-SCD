Article: https://community.cloudera.com/t5/tkb/workflowpage/tkb-id/CommunityArticles/article-id/6336

To kick off airflow job (download cde cli)
./cde job run --config c_stageTable='product_staged' --config c_sourceLoc='s3a://se-uat2/sunman/product_changes/' --config c_stageCleansedTable=product_staged_cleansed  --config c_dimTable=product --name EDW-SCD-WorkFlow


product_id,product_name,aisle_id,department_id



CREATE EXTERNAL TABLE IF NOT EXISTS product_ext(
  product_id INT,
  product_name STRING,
  aisle_id int,
  department_id int)
  ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '|'
  STORED AS TEXTFILE
  LOCATION 's3a://ca0-uet2/sunman/products/'
  tblproperties ("skip.header.line.count"="1");


CREATE TABLE IF NOT EXISTS product(
  product_id INT,
  product_name STRING,
  aisle_id int,
  department_id int,
  start_date date,
  end_date date,
  is_current string DEFAULT 'Y')


insert into product
select product_id, product_name, aisle_id, department_id, current_date() as start_date, null as end_date, 'Y' as is_current from product_ext


select * from product limit 5;


///spark

spark.sql("show tables").show

import org.apache.spark.sql.SaveMode

val df = spark.read.csv("s3a://se-uat2/sunman/product_changes/")



df.select(col("_c0").as("product_id"),
  col("_c1").as("product_name"),
  col("_c2").as("aisle_id"),
  col("_c3").as("department_id")).show()






df.select(col("_c0").as("product_id"),
  col("_c1").as("product_name"),
  col("_c2").as("aisle_id"),
  col("_c3").as("department_id")).write.mode(SaveMode.Overwrite).saveAsTable("product_staged")





----

drop table product

CREATE TABLE IF NOT EXISTS product(
  product_id INT,
  product_name STRING,
  aisle_id int,
  department_id int,
  start_date date,
  end_date date,
  is_current string DEFAULT 'Y')

insert into product
select product_id, product_name, aisle_id, department_id, current_date() as start_date, null as end_date, 'Y' as is_current from product_ext


//taking care of expiring old
MERGE INTO product a
using product_staged b
ON ( a.product_id = b.product_id and a.is_current='Y')
WHEN matched THEN UPDATE SET is_current = 'N', end_date = current_date()


MERGE INTO product a
using product_staged b
ON ( a.product_id = b.product_id and a.product_name = b.product_name and a.aisle_id = b.aisle_id and a.department_id = b.department_id)
WHEN NOT matched THEN INSERT VALUES (b.product_id, b.product_name, b.aisle_id, b.department_id, current_date(), null, 'Y');


select * from product where product.product_id = 232 or product.product_id = 999999

//get changed products
select * from product_staged ps join product p on ps.product_id = p.product_id and ps.product_name != p.product_name

get new products
select * from product_staged pswhere ps.product_id not in (select product_id from product p)

