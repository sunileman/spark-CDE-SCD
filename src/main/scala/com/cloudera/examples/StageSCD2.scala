package com.cloudera.examples

import org.apache.spark.sql.{SaveMode, SparkSession}

/*
Create a CDE job with the following configurations vars
c_stageTable
c_sourceLoc
c_stageCleansedTable
c_dimTable

and the cde job must have the following args
{{stageTable}}
{{sourceLoc}}
{{stageCleansedTable}}
{{dimTable}}

becuase in the airflow dag CDE operator sends those as args
process_scd_staging = CDEJobRunOperator(
    task_id='cde_spark_job',
    dag=dag,
    job_name='StageSCD2',
    variables={
        'stageTable': "{{ dag_run.conf['c_stageTable'] }}",
        'sourceLoc': "{{ dag_run.conf['c_sourceLoc'] }}",
        'stageCleansedTable': "{{ dag_run.conf['c_stageCleansedTable'] }}",
        'dimTable': "{{ dag_run.conf['c_dimTable'] }}",
    }


)

execute this via CDE cli
cde job run --config c_stageTable='product_staged' --config c_sourceLoc='s3a://se-uat2/sunman/product_changes/' --config c_stageCleansedTable=product_staged_cleansed  --config c_dimTable=product --name EDW-SCD-WorkFlow


 */
object StageSCD2 {

  def main(args: Array[String]): Unit = {


    val stageTable = args(0)
    val sourceLoc = args(1)
    val stageCleansedTable = args(2)
    val dimTable = args(3)

    println("\n*******************************")
    println("\n*******************************")
    println("\n**********Stage a SCD Type 2 Table***************")
    println("stage table name: " + stageTable)
    println("source file/directory: " + sourceLoc)
    println("stage cleansed table name: " + stageCleansedTable)
    println("dimension table: " + dimTable)
    println("\n*******************************")
    println("\n*******************************")


    val spark = SparkSession.builder
      .appName("Create Stage Table for SCD Type 2 workflow")
      .getOrCreate()


    val df = spark.read.option("header", "true").option("sep", "|").csv(sourceLoc)

    df.write.mode(SaveMode.Overwrite).saveAsTable(stageTable)


    val sql1 = "select ps.product_id, ps.product_name, ps.aisle_id, ps.department_id from "+stageTable+" ps join "+dimTable+" p on ps.product_id = p.product_id and ps.product_name != p.product_name"
    println("sql1: " + sql1)
    val sql2 = "select ps.product_id, ps.product_name, ps.aisle_id, ps.department_id from "+stageTable+" ps where ps.product_id not in (select product_id from "+dimTable+" p)"
    println("sql2: " + sql1)

    val dfChanged = spark.sql(sql1)
    val dfNew = spark.sql(sql2)

    val dfUnion = dfChanged.union(dfNew)

    dfUnion.write.mode(SaveMode.Overwrite).saveAsTable(stageCleansedTable)


  }
}
