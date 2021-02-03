// scalastyle:off

package com.sparkbyexamples.spark.rrhh

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.dsl.expressions.DslSymbol
import org.apache.spark.sql.functions.{col, expr}

object ejemplo {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local[1]")
      .appName("SparkByExample")
      .getOrCreate()



    //Primeros Ejemplos de Práctica

    val df_books_withnested_array = spark.sqlContext.read
      .format("com.databricks.spark.xml")
      .option("rowTag", "book")
      .load("src/main/resources/rrhh/books_withnested_array.xml")

    val df_books = spark.sqlContext.read
      .format("com.databricks.spark.xml")
      .option("excludeAttribute", "false")
      .option("rowTag", "book")
      .load("src/main/resources/rrhh/books.xml")

    val df_persons = spark.sqlContext.read
      .format("com.databricks.spark.xml")
      .option("rowTag", "person")
      .load("src/main/resources/rrhh/persons.xml")

    val df_persons_complex = spark.sqlContext.read
      .format("com.databricks.spark.xml")
      .option("rowTag", "person")
      .load("src/main/resources/rrhh/persons_complex.xml")

    val df_records = spark.sqlContext.read
      .format("com.databricks.spark.xml")
      .option("rowTag", "Rec")
      .load("src/main/resources/rrhh/records.xml")


    println("Imprimiendo el esquema de df_books")
    //df_books.printSchema()

    println("Imprimiendo los datos de df_books")
    //df_books.show()

    println("Imprimiendo solo algunas columnas de df_books")
    df_books
      .select(
        col("author")as("a"),
        col("title")as("t"),
        col("genre")as("g"),
        col("price")as("p"),
        col("publish_date")as("pd") )
    //.show()

    println("Imprimiendo algunas columnas de df_books y aplicando 1 expresión para aumentar la fecha actual según el precio que equivaldría a días")
    df_books
      .select(
        col("author")as("a"),
        col("title")as("t"),
        col("genre")as("g"),
        col("price")as("p"),
        col("publish_date")as("pd"),
        expr("add_months(to_date(publish_date,'yyyy-MM-dd'), cast(price as int))").as("inc_date") )
    //.show()

    println("Imprimiendo algunas columnas de df_books y aplicando 1 selectExpr para la selección")
    df_books
      .selectExpr("`_id` as ID_Book", "`title` as T", "`genre` as G")
    //.show()


    println("Aplicando where y filter en la selección")
    //Filter
    df_books
      .filter(col("_id") === "bk101")
      .selectExpr("`_id` as ID_Book", "`title` as T", "`genre` as G")
    //.show()

    println("Aplicando where y filter en la selección")
    //Filter
    df_books
      .filter(col("_id") === "OH")
      .selectExpr("`_id` as ID_Book", "`title` as T", "`genre` as G")
    //.show()

    println("Aplicando where y filter en la selección")
    //Where
    df_books
      .where(col("_id") === "bk101")
      .selectExpr("`_id` as ID_Book", "`title` as T", "`genre` as G")
    //.show()

    println("Aplicando where y filter en la selección")
    //Where
    df_books
      .where(col("_id") === "bk101")
      .selectExpr("`_id` as ID_Book", "`title` as T", "`genre` as G")
    //.show()

    println("Aplicando where y filter en la selección")
    //Condition
    df_books
      .where(df_books("_id") === "bk101")
      .selectExpr("`_id` as ID_Book", "`title` as T", "`genre` as G")
    //.show(false)

    println("SQL Expression aplicando where y filter en la selección")
    //SQL Expression
    df_books
      .where("title == 'Midnight Rain'")
      .selectExpr("`_id` as ID_Book", "`title` as T", "`genre` as G")
    //.show(false)

    println("Multiple condition aplicando where y filter en la selección")
    //multiple condition
    df_books
      .where(df_books("_id") === "bk102" && df_books("title") === "Midnight Rain")
      .selectExpr("`_id` as ID_Book", "`title` as T", "`genre` as G")
    //.show(false)

    println("Struct condition aplicando where y filter en la selección")
    //Struct condition
    df_books_withnested_array
      .where(df_books_withnested_array("otherInfo.address.state") === "CA")
      .select(
        col("author")as("a"),
        col("title")as("t"),
        col("genre")as("g"),
        col("price")as("p"),
        col("otherInfo.address.state")as("pd") )
    //.show()

    println("Creando una vista temporal y ejecutando una query sql")
    //TempView
    df_books_withnested_array
      .createOrReplaceTempView("BooksWithnested")
    spark
      .sql("SELECT price*100 as salary, price*-1 as CopiedColumn, 'CA' as country FROM BooksWithnested")
    //.show()






    //Primeros Ejemplos Reales sobre SGT

    val df_xml = spark.sqlContext.read
      .format("com.databricks.spark.xml")
      .option("rowTag", "book")
      .load("src/main/resources/rrhh/books_withnested_array.xml")

    println("PrintSchema de df_xml")
    //df_xml.printSchema()

    val df_Add_Compensation_Grade_Profile = df_xml.selectExpr(
      "`wd:Add_Compensation_Grade_Profile`.`_Descriptor` as descriptor",
      "explode(`wd:Add_Compensation_Grade_Profile`.`wd:ID`) as e",
      "e.`_VALUE` as value",
      "e.`_type` as type").na.fill(" ")

    val df_Bank_of_Origin = df_xml.selectExpr(
      "`wd:Bank_of_Origin`.`_Descriptor` as descriptor",
      "explode(`wd:Bank_of_Origin`.`wd:ID`) as e",
      "e.`_VALUE` as value",
      "e.`_parent_id` as parent_id",
      "e.`_parent_type` as parent_type",
      "e.`_type` as type").na.fill(" ")

    val df_Business_Unit = df_xml.selectExpr(
      "`wd:Business_Unit`.`_Descriptor` as descriptor",
      "explode(`wd:Business_Unit`.`wd:ID`) as e",
      "e.`_VALUE` as value",
      "e.`_type` as type").na.fill(" ")

    val df_CF1 = df_xml.selectExpr(
      "`wd:CF_INT0001_LRV-Employee_Contract_for_Worker_group`.`wd:Contract_Description` as contract_description ",
      "`wd:CF_INT0001_LRV-Employee_Contract_for_Worker_group`.`wd:Contract_End_Date` as contract_end_date ",
      "`wd:CF_INT0001_LRV-Employee_Contract_for_Worker_group`.`wd:Contract_ID` as contract_id ",
      "`wd:CF_INT0001_LRV-Employee_Contract_for_Worker_group`.`wd:Contract_Start_Date` as contract_start_date ",
      "`wd:CF_INT0001_LRV-Employee_Contract_for_Worker_group`.`wd:Contract_Status`.`_Descriptor` as contract_status_descriptor ",
      "explode(`wd:CF_INT0001_LRV-Employee_Contract_for_Worker_group`.`wd:Contract_Status`.`wd:ID`) as e",
      "e.`_VALUE` as contract_status_value",
      "e.`_type` as contract_status_type")
      .na.fill(" ")


    val df_CF2 = df_xml.selectExpr(
      "`wd:CF_INT0001_LRV-Employee_Contract_for_Worker_group`.`wd:Contract_Type`.`_Descriptor` as contract_type_descriptor ",
      "`wd:CF_INT0001_LRV-Employee_Contract_for_Worker_group`.`wd:Contract_ID` as contract_id ",
      "explode(`wd:CF_INT0001_LRV-Employee_Contract_for_Worker_group`.`wd:Contract_Type`.`wd:ID`) as d",
      "d.`_VALUE` as contract_type_value",
      "d.`_type` as contract_type_type")
      .na.fill(" ")


    val df_final_union = df_CF1.as("df_general").join(df_CF2.as("df_hierarchy"),
      expr("trim(df_hierarchy.contract_id)=trim(df_general.contract_id)"), "left_outer")
      .selectExpr(
        "df_general.contract_description",
        "df_general.contract_end_date",
        "df_general.contract_id",
        "df_general.contract_start_date",
        "df_general.contract_status_descriptor",
        "df_general.contract_status_value",
        "df_general.contract_status_type",
        "if(trim(df_general.contract_description) like '%A5%',df_general.contract_end_date, ' ') as address_line_data_1",
        "if(trim(df_general.contract_description) like '%A1%',df_general.contract_end_date, ' ') as address_line_data_2",
        "df_hierarchy.contract_type_descriptor",
        "df_hierarchy.contract_type_value",
        "df_hierarchy.contract_type_type")
      .na.fill(" ").distinct()

    //df_final_union.show(15, false)


    val df_testSGTXML = spark.sqlContext.read
      .format("com.databricks.spark.xml")
      .option("rowTag", "book")
      .load("src/main/resources/rrhh/testSGTXML.xml")

    println("Imprimiendo el esquema de df_testSGTXML")
    df_testSGTXML.printSchema()






    //Ejemplos que no funcionaron

    /*

     val df_CF_INT0001_LRV_Employee_Contract_for_Worker_group = df_xml.selectExpr(
       "`wd:CF_INT0001_LRV-Employee_Contract_for_Worker_group`.`wd:Contract_Description` as contract_description ",
       "`wd:CF_INT0001_LRV-Employee_Contract_for_Worker_group`.`wd:Contract_End_Date` as contract_end_date ",
       "`wd:CF_INT0001_LRV-Employee_Contract_for_Worker_group`.`wd:Contract_ID` as contract_id ",
       "`wd:CF_INT0001_LRV-Employee_Contract_for_Worker_group`.`wd:Contract_Start_Date` as contract_start_date ",
       "`wd:CF_INT0001_LRV-Employee_Contract_for_Worker_group`.`wd:Contract_Status`.`_Descriptor` as contract_status_descriptor ",
       "explode(`wd:CF_INT0001_LRV-Employee_Contract_for_Worker_group`.`wd:Contract_Status`.`wd:ID`) as e",
       "e.`_VALUE` as contract_status_value",
       "e.`_type` as contract_status_type")
       .selectExpr(
         "`wd:CF_INT0001_LRV-Employee_Contract_for_Worker_group`.`wd:Contract_Type`.`_Descriptor` as contract_type_descriptor ",
         "explode(`wd:CF_INT0001_LRV-Employee_Contract_for_Worker_group`.`wd:Contract_Type`.`wd:ID`) as e",
         "e.`_VALUE` as contract_type_value",
         "e.`_type` as contract_type_type"
       )
       .na.fill(" ").show()


      val df_CF_INT0001_LRV_Employee_Contract_for_Worker_group_c = df_xml.withColumn(
        "vars",
        explode(
          arrays_zip(
            col("`wd:CF_INT0001_LRV-Employee_Contract_for_Worker_group`.`wd:Contract_Status`.`wd:ID`"),
            col("`wd:CF_INT0001_LRV-Employee_Contract_for_Worker_group`.`wd:Contract_Type`.`wd:ID`")
          ))
      )
        .select(
          col("`wd:CF_INT0001_LRV-Employee_Contract_for_Worker_group`.`wd:Contract_Description`")as("contract_description"),
          col("`wd:CF_INT0001_LRV-Employee_Contract_for_Worker_group`.`wd:Contract_End_Date`")as("contract_end_date"),
          col("`wd:CF_INT0001_LRV-Employee_Contract_for_Worker_group`.`wd:Contract_ID`")as("contract_id"),
          col("`wd:CF_INT0001_LRV-Employee_Contract_for_Worker_group`.`wd:Contract_Start_Date`")as("contract_start_date"),
          col("`wd:CF_INT0001_LRV-Employee_Contract_for_Worker_group`.`wd:Contract_Status`.`_Descriptor`")as("contract_status_descriptor"),
          col("`wd:CF_INT0001_LRV-Employee_Contract_for_Worker_group`.`wd:Contract_Type`.`_Descriptor`")as("contract_type_descriptor"),
          //col("vars"),
        ).na.fill(" ")


      df_xml.selExpr(
       "exlode(`wd:CF_INT0001_LRV-Employee_Contract_for_Worker_group`.`wd:Contract_Status`.`wd:ID`) as e",
       "explode(`wd:CF_INT0001_LRV-Employee_Contract_for_Worker_group`.`wd:Contract_Type`.`wd:ID`) as c")
       .selectExpr("explode(e.elem) as e", "c")
       .select("e", "c").show(5)

      "`wd:CF_INT0001_LRV-Employee_Contract_for_Worker_group`.`wd:Contract_Type`.`_Descriptor` as contract_type_descriptor ",
      "explode(`wd:CF_INT0001_LRV-Employee_Contract_for_Worker_group`.`wd:Contract_Type`.`wd:ID`) as e",
      "e.`_VALUE` as contract_type_value",
      "e.`_type` as contract_type_type",


      df.selectExpr("explode(somethingelse.mySomething.arr) as e")
       .selectExpr("explode(e.elem) as e")
       .select("e").show(100)
    */


  }

}

// scalastyle:on
