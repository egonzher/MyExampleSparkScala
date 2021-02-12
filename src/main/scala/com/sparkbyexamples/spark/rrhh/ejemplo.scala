// scalastyle:off

package com.sparkbyexamples.spark.rrhh

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, explode, expr}

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
        col("author") as ("a"),
        col("title") as ("t"),
        col("genre") as ("g"),
        col("price") as ("p"),
        col("publish_date") as ("pd"))
    //.show()

    println("Imprimiendo algunas columnas de df_books y aplicando 1 expresión para aumentar la fecha actual según el precio que equivaldría a días")
    df_books
      .select(
        col("author") as ("a"),
        col("title") as ("t"),
        col("genre") as ("g"),
        col("price") as ("p"),
        col("publish_date") as ("pd"),
        expr("add_months(to_date(publish_date,'yyyy-MM-dd'), cast(price as int))").as("inc_date"))
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
        col("author") as ("a"),
        col("title") as ("t"),
        col("genre") as ("g"),
        col("price") as ("p"),
        col("otherInfo.address.state") as ("pd"))
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
      expr("`df_hierarchy`.`contract_id`==`df_general`.`contract_id`"), "left_outer")
      .selectExpr(
        "df_general.contract_description",
        "df_general.contract_end_date",
        "df_general.contract_id",
        "df_general.contract_start_date",
        "df_general.contract_status_descriptor",
        "df_general.contract_status_value",
        "df_general.contract_status_type",
        //"if(trim(df_general.contract_description) like '%A5%',df_general.contract_end_date, ' ') as address_line_data_1",
        //"if(trim(df_general.contract_description) like '%A1%',df_general.contract_end_date, ' ') as address_line_data_2",
        "df_hierarchy.contract_type_descriptor",
        "df_hierarchy.contract_type_value",
        "df_hierarchy.contract_type_type")
      .na.fill(" ").distinct()

    println("Imprimiendo el join")
    //df_final_union.show(15, false)


    val df_testSGTXML = spark.sqlContext.read
      .format("com.databricks.spark.xml")
      .option("rowTag", "book")
      .load("src/main/resources/rrhh/testSGTXML.xml")

    println("Imprimiendo el esquema de df_testSGTXML")
    //df_testSGTXML.printSchema()

    val explodedAnidado = df_xml
      .withColumn("contract_status", explode(col("`wd:CF_INT0001_LRV-Employee_Contract_for_Worker_group`.`wd:Contract_Status`.`wd:ID`")))
      .withColumn("contract_type", explode(col("`wd:CF_INT0001_LRV-Employee_Contract_for_Worker_group`.`wd:Contract_Type`.`wd:ID`")))
      .select(
        col("contract_status.`_VALUE`") as ("contract_status_value"),
        col("contract_type.`_VALUE`") as ("contract_type_value"),
        col("contract_status.`_type`") as ("contract_status_type"),
        col("contract_type.`_type`") as ("contract_type_type"),
        col("`wd:CF_INT0001_LRV-Employee_Contract_for_Worker_group`.`wd:Contract_End_Date`") as ("contract_end_date"),
        col("`wd:CF_INT0001_LRV-Employee_Contract_for_Worker_group`.`wd:Contract_ID`") as ("contract_id"),
        col("`wd:CF_INT0001_LRV-Employee_Contract_for_Worker_group`.`wd:Contract_Start_Date`") as ("contract_start_date"),
        col("`wd:CF_INT0001_LRV-Employee_Contract_for_Worker_group`.`wd:Contract_Status`.`_Descriptor`") as ("contract_status_descriptor"),
        col("`wd:CF_INT0001_LRV-Employee_Contract_for_Worker_group`.`wd:Contract_Type`.`_Descriptor`") as ("contract_type_descriptor"))
      .na
      .fill(" ")

    println("Imprimiendo el explode doble anidado en el mismo nivel")
    //explodedAnidado.show()

    println("PrintSchema de df_xml")
    df_xml.printSchema()

    val readAllXml = df_xml
      .withColumn("review_rating", explode(col("`wd:Performance_Review_Rating`.`wd:ID`")))
      .withColumn("performanceTres", explode(col("`wd:Performance_Cero`.`wd:Performance_Uno`.`wd:Performance_Dos`.`wd:Performance_Tres`.`wd:ID`")))
      .withColumn("performanceDos", explode(col("`wd:Performance_Cero`.`wd:Performance_Uno`.`wd:Performance_Dos`.`wd:ID`")))
      .select(
        col("`wd:Legal_Name_group`.`wd:FirstName`") as ("first_name"),
        col("`wd:Legal_Name_group`.`wd:LastName`") as ("last_name"),
        col("`wd:Legal_Name_group`.`wd:SecondaryLastName`") as ("secondary_last_name"),
        col("`wd:Performance_Review_Rating`.`_Descriptor`") as ("descriptor"),
        col("review_rating.*"),
        col("performanceTres._type_Tres"),
        col("performanceDos._type_Dos"))
      .na
      .fill(" ")

    println("Imprimiendo readAllXml")
    //readAllXml.show()


    //Ejemplo de Silvia
    val organizators = df_xml

    val df_int0082 = organizators
      .withColumn("natio", explode(col("`wd:National_Identifiers_group`")))
      .withColumn("educ", explode(col("`wd:education_group`")))
      .withColumn("job_class", explode(col("`wd:Job_Classifications_group`")))
      .selectExpr(
        //Estos son los explode necesarios para acceder a los campos array
        "`natio`.`wd:National_ID_Type` as National_ID_Type",
        "`educ`.`wd:Education_Is_Highest_Level_of_Education` as Education_Is_Highest_Level_of_Education",
        "`job_class`.`wd:Job_Classification_Groups`.`_Descriptor` as Job_Classification_Groups",
        //A partir de estos campos se mantiene igual
        "`wd:workdayID` as workdayID",
        "`wd:Hire_Date` as Hire_Date",
        "`wd:Hire_Reason` as Hire_Reason",
        "`wd:Original_Hire_Date` as Original_Hire_Date",
        "`wd:dateOfBirth` as date_of_Birth",
        "`wd:City_of_Birth` as City_of_Birth",
        //mexico ej descriptor pero viene de 4 formas cada una en un campo de array
        "`wd:Country_of_Birth`.`_Descriptor` as Country_of_Birth",
        //mexico ej descriptor pero viene de 4 formas cada una en un campo de array
        "`wd:Primary_Nationality`.`_Descriptor` as Primary_Nationality",
        "`wd:Preferred_Language`.`_Descriptor` as Preferred_Language", //array
        "`wd:Gender`.`_Descriptor` as Gender", //array
        "`wd:Marital_Status`.`_Descriptor` as Marital_Status", //array
        "`wd:Marital_Status_Date` as Marital_Status_Date",
        "`wd:Company_Service_Date` as Company_Service_Date",
        //array con parent ,type y value
        "`wd:Bank_of_Origin`.`_Descriptor` as Bank_of_Origin",
        "`wd:Manager_-_Level_01_group`.`wd:Manager_primaryWorkPhone` as Manager_primaryWorkPhone",
        "`wd:Supervisory_Organization_Name` as Supervisory_Organization_Name",
        "`wd:Collective_Agreement_group`.`wd:Collective_Agreement_Name` as Collective_Agreement_Name",
        "`wd:Collective_Agreement_group`.`wd:Collective_Agreement_Start_date` as Collective_Agreement_Start_date",
        "`wd:Total_Base_Pay_Amount` as Total_Base_Pay_Amount",
        "`wd:Total_Base_Pay_Currency`.`_Descriptor` as Total_Base_Pay_Currency", //array
        "`wd:Total_Base_Pay_Frequency`.`_Descriptor` as Total_Base_Pay_Frequency", //array
        //array con parent ,type y value
        "`wd:Worker_Contract_Type`.`_Descriptor` as Worker_Contract_Type",
        "`wd:Primary_Work_Address_group`.`wd:Work_Address_Line_1` as Work_Address_Line_1",
        "`wd:Primary_Work_Address_group`.`wd:Work_Postal_Code` as Work_Postal_Code",
        "`wd:Primary_Work_Address_group`.`wd:Work_city` as Work_city",
        "`wd:Primary_Work_Address_group`.`wd:Work_country`.`_Descriptor` as Work_country", //array
        "`wd:Name_of_HR_Partner_for_Worker` as Name_of_HR_Partner_for_Worker",
        "`wd:Email_-_Home_group`.`wd:Home_emailAddress` as Home_emailAddress",
        "`wd:Email_-_Work_group`.`wd:Email_Address` as work_group_Email_Address",
        "`wd:Phone_Number_of_HR_Partner_for_Worker` as Phone_Number_of_HR_Partner_for_Worker",
        //Estos tuve que crearlos porque no existían en el xml
        "`wd:Disability`.`_Descriptor` as Disability", //array
        "`wd:Disability_Status_Date`.`_Descriptor` as Disability_Status_Date", //array
        "`wd:Talent_Pool_Any`.`_Descriptor` as Talent_Pool_Any", //array
        "`wd:Management_Level`.`_Descriptor` as Management_Level", //array
        "`wd:Scheduled_Weekly_Hours` as Scheduled_Weekly_Hours",
        "`wd:Primary_Work_Address_group`.`wd:Work_Address_Line_2` as Work_Address_Line_2",
        //Este tuve que agregar el nivel inicial wd:Position_group pq o no era capaz de encontrar el .`wd:Home_Country`.`_Descriptor`
        "`wd:Position_group`.`wd:Home_Country`.`_Descriptor` as Home_Country"
      ).na.fill(" ")


    println("Imprimiendo df_int0082")
    //df_int0082.show()


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
