package com.sparkbyexamples.spark.rrhh

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, lit}

object etl_datos_weci_all_date_test {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local[1]")
      .appName("SparkByExample")
      .getOrCreate()


    //Trabajando con la nueva integración de weci
    //val dataDatePart = "2021-04-26"
    val dataDatePart = "2021-04-27"
    //val dataDatePart = "2021-04-28"
    //val dataDatePart = "2021-04-29"
    //val dataDatePart = "2021-05-04"
    //val dataDatePart = "2021-05-06"
    //val dataDatePart = "2021-05-13-05-02"
    //val dataDatePart = "2021-05-15-05-02"
    //val dataDatePart = "2021-05-17-13-05"
    //val dataDatePart = "2021-05-17-15-05"
    //val dataDatePart = "2021-05-17-17-30"
    //val dataDatePart = "2021-05-17-20-21"
    //val dataDatePart = "2021-05-18-13-19"
    //val dataDatePart = "2021-05-18-09-30"

    val rutaCvDatalakeWorker = s"src/main/resources/rrhh/example_weci_consolid_wd/data_date_part=$dataDatePart/*.xml"
    val rutaWeciAllBetaXml = s"src/main/resources/rrhh/example_weci_consolid_wd/data_date_part=Beta/*.xml"
    val rutaWeciAllPayRollXml = s"src/main/resources/rrhh/example_weci_consolid_wd/data_date_part=PayRoll/*.xml"



    val df_WeciAllBetaXml = spark.read
      .format("com.databricks.spark.xml")
      .option("excludeAttribute", "false")
      //Importante esta opción permite convertir todos los campos del xml a string para no tener que trabajar con otro tipo de datos
      .option("inferSchema", "false")
      ///////////////////////////////
      .option("rowTag", "ns1:EMPLOYEE_DELTA_INTEGRATION")
      .load(rutaWeciAllBetaXml)

    println("Imprimiendo el esquema general de df_WeciAllBetaXml")
    df_WeciAllBetaXml.printSchema()



    val df_WeciAllPayRollXml = spark.read
      .format("com.databricks.spark.xml")
      .option("excludeAttribute", "false")
      //Importante esta opción permite convertir todos los campos del xml a string para no tener que trabajar con otro tipo de datos
      .option("inferSchema", "false")
      ///////////////////////////////
      .option("rowTag", "ns2:EMPLOYEE_PAYROLL_INTEGRATION")
      .load(rutaWeciAllPayRollXml)

    println("Imprimiendo el esquema general de df_WeciAllPayRollXml")
    df_WeciAllPayRollXml.printSchema()



    var df_WeciReadXML = spark.read
      .format("com.databricks.spark.xml")
      .option("excludeAttribute", "false")
      //Importante esta opción permite convertir todos los campos del xml a string para no tener que trabajar con otro tipo de datos
      .option("inferSchema", "false")
      ///////////////////////////////
      .option("rowTag", "ns:Employees")
      .load(rutaCvDatalakeWorker)

    println("Imprimiendo el esquema general de df_WeciReadXML")
    df_WeciReadXML.printSchema()
    df_WeciReadXML.show()
    println(df_WeciReadXML.columns.contains("ns1:Worker_Status"))




    //Primer nivel de las columnas
    if (df_WeciReadXML.columns.contains("ns1:Worker_Status")) {
      df_WeciReadXML = df_WeciReadXML.withColumn("Worker_Status", col("ns1:Worker_Status"))
    } else {
      df_WeciReadXML = df_WeciReadXML.withColumn("Worker_Status", lit(" "))
    }

    if (df_WeciReadXML.columns.contains("ns1:Worker_Summary")) {
      df_WeciReadXML = df_WeciReadXML.withColumn("Worker_Summary", col("ns2:Worker_Summary"))
    } else {
      df_WeciReadXML = df_WeciReadXML.withColumn("Worker_Summary", lit(" "))
    }


    //Segundo nivel de la columnas
    if (df_WeciReadXML.columns.contains("Worker_Summary.ns1:Active")) {
      df_WeciReadXML = df_WeciReadXML.withColumn("Worker_Summary", col("ns2:Worker_Summary"))
    } else {
      df_WeciReadXML = df_WeciReadXML.withColumn("Worker_Summary", lit(" "))
    }


    df_WeciReadXML.show()
    //df_WeciReadXML.printSchema()


    val df_test = df_WeciReadXML
    //.withColumn("Worker_Status", col("`ns:Employees`.`ns1:Worker_Status`"))
    //.withColumn("Worker_Summary", col("`ns:Employees`.`ns2:Worker_Summary`"))


    val df_Worker_Status = df_WeciReadXML
      //.withColumn("salary_plan", col("`compensationPlan`.`ns1:Salary_and_Hourly_Plan`"))
      //.withColumn("other", explode_outer(col("`ns:Employees`.`ns1:Person_Identification`.`ns1:Other_Identifier`")))
      .selectExpr(
        "'' as Active",
        "'' as  Active_Status_Date",
        "'' as  Benefits_Service_Date",
        "'' as  Company_Service_Date",
        "'' as  Continuous_Service_Date",
        "'' as  End_Employment_Date",
        "'' as  First_Day_of_Work",
        "'' as  Hire_Date",
        "'' as  Hire_Reason",
        "'' as  Hire_Rescinded",
        "'' as  Is_Rehire",
        "'' as  Not_Returning",
        "'' as  Original_Hire_Date",
        "'' as  Regrettable_Termination",
        "'' as  Return_Unknown",
        "'' as  Secondary_Termination_Reason",
        "'' as  Seniority_Date",
        "'' as  Severance_Date",
        "'' as  Status",
        "'' as  Terminated",
        "'' as  Time_Off_Service_Date",
        "'' as  Vesting_Date",
        "'' as  Worker_Reference_Type",
        s"'$dataDatePart' as data_date_part",
      ).na.fill(" ").distinct()

    //df_Worker_Summary.show()


  } //End Main

} //End Class
