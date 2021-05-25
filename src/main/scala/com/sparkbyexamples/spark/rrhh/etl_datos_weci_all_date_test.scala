package com.sparkbyexamples.spark.rrhh

import com.databricks.spark.xml.util.XSDToSchema
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import java.nio.file.Paths


object etl_datos_weci_all_date_test {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local[1]")
      .appName("SparkByExample")
      .getOrCreate()


    //Trabajando con la nueva integración de weci
    //val dataDatePart = "2021-04-26"
    //val dataDatePart = "2021-04-27"
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
    val dataDatePart = "2021-05-19-11-48"

    val rutaCvDatalakeWorker = s"src/main/resources/rrhh/example_weci_consolid_wd/data_date_part=$dataDatePart/*.xml"
    //val rutaWeciAllBetaXml = s"src/main/resources/rrhh/example_weci_consolid_wd/data_date_part=Beta/*.xml"
    //val rutaWeciAllPayRollXml = s"src/main/resources/rrhh/example_weci_consolid_wd/data_date_part=PayRoll/*.xml"


    //////////////////////////////////////////////////////////////////////////////

    //Pruebas para importar el esquema xsd

    //val schema = XSDToSchema.read(Paths.get("src/main/resources/rrhh/example_weci_consolid_wd/schema-test/CDM_PayrollIntegrations.xsd"))
    //val schema = XSDToSchema.read(Paths.get("src/main/resources/rrhh/example_weci_consolid_wd/schema-test/CDM_CommonTypes.xsd"))
    //val schema = XSDToSchema.read(Paths.get("src/main/resources/rrhh/example_weci_consolid_wd/schema-test/CDM_CommonTypes_worker.xsd"))


/*
    var df_TestPayRoll = spark.read
      .schema(schema)
      .format("com.databricks.spark.xml")
      .option("excludeAttribute", "false")
      //Importante esta opción permite convertir todos los campos del xml a string para no tener que trabajar con otro tipo de datos
      .option("inferSchema", "false")
      ///////////////////////////////
      .option("rowTag", "ns2:EMPLOYEE_PAYROLL_INTEGRATION")
      .load(rutaCvDatalakeWorker)


    println("Imprimiendo el df de df_TestPayRoll")
    df_TestPayRoll.show()
    df_TestPayRoll.printSchema()

 */

    //////////////////////////////////////////////////////////////////////////////

    //Cargando los datos del 1er nivel según la variable datadatepart

    var df_WeciAllPayRollXmlNivel1 = spark.read
      .format("com.databricks.spark.xml")
      .option("excludeAttribute", "false")
      //Importante esta opción permite convertir todos los campos del xml a string para no tener que trabajar con otro tipo de datos
      .option("inferSchema", "false")
      ///////////////////////////////
      .option("rowTag", "ns2:EMPLOYEE_PAYROLL_INTEGRATION")
      .load(rutaCvDatalakeWorker)

    var df_WeciAllDeltaXmlNivel1 = spark.read
      .format("com.databricks.spark.xml")
      .option("excludeAttribute", "false")
      //Importante esta opción permite convertir todos los campos del xml a string para no tener que trabajar con otro tipo de datos
      .option("inferSchema", "false")
      ///////////////////////////////
      .option("rowTag", "ns1:EMPLOYEE_DELTA_INTEGRATION")
      .load(rutaCvDatalakeWorker)

    println("Imprimiendo los 2 df")
    df_WeciAllPayRollXmlNivel1.show()
    df_WeciAllPayRollXmlNivel1.printSchema()
    df_WeciAllDeltaXmlNivel1.show()
    df_WeciAllDeltaXmlNivel1.printSchema()

    var control = 0

    //////////////////////////////////////////////////////////////////////////////

    //Cargando los datos del 2do nivel según la variable datadatepart

    if (df_WeciAllPayRollXmlNivel1.columns.contains("ns2:Worker")) {
      control = 1
    }
    else if (df_WeciAllDeltaXmlNivel1.columns.contains("ns:Employees")) {
      control = 2
    }
    else {
      println("La columna de nivel 1 no coincide con las de payroll o delta")
    }

    var df_WeciAllNivel2 = df_WeciAllPayRollXmlNivel1
    var df_WeciAllEmployeeIDNivel2 = df_WeciAllPayRollXmlNivel1

    if (control == 1) {

      df_WeciAllNivel2 = spark.read
        .format("com.databricks.spark.xml")
        .option("excludeAttribute", "false")
        //Importante esta opción permite convertir
        // todos los campos del xml a string para no tener que trabajar con otro tipo de datos
        .option("inferSchema", "false")
        ///////////////////////////////
        .option("rowTag", "ns2:Employees")
        .option("ignoreNamespace", "true")
        .load(rutaCvDatalakeWorker)

      df_WeciAllEmployeeIDNivel2 = spark.read
        .format("com.databricks.spark.xml")
        .option("excludeAttribute", "false")
        //Importante esta opción permite convertir
        // todos los campos del xml a string para no tener que trabajar con otro tipo de datos
        .option("inferSchema", "false")
        ///////////////////////////////
        .option("rowTag", "ns2:Worker_Summary")
        .option("ignoreNamespace", "true")
        .load(rutaCvDatalakeWorker)
    }
    else if (control == 2) {

      df_WeciAllNivel2 = spark.read
        .format("com.databricks.spark.xml")
        .option("excludeAttribute", "false")
        //Importante esta opción permite convertir
        // todos los campos del xml a string para no tener que trabajar con otro tipo de datos
        .option("inferSchema", "false")
        ///////////////////////////////
        .option("rowTag", "ns:Employees")
        .option("ignoreNamespace", "true")
        .load(rutaCvDatalakeWorker)
    }
    else {
      println("La columna de nivel 2 no coincide con las de payroll o delta")
    }

    //println("Imprimiendo el df de nivel 2")
    //df_WeciAllNivel2.show()
    //df_WeciAllNivel2.printSchema()
    //df_WeciAllEmployeeIDNivel2.show()
    //df_WeciAllEmployeeIDNivel2.printSchema()


    //////////////////////////////////////////////////////////////////////////////

    //Cargando los datos del 3er nivel según la variable datadatepart

    var indexSummary = df_WeciAllNivel2.schema.fieldIndex("Summary")
    var isSummaryEmployeeIDPresent = df_WeciAllNivel2.schema(indexSummary).dataType.asInstanceOf[StructType].fieldNames.contains("Employee_ID")
    var isSummaryEmployeeWIDPresent = df_WeciAllNivel2.schema(indexSummary).dataType.asInstanceOf[StructType].fieldNames.contains("WID")

    var indexWorkerStatus = df_WeciAllNivel2.schema.fieldIndex("Worker_Status")
    var isWorkerStatusPresent = df_WeciAllNivel2.schema(indexWorkerStatus).dataType.asInstanceOf[StructType].fieldNames.contains("Active")


    if (isSummaryEmployeeIDPresent) {
      df_WeciAllNivel2 = df_WeciAllNivel2.withColumn("Employee_ID", col("`Summary`.`Employee_ID`"))
    } else {
      df_WeciAllNivel2 = df_WeciAllNivel2.withColumn("Employee_ID", lit(" "))
    }

    if (isSummaryEmployeeWIDPresent) {
      df_WeciAllNivel2 = df_WeciAllNivel2.withColumn("WID", col("`Summary`.`WID`"))
    } else {
      df_WeciAllNivel2 = df_WeciAllNivel2.withColumn("WID", lit(" "))
    }

    if (isWorkerStatusPresent) {
      df_WeciAllNivel2 = df_WeciAllNivel2.withColumn("Active", col("`Worker_Status`.`Active`"))
    } else {
      df_WeciAllNivel2 = df_WeciAllNivel2.withColumn("Active", lit(" "))
    }

    var indexActive = 0
    var isActiveValuePresent = false
    var isActivePriorValuePresent = false

    if (control == 1) {

      indexActive = df_WeciAllNivel2.schema.fieldIndex("Active")
      isActiveValuePresent = df_WeciAllNivel2.schema(indexActive).dataType.asInstanceOf[StructType].fieldNames.contains("_VALUE")
      isActivePriorValuePresent = df_WeciAllNivel2.schema(indexActive).dataType.asInstanceOf[StructType].fieldNames.contains("_priorValue")

      if (isActiveValuePresent) {
        df_WeciAllNivel2 = df_WeciAllNivel2.withColumn("Active_VALUE", col("`Active`.`_VALUE`"))
      } else {
        df_WeciAllNivel2 = df_WeciAllNivel2.withColumn("Active_VALUE", lit(" "))
      }

      if (isActivePriorValuePresent) {
        df_WeciAllNivel2 = df_WeciAllNivel2.withColumn("Active_PriorValue", col("`Active`.`_priorValue`"))
      } else {
        df_WeciAllNivel2 = df_WeciAllNivel2.withColumn("Active_PriorValue", lit(" "))
      }

    }
    else {
      df_WeciAllNivel2 = df_WeciAllNivel2.withColumn("Active_VALUE", lit(" "))
      df_WeciAllNivel2 = df_WeciAllNivel2.withColumn("Active_PriorValue", lit(" "))
    }

    //println("Imprimiendo el df de nivel 3")
    //df_WeciAllNivel2.show()
    //df_WeciAllNivel2.printSchema()



  } //End Main

} //End Class
