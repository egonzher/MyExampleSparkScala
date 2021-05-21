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
    val dataDatePart = "2021-04-26"
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
    //val dataDatePart = "2021-05-19-11-48"

    val rutaCvDatalakeWorker = s"src/main/resources/rrhh/example_weci_consolid_wd/data_date_part=$dataDatePart/*.xml"
    //val rutaWeciAllBetaXml = s"src/main/resources/rrhh/example_weci_consolid_wd/data_date_part=Beta/*.xml"
    //val rutaWeciAllPayRollXml = s"src/main/resources/rrhh/example_weci_consolid_wd/data_date_part=PayRoll/*.xml"


    //////////////////////////////////////////////////////////////////////////////

    //Pruebas para importar el esquema xsd

    //val schema = XSDToSchema.read(Paths.get("src/main/resources/rrhh/example_weci_consolid_wd/schema-test/CDM_PayrollIntegrations.xsd"))
    val schema = XSDToSchema.read(Paths.get("src/main/resources/rrhh/example_weci_consolid_wd/schema-test/CDM_CommonTypes.xsd"))

    //println(schema)

    var df_TestPayRoll = spark.read
      .schema(schema)
      .format("com.databricks.spark.xml")
      .option("excludeAttribute", "false")
      //Importante esta opción permite convertir todos los campos del xml a string para no tener que trabajar con otro tipo de datos
      .option("inferSchema", "false")
      ///////////////////////////////
      .option("rowTag", "ns2:EMPLOYEE_PAYROLL_INTEGRATION")
      .load(rutaCvDatalakeWorker)


    //println("Imprimiendo el df de df_TestPayRoll")
    //df_TestPayRoll.show()
    df_TestPayRoll.printSchema()

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

    //println("Imprimiendo los 2 df")
    //df_WeciAllPayRollXmlNivel1.show()
    //df_WeciAllPayRollXmlNivel1.printSchema()
    //df_WeciAllDeltaXmlNivel1.show()
    //df_WeciAllDeltaXmlNivel1.printSchema()

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


    /*


      if (df_WeciAllNivel2.columns.contains("Active_Status_Date")) {
        df_WeciAllNivel2 = df_WeciAllNivel2.withColumn("Active_Status_Date", col("Active_Status_Date"))

      } else {
        df_WeciAllNivel2 = df_WeciAllNivel2.withColumn("Active_Status_Date", lit(" "))
      }
      if (df_WeciAllNivel2.columns.contains("Benefits_Service_Date")) {
        df_WeciAllNivel2 = df_WeciAllNivel2.withColumn("Benefits_Service_Date", col("Benefits_Service_Date"))

      } else {
        df_WeciAllNivel2 = df_WeciAllNivel2.withColumn("Benefits_Service_Date", lit(" "))
      }
      if (df_WeciAllNivel2.columns.contains("Company_Service_Date")) {
        df_WeciAllNivel2 = df_WeciAllNivel2.withColumn("Company_Service_Date", col("Company_Service_Date"))

      } else {
        df_WeciAllNivel2 = df_WeciAllNivel2.withColumn("Company_Service_Date", lit(" "))
      }
      if (df_WeciAllNivel2.columns.contains("Continuous_Service_Date")) {
        df_WeciAllNivel2 = df_WeciAllNivel2.withColumn("Continuous_Service_Date", col("Continuous_Service_Date"))

      } else {
        df_WeciAllNivel2 = df_WeciAllNivel2.withColumn("Continuous_Service_Date", lit(" "))
      }
      if (df_WeciAllNivel2.columns.contains("End_Employment_Date")) {
        df_WeciAllNivel2 = df_WeciAllNivel2.withColumn("End_Employment_Date", col("End_Employment_Date"))

      } else {
        df_WeciAllNivel2 = df_WeciAllNivel2.withColumn("End_Employment_Date", lit(" "))
      }
      if (df_WeciAllNivel2.columns.contains("First_Day_of_Work")) {
        df_WeciAllNivel2 = df_WeciAllNivel2.withColumn("First_Day_of_Work", col("First_Day_of_Work"))

      } else {
        df_WeciAllNivel2 = df_WeciAllNivel2.withColumn("First_Day_of_Work", lit(" "))
      }
      if (df_WeciAllNivel2.columns.contains("Hire_Date")) {
        df_WeciAllNivel2 = df_WeciAllNivel2.withColumn("Hire_Date", col("Hire_Date"))

      } else {
        df_WeciAllNivel2 = df_WeciAllNivel2.withColumn("Hire_Date", lit(" "))
      }
      if (df_WeciAllNivel2.columns.contains("Hire_Reason")) {
        df_WeciAllNivel2 = df_WeciAllNivel2.withColumn("Hire_Reason", col("Hire_Reason"))

      } else {
        df_WeciAllNivel2 = df_WeciAllNivel2.withColumn("Hire_Reason", lit(" "))
      }
      if (df_WeciAllNivel2.columns.contains("Hire_Rescinded")) {
        df_WeciAllNivel2 = df_WeciAllNivel2.withColumn("Hire_Rescinded", col("Hire_Rescinded"))

      } else {
        df_WeciAllNivel2 = df_WeciAllNivel2.withColumn("Hire_Rescinded", lit(" "))
      }
      if (df_WeciAllNivel2.columns.contains("Is_Rehire")) {
        df_WeciAllNivel2 = df_WeciAllNivel2.withColumn("Is_Rehire", col("Is_Rehire"))

      } else {
        df_WeciAllNivel2 = df_WeciAllNivel2.withColumn("Is_Rehire", lit(" "))
      }
      if (df_WeciAllNivel2.columns.contains("Not_Returning")) {
        df_WeciAllNivel2 = df_WeciAllNivel2.withColumn("Not_Returning", col("Not_Returning"))

      } else {
        df_WeciAllNivel2 = df_WeciAllNivel2.withColumn("Not_Returning", lit(" "))
      }
      if (df_WeciAllNivel2.columns.contains("Original_Hire_Date")) {
        df_WeciAllNivel2 = df_WeciAllNivel2.withColumn("Original_Hire_Date", col("Original_Hire_Date"))

      } else {
        df_WeciAllNivel2 = df_WeciAllNivel2.withColumn("Original_Hire_Date", lit(" "))
      }
      if (df_WeciAllNivel2.columns.contains("Regrettable_Termination")) {
        df_WeciAllNivel2 = df_WeciAllNivel2.withColumn("Regrettable_Termination", col("Regrettable_Termination"))

      } else {
        df_WeciAllNivel2 = df_WeciAllNivel2.withColumn("Regrettable_Termination", lit(" "))
      }
      if (df_WeciAllNivel2.columns.contains("Return_Unknown")) {
        df_WeciAllNivel2 = df_WeciAllNivel2.withColumn("Return_Unknown", col("Return_Unknown"))

      } else {
        df_WeciAllNivel2 = df_WeciAllNivel2.withColumn("Return_Unknown", lit(" "))
      }
      if (df_WeciAllNivel2.columns.contains("Secondary_Termination_Reason")) {
        df_WeciAllNivel2 = df_WeciAllNivel2.withColumn("Secondary_Termination_Reason", col("Secondary_Termination_Reason"))

      } else {
        df_WeciAllNivel2 = df_WeciAllNivel2.withColumn("Secondary_Termination_Reason", lit(" "))
      }
      if (df_WeciAllNivel2.columns.contains("Seniority_Date")) {
        df_WeciAllNivel2 = df_WeciAllNivel2.withColumn("Seniority_Date", col("Seniority_Date"))

      } else {
        df_WeciAllNivel2 = df_WeciAllNivel2.withColumn("Seniority_Date", lit(" "))
      }
      if (df_WeciAllNivel2.columns.contains("Severance_Date")) {
        df_WeciAllNivel2 = df_WeciAllNivel2.withColumn("Severance_Date", col("Severance_Date"))

      } else {
        df_WeciAllNivel2 = df_WeciAllNivel2.withColumn("Severance_Date", lit(" "))
      }
      if (df_WeciAllNivel2.columns.contains("Status")) {
        df_WeciAllNivel2 = df_WeciAllNivel2.withColumn("Status", col("Status"))

      } else {
        df_WeciAllNivel2 = df_WeciAllNivel2.withColumn("Status", lit(" "))
      }
      if (df_WeciAllNivel2.columns.contains("Terminated")) {
        df_WeciAllNivel2 = df_WeciAllNivel2.withColumn("Terminated", col("Terminated"))

      } else {
        df_WeciAllNivel2 = df_WeciAllNivel2.withColumn("Terminated", lit(" "))
      }
      if (df_WeciAllNivel2.columns.contains("Time_Off_Service_Date")) {
        df_WeciAllNivel2 = df_WeciAllNivel2.withColumn("Time_Off_Service_Date", col("Time_Off_Service_Date"))

      } else {
        df_WeciAllNivel2 = df_WeciAllNivel2.withColumn("Time_Off_Service_Date", lit(" "))
      }
      if (df_WeciAllNivel2.columns.contains("Vesting_Date")) {
        df_WeciAllNivel2 = df_WeciAllNivel2.withColumn("Vesting_Date", col("Vesting_Date"))

      } else {
        df_WeciAllNivel2 = df_WeciAllNivel2.withColumn("Vesting_Date", lit(" "))
      }
      if (df_WeciAllNivel2.columns.contains("Worker_Reference_Type")) {
        df_WeciAllNivel2 = df_WeciAllNivel2.withColumn("Worker_Reference_Type", col("Worker_Reference_Type"))

      } else {
        df_WeciAllNivel2 = df_WeciAllNivel2.withColumn("Worker_Reference_Type", lit(" "))
      }
      if (df_WeciAllNivel2.columns.contains("_isAdded")) {
        df_WeciAllNivel2 = df_WeciAllNivel2.withColumn("isAdded", col("_isAdded"))

      } else {
        df_WeciAllNivel2 = df_WeciAllNivel2.withColumn("isAdded", lit(" "))
      }
      if (df_WeciAllNivel2.columns.contains("_isDeleted")) {
        df_WeciAllNivel2 = df_WeciAllNivel2.withColumn("isDeleted", col("_isDeleted"))

      } else {
        df_WeciAllNivel2 = df_WeciAllNivel2.withColumn("isDeleted", lit(" "))
      }
      if (df_WeciAllNivel2.columns.contains("_isUpdated")) {
        df_WeciAllNivel2 = df_WeciAllNivel2.withColumn("isUpdated", col("_isUpdated"))

      } else {
        df_WeciAllNivel2 = df_WeciAllNivel2.withColumn("isUpdated", lit(" "))
      }


     val df_Worker_Status = df_WeciAllNivel2
        .selectExpr(
          "`Active` as Active",
          "`Active_Status_Date` as  Active_Status_Date",
          "`Benefits_Service_Date` as  Benefits_Service_Date",
          "`Company_Service_Date` as  Company_Service_Date",
          "`Continuous_Service_Date` as  Continuous_Service_Date",
          "`End_Employment_Date` as  End_Employment_Date",
          "`First_Day_of_Work` as  First_Day_of_Work",
          "`Hire_Date` as  Hire_Date",
          "`Hire_Reason` as  Hire_Reason",
          "`Hire_Rescinded` as  Hire_Rescinded",
          "`Is_Rehire` as  Is_Rehire",
          "`Not_Returning` as  Not_Returning",
          "`Original_Hire_Date` as  Original_Hire_Date",
          "`Regrettable_Termination` as  Regrettable_Termination",
          "`Return_Unknown` as  Return_Unknown",
          "`Secondary_Termination_Reason` as  Secondary_Termination_Reason",
          "`Seniority_Date` as  Seniority_Date",
          "`Severance_Date` as  Severance_Date",
          "`Status` as  Status",
          "`Terminated` as  Terminated",
          "`Time_Off_Service_Date` as  Time_Off_Service_Date",
          "`Vesting_Date` as  Vesting_Date",
          "`Worker_Reference_Type` as  Worker_Reference_Type",
          "`isAdded` as  isAdded",
          "`isDeleted` as  isDeleted",
          "`isUpdated` as  isUpdated",
          s"'$dataDatePart' as data_date_part",
        ).na.fill(" ").distinct()

      //df_Worker_Status.show()


     */

  } //End Main

} //End Class
