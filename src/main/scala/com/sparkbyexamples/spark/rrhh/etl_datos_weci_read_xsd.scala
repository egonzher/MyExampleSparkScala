package com.sparkbyexamples.spark.rrhh

import com.databricks.spark.xml.util.XSDToSchema
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import java.nio.file.Paths


object etl_datos_weci_read_xsd {
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
    //val dataDatePart = "2021-05-19-11-48"
    //val dataDatePart = "2021-05-20-17-19"
    val dataDatePart = "2021-05-20-17-19-38"

    val rutaCvDatalakeWorker = s"src/main/resources/rrhh/example_weci_consolid_wd/data_date_part=$dataDatePart/*.xml"
    //val rutaWeciAllBetaXml = s"src/main/resources/rrhh/example_weci_consolid_wd/data_date_part=Beta/*.xml"
    //val rutaWeciAllPayRollXml = s"src/main/resources/rrhh/example_weci_consolid_wd/data_date_part=PayRoll/*.xml"


    //////////////////////////////////////////////////////////////////////////////

    //Pruebas para importar el esquema xsd

    //val schema = XSDToSchema.read(Paths.get("src/main/resources/rrhh/example_weci_consolid_wd/schema-test/CDM_PayrollIntegrations.xsd"))
    //val schema = XSDToSchema.read(Paths.get("src/main/resources/rrhh/example_weci_consolid_wd/schema-test/CDM_CommonTypes.xsd"))
    val schema = XSDToSchema.read(Paths.get("src/main/resources/rrhh/example_weci_consolid_wd/schema-test/CDM_CommonTypes_worker.xsd"))
    //val xsdTest = s"src/main/resources/rrhh/example_weci_consolid_wd/schema-test/CDM_CommonTypes_worker.xsd"

    //1er ejemplo buscando las rutas en hdfs
    //val fs = FileSystem.get(new Configuration())
    //val status = fs.listStatus(new Path(YOUR_HDFS_PATH))
    //status.foreach(x=> println(x.getPath))

    //2do ejemplo buscando las rutas en hdfs
    //val hdfs = FileSystem.get(new URI("hdfs://yourUrl:port/"), new Configuration())
    //val path = new Path("/path/to/file/")
    //val stream = hdfs.open(path)
    //def readLines = Stream.cons(stream.readLine, Stream.continually( stream.readLine))
    //This example checks line for null and prints every existing line consequentally
    //readLines.takeWhile(_ != null).foreach(line => println(line))

    //////////////////////////////////////////////////////////////////////////////

    /*
    var df_TestPayRoll = spark.read
      .schema(schema)
      .format("com.databricks.spark.xml")
      .option("excludeAttribute", "false")
      //Importante esta opción permite convertir todos los campos del xml a string para no tener que trabajar con otro tipo de datos
      .option("inferSchema", "false")
      .option("ignoreNamespace", "true")
      ///////////////////////////////
      //.option("rowTag", "ns:Employees")
      //.option("rowTag", "ns:Employees")
      .option("rowTag", "Summary")
      .load(rutaCvDatalakeWorker)


    println("Imprimiendo el df de df_TestPayRoll")
    df_TestPayRoll.printSchema()
    df_TestPayRoll.show()

     */

    /*

    val df_WorkerSumary = df_TestPayRoll
      .selectExpr(
        "`Summary`.`WID`.`_VALUE` as WID_VALUE",
        "`Summary`.`WID`.`_PriorValue` as WID_PriorValue",
        "`Summary`.`WID`.`_truncated` as WID_truncated",
        "`Summary`.`Employee_ID`.`_VALUE` as Employee_ID_VALUE",
        "`Summary`.`Employee_ID`.`_PriorValue` as Employee_ID_PriorValue",
        "`Summary`.`Employee_ID`.`_truncated` as Employee_ID_truncated",
        "`Summary`.`Prehire_ID`.`_VALUE` as Prehire_ID_VALUE",
        "`Summary`.`Prehire_ID`.`_PriorValue` as Prehire_ID_PriorValue",
        "`Summary`.`Prehire_ID`.`_truncated` as Prehire_ID_truncated",
        "`Summary`.`Name`.`_VALUE` as Name_VALUE",
        "`Summary`.`Name`.`_priorValue` as Name_PriorValue",
        "`Summary`.`Name`.`_truncated` as Name_truncated",
        "`Summary`.`Updated_From_Override` as Updated_From_Override",
        "`Summary`.`Workday_Account` as Workday_Account",
        s"'$dataDatePart' as data_date_part",
      ).na.fill(" ").distinct()

    df_WorkerSumary.printSchema()
    df_WorkerSumary.show(100,false)


     */


    //////////////////////////////////////////////////////////////////////////////

    //Usar si son weci antiguos
    var df_TestPayRoll = spark.read
      //.schema(schema)
      .format("com.databricks.spark.xml")
      .option("excludeAttribute", "false")
      //Importante esta opción permite convertir todos los campos del xml a string para no tener que trabajar con otro tipo de datos
      .option("inferSchema", "false")
      .option("ignoreNamespace", "true")
      ///////////////////////////////
      .option("rowTag", "ns:Employees") //
      .load(rutaCvDatalakeWorker) //

    // Si son weci nuevos
    var df_nuevos_weci = spark.read
      //.schema(schema)
      .format("com.databricks.spark.xml")
      .option("excludeAttribute", "false")
      .option("inferSchema", "false")
      .option("ignoreNamespace", "true")
      ///////////////////////////////
      .option("rowTag", "ns2:EMPLOYEE_DELTA_INTEGRATION")
      .load(rutaCvDatalakeWorker) // nuevos

    if (df_TestPayRoll.columns.contains("Worker")) {
      df_TestPayRoll = spark.read
        .schema(schema)
        .format("com.databricks.spark.xml")
        .option("excludeAttribute", "false")
        .option("inferSchema", "false")
        .option("ignoreNamespace", "true")
        ///////////////////////////////
        .option("rowTag", "ns2:EMPLOYEE_DELTA_INTEGRATION")
        .load(rutaCvDatalakeWorker)
      val df_final_nuevo = df_TestPayRoll
        //.withColumn("secondary_termination",col("`Employees`.`Worker_Status`.`Secondary_Termination_Reason`"))
        .selectExpr(
          "`Worker`.`Worker_Summary`.`Employee_ID` as Employee_ID_VALUE",
          "`Worker`.`Worker_Summary`.`WID` as WID_VALUE",
          s"'$dataDatePart' as data_date_part",
        ).na.fill(" ").distinct()
      df_final_nuevo.show()
    }
    else {
      df_TestPayRoll = spark.read
        //.schema(schema)
        .format("com.databricks.spark.xml")
        .option("excludeAttribute", "false")
        .option("inferSchema", "false")
        .option("ignoreNamespace", "true")
        ///////////////////////////////
        .option("rowTag", "ns2:EMPLOYEE_DELTA_INTEGRATION")
        .load(rutaCvDatalakeWorker) //

      df_TestPayRoll.show()
      df_TestPayRoll.printSchema()

      if (df_TestPayRoll.columns.contains("Worker")) {
        df_TestPayRoll = spark.read
          .schema(schema)
          .format("com.databricks.spark.xml")
          .option("excludeAttribute", "false")
          .option("inferSchema", "false")
          .option("ignoreNamespace", "true")
          ///////////////////////////////
          .option("rowTag", "ns2:EMPLOYEE_DELTA_INTEGRATION")
          .load(rutaCvDatalakeWorker) // Escribe rutaWeciAllPayRollXml para nuevos weci

        // Escribe rutaWeciAllBetaXml para viejos
        val df_final_nuevo = df_TestPayRoll
          //.withColumn("secondary_termination",col("`Employees`.`Worker_Status`.`Secondary_Termination_Reason`"))
          .selectExpr(
            "`Worker`.`Worker_Summary`.`Employee_ID` as Employee_ID_VALUE",
            "`Worker`.`Worker_Summary`.`WID` as WID_VALUE",
            s"'$dataDatePart' as data_date_part",
          ).na.fill(" ").distinct()

        df_final_nuevo.show()

        // Accediendo a los campos de worker
        var df_nuevos_weci_worker = spark.read
          .format("com.databricks.spark.xml")
          .option("excludeAttribute", "false")
          .option("inferSchema", "false")
          .option("ignoreNamespace", "true")
          ///////////////////////////////
          .option("rowTag", "ns2:Employees")
          .load(rutaCvDatalakeWorker) // nuevos

        df_nuevos_weci_worker = df_nuevos_weci_worker
          .withColumn("Summary_Employee_ID", col("`Summary`.`Employee_ID`"))
          .withColumn("Summary_WID", col("`Summary`.`WID`"))
          .withColumn("Worker_Status_Active_VALUE", col("`Worker_Status`.`Active`.`_VALUE`"))
          .withColumn("Worker_Status_Active_priorValue", col("`Worker_Status`.`Active`.`_priorValue`"))

        df_nuevos_weci_worker.show()
        //df_nuevos_weci_worker.printSchema()

      }
      else {
        df_TestPayRoll = spark.read
          .schema(schema)
          .format("com.databricks.spark.xml")
          .option("excludeAttribute", "false")
          .option("inferSchema", "false")
          .option("ignoreNamespace", "true")
          ///////////////////////////////
          .option("rowTag", "ns:Employees")
          .load(rutaCvDatalakeWorker)
        val df_final = df_TestPayRoll
          .withColumn("secondary_termination", col("`Worker_Status`.`Secondary_Termination_Reason`"))
          .selectExpr(
            "`Summary`.`Employee_ID`.`_VALUE` as Employee_ID_VALUE",
            "`Summary`.`WID`.`_VALUE` as WID_VALUE",
            "`Worker_Status`.`Staffing_Event`.`_VALUE` as Staffing_Event_VALUE",
            "`Worker_Status`.`Staffing_Event`.`_isAdded` as Staffing_Event_isAdded",
            "`Worker_Status`.`Staffing_Event`.`_isDeleted` as Staffing_Event_isDeleted",
            "`Worker_Status`.`Staffing_Event`.`_priorValue` as Staffing_Event_priorValue",
            "`Worker_Status`.`Staffing_Event_Date`.`_VALUE` as Staffing_Event_Date_VALUE",
            "`Worker_Status`.`Staffing_Event_Date`.`_isAdded` as Staffing_Event_Date_isAdded",
            "`Worker_Status`.`Staffing_Event_Date`.`_isDeleted` as Staffing_Event_Date_isDeleted",
            "`Worker_Status`.`Staffing_Event_Date`.`_priorValue` as Staffing_Event_Date_priorValue",
            s"'$dataDatePart' as data_date_part",
          ).na.fill(" ").distinct()
        df_final.show()
      }
    }




    //////////////////////////////////////////////////////////////////////////////


  } //End Main

} //End Class
