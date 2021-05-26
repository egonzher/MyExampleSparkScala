package com.sparkbyexamples.spark.rrhh

import com.databricks.spark.xml.util.XSDToSchema
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.dsl.expressions.{DslExpression, StringToAttributeConversionHelper}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, IntegerType, StringType, StructField, StructType}

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
    //val dataDatePart = "2021-05-20-17-19-38"
    //val dataDatePart = "2021-05-21-15-20-06"
    //val dataDatePart = "2021-05-25-05-16-06"
    //val dataDatePart = "2021-05-25-11-22-06"
    val dataDatePart = "2021-05-26-15-33-06"

    val rutaCvDatalakeWorker = s"src/main/resources/rrhh/example_weci_consolid_wd/data_date_part=$dataDatePart/*.xml"
    //val rutaWeciAllBetaXml = s"src/main/resources/rrhh/example_weci_consolid_wd/data_date_part=Beta/*.xml"
    //val rutaWeciAllPayRollXml = s"src/main/resources/rrhh/example_weci_consolid_wd/data_date_part=PayRoll/*.xml"


    //////////////////////////////////////////////////////////////////////////////

    //Pruebas para importar el esquema xsd

    //val schema = XSDToSchema.read(Paths.get("src/main/resources/rrhh/example_weci_consolid_wd/schema-test/CDM_PayrollIntegrations.xsd"))
    //val schema = XSDToSchema.read(Paths.get("src/main/resources/rrhh/example_weci_consolid_wd/schema-test/CDM_CommonTypes.xsd"))
    //val schema = XSDToSchema.read(Paths.get("src/main/resources/rrhh/example_weci_consolid_wd/schema-test/CDM_CommonTypes.xsd"))

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


    var df_directo = spark.read
      //.schema(schema)
      .format("com.databricks.spark.xml")
      .option("excludeAttribute", "false")
      //Importante esta opción permite convertir todos los campos del xml a string para no tener que trabajar con otro tipo de datos
      .option("inferSchema", "false")
      .option("ignoreNamespace", "true")
      ///////////////////////////////
      .option("rowTag", "ns2:EMPLOYEE_DELTA_INTEGRATION")
      .load(rutaCvDatalakeWorker)


    //println("Imprimiendo el df de df_directo")
    //df_directo.printSchema()
    //df_directo.show()



    val schemasssss = StructType(
      Array(
        StructField("Worker", ArrayType(StructType(Array(

          StructField("Worker_Summary",StructType(Seq(
            StructField("WID",StringType,true),
          )))

        ))))
      )
    )




    val schema = StructType(Seq(
      StructField("Worker",StructType(Seq(

        StructField("Employees",StructType(Seq(
          StructField("Person_Identification",StructType(Seq(
            StructField("National_Identifier",StructType(Seq(// Name == legal_name en xml
              StructField("Country",StructType(Seq(
                StructField("_VALUE",StringType,true),
              )))
            )))
          )))
        )))

      ))),
      StructField("Worker_Summary",StructType(Seq(
          StructField("WID",StringType,true),
        ))),
    ))



    var df_inicial = spark.read
      .schema(schemasssss)
      .format("com.databricks.spark.xml")
      .option("excludeAttribute", "false")
      //Importante esta opción permite convertir todos los campos del xml a string para no tener que trabajar con otro tipo de datos
      .option("inferSchema", "false")
      .option("ignoreNamespace", "true")
      ///////////////////////////////
      .option("rowTag", "ns2:EMPLOYEE_DELTA_INTEGRATION")
      .load(rutaCvDatalakeWorker)


    //println("Imprimiendo el df de df_inicial")
    df_inicial.printSchema()
    df_inicial.show()

    var df_WeciAllNivel2 = df_inicial
      .withColumn("ID_WKD", explode_outer(col("`Worker`.`Worker_Summary`.`WID`")))
    df_WeciAllNivel2.show()

    var df_final = df_inicial
      .selectExpr(
        "'' as EMPLID", // NO DATA XML
        "'' as ID_CORP_EXTERNAL_SYSTEM_ID", // NO DATA XML

        /*
        "`Worker`.`Worker_Summary`.`WID`.`_VALUE` as ID_WKD",
        "`Worker`.`Employees`.`Personal`.`Workday_Account`.`_VALUE` as WD_ACC",
        "'' as EFFDT", // N/A
        "'' as EFFSEQ", // Calculado
        "'' as ACTION", // N/A
        "'' as ACTION_DESCR", // null
        "'' as ACTION_EFFDT", // N/A
        "'' as ACTION_REASON", // null
        "`Worker`.`Employees`.`Worker_Status`.`Active`.`_VALUE` as ACTIVE_STATUS",
        "`Worker`.`Employees`.`Worker_Status`.`Status`.`_VALUE` as EMPLOYEE_STATUS",
        "'' as EMPL_STATUS_EFFDT",// N/A
        "`Worker`.`Employees`.`Position`.`Business_Title`.`_VALUE` as WORKER_TYPE",
        "'' as EMPLOYEE_TYPE", // null
        "'' as CONTINGENT_WORKER_TYPE", // null
        "`Worker`.`Employees`.`Personal`.`Name`.`First_Name`.`_VALUE` as FIRST_NAME",
        "`Worker`.`Employees`.`Personal`.`Name`.`Middle_Name`.`_VALUE` as MIDDLE_NAME",
        "`Worker`.`Employees`.`Personal`.`Name`.`Last_Name`.`_VALUE` as LAST_NAME",
        "`Worker`.`Employees`.`Personal`.`Name`.`Secondary_Last_Name`.`_VALUE` as SECOND_LAST_NAME",
        "`Worker`.`Employees`.`Person_Identification`.`National_Identifier`.`Country` as NATIONAL_ID_COUNTRY",
        "'' as NATIONAL_ID_TYPE", //
        "`Worker`.`Employees`.`Person_Identification`.`National_Identifier`.`National_ID_Type` as NATIONAL_ID_TYPE_DESCR",
        "`Worker`.`Employees`.`Person_Identification`.`National_Identifier`.`National_ID` as NATIONAL_ID",
        "'' as VISA_ID_COUNTRY",
        "'' as VISA_ID_TYPE",
        "'' as VISA_ID",
        "'' as PASSPORT_ID_COUNTRY",
        "'' as PASSPORT_ID_TYPE",
        "'' as PASSPORT_ID",
        "`Worker`.`Employees`.`Personal`.`Date_of_Birth`.`_VALUE` as BIRTHDATE",
        "'' as AGE", // CALCULATED
        "'' as ACT_AGE", // CALCULATED
        "'' as DT_OF_DEATH",
        "`Worker`.`Employees`.`Personal`.`Country_of_Birth`.`_VALUE` as BIRTHCOUNTRY",
        "`Worker`.`Employees`.`Personal`.`City_of_Birth`.`_VALUE` as BIRTHCITY_BIRTHPLACE",
        "'' as BIRTHSTATE",
        "`Worker`.`Employees`.`Personal`.`Nationality`.`_VALUE` as Nationality",
        "'' as NATIONALITY_DESCR",// N/A
        "'' as PREF_LANG_LANG_CD",
        "'' as LANG_CD_DESCR",
        "'' as GENDER",
        "'' as GENDER_DESCR",
        "'' as Nationality",
        "'' as MAR_STATUS",
        "'' as MAR_STATUS_DESCR",
        "'' as MAR_STATUS_EFFDT",
        "'' as DISABLE_DEGREE",
        "'' as DISABLE_DEGREE_DESCR",
        "'' as DISABLE_CERT_AUTHORITY",
        "'' as DISABLE_EFFDT",
        "'' as DISABLE_END_EFFDT",
        "'' as DISABLE_KNOW_EFFDT",
        "'' as EMPLID_CONYUGE",
        "'' as NUM_DISABLE_HIJOS_33_65",
        "'' as NUM_DISABLE_HIJOS_MAS_65",


        //Campos por identificar
        "'' as HIGHEST_EDUC_LVL",
        "'' as CONTRACT_TYPE",
        "'' as CONTRACT_TYPE_LOCAL",
        "'' as CONTRACT_STATUS",
        "'' as CONTRACT_START_DT",
        "'' as CONTRACT_END_DT",
        "'' as CONTRACT_TYPE_DESCR",
        "'' as PROBATION_N_DAYS",
        "'' as TYME_TYPE",
        "'' as JORNADA_EFFDT",
        "'' as COMPANY",
        "'' as COMPANY_DESCR",
        "'' as COUNTRY_COMPANY",
        "'' as COUNTRY_COMPANY_DESCR",
        "'' as COMPANY_EFFDT",
        "'' as HIRE_DT",
        "'' as HIRE_REASON",
        "'' as CMPNY_SENIORITY_DT",
        "'' as ORIGINAL_HIRE_DT",
        "'' as CONTINUOUS_SERV_DT",
        "'' as ZINGRESOBANCA_FI",
        "'' as TERMINATION_DT",
        "'' as TERMINATION_REASON",
        "'' as TERMINATION_REASON_LOCAL",
        "'' as RETIREMENT_DT",
        "'' as GBL_MRT",
        "'' as GBL_MRT_START_DT",
        "'' as GBL_MRT_END_DT",
        "'' as LCL_MRT",
        "'' as LCL_MRT_START_DT",
        "'' as LCL_MRT_END_DT",
        "'' as DEPTID",
        "'' as DEPTID_DESCR",
        "'' as DEPT_TYPE",
        "'' as DEPT_ENTRY_DT",
        "'' as DEPTID_SUP",
        "'' as POS_LVL_1_CODE",
        "'' as POS_LVL_1_NAME",
        "'' as POS_LVL_2_CODE",
        "'' as POS_LVL_2_NAME",
        "'' as POS_LVL_3_CODE",
        "'' as POS_LVL_3_NAME",
        "'' as POS_LVL_4_CODE",
        "'' as POS_LVL_4_NAME",
        "'' as POS_LVL_5_CODE",
        "'' as POS_LVL_5_NAME",
        "'' as POS_LVL_6_CODE",
        "'' as POS_LVL_6_NAME",
        "'' as POS_LVL_7_CODE",
        "'' as POS_LVL_7_NAME",
        "'' as POS_LVL_8_CODE",
        "'' as POS_LVL_8_NAME",
        "'' as POS_LVL_9_CODE",
        "'' as POS_LVL_9_NAME",
        "'' as POS_LVL_10_CODE",
        "'' as POS_LVL_10_NAME",
        "'' as POS_LVL_11_CODE",
        "'' as POS_LVL_11_NAME",
        "'' as POS_LVL_12_CODE",
        "'' as POS_LVL_12_NAME",
        "'' as POS_LVL_13_CODE",
        "'' as POS_LVL_13_NAME",
        "'' as POS_LVL_14_CODE",
        "'' as POS_LVL_14_NAME",
        "'' as POS_LVL_15_CODE",
        "'' as POS_LVL_15_NAME",
        "'' as POS_LVL_16_CODE",
        "'' as POS_LVL_16_NAME",
        "'' as POS_LVL_17_CODE",
        "'' as POS_LVL_17_NAME",
        "'' as POS_LVL_18_CODE",
        "'' as POS_LVL_18_NAME",
        "'' as POS_LVL_19_CODE",
        "'' as POS_LVL_19_NAME",
        "'' as POS_LVL_20_CODE",
        "'' as POS_LVL_20_NAME",
        "'' as JOB_FAMILY_GROUP",
        "'' as JOB_FAMILY",
        "'' as MANAGEMENT_LEVEL",
        "'' as MANAGEMENT_LEVEL_DESCR",
        "'' as JOBCODE",
        "'' as JOBCODE_DESCR",
        "'' as JOB_ENTRY_DT",
        "'' as POSITION_NBR",
        "'' as POSITION_NBR_DESCR",
        "'' as POSITION_ENTRY_DT",
        "'' as BUSINESS_UNIT",
        "'' as BUSINESS_UNIT_DESCR",
        "'' as PRODUCT",
        "'' as AGILE_ROLE",
        "'' as MANAGER_ID",
        "'' as MANAGER_NAME",
        "'' as MANAGER_MAIL",
        "'' as MANAGER_PHONE",
        "'' as MNGR_FUNCT_ID",
        "'' as SUPERVISOR_EFFDT",
        "'' as SUPERVISORY",
        "'' as MANAGEMENT_CHAIN_LEVEL1",
        "'' as MANAGEMENT_CHAIN_LEVEL2",
        "'' as MANAGEMENT_CHAIN_LEVEL3",
        "'' as MANAGEMENT_CHAIN_LEVEL4",
        "'' as MANAGEMENT_CHAIN_LEVEL5",
        "'' as MANAGEMENT_CHAIN_LEVEL6",
        "'' as MANAGEMENT_CHAIN_LEVEL7",
        "'' as MANAGEMENT_CHAIN_LEVEL8",
        "'' as MANAGEMENT_CHAIN_LEVEL9",
        "'' as MANAGEMENT_CHAIN_LEVEL10",
        "'' as MANAGEMENT_CHAIN_LEVEL11",
        "'' as MANAGEMENT_CHAIN_LEVEL12",
        "'' as MANAGEMENT_CHAIN_LEVEL13",
        "'' as MANAGEMENT_CHAIN_LEVEL14",
        "'' as MANAGEMENT_CHAIN_LEVEL15",
        "'' as MANAGEMENT_CHAIN_LEVEL16",
        "'' as MANAGEMENT_CHAIN_LEVEL17",
        "'' as MANAGEMENT_CHAIN_LEVEL18",
        "'' as MANAGEMENT_CHAIN_LEVEL19",
        "'' as MANAGEMENT_CHAIN_LEVEL20",
        "'' as COMP_GRADE_PROFILE",
        "'' as COMPENSATION_GRADE",
        "'' as MIN_SALARY_RANGE",
        "'' as MAX_SALARY_RANGE",
        "'' as JOB_CLASSIFICATION",
        "'' as JOB_CLASSIFICATION_EFFDT",
        "'' as PAY_GROUP",
        "'' as ADDITIONAL_JOB_CLASSIFICATION",
        "'' as COLLECTIVE_AGREEMENT",
        "'' as COLLECTIVE_AGREEMENT_DESCR",
        "'' as COLLECTIVE_AGREEMENT_FACTOR",
        "'' as COLLECTIVE_AGREEMENT_DT",
        "'' as EMPL_CTG_DESCR",
        "'' as EMPL_CTG_EFFDT",
        "'' as ZGADP_GRADOEMPL",
        "'' as ZGADP_GRADOEMPL_EFFDT",
        "'' as COST_CENTER",
        "'' as COST_CENTER_NAME",
        "'' as ZCCOSTE_FENTRADA_EFFDT",
        "'' as ZGADP_BANCOPROC",
        "'' as CORP_SEGMENT",
        "'' as SETID_DT_ALTA",
        "'' as CORP_SEGMENT_DT",
        "'' as IND_PODERES",
        "'' as IND_PODERES_EFFDT",
        "'' as Z02_DIVISION",
        "'' as MOTIVO_ALTA_HR",
        "'' as MOTIVO_ALTA_HR_DESCR",
        "'' as MOTIVO_ALTA_HR_EFFDT",
        "'' as ESCALA_JEFATURA_DT",
        "'' as ESCALA_JEFATURA_EFFDT",
        "'' as TRIENIOS_DT",
        "'' as TRIENIOS_EFFDT",
        "'' as TRIENIOS_JEFATURA_DT",
        "'' as TRIENIOS_JEFATURA_EFFDT",
        "'' as ESCALA_ADMIN_DT",
        "'' as ESCALA_ADMIN_EFFDT",
        "'' as HORAS_SEMANALES",
        "'' as HORAS_JORNADA_EFFDT",
        "'' as PORC_REDUC_JORNADA",
        "'' as REDUC_JORNADA_FINI",
        "'' as REDUC_JORNADA_FESTIMADA",
        "'' as REDUC_JORNADA_FCONFIRMADA",
        "'' as REDUC_JORNADA_TYPE",
        "'' as REDUC_JORNADA_REASON",
        "'' as TOTAL_BASE_PAY",
        "'' as TOTAL_BASE_PAY_CURRENCY",
        "'' as ANNUAL_BASE_SALARY_EFFDT",
        "'' as TOTAL_FIXED_COMP",
        "'' as TOTAL_VAR_COMP",
        "'' as TOTAL_REWARDS",
        "'' as BANK_ACC",
        "'' as BANK_IBAN",
        "'' as ANTICIPO_PDTE_IMPORTE",
        "'' as ANTICIPO_PDTE_MONEDA",
        "'' as ANTICIPO_MOTIVO",
        "'' as ANTICIPO_MOTIVO_DESCR",
        "'' as ANTICIPO_TIPO",
        "'' as ANTICIPO_TIPO_DESCR",
        "'' as ANTICIPO_PDTE_EFFDT",
        "'' as PRESTAMO_VIV_PDTE",
        "'' as PRESTAMO_VIV_PDTE_EFFDT",
        "'' as CREDITO_REDZP_TIPO",
        "'' as CREDITO_REDZP_TIPO_DESCR",
        "'' as CREDITO_REDZP_TIPO_EFFDT",
        "'' as CREDITO_REDZP_IMP",
        "'' as CREDITO_REDZP_IMP_EFFDT",
        "'' as IND_CR_REDZV",
        "'' as IND_CR_REDZV_EFFDT",
        "'' as IND_PR_REDZV",
        "'' as IND_PR_REDZV_EFFDT",
        "'' as PLURIEMPLEO_PORC",
        "'' as PLURIEMPLEO_EFFDT",
        "'' as IND_RETEN_JUD",
        "'' as IND_RETEN_JUD_EFFDT",
        "'' as AMBITO",
        "'' as AMBITO_DESCR",
        "'' as Z_PART_TIME_REASON",
        "'' as ZBANK_SENIORITY_DT",
        "'' as LOCATION",
        "'' as LOCATION_NAME",
        "'' as LOCATION_ADDR",
        "'' as POSTAL",
        "'' as CITY",
        "'' as STATE",
        "'' as PHONE_PERSLAND_PREF",
        "'' as PHONE_PERSLAND_NUM",
        "'' as PHONE_BUSSLAND_PREF",
        "'' as PHONE_BUSSLAND_NUM",
        "'' as PHONE_PERSMOB_PREF",
        "'' as PHONE_PERSMOB_NUM",
        "'' as PHONE_BUSSMOB_PREF",
        "'' as PHONE_BUSSMOB_NUM",
        "'' as MAIL_PW",
        "'' as MAIL_PERS",
        "'' as HRBP",
        "'' as HRBP_Name",
        "'' as HRBP_MAIL",
        "'' as HRBP_PHONE",
        "'' as PERS_ADDR_COUNTRY",
        "'' as PERS_ADDR_TYPE",
        "'' as PERS_ADDR_NAME",
        "'' as PERS_ADDR_NUMBER",
        "'' as PERS_ADDR_FLOOR",
        "'' as PERS_ADDR_DOOR",
        "'' as PERS_ADDR_STAIRWELL",
        "'' as PERS_ADDR_CITY",
        "'' as PERS_ADDR_POSTAL",
        "'' as PERS_ADDR_STATE_ID",
        "'' as PERS_ADDR_STATE_DESCR",
        "'' as FEC_PROC"

         */

      ).na.fill(" ").distinct()

    //println("Imprimiendo el df de df_final")
    //df_final.printSchema()
    //df_final.show()







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

    //En esta lógica comentada atacabamos los xml de los diferentes xsd y solo devolvía vaío el xml del 19

    /*

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

      //df_TestPayRoll.show()
      //df_TestPayRoll.printSchema()

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

        //df_final_nuevo.show()

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

        df_nuevos_weci_worker = df_nuevos_weci_worker.selectExpr(
          "`Summary_Employee_ID` as Summary_Employee_ID ",
          "`Summary_WID` as Summary_WID ",
          "`Worker_Status_Active_VALUE` as Worker_Status_Active_VALUE ",
          "`Worker_Status_Active_priorValue` as Worker_Status_Active_priorValue ",
        )

        val df_all = df_final_nuevo.join(df_nuevos_weci_worker,df_final_nuevo("WID_VALUE") === df_nuevos_weci_worker("Summary_WID") ,"left")

        //df_nuevos_weci_worker.show()
        //df_nuevos_weci_worker.printSchema()

        print("imprimiendo join de los dataframe")
        df_all.show(false)
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



     */


    //////////////////////////////////////////////////////////////////////////////


  } //End Main

} //End Class
