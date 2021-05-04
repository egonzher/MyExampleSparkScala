// scalastyle:off

package com.sparkbyexamples.spark.rrhh

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, explode}

object etl_datos_cvdatalake_worker {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local[1]")
      .appName("SparkByExample")
      .getOrCreate()


    //Trabajando con la nueva integración de weci en consolid_wd
    val rutaCvDatalakeWorker = "src/main/resources/rrhh/example_cvdatalake_worker/*.xml"

    val df_WeciConsolidWD = spark.read
      .format("com.databricks.spark.xml")
      .option("excludeAttribute", "false")
      .option("rowTag", "ns1:EMPLOYEE_DELTA_INTEGRATION")
      .load(rutaCvDatalakeWorker)

    println("Imprimiendo el esquema de df_WeciConsolidWD")
    df_WeciConsolidWD.printSchema()

/*
 |    |-- ns1:Position: struct (nullable = true)
 |    |    |-- ns1:Business_Site: struct (nullable = true)
 |    |    |    |-- ns1:Address_ID: string (nullable = true)
 |    |    |    |-- ns1:Address_Line_1: string (nullable = true)
 |    |    |    |-- ns1:Address_Line_3: string (nullable = true)
 |    |    |    |-- ns1:City: string (nullable = true)
 |    |    |    |-- ns1:Country: string (nullable = true)
 |    |    |    |-- ns1:Location_ID: string (nullable = true)
 |    |    |    |-- ns1:Location_Name: string (nullable = true)
 |    |    |    |-- ns1:Postal_Code: long (nullable = true)


    val df_WeciFinal = df_WeciConsolidWD
      .withColumn("natioId", explode(col("`ns:Employees`.`ns1:Person_Identification`.`ns1:National_Identifier`")))
      .withColumn("relatedPerson", explode(col("`ns:Employees`.`ns1:Related_Person`")))
      .selectExpr(

        //Estos son los de color Blanco que no había dudas
        "`natioId`.`ns1:National_ID_Type` as NATIONAL_ID_TYPE_DESCR",
        "`ns:Employees`.`ns1:Personal`.`ns1:Date_of_Birth` as BIRTHDATE",
        "`ns:Employees`.`ns1:Personal`.`ns1:City_of_Birth` as BIRTHPLACE",
        "`ns:Employees`.`ns1:Personal`.`ns1:Country_of_Birth` as BIRTHCOUNTRY",
        "`natioId`.`ns1:National_ID` as NATIONALITY_ID",
        "`ns:Employees`.`ns1:Personal`.`ns1:Nationality` as NATIONALITY_DESCR",
        "`ns:Employees`.`ns1:Personal`.`ns1:Preferred_Language` as LANG_CD_DESCR",
        "`ns:Employees`.`ns1:Personal`.`ns1:Gender` as GENDER_DESCR",
        "`ns:Employees`.`ns1:Position`.`ns1:Job_Classification`.`ns1:Job_Classification_Description` as JOB_CLASSIFICATION_EFFDT",
        "`ns:Employees`.`ns1:Collective_Agreement`.`ns1:Collective_Agreement` as COLLECTIVE_AGREEMENT_DESCR",
        "`ns:Employees`.`ns1:Collective_Agreement`.`ns1:Collective_Agreement_Factor`.`ns1:Factor` as COLLECTIVE_AGREEMENT_FACTOR_DESCR",
        "`ns:Employees`.`ns1:Position`.`ns1:Position_Time_Type` as JORNADA_EFFDT",
        "`ns:Employees`.`ns1:Compensation`.`ns1:Compensation_Summary_in_Annualized_Frequency`.`ns1:Total_Base_Pay` as ANNUAL_BASE_SALARY",
        "`ns:Employees`.`ns1:Compensation`.`ns1:Compensation_Summary_in_Annualized_Frequency`.`ns1:Currency` as ANNUAL_BASE_SALARY_CURRENCY",
        "`ns:Employees`.`ns1:Position`.`ns1:Business_Site`.`ns1:Location_ID` as BUSINESS_UNIT_DESCR",
        "`ns:Employees`.`ns1:Employee_Contract`.`ns1:Contract_ID` as CONTRACT_TYPE",
        "`ns:Employees`.`ns1:Employee_Contract`.`ns1:Contract_Type` as `CONTRACT_TYPE_DESCR`",

        //Duda sobre si hay que concatenar todos los address en una sola columna
        "`ns:Employees`.`ns1:Position`.`ns1:Business_Site`.`ns1:Address_ID` as ADDRESS1_2_ID",
        "`ns:Employees`.`ns1:Position`.`ns1:Business_Site`.`ns1:Address_Line_1` as ADDRESS1_2_Line_1",
        "`ns:Employees`.`ns1:Position`.`ns1:Business_Site`.`ns1:Address_Line_3` as ADDRESS1_2_Line_3",

        "`ns:Employees`.`ns1:Position`.`ns1:Business_Site`.`ns1:Postal_Code` as POSTAL",
        "`ns:Employees`.`ns1:Position`.`ns1:Business_Site`.`ns1:City` as CITY",
        "`natioId`.`ns1:Country` as `PERS_COUNTRY`",

        //Estos son los de color Amarillo que si había dudas
        "`ns:Employees`.`ns1:Worker_Status`.`ns1:Active_Status_Date` as `EMPL_STATUS_EFFDT`",
        "`ns:Employees`.`ns1:Personal`.`ns1:Preferred_Language` as LANG_CD",
        "`ns:Employees`.`ns1:Personal`.`ns1:Gender` as GENDER",
        "`ns:Employees`.`ns1:Personal`.`ns1:Disability_Status` as DISABLE_TYPE_ESP",
        "`ns:Employees`.`ns1:Personal`.`ns1:Disability_Status` as DISABLE_TYPE_ESP_DESCR",
        "`relatedPerson`.`ns1:Dependent_ID` as EMPLID_CONYUGE",
        "`ns:Employees`.`ns1:Worker_Status`.`ns1:Hire_Date` as COMPANY_EFFDT",
        "`ns:Employees`.`ns1:Worker_Status`.`ns1:Original_Hire_Date` as ZING_GRUPO_DT",
        "`ns:Employees`.`ns1:Collective_Agreement`.`ns1:Start_Date` as COLLECTIVE_YN_GLOBAL_EFFDT",
        "`ns:Employees`.`ns1:Collective_Agreement`.`ns1:Start_Date` as COLLECTIVE_YN_LOCAL_EFFDT",
        "`ns:Employees`.`ns1:Position`.`ns1:Organization` as TIP_DEPTID",
        "`ns:Employees`.`ns1:Position`.`ns1:Supervisor`.`ns1:ID` as SUPERVISOR_EFFDT",
        "`ns:Employees`.`ns1:Collective_Agreement`.`ns1:Collective_Agreement` as COLLECTIVE_AGREEMENT",
        "`ns:Employees`.`ns1:Collective_Agreement`.`ns1:Collective_Agreement_Factor`.`ns1:Factor` as COLLECTIVE_AGREEMENT_FACTOR",
        "`ns:Employees`.`ns1:Position`.`ns1:Organization` as CORP_SEGM_EFFDT",
        "`ns:Employees`.`ns1:Worker_Status`.`ns1:First_Day_of_Work` as `MOTIVO_ALTA_HR_EFFDT`",
        "`ns:Employees`.`ns1:Position`.`ns1:Job_Classification`.`ns1:Job_Classification_Description` as `ESCALA_JEFATURA_DT`",
        "`ns:Employees`.`ns1:Worker_Status`.`ns1:Seniority_Date` as `TRIENIOS_DT`",
        "`ns:Employees`.`ns1:Worker_Status`.`ns1:Seniority_Date` as `TRIENIOS_EFFDT`",
        "`ns:Employees`.`ns1:Worker_Status`.`ns1:Seniority_Date` as `TRIENIOS_JEFATURA_DT`",
        "`ns:Employees`.`ns1:Worker_Status`.`ns1:Seniority_Date` as `TRIENIOS_JEFATURA_EFFDT`",
        "`ns:Employees`.`ns1:Compensation`.`ns1:Compensation_Grade` as `ESCALA_ADMIN_DT`",
        "`ns:Employees`.`ns1:Worker_Status`.`ns1:Seniority_Date` as `ZBANK_SENIORITY_DT`",

      ).na.fill(" ").distinct().show()

      */
















  }

}

// scalastyle:on
