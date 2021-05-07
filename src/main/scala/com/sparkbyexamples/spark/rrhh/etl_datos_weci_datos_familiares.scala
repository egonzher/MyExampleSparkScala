package com.sparkbyexamples.spark.rrhh

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, concat, explode, expr}
import org.apache.spark.sql.types.StringType
object etl_datos_weci_datos_familiares {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local[1]")
      .appName("SparkByExample")
      .getOrCreate()


    //Trabajando con la nueva integración de weci en consolid_wd
    val rutaWeciDatosFamiliares = "src/main/resources/rrhh/example_weci_consolid_wd/*.xml"

    val df_WeciFamiliaWD = spark.read
      .format("com.databricks.spark.xml")
      .option("excludeAttribute", "false")
      .option("rowTag", "ns1:EMPLOYEE_DELTA_INTEGRATION")
      .load(rutaWeciDatosFamiliares)

    println("Imprimiendo el esquema de df_WeciConsolidWD")
    df_WeciFamiliaWD.printSchema()



    val df_FamiliaFinal = df_WeciFamiliaWD
      .withColumn("relatedPerson", explode(col("`ns:Employees`.`ns1:Related_Person`")))
      .withColumn("relatedNacionalID", explode(col("`ns:Employees`.`ns1:Related_Person_Identification`")))
      .withColumn("BenefiCast",col("`relatedPerson`.`ns1:Beneficiary`").cast("String")).

      selectExpr(
        "`ns:Employees`.`ns1:Position`.`ns1:Organization` as COMPANY",
        "`relatedPerson`.`ns1:Related_Person_ID` as EMPLID",
        "`relatedPerson`.`ns1:Dependent_ID` as EMPL_RCD",
        "`BenefiCast` as DEPENDENT_BENEF",
        "`relatedPerson`.`ns1:Relationship_Type` as RELATIONSHIP",
        "`ns:Employees`.`ns1:Position`.`ns1:Organization` as COMPANY_DESCR",
        "`relatedPerson`.`ns1:Relationship_Type` as RELATIONSHIP_DESCR",
        "`relatedPerson`.`ns1:Legal_Name`.`ns1:First_Name` as FIRST_NAME",
        "`relatedPerson`.`ns1:Legal_Name`.`ns1:Last_Name` as LAST_NAME",
        "`relatedPerson`.`ns1:Legal_Name`.`ns1:Secondary_Last_Name` as SECOND_LAST_NAME",
        "'' as MIDDLE_NAME ",
        "`relatedPerson`.`ns1:Birth_Date` as BIRTHDATE ",
        "'' as DT_OF_DEATH",
        "'' as TIPMINUS",
        "'' as TIPMINUS_DESCR",
        "'' as MAR_STATUS",
        "'' as MAR_STATUS_DESCR",
        "'' as MAR_STATUS_DT",
        "`relatedNacionalID`.`ns1:National_Identifier`.`ns1:National_ID_Type` as NATIONAL_ID_TYPE",
        "`relatedNacionalID`.`ns1:National_Identifier`.`ns1:National_ID_Type` as NATIONAL_ID_TYPE_DESCR",
        "`relatedNacionalID`.`ns1:National_Identifier`.`ns1:National_ID` as NATIONAL_ID",
        "'' as FECHA"
      ).na.fill(" ").distinct().show()











  }
}
