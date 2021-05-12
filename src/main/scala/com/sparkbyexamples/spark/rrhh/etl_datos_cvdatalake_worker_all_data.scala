// scalastyle:off

package com.sparkbyexamples.spark.rrhh

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, explode}

object etl_datos_cvdatalake_worker_all_data {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local[1]")
      .appName("SparkByExample")
      .getOrCreate()


    //Trabajando con la nueva integración de cvdatalake_worker
    val dataDatePart = "2021-05-03"
    val rutaCvDatalakeWorker = s"src/main/resources/rrhh/example_cv_datalake_worker/data_date_part=$dataDatePart/*.xml"


    val df_CvDatalakeWorker = spark.read
      .format("com.databricks.spark.xml")
      .option("excludeAttribute", "false")
      .option("rowTag", "ns1:EMPLOYEE_DELTA_INTEGRATION")
      .load(rutaCvDatalakeWorker)

    println("Imprimiendo el esquema de df_CvDatalakeWorker")
    df_CvDatalakeWorker.printSchema()

/*
 |-- ns:Employees: struct (nullable = true)
 |    |-- ns1:Additional_Information: struct (nullable = true)
 |    |    |-- ns1:Company: string (nullable = true)
 |    |-- ns1:Qualifications: struct (nullable = true)
 |    |    |-- ns1:Certification_Achievement: array (nullable = true)
 |    |    |    |-- element: struct (containsNull = true)
 |    |    |    |    |-- ns1:Certification: string (nullable = true)
 |    |    |    |    |-- ns1:Certification_Country: string (nullable = true)
 |    |    |    |    |-- ns1:Certification_Issuer: string (nullable = true)
 |    |    |    |    |-- ns1:Certification_Name: string (nullable = true)
 |    |    |    |    |-- ns1:Issued_Date: string (nullable = true)
 |    |    |-- ns1:External_Job: struct (nullable = true)
 |    |    |    |-- ns1:Company: string (nullable = true)
 |    |    |    |-- ns1:End_Date: string (nullable = true)
 |    |    |    |-- ns1:Job_Reference: long (nullable = true)
 |    |    |    |-- ns1:Job_Title: string (nullable = true)
 |    |    |    |-- ns1:Location: string (nullable = true)
 |    |    |    |-- ns1:Responsibilities_And_Achievements: string (nullable = true)
 |    |    |    |-- ns1:Start_Date: string (nullable = true)
 |    |-- ns1:Summary: struct (nullable = true)
 |    |    |-- ns1:Employee_ID: long (nullable = true)
*/

    val df_CvDatalakeWorkerFinal = df_CvDatalakeWorker
      .withColumn("CertAchievement", explode(col("`ns:Employees`.`ns1:Qualifications`.`ns1:Certification_Achievement`")))
      .withColumn("IDCast",col("`ns:Employees`.`ns1:Summary`.`ns1:Employee_ID`").cast("String"))
      .withColumn("External_Job_Reference",col("`ns:Employees`.`ns1:Qualifications`.`ns1:External_Job`.`ns1:Job_Reference`").cast("String"))
      .selectExpr(
        //ID para los join
        "IDCast as Employee_ID",
        //Campos identificados
        "`CertAchievement`.`ns1:Certification_Country` as Certification_Country",
        "`CertAchievement`.`ns1:Certification` as Certification",
        "`CertAchievement`.`ns1:Certification_Name` as Certification_Name",
        "`CertAchievement`.`ns1:Certification_Issuer` as Certification_Issuer",
        "`CertAchievement`.`ns1:Issued_Date` as Issued_Date",
        //Campos pendientes de identificar
        "`ns:Employees`.`ns1:Additional_Information`.`ns1:Company` as Additional_Information_Company",
        "`ns:Employees`.`ns1:Qualifications`.`ns1:External_Job`.`ns1:Job_Title` as Job_Title",
        "`ns:Employees`.`ns1:Qualifications`.`ns1:External_Job`.`ns1:Start_Date` as Start_Date",
        "`ns:Employees`.`ns1:Qualifications`.`ns1:External_Job`.`ns1:End_Date` as End_Date",
        "`ns:Employees`.`ns1:Qualifications`.`ns1:External_Job`.`ns1:Location` as External_Job_Location",
        "`ns:Employees`.`ns1:Qualifications`.`ns1:External_Job`.`ns1:Responsibilities_And_Achievements` as Responsibilities_And_Achievements",
        //Campos que no están mapeados
        "`ns:Employees`.`ns1:Qualifications`.`ns1:External_Job`.`ns1:Company` as External_Job_Company",
        "External_Job_Reference",
        s"'$dataDatePart' as data_date_part").na.fill(" ").distinct()

    df_CvDatalakeWorkerFinal.show()

  }

}

// scalastyle:on
