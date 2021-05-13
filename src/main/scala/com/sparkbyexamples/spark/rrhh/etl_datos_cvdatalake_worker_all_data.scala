// scalastyle:off

package com.sparkbyexamples.spark.rrhh

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, explode, explode_outer, lit, when}

object etl_datos_cvdatalake_worker_all_data {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local[1]")
      .appName("SparkByExample")
      .getOrCreate()


    //Trabajando con la nueva integración de cvdatalake_worker
    //val dataDatePart = "2021-04-23"
    //val dataDatePart = "2021-05-04"
    //val dataDatePart = "2021-05-05"
    val dataDatePart = "2021-05-06"
    val rutaCvDatalakeWorker = s"src/main/resources/rrhh/example_cv_datalake_worker/data_date_part=$dataDatePart/*.xml"



    val df_CvDatalakeWorker = spark.read
      .format("com.databricks.spark.xml")
      .option("excludeAttribute", "false")
      .option("rowTag", "ns1:EMPLOYEE_DELTA_INTEGRATION")
      .load(rutaCvDatalakeWorker)

    println("Imprimiendo el esquema de df_CvDatalakeWorker")
    df_CvDatalakeWorker.printSchema()



    //////////////////////////////////////////////////////////////////////////////

    /*

      df_CvDatalakeWorkerFinal = df_CvDatalakeWorker
        .withColumn("CertAchievement", explode_outer(col("`ns:Employees`.`ns1:Qualifications`.`ns1:Certification_Achievement`")))
        .withColumn("IDCast",col("`ns:Employees`.`ns1:Summary`.`ns1:Employee_ID`").cast("String"))
        .withColumn("External_Job_Reference",col("`ns:Employees`.`ns1:Qualifications`.`ns1:External_Job`.`ns1:Job_Reference`").cast("String"))
        .selectExpr(
          "IDCast as Employee_ID",
          "`CertAchievement`.`ns1:Certification_Country` as Certification_Country",
          "`CertAchievement`.`ns1:Certification` as Certification",
          "`CertAchievement`.`ns1:Certification_Name` as Certification_Name",
          "`CertAchievement`.`ns1:Certification_Issuer` as Certification_Issuer",
          "`CertAchievement`.`ns1:Issued_Date` as Issued_Date",
          "`ns:Employees`.`ns1:Additional_Information`.`ns1:Company` as Additional_Information_Company",
          "`ns:Employees`.`ns1:Qualifications`.`ns1:External_Job`.`ns1:Job_Title` as Job_Title",
          "`ns:Employees`.`ns1:Qualifications`.`ns1:External_Job`.`ns1:Start_Date` as Start_Date",
          "`ns:Employees`.`ns1:Qualifications`.`ns1:External_Job`.`ns1:End_Date` as End_Date",
          "`ns:Employees`.`ns1:Qualifications`.`ns1:External_Job`.`ns1:Location` as External_Job_Location",
          "`ns:Employees`.`ns1:Qualifications`.`ns1:External_Job`.`ns1:Responsibilities_And_Achievements` as Responsibilities_And_Achievements",
          "`ns:Employees`.`ns1:Qualifications`.`ns1:External_Job`.`ns1:Company` as External_Job_Company",
          "External_Job_Reference",
          s"'$dataDatePart' as data_date_part").na.fill(" ").distinct()


     */

    //////////////////////////////////////////////////////////////////////////////




    var df_CvDatalakeWorkerFinal = df_CvDatalakeWorker

    if (dataDatePart == "2021-05-06"){

      df_CvDatalakeWorkerFinal = df_CvDatalakeWorker
        .withColumn("CertAchievement", explode_outer(col("`ns:Employees`.`ns1:Qualifications`.`ns1:Certification_Achievement`")))
        .withColumn("IDCast",col("`ns:Employees`.`ns1:Summary`.`ns1:Employee_ID`").cast("String"))
        .selectExpr(
          "IDCast as Employee_ID",
          "`CertAchievement`.`ns1:Certification_Country` as Certification_Country",
          "`CertAchievement`.`ns1:Certification` as Certification",
          "`CertAchievement`.`ns1:Certification_Name` as Certification_Name",
          "`CertAchievement`.`ns1:Certification_Issuer` as Certification_Issuer",
          "'' as Issued_Date",
          "`ns:Employees`.`ns1:Additional_Information`.`ns1:Company` as Additional_Information_Company",
          "`ns:Employees`.`ns1:Qualifications`.`ns1:External_Job`.`ns1:Job_Title` as Job_Title",
          "`ns:Employees`.`ns1:Qualifications`.`ns1:External_Job`.`ns1:Start_Date` as Start_Date",
          "`ns:Employees`.`ns1:Qualifications`.`ns1:External_Job`.`ns1:End_Date` as End_Date",
          "`ns:Employees`.`ns1:Qualifications`.`ns1:External_Job`.`ns1:Location` as External_Job_Location",
          "`ns:Employees`.`ns1:Qualifications`.`ns1:External_Job`.`ns1:Responsibilities_And_Achievements` as Responsibilities_And_Achievements",
          "`ns:Employees`.`ns1:Qualifications`.`ns1:External_Job`.`ns1:Company` as External_Job_Company",
          "'' as External_Job_Reference",
          s"'$dataDatePart' as data_date_part").na.fill(" ").distinct()

    }

    else if (dataDatePart == "2021-05-05"){

      df_CvDatalakeWorkerFinal = df_CvDatalakeWorker
        .withColumn("CertAchievement", col("`ns:Employees`.`ns1:Qualifications`.`ns1:Certification_Achievement`"))
        .withColumn("IDCast",col("`ns:Employees`.`ns1:Summary`.`ns1:Employee_ID`").cast("String"))
        .withColumn("External_Job_Reference",col("`ns:Employees`.`ns1:Qualifications`.`ns1:External_Job`.`ns1:Job_Reference`").cast("String"))
        .selectExpr(
          "IDCast as Employee_ID",
          "'' as Certification_Country",
          "`CertAchievement`.`ns1:Certification` as Certification",
          "`CertAchievement`.`ns1:Certification_Name` as Certification_Name",
          "`CertAchievement`.`ns1:Certification_Issuer` as Certification_Issuer",
          "'' as Issued_Date",
          "`ns:Employees`.`ns1:Additional_Information`.`ns1:Company` as Additional_Information_Company",
          "`ns:Employees`.`ns1:Qualifications`.`ns1:External_Job`.`ns1:Job_Title` as Job_Title",
          "`ns:Employees`.`ns1:Qualifications`.`ns1:External_Job`.`ns1:Start_Date` as Start_Date",
          "'' as End_Date",
          "`ns:Employees`.`ns1:Qualifications`.`ns1:External_Job`.`ns1:Location` as External_Job_Location",
          "'' as Responsibilities_And_Achievements",
          "`ns:Employees`.`ns1:Qualifications`.`ns1:External_Job`.`ns1:Company` as External_Job_Company",
          "External_Job_Reference",
          s"'$dataDatePart' as data_date_part").na.fill(" ").distinct()

    }

    else if (dataDatePart == "2021-05-04"){

      df_CvDatalakeWorkerFinal = df_CvDatalakeWorker
        .withColumn("CertAchievement", explode_outer(col("`ns:Employees`.`ns1:Qualifications`.`ns1:Certification_Achievement`")))
        .withColumn("IDCast",col("`ns:Employees`.`ns1:Summary`.`ns1:Employee_ID`").cast("String"))
        .selectExpr(
          "IDCast as Employee_ID",
          "`CertAchievement`.`ns1:Certification_Country` as Certification_Country",
          "`CertAchievement`.`ns1:Certification` as Certification",
          "`CertAchievement`.`ns1:Certification_Name` as Certification_Name",
          "`CertAchievement`.`ns1:Certification_Issuer` as Certification_Issuer",
          "'' as Issued_Date",
          "`ns:Employees`.`ns1:Additional_Information`.`ns1:Company` as Additional_Information_Company",
          "`ns:Employees`.`ns1:Qualifications`.`ns1:External_Job`.`ns1:Job_Title` as Job_Title",
          "`ns:Employees`.`ns1:Qualifications`.`ns1:External_Job`.`ns1:Start_Date` as Start_Date",
          "`ns:Employees`.`ns1:Qualifications`.`ns1:External_Job`.`ns1:End_Date` as End_Date",
          "`ns:Employees`.`ns1:Qualifications`.`ns1:External_Job`.`ns1:Location` as External_Job_Location",
          "`ns:Employees`.`ns1:Qualifications`.`ns1:External_Job`.`ns1:Responsibilities_And_Achievements` as Responsibilities_And_Achievements",
          "'' as External_Job_Company",
          "'' as External_Job_Reference",
          s"'$dataDatePart' as data_date_part").na.fill(" ").distinct()

    }

    else if (dataDatePart == "2021-04-23"){

      df_CvDatalakeWorkerFinal = df_CvDatalakeWorker
        .withColumn("CertAchievement", col("`ns:Employees`.`ns1:Qualifications`.`ns1:Certification_Achievement`"))
        .withColumn("IDCast",col("`ns:Employees`.`ns1:Summary`.`ns1:Employee_ID`").cast("String"))
        .selectExpr(
          "IDCast as Employee_ID",
          "`CertAchievement`.`ns1:Certification_Country` as Certification_Country",
          "`CertAchievement`.`ns1:Certification` as Certification",
          "`CertAchievement`.`ns1:Certification_Name` as Certification_Name",
          "`CertAchievement`.`ns1:Certification_Issuer` as Certification_Issuer",
          "`CertAchievement`.`ns1:Issued_Date` as Issued_Date",
          "'' as Additional_Information_Company",
          "'' as Job_Title",
          "'' as Start_Date",
          "'' as End_Date",
          "'' as External_Job_Location",
          "'' as Responsibilities_And_Achievements",
          "'' as External_Job_Company",
          "'' as External_Job_Reference",
          s"'$dataDatePart' as data_date_part").na.fill(" ").distinct()

    }

    else {
      println("La partición insertada no coincide on las que existen actualmente")
    }


    df_CvDatalakeWorkerFinal.show()

  }

}

// scalastyle:on
