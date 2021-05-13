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
    val dataDatePart = "2021-05-03"
    //val rutaCvDatalakeWorker = s"src/main/resources/rrhh/example_cv_datalake_worker/data_date_part=$dataDatePart/CvDatalake_Worker_20210505.xml"
    val rutaCvDatalakeWorker = s"src/main/resources/rrhh/example_cv_datalake_worker/data_date_part=$dataDatePart/CvDatalake_Worker_20210506.xml"


    val df_CvDatalakeWorker = spark.read
      .format("com.databricks.spark.xml")
      .option("excludeAttribute", "false")
      .option("rowTag", "ns1:EMPLOYEE_DELTA_INTEGRATION")
      .load(rutaCvDatalakeWorker)

    println("Imprimiendo el esquema de df_CvDatalakeWorker")
    df_CvDatalakeWorker.printSchema()


    val df_CvDatalakeWorkerFinal = df_CvDatalakeWorker

      //2021-05-05 no es de tipo array por tanto se accede como struct
      .withColumn("CertAchievement", explode_outer(col("`ns:Employees`.`ns1:Qualifications`.`ns1:Certification_Achievement`")))
      //.withColumn("CertAchievement", col("`ns:Employees`.`ns1:Qualifications`.`ns1:Certification_Achievement`"))

      .withColumn("IDCast",col("`ns:Employees`.`ns1:Summary`.`ns1:Employee_ID`").cast("String"))

      //2021-05-06 no aparece
      //.withColumn("External_Job_Reference",col("`ns:Employees`.`ns1:Qualifications`.`ns1:External_Job`.`ns1:Job_Reference`").cast("String"))

      .selectExpr(

        "IDCast as Employee_ID",

        //2021-05-05 no aparece
        "`CertAchievement`.`ns1:Certification_Country` as Certification_Country",
        //"'' as Certification_Country",

        "`CertAchievement`.`ns1:Certification` as Certification",
        "`CertAchievement`.`ns1:Certification_Name` as Certification_Name",
        "`CertAchievement`.`ns1:Certification_Issuer` as Certification_Issuer",

        //2021-05-06 no aparece
        //2021-05-05 no aparece
        //"`CertAchievement`.`ns1:Issued_Date` as Issued_Date",
        "'' as Issued_Date",

        "`ns:Employees`.`ns1:Additional_Information`.`ns1:Company` as Additional_Information_Company",
        "`ns:Employees`.`ns1:Qualifications`.`ns1:External_Job`.`ns1:Job_Title` as Job_Title",
        "`ns:Employees`.`ns1:Qualifications`.`ns1:External_Job`.`ns1:Start_Date` as Start_Date",

        //2021-05-05 no aparece
        "`ns:Employees`.`ns1:Qualifications`.`ns1:External_Job`.`ns1:End_Date` as End_Date",
        //"'' as End_Date",

        "`ns:Employees`.`ns1:Qualifications`.`ns1:External_Job`.`ns1:Location` as External_Job_Location",

        //2021-05-05 no aparece
        "`ns:Employees`.`ns1:Qualifications`.`ns1:External_Job`.`ns1:Responsibilities_And_Achievements` as Responsibilities_And_Achievements",
        //"'' as Responsibilities_And_Achievements",

        "`ns:Employees`.`ns1:Qualifications`.`ns1:External_Job`.`ns1:Company` as External_Job_Company",

        //2021-05-06 no aparece
        //"External_Job_Reference",
        "'' as External_Job_Reference",

        s"'$dataDatePart' as data_date_part").na.fill(" ").distinct()

    df_CvDatalakeWorkerFinal.show()

  }

}

// scalastyle:on
