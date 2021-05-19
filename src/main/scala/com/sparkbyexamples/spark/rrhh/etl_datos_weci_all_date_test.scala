package com.sparkbyexamples.spark.rrhh

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, explode_outer}

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

    val rutaCvDatalakeWorker = s"src/main/resources/rrhh/example_weci_consolid_wd/data_date_part=$dataDatePart/*.xml"

    val df_WeciReadXML = spark.read
      .format("com.databricks.spark.xml")
      .option("excludeAttribute", "false")
      .option("rowTag", "ns1:EMPLOYEE_DELTA_INTEGRATION")
      .load(rutaCvDatalakeWorker)

    println("Imprimiendo el esquema de df_WeciReadXML")
    df_WeciReadXML.printSchema()


    val df_Worker_Summary = df_WeciReadXML
      .withColumn("salary_plan", col("`compensationPlan`.`ns1:Salary_and_Hourly_Plan`"))
      .withColumn("other", explode_outer(col("`ns:Employees`.`ns1:Person_Identification`.`ns1:Other_Identifier`")))
      .selectExpr(
        "`ns:Employees`.`ns1:Worker_Status`.`ns1:Severance_Date` as Worker_Status_Severance_Date",
        // fin worker_status
        "`ns:Employees`.`ns1:Worker_Status`.`ns1:Status` as Worker_Status_Status",
        "`cast39` as Worker_Status_Terminated",
        // nuevos campos worker_status
        "'' as Worker_Status_Time_Off_Service_Date",
        "`ns:Employees`.`ns1:Worker_Status`.`ns1:Vesting_Date` as Worker_Status_Vesting_Date",
        // fin worker_status
        "`ns:Employees`.`ns1:Worker_Status`.`ns1:Worker_Reference_Type` as Worker_Status_Worker_Reference_Type",
        s"'$dataDatePart' as data_date_part"
      ).na.fill(" ").distinct()


        df_Worker_Summary.show()


  } //End Main

} //End Class
