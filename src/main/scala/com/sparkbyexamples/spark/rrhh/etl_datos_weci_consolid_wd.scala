// scalastyle:off

package com.sparkbyexamples.spark.rrhh

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, explode, expr}
import org.apache.spark.sql.types.StringType

object etl_datos_weci_consolid_wd {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local[1]")
      .appName("SparkByExample")
      .getOrCreate()


    //Trabajando con la nueva integración de weci en consolid_wd
    val rutaWeciConsolidWD = "src/main/resources/rrhh/example_weci_consolid_wd/*.xml"

    val df_WeciConsolidWD = spark.read
      .format("com.databricks.spark.xml")
      .option("excludeAttribute", "false")
      .option("rowTag", "ns1:EMPLOYEE_DELTA_INTEGRATION")
      .load(rutaWeciConsolidWD)

    println("Imprimiendo el esquema de df_WeciConsolidWD")
    df_WeciConsolidWD.printSchema()

/*
 |    |-- ns1:Person_Identification: struct (nullable = true)
 |    |    |-- ns1:National_Identifier: array (nullable = true)
 |    |    |    |-- element: struct (containsNull = true)
 |    |    |    |    |-- ns1:Country: string (nullable = true)
 |    |    |    |    |-- ns1:National_ID: string (nullable = true)
 |    |    |    |    |-- ns1:National_ID_Type: string (nullable = true)
 |    |    |-- ns1:Other_Identifier: array (nullable = true)
 |    |    |    |-- element: struct (containsNull = true)
 |    |    |    |    |-- ns1:Custom_ID_Type: string (nullable = true)
 |    |    |-- ns1:Passport: string (nullable = true)
 |    |    |-- ns1:Visa: string (nullable = true)
 */

    val df_WeciFinal = df_WeciConsolidWD
      .withColumn("natioId", explode(col("`ns:Employees`.`ns1:Person_Identification`.`ns1:National_Identifier`")))
      //.withColumn("job_class", explode(col("`wd:Job_Classifications_group`")))
      .selectExpr(
        "`natioId`.`ns1:National_ID_Type` as NATIONAL_ID_TYPE_DESCR",
        "`ns:Employees`.`ns1:Personal`.`ns1:Date_of_Birth` as BIRTHDATE",
        "`ns:Employees`.`ns1:Personal`.`ns1:City_of_Birth` as BIRTHPLACE",
        "`ns:Employees`.`ns1:Personal`.`ns1:Country_of_Birth` as BIRTHCOUNTRY",
        "`natioId`.`ns1:National_ID` as NATIONALITY_ID",
        //"`wd:Hire_Reason` as Hire_Reason",
        //"`wd:Original_Hire_Date` as Original_Hire_Date",
        //"`wd:dateOfBirth` as date_of_Birth",
        //"`wd:City_of_Birth` as City_of_Birth",
        //Este tuve que agregar el nivel inicial wd:Position_group pq o no era capaz de encontrar el .`wd:Home_Country`.`_Descriptor`
        //"`wd:Position_group`.`wd:Home_Country`.`_Descriptor` as Home_Country"
      ).na.fill(" ").distinct().show()





  }

}

// scalastyle:on
