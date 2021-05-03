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
    val df_WeciFinal = df_WeciConsolidWD
      .withColumn("natio", explode(col("`wd:National_Identifiers_group`")))
      .withColumn("educ", explode(col("`wd:education_group`")))
      .withColumn("job_class", explode(col("`wd:Job_Classifications_group`")))
      .selectExpr(
        //Estos son los explode necesarios para acceder a los campos array
        "`natio`.`wd:National_ID_Type` as National_ID_Type",
        "`educ`.`wd:Education_Is_Highest_Level_of_Education` as Education_Is_Highest_Level_of_Education",
        "`job_class`.`wd:Job_Classification_Groups`.`_Descriptor` as Job_Classification_Groups",
        //A partir de estos campos se mantiene igual
        "`wd:workdayID` as workdayID",
        "`wd:Hire_Date` as Hire_Date",
        "`wd:Hire_Reason` as Hire_Reason",
        "`wd:Original_Hire_Date` as Original_Hire_Date",
        "`wd:dateOfBirth` as date_of_Birth",
        "`wd:City_of_Birth` as City_of_Birth",
        //Este tuve que agregar el nivel inicial wd:Position_group pq o no era capaz de encontrar el .`wd:Home_Country`.`_Descriptor`
        "`wd:Position_group`.`wd:Home_Country`.`_Descriptor` as Home_Country"
      ).na.fill(" ")

    */



  }

}

// scalastyle:on
