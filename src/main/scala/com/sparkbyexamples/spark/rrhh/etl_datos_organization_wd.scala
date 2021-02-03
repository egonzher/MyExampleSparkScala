// scalastyle:off

//package com.santander.rrhh

package com.sparkbyexamples.spark.rrhh

//import com.santander.common.TemplateRRHH
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}

object etl_datos_organization_wd /* extends TemplateRRHH */ {


  def main(args: Array[String]): Unit = {

    val className = this.getClass.getCanonicalName
    println(s" Running $className")
    //super.main(args, this)
  }

  def apply(spark: SparkSession, args: Map[String, String]): Unit = {

    val SAVEMODE = SaveMode.Overwrite

    //Params on file conf needed on process

    val HDFS_LANDING_WD_XML = args("HDFS_LANDING_WD_XML")
    val PARAM_BU_WORKDAY = args("PARAM_BU_WORKDAY")
    val dataDatePart = args("PARAM_FECHA_DATA_DATE")

    val format = "parquet"
    val table = ".datos_organization"
    //val ruta = s"hdfs://isbanhdfscert/cer/landing/rrhh/organization_xml/data_date_part=$dataDatePart/*.xml"
    //val ruta = s"hdfs://suprapre-hdfs/pre/business/rrhh/workday/organization_xml/data_date_part=$dataDatePart/*.xml"
    val ruta =s"${HDFS_LANDING_WD_XML}organization_wd/data_date_part=$dataDatePart/*/*.xml"
    //para solventar un error de inserción de datos, ponemos  esta configuración al spark
    spark.sqlContext.setConf("hive.exec.dynamic.partition", "true")
    spark.sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
    spark.conf.set("spark.sql.crossJoin.enabled", "true")


      //Se lee el XML

      val organizators = spark.read
        .format("com.databricks.spark.xml")
        .option("excludeAttribute", "false")
        .option("rowTag", "ns4:Organizations")
        .load(ruta)


      //organizators.printSchema()

      val df_hierarchy = organizators.selectExpr(
        "`ns:Organization_ID`.`_VALUE` as organization_id",
        "explode(`ns:Department_Hierarchy`) as hierarchy",
        "hierarchy.`ns:Organization_Level` as organization_level",
        "hierarchy.`ns:Organization_Code` as organization_code",
        "hierarchy.`ns:Organization_Description` as organization_description").na.fill(" ")


      val df_adress = organizators.selectExpr(
        "`ns:Organization_ID`.`_VALUE` as organization_id",
        "explode(`ns:Address_Data`.`ns:Address_Line_Data`) as e",
        "e.`_VALUE` as address_line_data",
        "e.`ns:Label` as address_line_label").na.fill(" ")




      val df_general = organizators.selectExpr(
        "`ns:Organization_ID`.`_VALUE` as organization_id",
        "`ns:Organization_Code`.`_VALUE` as organization_code",
        "`ns:Organization_Type`.`_VALUE` as organization_type",
        "`ns:Organization_Name`.`_VALUE` as organization_name",
        "`ns:Organization_Description`.`_VALUE` as organization_description",
        "`ns:Organization_Subtype`.`_VALUE` as organization_subtype",
        "`ns:Level_from_top`.`_VALUE` as level_from_top",
        "`ns:Inactive`.`_VALUE` as inactive",
        //"`ns:Location`.`ns:Location` as location",
        //"`ns:Location`.`ns:Location_Hierarchy` as location_hierarchy",
        "`ns:Location`.`ns:City` as location_city",
        "`ns:Location`.`ns:Country` as location_country",
        "`ns:Location`.`ns:Location_Code` as location_code",
        "`ns:Manager`.`ns:Employee_ID` as manager_id",
        "`ns:Manager`.`ns:Name` as name",
        "`ns:Manager`.`ns:Position_ID` as position_id",
        "`ns:Manager`.`ns:Company_Code` as company_code",
        //"`ns:Manager`.`ns:Location_Code` as manager_location_code",
        "`ns:Manager`.`ns:Country_Local_ID` as country_local_id",
        //"explode(`ns:Department_Hierarchy`) as hierarchy",
        //"hierarchy.`ns:Organization_Level` as organization_level",
        //"hierarchy.`ns:Organization_Code` as organization_code",
        //"hierarchy.`ns:Organization_Description` as organization_description",
        //"`ns:Company`.`ns:Company_ID` as company_id",
        //"`ns:Company`.`ns:Company_Code` as company_code",
        //"`ns:Cost_Center`.`_VALUE` as cost_center",
        "`ns:Cost_Center`.`ns:Cost_Center_Code` as cost_center",
        "`ns:Superior_Organization`.`_VALUE` as superior_organization",
        "`ns:Superior_Organization_Code`.`_VALUE` as superior_organization_code ",
        "`ns:Availability_Date`.`_VALUE` as available_date",
        "`ns:Address_Data`.`ns:Country` as address_data_country",
        "`ns:Address_Data`.`ns:Country_region` as country_region",
        //"explode(`ns:Address_Data`.`ns:Address_Line_Data`) as e",
        //"e.`_Label` as address_line_data",
        //"e.`_VALUE` as address_line_data",
        "`ns:Address_Data`.`ns:Postal_Code` as postal_code",
        "`ns:Address_Data`.`ns:City_Subdivision_1` as city_subdivision_1",
        //"`ns:Company_Data`.`_VALUE` as available_date",
        "`ns:Inactivation_Date`.`_VALUE` as inactivation_date",
        "`ns:Effective_Date_For_CC`.`_VALUE` as effective_date_for_cc",
        "`ns:Effective_Date`.`_VALUE` as effective_date ",
        "`ns:WID`.`_VALUE` as wid",
        "`ns:Department_Local_Subtype`.`_VALUE` as department_local_subtype"
        //"`ns:Company_Hierarchy`.`_VALUE` as company_hierarchy",
      ).na.fill(" ")


      val df_final = df_general.as("df_general").join(df_hierarchy.as("df_hierarchy"), expr("trim(df_hierarchy.organization_id)=trim(df_general.organization_id)"), "left_outer")
        .join(df_adress.as("df_adress"), expr("trim(df_adress.organization_id)=trim(df_general.organization_id)"), "left_outer")
        .selectExpr(
          "df_general.organization_id",
          "df_general.organization_code",
          "df_general.organization_type",
          "df_general.organization_name",
          "df_general.organization_description",
          "df_general.organization_subtype",
          "df_general.level_from_top",
          "df_general.inactive",
          "df_general.location_city",
          "df_general.location_country",
          "df_general.location_code",
          "df_general.manager_id",
          "df_general.name",
          "df_general.position_id",
          "df_general.company_code",
          "df_general.country_local_id",
          "df_hierarchy.organization_level as hierarchy_level",
          "df_hierarchy.organization_code as hierarchy_code",
          "df_hierarchy.organization_description as hierarchy_description",
          "df_general.cost_center",
          "df_general.superior_organization",
          "df_general.superior_organization_code ",
          "df_general.available_date",
          "df_general.address_data_country",
          "df_general.country_region",
          "if(trim(df_adress.address_line_label) like '%Address_Line_1%',df_adress.address_line_data, ' ' as address_line_data_1",
          "if(trim(df_adress.address_line_label) like '%Address_Line_2%',df_adress.address_line_data, ' ' as address_line_data_2",
          //""df_adress.address_line_data",
          "df_general.postal_code",
          "df_general.city_subdivision_1",
          "df_general.inactivation_date",
          "df_general.effective_date_for_cc",
          "df_general.effective_date ",
          "df_general.wid",
          "df_general.department_local_subtype",
          s"'$dataDatePart' as data_date_part").na.fill(" ").distinct()

      df_final.show(15,false)
      //df_final.write.format("parquet").mode(SAVEMODE).insertInto(PARAM_BU_WORKDAY.concat(".datos_organization"))
    }
}

// scalastyle:on