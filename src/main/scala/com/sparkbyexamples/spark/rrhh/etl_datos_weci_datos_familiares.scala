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
    val dataDatePart = "2021-05-03"
    val rutaWeciDatosFamiliares = s"src/main/resources/rrhh/example_weci_consolid_wd/data_date_part=$dataDatePart/*.xml"

    val df_WeciFamiliaWD = spark.read
      .format("com.databricks.spark.xml")
      .option("excludeAttribute", "false")
      .option("rowTag", "ns1:EMPLOYEE_DELTA_INTEGRATION")
      .load(rutaWeciDatosFamiliares)

    println("Imprimiendo el esquema de df_WeciFamiliaWD")
    df_WeciFamiliaWD.printSchema()


    val df_FamiliaFinal = df_WeciFamiliaWD
      .withColumn("relatedPerson", explode(col("`ns:Employees`.`ns1:Related_Person`")))
      .withColumn("relatedNacionalID", explode(col("`ns:Employees`.`ns1:Related_Person_Identification`"))).

      selectExpr(
        "`relatedPerson`.`ns1:Dependent_ID` as ID_FAMILIAR",
        "`relatedPerson`.`ns1:Related_Person_ID` as TIPO_DE_PARENTESCO",
        "`relatedPerson`.`ns1:Legal_Name`.`ns1:First_Name` as NOMBRE_FAMILIAR",
        "`relatedPerson`.`ns1:Legal_Name`.`ns1:Last_Name` as PRIMER_APELLIDO",
        "`relatedPerson`.`ns1:Legal_Name`.`ns1:Secondary_Last_Name` as SEGUNDO_APELLIDO",
        "'' as SEGUNDO_NOMBRE",
        "`relatedPerson`.`ns1:Birth_Date` as FECHA_NACIMIENTO",
        "'' as FECHA_DEFUNCION",
        "'' as GRADO_MINUSVALIA",
        "`relatedNacionalID`.`ns1:National_Identifier`.`ns1:Country` as PAIS_DOCUMENTO_FAMILIAR",
        "`relatedNacionalID`.`ns1:National_Identifier`.`ns1:National_ID_Type` as TIPO_DOCUMENTO_FAMILIAR",
        "`relatedNacionalID`.`ns1:National_Identifier`.`ns1:National_ID` as CODIGO_DOCUMENTO_FAMILIAR",
        "`relatedPerson`.`ns1:Gender` as GENERO_DEL_DEPENDIENTE",
        "'' as PAIS_DE_NACIMIENTO",
        "'' as CIUDAD_DE_NACIMIENTO",
        "`relatedNacionalID`.`ns1:Other_Identifier`.`ns1:Custom_ID_Type` as ESTADO_CIVIL_DEPENDIENTE",
        "`relatedNacionalID`.`ns1:Other_Identifier`.`ns1:Custom_ID_Type` as SITUACION_FAMILIAR_DEPENDIENTE",
        "`relatedNacionalID`.`ns1:Other_Identifier`.`ns1:Custom_ID_Type` as NUMERO_MATRICULA_DEPENDIENTE",
        "'' as NUMERO_MATRICULA_DEPENDIENTE_CALCULADA",
        "'' as NOMBRE_CONTACTO_EMERGENCIA",
        "'' as APELLIDO_CONTACTO_EMERGENCIA",
        "'' as TELEFONO_CONTACTO_EMERGENCIA",
        s"'$dataDatePart' as data_date_part"
      ).na.fill(" ").distinct().show()











  }
}
