package com.sparkbyexamples.spark.rrhh

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, explode}

object etl_datos_weci_all_date {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local[1]")
      .appName("SparkByExample")
      .getOrCreate()


    //Trabajando con la nueva integración de cvdatalake_worker
    val dataDatePart = "2021-05-03"
    val rutaCvDatalakeWorker = s"src/main/resources/rrhh/example_weci_consolid_wd/data_date_part=$dataDatePart/*.xml"


    val df_todo_weci = spark.read
      .format("com.databricks.spark.xml")
      .option("excludeAttribute", "false")
      .option("rowTag", "ns1:EMPLOYEE_DELTA_INTEGRATION")
      .load(rutaCvDatalakeWorker)

    println("Imprimiendo el esquema de df_CvDatalakeWorker")


    val df_WeciFinal = df_todo_weci
      .withColumn("natioId", explode(col("`ns:Employees`.`ns1:Person_Identification`.`ns1:National_Identifier`")))
      .withColumn("relatedPerson", explode(col("`ns:Employees`.`ns1:Related_Person`")))
      .withColumn("relatedNacionalID", explode(col("`ns:Employees`.`ns1:Related_Person_Identification`")))
      .withColumn("allowance_plan", explode(col("`ns:Employees`.`ns1:Compensation_Plans`.`ns1:Allowance_Plan`")))
      .withColumn("job_family", explode(col("`ns:Employees`.`ns1:Position`.`ns1:Job_Family`")))
      .withColumn("related_communication", explode(col("`ns:Employees`.`ns1:Related_Person_Communication`")))
      .withColumn("PostalCast",col("`ns:Employees`.`ns1:Position`.`ns1:Business_Site`.`ns1:Postal_Code`").cast("String"))
      .withColumn("SuperCast",col("`ns:Employees`.`ns1:Position`.`ns1:Supervisor`.`ns1:ID`").cast("String"))
      .withColumn("IDCast",col("`ns:Employees`.`ns1:Summary`.`ns1:Employee_ID`").cast("String"))
      // .withColumn("concatAddress",concat(col("`ns:Employees`.`ns1:Position`.`ns1:Business_Site`.`ns1:Address_Line_1`"),col("`ns:Employees`.`ns1:Position`.`ns1:Business_Site`.`ns1:Address_Line_3`")))
      .withColumn("cast1",col("`ns:Employees`.`ns1:Compensation`.`ns1:Compensation_Summary_Based_on_Compensation_Grade`.`ns1:Primary_Compensation_Basis`").cast("String"))
      .withColumn("cast2",col("`ns:Employees`.`ns1:Compensation`.`ns1:Compensation_Summary_Based_on_Compensation_Grade`.`ns1:Total_Base_Pay`").cast("String"))
      .withColumn("cast3",col("`ns:Employees`.`ns1:Compensation`.`ns1:Compensation_Summary_in_Annualized_Frequency`.`ns1:Primary_Compensation_Basis`").cast("String"))
      .withColumn("cast4",col("`ns:Employees`.`ns1:Compensation`.`ns1:Compensation_Summary_in_Annualized_Frequency`.`ns1:Total_Base_Pay`").cast("String"))
      .withColumn("cast5",col("`allowance_plan`.`ns1:Amount`").cast("String"))
      .withColumn("cast6",col("`allowance_plan`.`ns1:Apply_FTE`").cast("String"))
      .withColumn("cast7",col("`ns:Employees`.`ns1:Compensation_Plans`.`ns1:Bonus_Plan`.`ns1:Amount`").cast("String"))
      .withColumn("cast8",col("`ns:Employees`.`ns1:Compensation_Plans`.`ns1:Bonus_Plan`.`ns1:Prorated_Amount`").cast("String"))
      .withColumn("cast9",col("`ns:Employees`.`ns1:Compensation_Plans`.`ns1:Salary_and_Hourly_Plan`.`ns1:Amount`").cast("String"))
      .withColumn("cast11",col("`ns:Employees`.`ns1:Compensation_Plans`.`ns1:Salary_and_Hourly_Plan`.`ns1:Apply_FTE`").cast("String"))
      .withColumn("cast12",col("`ns:Employees`.`ns1:Compensation_Plans`.`ns1:Salary_and_Hourly_Plan`.`ns1:Prorated_Amount`").cast("String"))
      .withColumn("cast13",col("`ns:Employees`.`ns1:Employee_Contract`.`ns1:Maximum_Weekly_Hours`").cast("String"))
      .withColumn("cast14",col("`ns:Employees`.`ns1:Employee_Contract`.`ns1:Minimum_Weekly_Hours`").cast("String"))
      .withColumn("cast15",col("`natioId`.`ns1:Verified_By`").cast("String"))
      .withColumn("cast16",col("`ns:Employees`.`ns1:Personal`.`ns1:Local_Hukou`").cast("String"))
      .withColumn("cast17",col("`ns:Employees`.`ns1:Personal`.`ns1:Number_of_Payroll_Dependents`").cast("String"))
      .withColumn("cast18",col("`ns:Employees`.`ns1:Personal`.`ns1:Uses_Tobacco`").cast("String"))
      .withColumn("cast19",col("`ns:Employees`.`ns1:Position`.`ns1:Job_Classification`.`ns1:Job_Classification_ID`").cast("String"))
      .withColumn("cast20",col("`job_family`.`ns1:ID`").cast("String"))
      .withColumn("cast21",col("`relatedPerson`.`ns1:Allowed_for_Tax_Deduction`").cast("String"))
      .withColumn("cast22",col("`relatedPerson`.`ns1:Annual_Income_Amount`").cast("String"))
      .withColumn("cast23",col("`relatedPerson`.`ns1:Beneficiary`").cast("String"))
      .withColumn("cast24",col("`relatedPerson`.`ns1:Dependent`").cast("String"))
      .withColumn("cast25",col("`relatedPerson`.`ns1:Emergency_Contact`").cast("String"))
      .withColumn("cast26",col("`relatedPerson`.`ns1:Full_Time_Student`").cast("String"))
      .withColumn("cast27",col("`relatedPerson`.`ns1:Has_Health_Insurance`").cast("String"))
      .withColumn("cast28",col("`relatedPerson`.`ns1:Is_Dependent_for_Payroll_Purposes`").cast("String"))
      .withColumn("cast29",col("`relatedPerson`.`ns1:Is_Disabled`").cast("String"))
      .withColumn("cast30",col("`relatedPerson`.`ns1:Lives_with_Worker`").cast("String"))
      .withColumn("cast31",col("`relatedPerson`.`ns1:Relative`").cast("String"))
      .withColumn("cast32",col("`relatedPerson`.`ns1:Tobacco_Use`").cast("String"))
      .withColumn("cast33",col("`ns:Employees`.`ns1:Worker_Status`.`ns1:Active`").cast("String"))
      .withColumn("cast34",col("`ns:Employees`.`ns1:Worker_Status`.`ns1:Hire_Rescinded`").cast("String"))
      .withColumn("cast35",col("`ns:Employees`.`ns1:Worker_Status`.`ns1:Is_Rehire`").cast("String"))
      .withColumn("cast36",col("`ns:Employees`.`ns1:Worker_Status`.`ns1:Not_Returning`").cast("String"))
      .withColumn("cast37",col("`ns:Employees`.`ns1:Worker_Status`.`ns1:Regrettable_Termination`").cast("String"))
      .withColumn("cast38",col("`ns:Employees`.`ns1:Worker_Status`.`ns1:Return_Unknown`").cast("String"))
      .withColumn("cast39",col("`ns:Employees`.`ns1:Worker_Status`.`ns1:Terminated`").cast("String"))


      .selectExpr(

        //Campo ID para realizar el join
        "`IDCast` as Employee_ID",
        "`ns:Employees`.`ns1:Summary`.`ns1:WID` as WID",

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
        "`cast4` as ANNUAL_BASE_SALARY",
        "`ns:Employees`.`ns1:Compensation`.`ns1:Compensation_Summary_in_Annualized_Frequency`.`ns1:Currency` as ANNUAL_BASE_SALARY_CURRENCY",
        "`ns:Employees`.`ns1:Position`.`ns1:Business_Site`.`ns1:Location_ID` as BUSINESS_UNIT_DESCR",
        "`ns:Employees`.`ns1:Employee_Contract`.`ns1:Contract_ID` as CONTRACT_TYPE",
        "`ns:Employees`.`ns1:Employee_Contract`.`ns1:Contract_Type` as `CONTRACT_TYPE_DESCR`",

        //Duda sobre si hay que concatenar todos los address en una sola columna
        "`ns:Employees`.`ns1:Position`.`ns1:Business_Site`.`ns1:Address_ID` as ADDRESS1_2_ID",
        "`ns:Employees`.`ns1:Position`.`ns1:Business_Site`.`ns1:Address_Line_1` as ADDRESS1_2_Line_1",
        "`ns:Employees`.`ns1:Position`.`ns1:Business_Site`.`ns1:Address_Line_3` as ADDRESS1_2_Line_3",
        // "`concatAddress` as ADDRESS1_3",

        "`PostalCast` as POSTAL",
        "`ns:Employees`.`ns1:Position`.`ns1:Business_Site`.`ns1:City` as CITY",
        "`natioId`.`ns1:Country` as `PERS_COUNTRY`",

        //Estos son los de color Amarillo que si había dudas
        "`ns:Employees`.`ns1:Worker_Status`.`ns1:Active_Status_Date` as `EMPL_STATUS_EFFDT`",
        "`ns:Employees`.`ns1:Personal`.`ns1:Preferred_Language` as LANG_CD",
        "`ns:Employees`.`ns1:Personal`.`ns1:Gender` as GENDER",
        "`ns:Employees`.`ns1:Personal`.`ns1:Disability_Status` as DISABLE_TYPE_ESP",
        "`ns:Employees`.`ns1:Personal`.`ns1:Disability_Status` as DISABLE_TYPE_ESP_DESCR",
        "`relatedPerson`.`ns1:Dependent_ID` as EMPLID_CONYUGE",
        /*
                "`ns:Employees`.`ns1:Worker_Status`.`ns1:Hire_Date` as COMPANY_EFFDT",
        */
        "`ns:Employees`.`ns1:Worker_Status`.`ns1:Original_Hire_Date` as ZING_GRUPO_DT",
        "`ns:Employees`.`ns1:Collective_Agreement`.`ns1:Start_Date` as COLLECTIVE_YN_GLOBAL_EFFDT",
        "`ns:Employees`.`ns1:Collective_Agreement`.`ns1:Start_Date` as COLLECTIVE_YN_LOCAL_EFFDT",
        "`ns:Employees`.`ns1:Position`.`ns1:Organization` as TIP_DEPTID",
        "`SuperCast` as SUPERVISOR_EFFDT",
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

        // a partir de aqui son weci datos familiares
        "`relatedPerson`.`ns1:Dependent_ID` as ID_FAMILIAR",
        "`relatedPerson`.`ns1:Related_Person_ID` as TIPO_DE_PARENTESCO",
        "`relatedPerson`.`ns1:Legal_Name`.`ns1:First_Name` as NOMBRE_FAMILIAR",
        "`relatedPerson`.`ns1:Legal_Name`.`ns1:Last_Name` as PRIMER_APELLIDO",
        "`relatedPerson`.`ns1:Legal_Name`.`ns1:Secondary_Last_Name` as SEGUNDO_APELLIDO",
        "'' as SEGUNDO_NOMBRE ",
        "`relatedPerson`.`ns1:Birth_Date` as FECHA_NACIMIENTO ",
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

        // Campos que no estan mapeados
        "`ns:Employees`.`ns1:Additional_Information`.`ns1:Company` as `Additional_Information_Company`",
        "`ns:Employees`.`ns1:Collective_Agreement`.`ns1:Position_ID` as Collective_Agreement_Position_ID",
        "`ns:Employees`.`ns1:Compensation`.`ns1:Compensation_Change_Reason` as `Compensation_Change_Reason`",
        "`ns:Employees`.`ns1:Compensation`.`ns1:Compensation_Summary_Based_on_Compensation_Grade`.`ns1:Currency` as `Compensation_Summary_Based_on_Compensation_Grade_Currency`",
        "`ns:Employees`.`ns1:Compensation`.`ns1:Compensation_Summary_Based_on_Compensation_Grade`.`ns1:Frequency` as `Compensation_Summary_Based_on_Compensation_Grade_Frequency`",
        "`cast1` as `Compensation_Summary_Based_on_Compensation_Grade_Primary_Compensation_Basis`",
        "`cast2` as `Compensation_Summary_Based_on_Compensation_Grade_Total_Base_Pay`",
        "`ns:Employees`.`ns1:Compensation`.`ns1:Compensation_Summary_in_Annualized_Frequency`.`ns1:Frequency` as Compensation_Summary_in_Annualized_Frequency_Frequency",
        "`cast3` as Compensation_Summary_in_Annualized_Frequency_Primary_Compensation_Basis",
        "`cast5` as `Allowance_Plan_Amount`",
        "`cast6` as `Allowance_Plan_Apply_FTE`",
        "`allowance_plan`.`ns1:Compensation_Plan` as `Allowance_Plan_Compensation_Plan`",
        "`allowance_plan`.`ns1:Currency` as `Allowance_Plan_Currency`",
        "`allowance_plan`.`ns1:Frequency` as `Allowance_Plan_Frequency`",
        "`allowance_plan`.`ns1:Start_Date` as `Allowance_Plan_Start_Date`",
        "`cast7` as Compensation_Plans_Amount",
        "`ns:Employees`.`ns1:Compensation_Plans`.`ns1:Bonus_Plan`.`ns1:Compensation_Plan` as Compensation_Plans_Compensation_Plan",
        "`ns:Employees`.`ns1:Compensation_Plans`.`ns1:Bonus_Plan`.`ns1:Currency` as Compensation_Plans_Currency",
        "`ns:Employees`.`ns1:Compensation_Plans`.`ns1:Bonus_Plan`.`ns1:Frequency` as Compensation_Plans_Frequency",
        "`cast8` as Compensation_Plans_Prorated_Amount",
        "`ns:Employees`.`ns1:Compensation_Plans`.`ns1:Bonus_Plan`.`ns1:Start_Date` as Compensation_Plans_Start_Date",
        "`ns:Employees`.`ns1:Compensation_Plans`.`ns1:Position_ID` as Compensation_Plans_Position_ID",
        "`cast9` as Salary_and_Hourly_Plan_Amount",
        "`cast11` as Salary_and_Hourly_Plan_Apply_FTE",
        "`ns:Employees`.`ns1:Compensation_Plans`.`ns1:Salary_and_Hourly_Plan`.`ns1:Compensation_Plan` as Salary_and_Hourly_Plan_Compensation_Plan",
        "`ns:Employees`.`ns1:Compensation_Plans`.`ns1:Salary_and_Hourly_Plan`.`ns1:Currency` as Salary_and_Hourly_Plan_Currency",
        "`ns:Employees`.`ns1:Compensation_Plans`.`ns1:Salary_and_Hourly_Plan`.`ns1:Frequency` as Salary_and_Hourly_Plan_Frequency",
        "`cast12` as Salary_and_Hourly_Plan_Prorated_Amount",
        "`ns:Employees`.`ns1:Compensation_Plans`.`ns1:Salary_and_Hourly_Plan`.`ns1:Start_Date` as Salary_and_Hourly_Plan_Start_Date",
        "`ns:Employees`.`ns1:Derived_Event_Code` as Derived_Event_Code",
        "`ns:Employees`.`ns1:Effective_Moment` as Effective_Moment",
        "`ns:Employees`.`ns1:Employee_Contract`.`ns1:Collective_Agreement` as Employee_Contract_Collective_Agreement",
        "`ns:Employees`.`ns1:Employee_Contract`.`ns1:Contract_Status` as Employee_Contract_Contract_Status",
        "`ns:Employees`.`ns1:Employee_Contract`.`ns1:Description` as Employee_Contract_Description",
        "`cast13` as Employee_Contract_Maximum_Weekly_Hours",
        "`cast14` as Employee_Contract_Minimum_Weekly_Hours",
        "`ns:Employees`.`ns1:Employee_Contract`.`ns1:Position_ID` as Employee_Contract_Position_ID",
        "`ns:Employees`.`ns1:Employee_Contract`.`ns1:Start_Date` as Employee_Contract_Start_Date",
        "`ns:Employees`.`ns1:Entry_Moment` as Entry_Moment",
        "`ns:Employees`.`ns1:Person_Communication`.`ns1:Email` as Person_Communication_Email",
        "`ns:Employees`.`ns1:Person_Communication`.`ns1:Phone` as Person_Communication_Phone",
        "`natioId`.`ns1:Verification_Date` as National_Identifier_Verification_Date",
        "`cast15` as National_Identifier_Verified_By",
        "`ns:Employees`.`ns1:Person_Identification`.`ns1:Passport` as Person_Identification_Passport",
        "`ns:Employees`.`ns1:Person_Identification`.`ns1:Visa` as Person_Identification_Visa",
        "`cast16` as Personal_Local_Hukou",
        "`ns:Employees`.`ns1:Personal`.`ns1:Name`.`ns1:Country` as Personal_Name_Country",
        "`ns:Employees`.`ns1:Personal`.`ns1:Name`.`ns1:First_Name` as Personal_Name_First_Name",
        "`ns:Employees`.`ns1:Personal`.`ns1:Name`.`ns1:General_Display_Name` as Personal_Name_General_Display_Name",
        "`ns:Employees`.`ns1:Personal`.`ns1:Name`.`ns1:Last_Name` as Personal_Name_Last_Name",
        "`ns:Employees`.`ns1:Personal`.`ns1:Name`.`ns1:Middle_Name` as Personal_Name_Middle_Name",
        "`ns:Employees`.`ns1:Personal`.`ns1:Name`.`ns1:Secondary_Last_Name` as Personal_Name_Secondary_Last_Name",
        "`ns:Employees`.`ns1:Personal`.`ns1:Name`.`ns1:Title` as Personal_Name_Title",
        "`cast17` as Personal_Number_of_Payroll_Dependents",
        "`ns:Employees`.`ns1:Personal`.`ns1:Preferred_Locale` as Personal_Preferred_Locale",
        "`cast18` as Personal_Uses_Tobacco",
        "`ns:Employees`.`ns1:Personal`.`ns1:Workday_Account` as Personal_Workday_Account",
        "`ns:Employees`.`ns1:Position`.`ns1:Business_Site`.`ns1:Country` as Business_Site_Country",
        "`ns:Employees`.`ns1:Position`.`ns1:Business_Site`.`ns1:Location_Name` as Business_Site_Location_Name",
        "`ns:Employees`.`ns1:Position`.`ns1:Job_Classification`.`ns1:Job_Classification_Group` as Job_Classification_Group",
        "`cast19` as Job_Classification_ID",
        "`ns:Employees`.`ns1:Position`.`ns1:Job_Classification`.`ns1:Mapped_Value` as Job_Classification_Mapped_Value",
        "`cast20` as Job_Family_ID",
        "`job_family`.`ns1:Mapped_Value` as Job_Family_Mapped_Value",
        "`job_family`.`ns1:Name` as Job_Family_Name",
        "`ns:Employees`.`ns1:Position`.`ns1:Job_Profile` as Position_Job_Profile",
        "`ns:Employees`.`ns1:Position`.`ns1:Management_Level` as Position_Management_Level",
        "`ns:Employees`.`ns1:Position`.`ns1:Position_ID` as Position_Position_ID",
        "`ns:Employees`.`ns1:Position`.`ns1:Supervisor`.`ns1:Name` as Supervisor_Name",
        "`ns:Employees`.`ns1:Position`.`ns1:Supervisor`.`ns1:Other_ID` as Supervisor_Other_ID",
        "`ns:Employees`.`ns1:Qualifications`.`ns1:Certification_Achievement` as Qualifications_Certification_Achievement",
        "`cast21` as Related_Person_Allowed_for_Tax_Deduction",
        "`cast22` as Related_Person_Annual_Income_Amount",
        "`cast23` as Related_Person_Beneficiary",
        "`cast24` as Related_Person_Dependent",
        "`cast25` as Related_Person_Emergency_Contact",
        "`cast26` as Related_Person_Full_Time_Student",
        "`cast27` as Related_Person_Has_Health_Insurance",
        "`cast28` as Related_Person_Is_Dependent_for_Payroll_Purposes",
        "`cast29` as Related_Person_Is_Disabled",
        "`relatedPerson`.`ns1:Legal_Name`.`ns1:Country` as Legal_Name_Country",
        "`relatedPerson`.`ns1:Legal_Name`.`ns1:General_Display_Name` as Legal_Name_General_Display_Name",
        "`cast30` as Related_Person_Lives_with_Worker",
        "`relatedPerson`.`ns1:Relationship_Type` as Related_Person_Relationship_Type",
        "`cast31` as Related_Person_Relative",
        "`cast32` as Related_Person_Tobacco_Use",
        "`related_communication`.`ns1:Related_Person_ID` as Related_Person_Communication_Related_Person_ID",
        "`relatedNacionalID`.`ns1:Government_Identifier` as Government_Identifier",
        "`relatedNacionalID`.`ns1:Other_Identifier`.`ns1:Custom_ID` as Other_Identifier_Custom_ID",
        "`ns:Employees`.`ns1:Summary`.`ns1:Name` as Summary_Name",
        "`cast33` as Worker_Status_Active",
        "`ns:Employees`.`ns1:Worker_Status`.`ns1:Continuous_Service_Date` as Worker_Status_Continuous_Service_Date",
        "`cast34` as Worker_Status_Hire_Rescinded",
        "`cast35` as Worker_Status_Is_Rehire",
        "`cast36` as Worker_Status_Not_Returning",
        "`cast37` as Worker_Status_Regrettable_Termination",
        "`cast38` as Worker_Status_Return_Unknown",
        "`ns:Employees`.`ns1:Worker_Status`.`ns1:Secondary_Termination_Reason` as Worker_Status_Secondary_Termination_Reason",
        "`ns:Employees`.`ns1:Worker_Status`.`ns1:Status` as Worker_Status_Status",
        "`cast39` as Worker_Status_Terminated",
        "`ns:Employees`.`ns1:Worker_Status`.`ns1:Worker_Reference_Type` as Worker_Status_Worker_Reference_Type",
        s"'$dataDatePart' as data_date_part"
      ).na.fill(" ").distinct()

    df_WeciFinal.show()
    df_WeciFinal.printSchema()


  }
}
