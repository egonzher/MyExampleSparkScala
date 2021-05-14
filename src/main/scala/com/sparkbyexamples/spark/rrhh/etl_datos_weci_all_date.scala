package com.sparkbyexamples.spark.rrhh

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, explode, explode_outer}

object etl_datos_weci_all_date {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local[1]")
      .appName("SparkByExample")
      .getOrCreate()


    //Trabajando con la nueva integración de cvdatalake_worker
    val dataDatePart = "2021-04-26"
    //val dataDatePart = "2021-04-27"
    //val dataDatePart = "2021-04-28"
    //val dataDatePart = "2021-04-29"
    //val dataDatePart = "2021-05-04"
    //val dataDatePart = "2021-05-06"
    val rutaCvDatalakeWorker = s"src/main/resources/rrhh/example_weci_consolid_wd/data_date_part=$dataDatePart/*.xml"


    val df_WeciReadXML = spark.read
      .format("com.databricks.spark.xml")
      .option("excludeAttribute", "false")
      .option("rowTag", "ns1:EMPLOYEE_DELTA_INTEGRATION")
      .load(rutaCvDatalakeWorker)

    println("Imprimiendo el esquema de df_WeciReadXML")
    df_WeciReadXML.printSchema()

    //////////////////////////////////////////////////////////////////////////////

    /*

    df_WeciFinal = df_WeciReadXML
      .withColumn("natioId", explode_outer(col("`ns:Employees`.`ns1:Person_Identification`.`ns1:National_Identifier`")))
      .withColumn("relatedPerson", explode_outer(col("`ns:Employees`.`ns1:Related_Person`")))
      .withColumn("relatedNacionalID", explode_outer(col("`ns:Employees`.`ns1:Related_Person_Identification`")))
      .withColumn("allowance_plan", explode_outer(col("`ns:Employees`.`ns1:Compensation_Plans`.`ns1:Allowance_Plan`")))
      .withColumn("job_family", explode_outer(col("`ns:Employees`.`ns1:Position`.`ns1:Job_Family`")))
      .withColumn("related_communication", explode_outer(col("`ns:Employees`.`ns1:Related_Person_Communication`")))
      .withColumn("PostalCast",col("`ns:Employees`.`ns1:Position`.`ns1:Business_Site`.`ns1:Postal_Code`").cast("String"))
      .withColumn("SuperCast",col("`ns:Employees`.`ns1:Position`.`ns1:Supervisor`.`ns1:ID`").cast("String"))
      .withColumn("IDCast",col("`ns:Employees`.`ns1:Summary`.`ns1:Employee_ID`").cast("String"))
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
            // root
            "`ns:Employees`.`ns1:Additional_Information`.`ns1:Company` as Additional_Information_Company",
            "`ns:Employees`.`ns1:Collective_Agreement`.`ns1:Collective_Agreement` as Collective_Agreement",
            "`ns:Employees`.`ns1:Collective_Agreement`.`ns1:Collective_Agreement_Factor`.`ns1:Factor` as Collective_Agreement_Factor",
            "`ns:Employees`.`ns1:Collective_Agreement`.`ns1:Position_ID` as Collective_Agreement_Position_ID",
            "`ns:Employees`.`ns1:Collective_Agreement`.`ns1:Start_Date` as Collective_Agreement_Start_Date",
            "`ns:Employees`.`ns1:Compensation`.`ns1:Compensation_Change_Reason` as Compensation_Change_Reason",
            "`ns:Employees`.`ns1:Compensation`.`ns1:Compensation_Grade` as Compensation_Grade",
            "`ns:Employees`.`ns1:Compensation`.`ns1:Compensation_Summary_Based_on_Compensation_Grade`.`ns1:Currency` as Compensation_Summary_Based_on_Compensation_Grade_Currency",
            "`ns:Employees`.`ns1:Compensation`.`ns1:Compensation_Summary_Based_on_Compensation_Grade`.`ns1:Frequency` as Compensation_Summary_Based_on_Compensation_Grade_Frequency",
            "`cast1` as Compensation_Summary_Based_on_Compensation_Grade_Primary_Compensation_Basis",
            "`cast2` as Compensation_Summary_Based_on_Compensation_Grade_Total_Base_Pay",
            "`ns:Employees`.`ns1:Compensation`.`ns1:Compensation_Summary_in_Annualized_Frequency`.`ns1:Currency` as Compensation_Summary_in_Annualized_Frequency_Currency",
            "`ns:Employees`.`ns1:Compensation`.`ns1:Compensation_Summary_in_Annualized_Frequency`.`ns1:Frequency` as Compensation_Summary_in_Annualized_Frequency_Frequency",
            "`cast3` as Compensation_Summary_in_Annualized_Frequency_Primary_Compensation_Basis",
            "`cast4` as Compensation_Summary_in_Annualized_Frequency_Primary_Total_Base_Pay",
            "`ns:Employees`.`ns1:Compensation`.`ns1:Position_ID` as Compensation_Position_ID",
            "`cast5` as Allowance_Plan_Amount",
            "`cast6` as Allowance_Plan_Apply_FTE",
            "`allowance_plan`.`ns1:Compensation_Plan` as Allowance_Plan_Compensation_Plan",
            "`allowance_plan`.`ns1:Currency` as Allowance_Plan_Currency",
            "`allowance_plan`.`ns1:Frequency` as Allowance_Plan_Frequency",
            "`allowance_plan`.`ns1:Start_Date` as Allowance_Plan_Start_Date",
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
            "`ns:Employees`.`ns1:Employee_Contract`.`ns1:Contract_ID` as Contract_ID",
            "`ns:Employees`.`ns1:Employee_Contract`.`ns1:Contract_Status` as Employee_Contract_Contract_Status",
            "`ns:Employees`.`ns1:Employee_Contract`.`ns1:Contract_Type` as Contract_Type",
            "`ns:Employees`.`ns1:Employee_Contract`.`ns1:Description` as Employee_Contract_Description",
            "`cast13` as Employee_Contract_Maximum_Weekly_Hours",
            "`cast14` as Employee_Contract_Minimum_Weekly_Hours",
            "`ns:Employees`.`ns1:Employee_Contract`.`ns1:Position_ID` as Employee_Contract_Position_ID",
            "`ns:Employees`.`ns1:Employee_Contract`.`ns1:Start_Date` as Employee_Contract_Start_Date",
            "`ns:Employees`.`ns1:Entry_Moment` as Entry_Moment",
            "`ns:Employees`.`ns1:Person_Communication`.`ns1:Email` as Person_Communication_Email",
            "`ns:Employees`.`ns1:Person_Communication`.`ns1:Phone` as Person_Communication_Phone",
            "`natioId`.`ns1:Country` as National_Identifier_Country",
            "`natioId`.`ns1:National_ID` as National_Identifier_National_ID",
            "`natioId`.`ns1:National_ID_Type` as National_Identifier_National_ID_Type",
            "`natioId`.`ns1:Verification_Date` as National_Identifier_Verification_Date",
            "`cast15` as National_Identifier_Verified_By",
            "`relatedNacionalID`.`ns1:Other_Identifier`.`ns1:Custom_ID_Type` as Other_Identifier_Custom_ID_Type",
            "`ns:Employees`.`ns1:Person_Identification`.`ns1:Passport` as Person_Identification_Passport",
            "`ns:Employees`.`ns1:Person_Identification`.`ns1:Visa` as Person_Identification_Visa",
            "`ns:Employees`.`ns1:Personal`.`ns1:City_of_Birth` as Personal_City_of_Birth",
            "`ns:Employees`.`ns1:Personal`.`ns1:Country_of_Birth` as Personal_Country_of_Birth",
            "`ns:Employees`.`ns1:Personal`.`ns1:Date_of_Birth` as Personal_Date_of_Birth",
            "`ns:Employees`.`ns1:Personal`.`ns1:Disability_Status` as Personal_Disability_Status",
            "`ns:Employees`.`ns1:Personal`.`ns1:Gender` as Personal_Gender",
            "`cast16` as Personal_Local_Hukou",
            "`ns:Employees`.`ns1:Personal`.`ns1:Name`.`ns1:Country` as Personal_Name_Country",
            "`ns:Employees`.`ns1:Personal`.`ns1:Name`.`ns1:First_Name` as Personal_Name_First_Name",
            "`ns:Employees`.`ns1:Personal`.`ns1:Name`.`ns1:General_Display_Name` as Personal_Name_General_Display_Name",
            "`ns:Employees`.`ns1:Personal`.`ns1:Name`.`ns1:Last_Name` as Personal_Name_Last_Name",
            "`ns:Employees`.`ns1:Personal`.`ns1:Name`.`ns1:Middle_Name` as Personal_Name_Middle_Name",
            "`ns:Employees`.`ns1:Personal`.`ns1:Name`.`ns1:Secondary_Last_Name` as Personal_Name_Secondary_Last_Name",
            "`ns:Employees`.`ns1:Personal`.`ns1:Name`.`ns1:Title` as Personal_Name_Title",
            "`ns:Employees`.`ns1:Personal`.`ns1:Nationality` as Personal_Nationality",
            "`cast17` as Personal_Number_of_Payroll_Dependents",
            "`ns:Employees`.`ns1:Personal`.`ns1:Preferred_Language` as Personal_Preferred_Language",
            "`ns:Employees`.`ns1:Personal`.`ns1:Preferred_Locale` as Personal_Preferred_Locale",
            "`cast18` as Personal_Uses_Tobacco",
            "`ns:Employees`.`ns1:Personal`.`ns1:Workday_Account` as Personal_Workday_Account",
            "`ns:Employees`.`ns1:Position`.`ns1:Business_Site`.`ns1:Address_ID` as Address_ID",
            "`ns:Employees`.`ns1:Position`.`ns1:Business_Site`.`ns1:Address_Line_1` as Address_Line_1",
            "`ns:Employees`.`ns1:Position`.`ns1:Business_Site`.`ns1:Address_Line_3` as Address_Line_3",
            "`ns:Employees`.`ns1:Position`.`ns1:Business_Site`.`ns1:City` as Business_Site_City",
            "`ns:Employees`.`ns1:Position`.`ns1:Business_Site`.`ns1:Country` as Business_Site_Country",
            "`ns:Employees`.`ns1:Position`.`ns1:Business_Site`.`ns1:Location_ID` as Business_Site_Location_ID",
            "`ns:Employees`.`ns1:Position`.`ns1:Business_Site`.`ns1:Location_Name` as Business_Site_Location_Name",
            "`PostalCast` as PostalCast",
            "`ns:Employees`.`ns1:Position`.`ns1:Job_Classification`.`ns1:Job_Classification_Description` as Job_Classification_Description",
            "`ns:Employees`.`ns1:Position`.`ns1:Job_Classification`.`ns1:Job_Classification_Group` as Job_Classification_Group",
            "`cast19` as Job_Classification_ID",
            "`ns:Employees`.`ns1:Position`.`ns1:Job_Classification`.`ns1:Mapped_Value` as Job_Classification_Mapped_Value",
            "`cast20` as Job_Family_ID",
            "`job_family`.`ns1:Mapped_Value` as Job_Family_Mapped_Value",
            "`job_family`.`ns1:Name` as Job_Family_Name",
            "`ns:Employees`.`ns1:Position`.`ns1:Job_Profile` as Position_Job_Profile",
            "`ns:Employees`.`ns1:Position`.`ns1:Management_Level` as Position_Management_Level",
            "`ns:Employees`.`ns1:Position`.`ns1:Position_ID` as Position_Position_ID",
            "`ns:Employees`.`ns1:Position`.`ns1:Position_Time_Type` as Position_Time_Type",
            "`SuperCast` as Supervisor_ID",
            "`ns:Employees`.`ns1:Position`.`ns1:Supervisor`.`ns1:Name` as Supervisor_Name",
            "`ns:Employees`.`ns1:Position`.`ns1:Supervisor`.`ns1:Other_ID` as Supervisor_Other_ID",
            "`ns:Employees`.`ns1:Qualifications`.`ns1:Certification_Achievement` as Qualifications_Certification_Achievement",
            "`cast21` as Related_Person_Allowed_for_Tax_Deduction",
            "`cast22` as Related_Person_Annual_Income_Amount",
            "`cast23` as Related_Person_Beneficiary",
            "`relatedPerson`.`ns1:Birth_Date` as Related_Person_Birth_Date",
            "`cast24` as Related_Person_Dependent",
            "`relatedPerson`.`ns1:Dependent_ID` as Related_Person_Dependent_ID",
            "`cast25` as Related_Person_Emergency_Contact",
            "`cast26` as Related_Person_Full_Time_Student",
            "`relatedPerson`.`ns1:Gender` as Related_Person_Gender",
            "`cast27` as Related_Person_Has_Health_Insurance",
            "`cast28` as Related_Person_Is_Dependent_for_Payroll_Purposes",
            "`cast29` as Related_Person_Is_Disabled",
            "`relatedPerson`.`ns1:Legal_Name`.`ns1:Country` as Legal_Name_Country",
            "`relatedPerson`.`ns1:Legal_Name`.`ns1:First_Name` as Legal_Name_First_Name",
            "`relatedPerson`.`ns1:Legal_Name`.`ns1:General_Display_Name` as Legal_Name_General_Display_Name",
            "`relatedPerson`.`ns1:Legal_Name`.`ns1:Last_Name` as Legal_Name_Last_Name",
            "`relatedPerson`.`ns1:Legal_Name`.`ns1:Secondary_Last_Name` as Legal_Name_Secondary_Last_Name",
            "`cast30` as Related_Person_Lives_with_Worker",
            "`relatedPerson`.`ns1:Related_Person_ID` as Related_Person_ID",
            "`relatedPerson`.`ns1:Relationship_Type` as Related_Person_Relationship_Type",
            "`cast31` as Related_Person_Relative",
            "`cast32` as Related_Person_Tobacco_Use",
            "`related_communication`.`ns1:Related_Person_ID` as Related_Person_Communication_ID",
            "`relatedNacionalID`.`ns1:Government_Identifier` as Government_Identifier",
            "`relatedNacionalID`.`ns1:National_Identifier`.`ns1:Country` as Related_Person_Identification_Country",
            "`relatedNacionalID`.`ns1:National_Identifier`.`ns1:National_ID` as Related_Person_Identification_ID",
            "`relatedNacionalID`.`ns1:National_Identifier`.`ns1:National_ID_Type` as Related_Person_Identification_National_ID_Type",
            "`relatedNacionalID`.`ns1:Other_Identifier`.`ns1:Custom_ID` as Related_Person_Identification_Custom_ID",
            "`relatedNacionalID`.`ns1:Other_Identifier`.`ns1:Custom_ID_Type` as Related_Person_Identification_Custom_ID_Type",
            "`IDCast` as Employee_ID",
            "`ns:Employees`.`ns1:Summary`.`ns1:Name` as Summary_Name",
            "`ns:Employees`.`ns1:Summary`.`ns1:WID` as WID",
            "`cast33` as Worker_Status_Active",
            "`ns:Employees`.`ns1:Worker_Status`.`ns1:Active_Status_Date` as Worker_Status_Active_Status_Date",
            "`ns:Employees`.`ns1:Worker_Status`.`ns1:Continuous_Service_Date` as Worker_Status_Continuous_Service_Date",
            "`ns:Employees`.`ns1:Worker_Status`.`ns1:First_Day_of_Work` as Worker_Status_First_Day_of_Work",
            "`ns:Employees`.`ns1:Worker_Status`.`ns1:Hire_Date` as Worker_Status_Hire_Date",
            "`cast34` as Worker_Status_Hire_Rescinded",
            "`cast35` as Worker_Status_Is_Rehire",
            "`cast36` as Worker_Status_Not_Returning",
            "`ns:Employees`.`ns1:Worker_Status`.`ns1:Original_Hire_Date` as Worker_Status_Original_Hire_Date",
            "`cast37` as Worker_Status_Regrettable_Termination",
            "`cast38` as Worker_Status_Return_Unknown",
            "`ns:Employees`.`ns1:Worker_Status`.`ns1:Secondary_Termination_Reason` as Worker_Status_Secondary_Termination_Reason",
            "`ns:Employees`.`ns1:Worker_Status`.`ns1:Seniority_Date` as Worker_Status_Seniority_Date",
            "`ns:Employees`.`ns1:Worker_Status`.`ns1:Status` as Worker_Status_Status",
            "`cast39` as Worker_Status_Terminated",
            "`ns:Employees`.`ns1:Worker_Status`.`ns1:Worker_Reference_Type` as Worker_Status_Worker_Reference_Type",
            s"'$dataDatePart' as data_date_part"
      ).na.fill(" ").distinct()

     */

    //////////////////////////////////////////////////////////////////////////////

    var df_WeciFinal = df_WeciReadXML

    if (dataDatePart == "2021-04-26"){

      df_WeciFinal = df_WeciReadXML
        .withColumn("natioId", col("`ns:Employees`.`ns1:Person_Identification`.`ns1:National_Identifier`"))
        .withColumn("relatedPerson", col("`ns:Employees`.`ns1:Related_Person`"))
        .withColumn("relatedNacionalID", col("`ns:Employees`.`ns1:Related_Person_Identification`"))
        .withColumn("allowance_plan", col("`ns:Employees`.`ns1:Compensation_Plans`.`ns1:Allowance_Plan`"))
        //.withColumn("job_family", explode_outer(col("`ns:Employees`.`ns1:Position`.`ns1:Job_Family`")))
        //.withColumn("related_communication", explode_outer(col("`ns:Employees`.`ns1:Related_Person_Communication`")))
        //.withColumn("PostalCast",col("`ns:Employees`.`ns1:Position`.`ns1:Business_Site`.`ns1:Postal_Code`").cast("String"))
        //.withColumn("SuperCast",col("`ns:Employees`.`ns1:Position`.`ns1:Supervisor`.`ns1:ID`").cast("String"))
        .withColumn("IDCast",col("`ns:Employees`.`ns1:Summary`.`ns1:Employee_ID`").cast("String"))
        //.withColumn("cast1",col("`ns:Employees`.`ns1:Compensation`.`ns1:Compensation_Summary_Based_on_Compensation_Grade`.`ns1:Primary_Compensation_Basis`").cast("String"))
        //.withColumn("cast2",col("`ns:Employees`.`ns1:Compensation`.`ns1:Compensation_Summary_Based_on_Compensation_Grade`.`ns1:Total_Base_Pay`").cast("String"))
        //.withColumn("cast3",col("`ns:Employees`.`ns1:Compensation`.`ns1:Compensation_Summary_in_Annualized_Frequency`.`ns1:Primary_Compensation_Basis`").cast("String"))
        .withColumn("cast4",col("`ns:Employees`.`ns1:Compensation`.`ns1:Compensation_Summary_in_Annualized_Frequency`.`ns1:Total_Base_Pay`").cast("String"))
        .withColumn("cast5",col("`allowance_plan`.`ns1:Amount`").cast("String"))
        //.withColumn("cast6",col("`allowance_plan`.`ns1:Apply_FTE`").cast("String"))
        .withColumn("cast7",col("`ns:Employees`.`ns1:Compensation_Plans`.`ns1:Bonus_Plan`.`ns1:Amount`").cast("String"))
        //.withColumn("cast8",col("`ns:Employees`.`ns1:Compensation_Plans`.`ns1:Bonus_Plan`.`ns1:Prorated_Amount`").cast("String"))
        //.withColumn("cast9",col("`ns:Employees`.`ns1:Compensation_Plans`.`ns1:Salary_and_Hourly_Plan`.`ns1:Amount`").cast("String"))
        //.withColumn("cast11",col("`ns:Employees`.`ns1:Compensation_Plans`.`ns1:Salary_and_Hourly_Plan`.`ns1:Apply_FTE`").cast("String"))
        //.withColumn("cast12",col("`ns:Employees`.`ns1:Compensation_Plans`.`ns1:Salary_and_Hourly_Plan`.`ns1:Prorated_Amount`").cast("String"))
        //.withColumn("cast13",col("`ns:Employees`.`ns1:Employee_Contract`.`ns1:Maximum_Weekly_Hours`").cast("String"))
        //.withColumn("cast14",col("`ns:Employees`.`ns1:Employee_Contract`.`ns1:Minimum_Weekly_Hours`").cast("String"))
        //.withColumn("cast15",col("`natioId`.`ns1:Verified_By`").cast("String"))
        //.withColumn("cast16",col("`ns:Employees`.`ns1:Personal`.`ns1:Local_Hukou`").cast("String"))
        //.withColumn("cast17",col("`ns:Employees`.`ns1:Personal`.`ns1:Number_of_Payroll_Dependents`").cast("String"))
        //.withColumn("cast18",col("`ns:Employees`.`ns1:Personal`.`ns1:Uses_Tobacco`").cast("String"))
        //.withColumn("cast19",col("`ns:Employees`.`ns1:Position`.`ns1:Job_Classification`.`ns1:Job_Classification_ID`").cast("String"))
        //.withColumn("cast20",col("`job_family`.`ns1:ID`").cast("String"))
        //.withColumn("cast21",col("`relatedPerson`.`ns1:Allowed_for_Tax_Deduction`").cast("String"))
        //.withColumn("cast22",col("`relatedPerson`.`ns1:Annual_Income_Amount`").cast("String"))
        //.withColumn("cast23",col("`relatedPerson`.`ns1:Beneficiary`").cast("String"))
        //.withColumn("cast24",col("`relatedPerson`.`ns1:Dependent`").cast("String"))
        //.withColumn("cast25",col("`relatedPerson`.`ns1:Emergency_Contact`").cast("String"))
        //.withColumn("cast26",col("`relatedPerson`.`ns1:Full_Time_Student`").cast("String"))
        //.withColumn("cast27",col("`relatedPerson`.`ns1:Has_Health_Insurance`").cast("String"))
        //.withColumn("cast28",col("`relatedPerson`.`ns1:Is_Dependent_for_Payroll_Purposes`").cast("String"))
        //.withColumn("cast29",col("`relatedPerson`.`ns1:Is_Disabled`").cast("String"))
        //.withColumn("cast30",col("`relatedPerson`.`ns1:Lives_with_Worker`").cast("String"))
        //.withColumn("cast31",col("`relatedPerson`.`ns1:Relative`").cast("String"))
        //.withColumn("cast32",col("`relatedPerson`.`ns1:Tobacco_Use`").cast("String"))
        .withColumn("cast33",col("`ns:Employees`.`ns1:Worker_Status`.`ns1:Active`").cast("String"))
        //.withColumn("cast34",col("`ns:Employees`.`ns1:Worker_Status`.`ns1:Hire_Rescinded`").cast("String"))
        //.withColumn("cast35",col("`ns:Employees`.`ns1:Worker_Status`.`ns1:Is_Rehire`").cast("String"))
        //.withColumn("cast36",col("`ns:Employees`.`ns1:Worker_Status`.`ns1:Not_Returning`").cast("String"))
        //.withColumn("cast37",col("`ns:Employees`.`ns1:Worker_Status`.`ns1:Regrettable_Termination`").cast("String"))
        //.withColumn("cast38",col("`ns:Employees`.`ns1:Worker_Status`.`ns1:Return_Unknown`").cast("String"))
        //.withColumn("cast39",col("`ns:Employees`.`ns1:Worker_Status`.`ns1:Terminated`").cast("String"))
        .selectExpr(
          // root

          //"`ns:Employees`.`ns1:Additional_Information`.`ns1:Company` as Additional_Information_Company",
          "'' as Additional_Information_Company",

          "`ns:Employees`.`ns1:Collective_Agreement`.`ns1:Collective_Agreement` as Collective_Agreement",
          "`ns:Employees`.`ns1:Collective_Agreement`.`ns1:Collective_Agreement_Factor`.`ns1:Factor` as Collective_Agreement_Factor",

          //"`ns:Employees`.`ns1:Collective_Agreement`.`ns1:Position_ID` as Collective_Agreement_Position_ID",
          "'' as Collective_Agreement_Position_ID",

          //"`ns:Employees`.`ns1:Collective_Agreement`.`ns1:Start_Date` as Collective_Agreement_Start_Date",
          "'' as Collective_Agreement_Start_Date",

          //"`ns:Employees`.`ns1:Compensation`.`ns1:Compensation_Change_Reason` as Compensation_Change_Reason",
          "'' as Compensation_Change_Reason",

          "`ns:Employees`.`ns1:Compensation`.`ns1:Compensation_Grade` as Compensation_Grade",

          //"`ns:Employees`.`ns1:Compensation`.`ns1:Compensation_Summary_Based_on_Compensation_Grade`.`ns1:Currency` as Compensation_Summary_Based_on_Compensation_Grade_Currency",
          "'' as Compensation_Summary_Based_on_Compensation_Grade_Currency",

          //"`ns:Employees`.`ns1:Compensation`.`ns1:Compensation_Summary_Based_on_Compensation_Grade`.`ns1:Frequency` as Compensation_Summary_Based_on_Compensation_Grade_Frequency",
          "'' as Compensation_Summary_Based_on_Compensation_Grade_Frequency",

          //"`cast1` as Compensation_Summary_Based_on_Compensation_Grade_Primary_Compensation_Basis",
          "'' as Compensation_Summary_Based_on_Compensation_Grade_Primary_Compensation_Basis",

          //"`cast2` as Compensation_Summary_Based_on_Compensation_Grade_Total_Base_Pay",
          "'' as Compensation_Summary_Based_on_Compensation_Grade_Total_Base_Pay",

          //"`ns:Employees`.`ns1:Compensation`.`ns1:Compensation_Summary_in_Annualized_Frequency`.`ns1:Currency` as Compensation_Summary_in_Annualized_Frequency_Currency",
          "'' as Compensation_Summary_in_Annualized_Frequency_Currency",

          //"`ns:Employees`.`ns1:Compensation`.`ns1:Compensation_Summary_in_Annualized_Frequency`.`ns1:Frequency` as Compensation_Summary_in_Annualized_Frequency_Frequency",
          "'' as Compensation_Summary_in_Annualized_Frequency_Frequency",

          //"`cast3` as Compensation_Summary_in_Annualized_Frequency_Primary_Compensation_Basis",
          "'' as Compensation_Summary_in_Annualized_Frequency_Primary_Compensation_Basis",

          "`cast4` as Compensation_Summary_in_Annualized_Frequency_Primary_Total_Base_Pay",

          //"`ns:Employees`.`ns1:Compensation`.`ns1:Position_ID` as Compensation_Position_ID",
          "'' as Compensation_Position_ID",

          "`cast5` as Allowance_Plan_Amount",

          //"`cast6` as Allowance_Plan_Apply_FTE",
          "'' as Allowance_Plan_Apply_FTE",

          "`allowance_plan`.`ns1:Compensation_Plan` as Allowance_Plan_Compensation_Plan",
          "`allowance_plan`.`ns1:Currency` as Allowance_Plan_Currency",

          //"`allowance_plan`.`ns1:Frequency` as Allowance_Plan_Frequency",
          "'' as Allowance_Plan_Frequency",

          "`allowance_plan`.`ns1:Start_Date` as Allowance_Plan_Start_Date",
          "`cast7` as Compensation_Plans_Amount",
          "`ns:Employees`.`ns1:Compensation_Plans`.`ns1:Bonus_Plan`.`ns1:Compensation_Plan` as Compensation_Plans_Compensation_Plan",
          "`ns:Employees`.`ns1:Compensation_Plans`.`ns1:Bonus_Plan`.`ns1:Currency` as Compensation_Plans_Currency",
          "`ns:Employees`.`ns1:Compensation_Plans`.`ns1:Bonus_Plan`.`ns1:Frequency` as Compensation_Plans_Frequency",

          //"`cast8` as Compensation_Plans_Prorated_Amount",
          "'' as Compensation_Plans_Prorated_Amount",

          "`ns:Employees`.`ns1:Compensation_Plans`.`ns1:Bonus_Plan`.`ns1:Start_Date` as Compensation_Plans_Start_Date",

          //"`ns:Employees`.`ns1:Compensation_Plans`.`ns1:Position_ID` as Compensation_Plans_Position_ID",
          "'' as Compensation_Plans_Position_ID",

          //"`cast9` as Salary_and_Hourly_Plan_Amount",
          "'' as Salary_and_Hourly_Plan_Amount",

          //"`cast11` as Salary_and_Hourly_Plan_Apply_FTE",
          "'' as Salary_and_Hourly_Plan_Apply_FTE",

          //"`ns:Employees`.`ns1:Compensation_Plans`.`ns1:Salary_and_Hourly_Plan`.`ns1:Compensation_Plan` as Salary_and_Hourly_Plan_Compensation_Plan",
          "'' as Salary_and_Hourly_Plan_Compensation_Plan",

          //"`ns:Employees`.`ns1:Compensation_Plans`.`ns1:Salary_and_Hourly_Plan`.`ns1:Currency` as Salary_and_Hourly_Plan_Currency",
          "'' as Salary_and_Hourly_Plan_Currency",

          //"`ns:Employees`.`ns1:Compensation_Plans`.`ns1:Salary_and_Hourly_Plan`.`ns1:Frequency` as Salary_and_Hourly_Plan_Frequency",
          "'' as Salary_and_Hourly_Plan_Frequency",

          //"`cast12` as Salary_and_Hourly_Plan_Prorated_Amount",
          "'' as Salary_and_Hourly_Plan_Prorated_Amount",

          //"`ns:Employees`.`ns1:Compensation_Plans`.`ns1:Salary_and_Hourly_Plan`.`ns1:Start_Date` as Salary_and_Hourly_Plan_Start_Date",
          "'' as Salary_and_Hourly_Plan_Start_Date",

          //"`ns:Employees`.`ns1:Derived_Event_Code` as Derived_Event_Code",
          "'' as Derived_Event_Code",

          //"`ns:Employees`.`ns1:Effective_Moment` as Effective_Moment",
          "'' as Effective_Moment",

          //"`ns:Employees`.`ns1:Employee_Contract`.`ns1:Collective_Agreement` as Employee_Contract_Collective_Agreement",
          "'' as Employee_Contract_Collective_Agreement",

          //"`ns:Employees`.`ns1:Employee_Contract`.`ns1:Contract_ID` as Contract_ID",
          "'' as Contract_ID",

          "`ns:Employees`.`ns1:Employee_Contract`.`ns1:Contract_Status` as Employee_Contract_Contract_Status",
          "`ns:Employees`.`ns1:Employee_Contract`.`ns1:Contract_Type` as Contract_Type",

          //"`ns:Employees`.`ns1:Employee_Contract`.`ns1:Description` as Employee_Contract_Description",
          "'' as Employee_Contract_Description",

          //"`cast13` as Employee_Contract_Maximum_Weekly_Hours",
          "'' as Employee_Contract_Maximum_Weekly_Hours",

          //"`cast14` as Employee_Contract_Minimum_Weekly_Hours",
          "'' as Employee_Contract_Minimum_Weekly_Hours",

          "`ns:Employees`.`ns1:Employee_Contract`.`ns1:Position_ID` as Employee_Contract_Position_ID",
          "`ns:Employees`.`ns1:Employee_Contract`.`ns1:Start_Date` as Employee_Contract_Start_Date",
          "`ns:Employees`.`ns1:Entry_Moment` as Entry_Moment",
          "`ns:Employees`.`ns1:Person_Communication`.`ns1:Email` as Person_Communication_Email",
          "`ns:Employees`.`ns1:Person_Communication`.`ns1:Phone` as Person_Communication_Phone",
          "`natioId`.`ns1:Country` as National_Identifier_Country",
          "`natioId`.`ns1:National_ID` as National_Identifier_National_ID",
          "`natioId`.`ns1:National_ID_Type` as National_Identifier_National_ID_Type",
          "`natioId`.`ns1:Verification_Date` as National_Identifier_Verification_Date",

          //"`cast15` as National_Identifier_Verified_By",
          "'' as National_Identifier_Verified_By",

          "`relatedNacionalID`.`ns1:Other_Identifier`.`ns1:Custom_ID_Type` as Other_Identifier_Custom_ID_Type",
          "`ns:Employees`.`ns1:Person_Identification`.`ns1:Passport` as Person_Identification_Passport",
          "`ns:Employees`.`ns1:Person_Identification`.`ns1:Visa` as Person_Identification_Visa",
          "`ns:Employees`.`ns1:Personal`.`ns1:City_of_Birth` as Personal_City_of_Birth",
          "`ns:Employees`.`ns1:Personal`.`ns1:Country_of_Birth` as Personal_Country_of_Birth",
          "`ns:Employees`.`ns1:Personal`.`ns1:Date_of_Birth` as Personal_Date_of_Birth",
          "`ns:Employees`.`ns1:Personal`.`ns1:Disability_Status` as Personal_Disability_Status",
          "`ns:Employees`.`ns1:Personal`.`ns1:Gender` as Personal_Gender",

          //"`cast16` as Personal_Local_Hukou",
          "'' as Personal_Local_Hukou",

          "`ns:Employees`.`ns1:Personal`.`ns1:Name`.`ns1:Country` as Personal_Name_Country",
          "`ns:Employees`.`ns1:Personal`.`ns1:Name`.`ns1:First_Name` as Personal_Name_First_Name",
          "`ns:Employees`.`ns1:Personal`.`ns1:Name`.`ns1:General_Display_Name` as Personal_Name_General_Display_Name",
          "`ns:Employees`.`ns1:Personal`.`ns1:Name`.`ns1:Last_Name` as Personal_Name_Last_Name",
          "`ns:Employees`.`ns1:Personal`.`ns1:Name`.`ns1:Middle_Name` as Personal_Name_Middle_Name",
          "`ns:Employees`.`ns1:Personal`.`ns1:Name`.`ns1:Secondary_Last_Name` as Personal_Name_Secondary_Last_Name",
          "`ns:Employees`.`ns1:Personal`.`ns1:Name`.`ns1:Title` as Personal_Name_Title",
          "`ns:Employees`.`ns1:Personal`.`ns1:Nationality` as Personal_Nationality",

          //"`cast17` as Personal_Number_of_Payroll_Dependents",
          "'' as Personal_Number_of_Payroll_Dependents",

          "`ns:Employees`.`ns1:Personal`.`ns1:Preferred_Language` as Personal_Preferred_Language",
          "`ns:Employees`.`ns1:Personal`.`ns1:Preferred_Locale` as Personal_Preferred_Locale",

          //"`cast18` as Personal_Uses_Tobacco",
          "'' as Personal_Uses_Tobacco",

          "`ns:Employees`.`ns1:Personal`.`ns1:Workday_Account` as Personal_Workday_Account",
          "`ns:Employees`.`ns1:Position`.`ns1:Business_Site`.`ns1:Address_ID` as Address_ID",
          "`ns:Employees`.`ns1:Position`.`ns1:Business_Site`.`ns1:Address_Line_1` as Address_Line_1",
          "`ns:Employees`.`ns1:Position`.`ns1:Business_Site`.`ns1:Address_Line_3` as Address_Line_3",
          "`ns:Employees`.`ns1:Position`.`ns1:Business_Site`.`ns1:City` as Business_Site_City",
          "`ns:Employees`.`ns1:Position`.`ns1:Business_Site`.`ns1:Country` as Business_Site_Country",
          "`ns:Employees`.`ns1:Position`.`ns1:Business_Site`.`ns1:Location_ID` as Business_Site_Location_ID",
          "`ns:Employees`.`ns1:Position`.`ns1:Business_Site`.`ns1:Location_Name` as Business_Site_Location_Name",

          //"`PostalCast` as PostalCast",
          "'' as PostalCast",

          "`ns:Employees`.`ns1:Position`.`ns1:Job_Classification`.`ns1:Job_Classification_Description` as Job_Classification_Description",
          "`ns:Employees`.`ns1:Position`.`ns1:Job_Classification`.`ns1:Job_Classification_Group` as Job_Classification_Group",

          //"`cast19` as Job_Classification_ID",
          "'' as Job_Classification_ID",

          "`ns:Employees`.`ns1:Position`.`ns1:Job_Classification`.`ns1:Mapped_Value` as Job_Classification_Mapped_Value",

          //"`cast20` as Job_Family_ID",
          "'' as Job_Family_ID",

          //"`job_family`.`ns1:Mapped_Value` as Job_Family_Mapped_Value",
          "'' as Job_Family_Mapped_Value",

          //"`job_family`.`ns1:Name` as Job_Family_Name",
          "'' as Job_Family_Name",

          "`ns:Employees`.`ns1:Position`.`ns1:Job_Profile` as Position_Job_Profile",
          "`ns:Employees`.`ns1:Position`.`ns1:Management_Level` as Position_Management_Level",
          "`ns:Employees`.`ns1:Position`.`ns1:Position_ID` as Position_Position_ID",
          "`ns:Employees`.`ns1:Position`.`ns1:Position_Time_Type` as Position_Time_Type",

          //"`SuperCast` as Supervisor_ID",
          "'' as Supervisor_ID",

          "`ns:Employees`.`ns1:Position`.`ns1:Supervisor`.`ns1:Name` as Supervisor_Name",
          "`ns:Employees`.`ns1:Position`.`ns1:Supervisor`.`ns1:Other_ID` as Supervisor_Other_ID",
          "`ns:Employees`.`ns1:Qualifications`.`ns1:Certification_Achievement` as Qualifications_Certification_Achievement",

          //"`cast21` as Related_Person_Allowed_for_Tax_Deduction",
          "'' as Related_Person_Allowed_for_Tax_Deduction",

          //"`cast22` as Related_Person_Annual_Income_Amount",
          "'' as Related_Person_Annual_Income_Amount",

          //"`cast23` as Related_Person_Beneficiary",
          "'' as Related_Person_Beneficiary",


          "`relatedPerson`.`ns1:Birth_Date` as Related_Person_Birth_Date",

          //"`cast24` as Related_Person_Dependent",
          "'' as Related_Person_Dependent",

          "`relatedPerson`.`ns1:Dependent_ID` as Related_Person_Dependent_ID",

          //"`cast25` as Related_Person_Emergency_Contact",
          "'' as Related_Person_Emergency_Contact",

          //"`cast26` as Related_Person_Full_Time_Student",
          "'' as Related_Person_Full_Time_Student",

          "`relatedPerson`.`ns1:Gender` as Related_Person_Gender",

          //"`cast27` as Related_Person_Has_Health_Insurance",
          "'' as Related_Person_Has_Health_Insurance",

          //"`cast28` as Related_Person_Is_Dependent_for_Payroll_Purposes",
          "'' as Related_Person_Is_Dependent_for_Payroll_Purposes",

          //"`cast29` as Related_Person_Is_Disabled",
          "'' as Related_Person_Is_Disabled",

          "`relatedPerson`.`ns1:Legal_Name`.`ns1:Country` as Legal_Name_Country",
          "`relatedPerson`.`ns1:Legal_Name`.`ns1:First_Name` as Legal_Name_First_Name",
          "`relatedPerson`.`ns1:Legal_Name`.`ns1:General_Display_Name` as Legal_Name_General_Display_Name",
          "`relatedPerson`.`ns1:Legal_Name`.`ns1:Last_Name` as Legal_Name_Last_Name",
          "`relatedPerson`.`ns1:Legal_Name`.`ns1:Secondary_Last_Name` as Legal_Name_Secondary_Last_Name",

          //"`cast30` as Related_Person_Lives_with_Worker",
          "'' as Related_Person_Lives_with_Worker",

          "`relatedPerson`.`ns1:Related_Person_ID` as Related_Person_ID",
          "`relatedPerson`.`ns1:Relationship_Type` as Related_Person_Relationship_Type",

          //"`cast31` as Related_Person_Relative",
          "'' as Related_Person_Relative",

          //"`cast32` as Related_Person_Tobacco_Use",
          "'' as Related_Person_Tobacco_Use",

          //"`related_communication`.`ns1:Related_Person_ID` as Related_Person_Communication_ID",
          "'' as Related_Person_Communication_ID",

          "`relatedNacionalID`.`ns1:Government_Identifier` as Government_Identifier",
          "`relatedNacionalID`.`ns1:National_Identifier`.`ns1:Country` as Related_Person_Identification_Country",
          "`relatedNacionalID`.`ns1:National_Identifier`.`ns1:National_ID` as Related_Person_Identification_ID",
          "`relatedNacionalID`.`ns1:National_Identifier`.`ns1:National_ID_Type` as Related_Person_Identification_National_ID_Type",
          "`relatedNacionalID`.`ns1:Other_Identifier`.`ns1:Custom_ID` as Related_Person_Identification_Custom_ID",
          "`relatedNacionalID`.`ns1:Other_Identifier`.`ns1:Custom_ID_Type` as Related_Person_Identification_Custom_ID_Type",
          "`IDCast` as Employee_ID",
          "`ns:Employees`.`ns1:Summary`.`ns1:Name` as Summary_Name",
          "`ns:Employees`.`ns1:Summary`.`ns1:WID` as WID",
          "`cast33` as Worker_Status_Active",
          "`ns:Employees`.`ns1:Worker_Status`.`ns1:Active_Status_Date` as Worker_Status_Active_Status_Date",
          "`ns:Employees`.`ns1:Worker_Status`.`ns1:Continuous_Service_Date` as Worker_Status_Continuous_Service_Date",
          "`ns:Employees`.`ns1:Worker_Status`.`ns1:First_Day_of_Work` as Worker_Status_First_Day_of_Work",
          "`ns:Employees`.`ns1:Worker_Status`.`ns1:Hire_Date` as Worker_Status_Hire_Date",

          //"`cast34` as Worker_Status_Hire_Rescinded",
          "'' as Worker_Status_Hire_Rescinded",

          //"`cast35` as Worker_Status_Is_Rehire",
          "'' as Worker_Status_Is_Rehire",

          //"`cast36` as Worker_Status_Not_Returning",
          "'' as Worker_Status_Not_Returning",

          "`ns:Employees`.`ns1:Worker_Status`.`ns1:Original_Hire_Date` as Worker_Status_Original_Hire_Date",

          //"`cast37` as Worker_Status_Regrettable_Termination",
          "'' as Worker_Status_Regrettable_Termination",

          //"`cast38` as Worker_Status_Return_Unknown",
          "'' as Worker_Status_Return_Unknown",

          "`ns:Employees`.`ns1:Worker_Status`.`ns1:Secondary_Termination_Reason` as Worker_Status_Secondary_Termination_Reason",
          "`ns:Employees`.`ns1:Worker_Status`.`ns1:Seniority_Date` as Worker_Status_Seniority_Date",
          "`ns:Employees`.`ns1:Worker_Status`.`ns1:Status` as Worker_Status_Status",

          //"`cast39` as Worker_Status_Terminated",
          "'' as Worker_Status_Terminated",

          "`ns:Employees`.`ns1:Worker_Status`.`ns1:Worker_Reference_Type` as Worker_Status_Worker_Reference_Type",
          s"'$dataDatePart' as data_date_part"
        ).na.fill(" ").distinct()

    }

    else if (dataDatePart == "2021-04-27"){

      df_WeciFinal = df_WeciReadXML
        .withColumn("natioId", explode_outer(col("`ns:Employees`.`ns1:Person_Identification`.`ns1:National_Identifier`")))
        .withColumn("relatedPerson", explode_outer(col("`ns:Employees`.`ns1:Related_Person`")))
        .withColumn("relatedNacionalID", (col("`ns:Employees`.`ns1:Related_Person_Identification`")))
        .withColumn("allowance_plan", explode_outer(col("`ns:Employees`.`ns1:Compensation_Plans`.`ns1:Allowance_Plan`")))
        // .withColumn("job_family", explode_outer(col("`ns:Employees`.`ns1:Position`.`ns1:Job_Family`")))
        // .withColumn("related_communication", explode_outer(col("`ns:Employees`.`ns1:Related_Person_Communication`")))
        // .withColumn("PostalCast",col("`ns:Employees`.`ns1:Position`.`ns1:Business_Site`.`ns1:Postal_Code`").cast("String"))
        // .withColumn("SuperCast",col("`ns:Employees`.`ns1:Position`.`ns1:Supervisor`.`ns1:ID`").cast("String"))
        .withColumn("IDCast",col("`ns:Employees`.`ns1:Summary`.`ns1:Employee_ID`").cast("String"))
        .withColumn("cast1",col("`ns:Employees`.`ns1:Compensation`.`ns1:Compensation_Summary_Based_on_Compensation_Grade`.`ns1:Primary_Compensation_Basis`").cast("String"))
        .withColumn("cast2",col("`ns:Employees`.`ns1:Compensation`.`ns1:Compensation_Summary_Based_on_Compensation_Grade`.`ns1:Total_Base_Pay`").cast("String"))
        .withColumn("cast3",col("`ns:Employees`.`ns1:Compensation`.`ns1:Compensation_Summary_in_Annualized_Frequency`.`ns1:Primary_Compensation_Basis`").cast("String"))
        .withColumn("cast4",col("`ns:Employees`.`ns1:Compensation`.`ns1:Compensation_Summary_in_Annualized_Frequency`.`ns1:Total_Base_Pay`").cast("String"))
        .withColumn("cast5",col("`allowance_plan`.`ns1:Amount`").cast("String"))
        //.withColumn("cast6",col("`allowance_plan`.`ns1:Apply_FTE`").cast("String"))
        .withColumn("cast7",col("`ns:Employees`.`ns1:Compensation_Plans`.`ns1:Bonus_Plan`.`ns1:Amount`").cast("String"))
        //.withColumn("cast8",col("`ns:Employees`.`ns1:Compensation_Plans`.`ns1:Bonus_Plan`.`ns1:Prorated_Amount`").cast("String"))
        .withColumn("cast9",col("`ns:Employees`.`ns1:Compensation_Plans`.`ns1:Salary_and_Hourly_Plan`.`ns1:Amount`").cast("String"))
        .withColumn("cast11",col("`ns:Employees`.`ns1:Compensation_Plans`.`ns1:Salary_and_Hourly_Plan`.`ns1:Apply_FTE`").cast("String"))
        .withColumn("cast12",col("`ns:Employees`.`ns1:Compensation_Plans`.`ns1:Salary_and_Hourly_Plan`.`ns1:Prorated_Amount`").cast("String"))
        //.withColumn("cast13",col("`ns:Employees`.`ns1:Employee_Contract`.`ns1:Maximum_Weekly_Hours`").cast("String"))
        //.withColumn("cast14",col("`ns:Employees`.`ns1:Employee_Contract`.`ns1:Minimum_Weekly_Hours`").cast("String"))
        //.withColumn("cast15",col("`natioId`.`ns1:Verified_By`").cast("String"))
        //.withColumn("cast16",col("`ns:Employees`.`ns1:Personal`.`ns1:Local_Hukou`").cast("String"))
        //.withColumn("cast17",col("`ns:Employees`.`ns1:Personal`.`ns1:Number_of_Payroll_Dependents`").cast("String"))
        //.withColumn("cast18",col("`ns:Employees`.`ns1:Personal`.`ns1:Uses_Tobacco`").cast("String"))
        //.withColumn("cast19",col("`ns:Employees`.`ns1:Position`.`ns1:Job_Classification`.`ns1:Job_Classification_ID`").cast("String"))
        //.withColumn("cast20",col("`job_family`.`ns1:ID`").cast("String"))
        //.withColumn("cast21",col("`relatedPerson`.`ns1:Allowed_for_Tax_Deduction`").cast("String"))
        //.withColumn("cast22",col("`relatedPerson`.`ns1:Annual_Income_Amount`").cast("String"))
        //.withColumn("cast23",col("`relatedPerson`.`ns1:Beneficiary`").cast("String"))
        //.withColumn("cast24",col("`relatedPerson`.`ns1:Dependent`").cast("String"))
        //.withColumn("cast25",col("`relatedPerson`.`ns1:Emergency_Contact`").cast("String"))
        //.withColumn("cast26",col("`relatedPerson`.`ns1:Full_Time_Student`").cast("String"))
        //.withColumn("cast27",col("`relatedPerson`.`ns1:Has_Health_Insurance`").cast("String"))
        //.withColumn("cast28",col("`relatedPerson`.`ns1:Is_Dependent_for_Payroll_Purposes`").cast("String"))
        //.withColumn("cast29",col("`relatedPerson`.`ns1:Is_Disabled`").cast("String"))
        //.withColumn("cast30",col("`relatedPerson`.`ns1:Lives_with_Worker`").cast("String"))
        //.withColumn("cast31",col("`relatedPerson`.`ns1:Relative`").cast("String"))
        //.withColumn("cast32",col("`relatedPerson`.`ns1:Tobacco_Use`").cast("String"))
        .withColumn("cast33",col("`ns:Employees`.`ns1:Worker_Status`.`ns1:Active`").cast("String"))
        //.withColumn("cast34",col("`ns:Employees`.`ns1:Worker_Status`.`ns1:Hire_Rescinded`").cast("String"))
        //.withColumn("cast35",col("`ns:Employees`.`ns1:Worker_Status`.`ns1:Is_Rehire`").cast("String"))
        //.withColumn("cast36",col("`ns:Employees`.`ns1:Worker_Status`.`ns1:Not_Returning`").cast("String"))
        //.withColumn("cast37",col("`ns:Employees`.`ns1:Worker_Status`.`ns1:Regrettable_Termination`").cast("String"))
        //.withColumn("cast38",col("`ns:Employees`.`ns1:Worker_Status`.`ns1:Return_Unknown`").cast("String"))
        //.withColumn("cast39",col("`ns:Employees`.`ns1:Worker_Status`.`ns1:Terminated`").cast("String"))
        .selectExpr(
          // root
          "'' as Additional_Information_Company",
          "`ns:Employees`.`ns1:Collective_Agreement`.`ns1:Collective_Agreement` as Collective_Agreement",
          "`ns:Employees`.`ns1:Collective_Agreement`.`ns1:Collective_Agreement_Factor`.`ns1:Factor` as Collective_Agreement_Factor",
          "'' as Collective_Agreement_Position_ID",
          "'' as Collective_Agreement_Start_Date",
          "'' as Compensation_Change_Reason",
          "`ns:Employees`.`ns1:Compensation`.`ns1:Compensation_Grade` as Compensation_Grade",
          "`ns:Employees`.`ns1:Compensation`.`ns1:Compensation_Summary_Based_on_Compensation_Grade`.`ns1:Currency` as Compensation_Summary_Based_on_Compensation_Grade_Currency",
          "`ns:Employees`.`ns1:Compensation`.`ns1:Compensation_Summary_Based_on_Compensation_Grade`.`ns1:Frequency` as Compensation_Summary_Based_on_Compensation_Grade_Frequency",
          "`cast1` as Compensation_Summary_Based_on_Compensation_Grade_Primary_Compensation_Basis",
          "`cast2` as Compensation_Summary_Based_on_Compensation_Grade_Total_Base_Pay",
          "`ns:Employees`.`ns1:Compensation`.`ns1:Compensation_Summary_in_Annualized_Frequency`.`ns1:Currency` as Compensation_Summary_in_Annualized_Frequency_Currency",
          "`ns:Employees`.`ns1:Compensation`.`ns1:Compensation_Summary_in_Annualized_Frequency`.`ns1:Frequency` as Compensation_Summary_in_Annualized_Frequency_Frequency",
          "`cast3` as Compensation_Summary_in_Annualized_Frequency_Primary_Compensation_Basis",
          "`cast4` as Compensation_Summary_in_Annualized_Frequency_Primary_Total_Base_Pay",
          "'' as Compensation_Position_ID",
          "`cast5` as Allowance_Plan_Amount",
          "'' as Allowance_Plan_Apply_FTE",
          "`allowance_plan`.`ns1:Compensation_Plan` as Allowance_Plan_Compensation_Plan",
          "`allowance_plan`.`ns1:Currency` as Allowance_Plan_Currency",
          "'' as Allowance_Plan_Frequency",
          "`allowance_plan`.`ns1:Start_Date` as Allowance_Plan_Start_Date",
          "`cast7` as Compensation_Plans_Amount",
          "`ns:Employees`.`ns1:Compensation_Plans`.`ns1:Bonus_Plan`.`ns1:Compensation_Plan` as Compensation_Plans_Compensation_Plan",
          "`ns:Employees`.`ns1:Compensation_Plans`.`ns1:Bonus_Plan`.`ns1:Currency` as Compensation_Plans_Currency",
          "`ns:Employees`.`ns1:Compensation_Plans`.`ns1:Bonus_Plan`.`ns1:Frequency` as Compensation_Plans_Frequency",
          "'' as Compensation_Plans_Prorated_Amount",
          "`ns:Employees`.`ns1:Compensation_Plans`.`ns1:Bonus_Plan`.`ns1:Start_Date` as Compensation_Plans_Start_Date",
          "'' as Compensation_Plans_Position_ID",
          "`cast9` as Salary_and_Hourly_Plan_Amount",
          "`cast11` as Salary_and_Hourly_Plan_Apply_FTE",
          "`ns:Employees`.`ns1:Compensation_Plans`.`ns1:Salary_and_Hourly_Plan`.`ns1:Compensation_Plan` as Salary_and_Hourly_Plan_Compensation_Plan",
          "`ns:Employees`.`ns1:Compensation_Plans`.`ns1:Salary_and_Hourly_Plan`.`ns1:Currency` as Salary_and_Hourly_Plan_Currency",
          "`ns:Employees`.`ns1:Compensation_Plans`.`ns1:Salary_and_Hourly_Plan`.`ns1:Frequency` as Salary_and_Hourly_Plan_Frequency",
          "`cast12` as Salary_and_Hourly_Plan_Prorated_Amount",
          "`ns:Employees`.`ns1:Compensation_Plans`.`ns1:Salary_and_Hourly_Plan`.`ns1:Start_Date` as Salary_and_Hourly_Plan_Start_Date",
          "'' as Derived_Event_Code",
          "'' as Effective_Moment",
          "'' as Employee_Contract_Collective_Agreement",
          "'' as Contract_ID",
          "`ns:Employees`.`ns1:Employee_Contract`.`ns1:Contract_Status` as Employee_Contract_Contract_Status",
          "`ns:Employees`.`ns1:Employee_Contract`.`ns1:Contract_Type` as Contract_Type",
          "'' as Employee_Contract_Description",
          "'' as Employee_Contract_Maximum_Weekly_Hours",
          "'' as Employee_Contract_Minimum_Weekly_Hours",
          "'' as Employee_Contract_Position_ID",
          "`ns:Employees`.`ns1:Employee_Contract`.`ns1:Start_Date` as Employee_Contract_Start_Date",
          "'' as Entry_Moment",
          "'' as Person_Communication_Email",
          "'' as Person_Communication_Phone",
          "`natioId`.`ns1:Country` as National_Identifier_Country",
          "`natioId`.`ns1:National_ID` as National_Identifier_National_ID",
          "`natioId`.`ns1:National_ID_Type` as National_Identifier_National_ID_Type",
          "'' as National_Identifier_Verification_Date",
          "'' as National_Identifier_Verified_By",
          "'' as Other_Identifier_Custom_ID_Type",
          "'' as Person_Identification_Passport",
          "'' as Person_Identification_Visa",
          "`ns:Employees`.`ns1:Personal`.`ns1:City_of_Birth` as Personal_City_of_Birth",
          "`ns:Employees`.`ns1:Personal`.`ns1:Country_of_Birth` as Personal_Country_of_Birth",
          "`ns:Employees`.`ns1:Personal`.`ns1:Date_of_Birth` as Personal_Date_of_Birth",
          "'' as Personal_Disability_Status",
          "`ns:Employees`.`ns1:Personal`.`ns1:Gender` as Personal_Gender",
          "'' as Personal_Local_Hukou",
          "'' as Personal_Name_Country",
          "`ns:Employees`.`ns1:Personal`.`ns1:Name`.`ns1:First_Name` as Personal_Name_First_Name",
          "'' as Personal_Name_General_Display_Name",
          "'' as Personal_Name_Last_Name",
          "'' as Personal_Name_Middle_Name",
          "'' as Personal_Name_Secondary_Last_Name",
          "'' as Personal_Name_Title",
          "`ns:Employees`.`ns1:Personal`.`ns1:Nationality` as Personal_Nationality",
          "'' as Personal_Number_of_Payroll_Dependents",
          "`ns:Employees`.`ns1:Personal`.`ns1:Preferred_Language` as Personal_Preferred_Language",
          "'' as Personal_Preferred_Locale",
          "'' as Personal_Uses_Tobacco",
          "`ns:Employees`.`ns1:Personal`.`ns1:Workday_Account` as Personal_Workday_Account",
          "'' as Address_ID",
          "'' as Address_Line_1",
          "'' as Address_Line_3",
          "'' as Business_Site_City",
          "'' as Business_Site_Country",
          "`ns:Employees`.`ns1:Position`.`ns1:Business_Site`.`ns1:Location_ID` as Business_Site_Location_ID",
          "'' as Business_Site_Location_Name",
          "'' as PostalCast",
          "'' as Job_Classification_Description",
          "'' as Job_Classification_Group",
          "'' as Job_Classification_ID",
          "'' as Job_Classification_Mapped_Value",
          "'' as Job_Family_ID",
          "'' as Job_Family_Mapped_Value",
          "'' as Job_Family_Name",
          "'' as Position_Job_Profile",
          "`ns:Employees`.`ns1:Position`.`ns1:Management_Level` as Position_Management_Level",
          "`ns:Employees`.`ns1:Position`.`ns1:Position_ID` as Position_Position_ID",
          "`ns:Employees`.`ns1:Position`.`ns1:Position_Time_Type` as Position_Time_Type",
          "'' as Supervisor_ID",
          "'' as Supervisor_Name",
          "'' as Supervisor_Other_ID",
          "`ns:Employees`.`ns1:Qualifications`.`ns1:Certification_Achievement` as Qualifications_Certification_Achievement",
          "'' as Related_Person_Allowed_for_Tax_Deduction",
          "'' as Related_Person_Annual_Income_Amount",
          "'' as Related_Person_Beneficiary",
          "`relatedPerson`.`ns1:Birth_Date` as Related_Person_Birth_Date",
          "'' as Related_Person_Dependent",
          "`relatedPerson`.`ns1:Dependent_ID` as Related_Person_Dependent_ID",
          "'' as Related_Person_Emergency_Contact",
          "'' as Related_Person_Full_Time_Student",
          "`relatedPerson`.`ns1:Gender` as Related_Person_Gender",
          "'' as Related_Person_Has_Health_Insurance",
          "'' as Related_Person_Is_Dependent_for_Payroll_Purposes",
          "'' as Related_Person_Is_Disabled",
          "'' as Legal_Name_Country",
          "`relatedPerson`.`ns1:Legal_Name`.`ns1:First_Name` as Legal_Name_First_Name",
          "'' as Legal_Name_General_Display_Name",
          "`relatedPerson`.`ns1:Legal_Name`.`ns1:Last_Name` as Legal_Name_Last_Name",
          "`relatedPerson`.`ns1:Legal_Name`.`ns1:Secondary_Last_Name` as Legal_Name_Secondary_Last_Name",
          "'' as Related_Person_Lives_with_Worker",
          "`relatedPerson`.`ns1:Related_Person_ID` as Related_Person_ID",
          "'' as Related_Person_Relationship_Type",
          "'' as Related_Person_Relative",
          "'' as Related_Person_Tobacco_Use",
          "'' as Related_Person_Communication_ID",
          "'' as Government_Identifier",
          "'' as Related_Person_Identification_Country",
          "'' as Related_Person_Identification_ID",
          "'' as Related_Person_Identification_National_ID_Type",
          "'' as Related_Person_Identification_Custom_ID",
          "'' as Related_Person_Identification_Custom_ID_Type",
          "`IDCast` as Employee_ID",
          "'' as Summary_Name",
          "'' as WID",
          "`cast33` as Worker_Status_Active",
          "'' as Worker_Status_Active_Status_Date",
          "`ns:Employees`.`ns1:Worker_Status`.`ns1:Continuous_Service_Date` as Worker_Status_Continuous_Service_Date",
          "'' as Worker_Status_First_Day_of_Work",
          "`ns:Employees`.`ns1:Worker_Status`.`ns1:Hire_Date` as Worker_Status_Hire_Date",
          "'' as Worker_Status_Hire_Rescinded",
          "'' as Worker_Status_Is_Rehire",
          "'' as Worker_Status_Not_Returning",
          "`ns:Employees`.`ns1:Worker_Status`.`ns1:Original_Hire_Date` as Worker_Status_Original_Hire_Date",
          "'' as Worker_Status_Regrettable_Termination",
          "'' as Worker_Status_Return_Unknown",
          "'' as Worker_Status_Secondary_Termination_Reason",
          "`ns:Employees`.`ns1:Worker_Status`.`ns1:Seniority_Date` as Worker_Status_Seniority_Date",
          "`ns:Employees`.`ns1:Worker_Status`.`ns1:Status` as Worker_Status_Status",
          "'' as Worker_Status_Terminated",
          "'' as Worker_Status_Worker_Reference_Type",
          s"'$dataDatePart' as data_date_part"
        ).na.fill(" ").distinct()

    }

    else if (dataDatePart == "2021-04-28"){

      df_WeciFinal = df_WeciReadXML
        .withColumn("natioId", explode_outer(col("`ns:Employees`.`ns1:Person_Identification`.`ns1:National_Identifier`")))
        .withColumn("relatedPerson", explode_outer(col("`ns:Employees`.`ns1:Related_Person`")))
        .withColumn("relatedNacionalID",(col("`ns:Employees`.`ns1:Related_Person_Identification`")))
        .withColumn("allowance_plan", explode_outer(col("`ns:Employees`.`ns1:Compensation_Plans`.`ns1:Allowance_Plan`")))
        //.withColumn("job_family", explode_outer(col("`ns:Employees`.`ns1:Position`.`ns1:Job_Family`")))
        .withColumn("related_communication", explode_outer(col("`ns:Employees`.`ns1:Related_Person_Communication`")))
        //.withColumn("PostalCast",col("`ns:Employees`.`ns1:Position`.`ns1:Business_Site`.`ns1:Postal_Code`").cast("String"))
        //.withColumn("SuperCast",col("`ns:Employees`.`ns1:Position`.`ns1:Supervisor`.`ns1:ID`").cast("String"))
        .withColumn("IDCast",col("`ns:Employees`.`ns1:Summary`.`ns1:Employee_ID`").cast("String"))
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
        //.withColumn("cast13",col("`ns:Employees`.`ns1:Employee_Contract`.`ns1:Maximum_Weekly_Hours`").cast("String"))
        //.withColumn("cast14",col("`ns:Employees`.`ns1:Employee_Contract`.`ns1:Minimum_Weekly_Hours`").cast("String"))
        //.withColumn("cast15",col("`natioId`.`ns1:Verified_By`").cast("String"))
        //.withColumn("cast16",col("`ns:Employees`.`ns1:Personal`.`ns1:Local_Hukou`").cast("String"))
        //.withColumn("cast17",col("`ns:Employees`.`ns1:Personal`.`ns1:Number_of_Payroll_Dependents`").cast("String"))
        //.withColumn("cast18",col("`ns:Employees`.`ns1:Personal`.`ns1:Uses_Tobacco`").cast("String"))
        .withColumn("cast19",col("`ns:Employees`.`ns1:Position`.`ns1:Job_Classification`.`ns1:Job_Classification_ID`").cast("String"))
        //.withColumn("cast20",col("`job_family`.`ns1:ID`").cast("String"))
        //.withColumn("cast21",col("`relatedPerson`.`ns1:Allowed_for_Tax_Deduction`").cast("String"))
        //.withColumn("cast22",col("`relatedPerson`.`ns1:Annual_Income_Amount`").cast("String"))
        //.withColumn("cast23",col("`relatedPerson`.`ns1:Beneficiary`").cast("String"))
        //.withColumn("cast24",col("`relatedPerson`.`ns1:Dependent`").cast("String"))
        .withColumn("cast25",col("`relatedPerson`.`ns1:Emergency_Contact`").cast("String"))
        //.withColumn("cast26",col("`relatedPerson`.`ns1:Full_Time_Student`").cast("String"))
        //.withColumn("cast27",col("`relatedPerson`.`ns1:Has_Health_Insurance`").cast("String"))
        //.withColumn("cast28",col("`relatedPerson`.`ns1:Is_Dependent_for_Payroll_Purposes`").cast("String"))
        //.withColumn("cast29",col("`relatedPerson`.`ns1:Is_Disabled`").cast("String"))
        //.withColumn("cast30",col("`relatedPerson`.`ns1:Lives_with_Worker`").cast("String"))
        //.withColumn("cast31",col("`relatedPerson`.`ns1:Relative`").cast("String"))
        //.withColumn("cast32",col("`relatedPerson`.`ns1:Tobacco_Use`").cast("String"))
        .withColumn("cast33",col("`ns:Employees`.`ns1:Worker_Status`.`ns1:Active`").cast("String"))
        //.withColumn("cast34",col("`ns:Employees`.`ns1:Worker_Status`.`ns1:Hire_Rescinded`").cast("String"))
        //.withColumn("cast35",col("`ns:Employees`.`ns1:Worker_Status`.`ns1:Is_Rehire`").cast("String"))
        //.withColumn("cast36",col("`ns:Employees`.`ns1:Worker_Status`.`ns1:Not_Returning`").cast("String"))
        //.withColumn("cast37",col("`ns:Employees`.`ns1:Worker_Status`.`ns1:Regrettable_Termination`").cast("String"))
        //.withColumn("cast38",col("`ns:Employees`.`ns1:Worker_Status`.`ns1:Return_Unknown`").cast("String"))
        .withColumn("cast39",col("`ns:Employees`.`ns1:Worker_Status`.`ns1:Terminated`").cast("String"))
        .selectExpr(
          // root
          "'' as Additional_Information_Company",
          "`ns:Employees`.`ns1:Collective_Agreement`.`ns1:Collective_Agreement` as Collective_Agreement",
          "`ns:Employees`.`ns1:Collective_Agreement`.`ns1:Collective_Agreement_Factor`.`ns1:Factor` as Collective_Agreement_Factor",
          "'' as Collective_Agreement_Position_ID",
          "'' as Collective_Agreement_Start_Date",
          "'' as Compensation_Change_Reason",
          "`ns:Employees`.`ns1:Compensation`.`ns1:Compensation_Grade` as Compensation_Grade",
          "`ns:Employees`.`ns1:Compensation`.`ns1:Compensation_Summary_Based_on_Compensation_Grade`.`ns1:Currency` as Compensation_Summary_Based_on_Compensation_Grade_Currency",
          "`ns:Employees`.`ns1:Compensation`.`ns1:Compensation_Summary_Based_on_Compensation_Grade`.`ns1:Frequency` as Compensation_Summary_Based_on_Compensation_Grade_Frequency",
          "`cast1` as Compensation_Summary_Based_on_Compensation_Grade_Primary_Compensation_Basis",
          "`cast2` as Compensation_Summary_Based_on_Compensation_Grade_Total_Base_Pay",
          "`ns:Employees`.`ns1:Compensation`.`ns1:Compensation_Summary_in_Annualized_Frequency`.`ns1:Currency` as Compensation_Summary_in_Annualized_Frequency_Currency",
          "`ns:Employees`.`ns1:Compensation`.`ns1:Compensation_Summary_in_Annualized_Frequency`.`ns1:Frequency` as Compensation_Summary_in_Annualized_Frequency_Frequency",
          "`cast3` as Compensation_Summary_in_Annualized_Frequency_Primary_Compensation_Basis",
          "`cast4` as Compensation_Summary_in_Annualized_Frequency_Primary_Total_Base_Pay",
          "'' as Compensation_Position_ID",
          "`cast5` as Allowance_Plan_Amount",
          "`cast6` as Allowance_Plan_Apply_FTE",
          "`allowance_plan`.`ns1:Compensation_Plan` as Allowance_Plan_Compensation_Plan",
          "`allowance_plan`.`ns1:Currency` as Allowance_Plan_Currency",
          "`allowance_plan`.`ns1:Frequency` as Allowance_Plan_Frequency",
          "`allowance_plan`.`ns1:Start_Date` as Allowance_Plan_Start_Date",
          "`cast7` as Compensation_Plans_Amount",
          "`ns:Employees`.`ns1:Compensation_Plans`.`ns1:Bonus_Plan`.`ns1:Compensation_Plan` as Compensation_Plans_Compensation_Plan",
          "`ns:Employees`.`ns1:Compensation_Plans`.`ns1:Bonus_Plan`.`ns1:Currency` as Compensation_Plans_Currency",
          "`ns:Employees`.`ns1:Compensation_Plans`.`ns1:Bonus_Plan`.`ns1:Frequency` as Compensation_Plans_Frequency",
          "`cast8` as Compensation_Plans_Prorated_Amount",
          "`ns:Employees`.`ns1:Compensation_Plans`.`ns1:Bonus_Plan`.`ns1:Start_Date` as Compensation_Plans_Start_Date",
          "'' as Compensation_Plans_Position_ID",
          "`cast9` as Salary_and_Hourly_Plan_Amount",
          "`cast11` as Salary_and_Hourly_Plan_Apply_FTE",
          "`ns:Employees`.`ns1:Compensation_Plans`.`ns1:Salary_and_Hourly_Plan`.`ns1:Compensation_Plan` as Salary_and_Hourly_Plan_Compensation_Plan",
          "`ns:Employees`.`ns1:Compensation_Plans`.`ns1:Salary_and_Hourly_Plan`.`ns1:Currency` as Salary_and_Hourly_Plan_Currency",
          "`ns:Employees`.`ns1:Compensation_Plans`.`ns1:Salary_and_Hourly_Plan`.`ns1:Frequency` as Salary_and_Hourly_Plan_Frequency",
          "`cast12` as Salary_and_Hourly_Plan_Prorated_Amount",
          "`ns:Employees`.`ns1:Compensation_Plans`.`ns1:Salary_and_Hourly_Plan`.`ns1:Start_Date` as Salary_and_Hourly_Plan_Start_Date",
          "`ns:Employees`.`ns1:Derived_Event_Code` as Derived_Event_Code",
          "`ns:Employees`.`ns1:Effective_Moment` as Effective_Moment",
          "'' as Employee_Contract_Collective_Agreement",
          "'' as Contract_ID",
          "`ns:Employees`.`ns1:Employee_Contract`.`ns1:Contract_Status` as Employee_Contract_Contract_Status",
          "`ns:Employees`.`ns1:Employee_Contract`.`ns1:Contract_Type` as Contract_Type",
          "'' as Employee_Contract_Description",
          "'' as Employee_Contract_Maximum_Weekly_Hours",
          "'' as Employee_Contract_Minimum_Weekly_Hours",
          "'' as Employee_Contract_Position_ID",
          "`ns:Employees`.`ns1:Employee_Contract`.`ns1:Start_Date` as Employee_Contract_Start_Date",
          "`ns:Employees`.`ns1:Entry_Moment` as Entry_Moment",
          "'' as Person_Communication_Email",
          "'' as Person_Communication_Phone",
          "`natioId`.`ns1:Country` as National_Identifier_Country",
          "`natioId`.`ns1:National_ID` as National_Identifier_National_ID",
          "`natioId`.`ns1:National_ID_Type` as National_Identifier_National_ID_Type",
          "'' as National_Identifier_Verification_Date",
          "'' as National_Identifier_Verified_By",
          "'' as Other_Identifier_Custom_ID_Type",
          "'' as Person_Identification_Passport",
          "'' as Person_Identification_Visa",
          "`ns:Employees`.`ns1:Personal`.`ns1:City_of_Birth` as Personal_City_of_Birth",
          "`ns:Employees`.`ns1:Personal`.`ns1:Country_of_Birth` as Personal_Country_of_Birth",
          "`ns:Employees`.`ns1:Personal`.`ns1:Date_of_Birth` as Personal_Date_of_Birth",
          "'' as Personal_Disability_Status",
          "`ns:Employees`.`ns1:Personal`.`ns1:Gender` as Personal_Gender",
          "'' as Personal_Local_Hukou",
          "'' as Personal_Name_Country",
          "`ns:Employees`.`ns1:Personal`.`ns1:Name`.`ns1:First_Name` as Personal_Name_First_Name",
          "'' as Personal_Name_General_Display_Name",
          "'' as Personal_Name_Last_Name",
          "'' as Personal_Name_Middle_Name",
          "'' as Personal_Name_Secondary_Last_Name",
          "'' as Personal_Name_Title",
          "`ns:Employees`.`ns1:Personal`.`ns1:Nationality` as Personal_Nationality",
          "'' as Personal_Number_of_Payroll_Dependents",
          "`ns:Employees`.`ns1:Personal`.`ns1:Preferred_Language` as Personal_Preferred_Language",
          "'' as Personal_Preferred_Locale",
          "'' as Personal_Uses_Tobacco",
          "`ns:Employees`.`ns1:Personal`.`ns1:Workday_Account` as Personal_Workday_Account",
          "'' as Address_ID",
          "'' as Address_Line_1",
          "'' as Address_Line_3",
          "'' as Business_Site_City",
          "'' as Business_Site_Country",
          "`ns:Employees`.`ns1:Position`.`ns1:Business_Site`.`ns1:Location_ID` as Business_Site_Location_ID",
          "'' as Business_Site_Location_Name",
          "'' as PostalCast",
          "`ns:Employees`.`ns1:Position`.`ns1:Job_Classification`.`ns1:Job_Classification_Description` as Job_Classification_Description",
          "`ns:Employees`.`ns1:Position`.`ns1:Job_Classification`.`ns1:Job_Classification_Group` as Job_Classification_Group",
          "`cast19` as Job_Classification_ID",
          "'' as Job_Classification_Mapped_Value",
          "'' as Job_Family_ID",
          "'' as Job_Family_Mapped_Value",
          "'' as Job_Family_Name",
          "`ns:Employees`.`ns1:Position`.`ns1:Job_Profile` as Position_Job_Profile",
          "`ns:Employees`.`ns1:Position`.`ns1:Management_Level` as Position_Management_Level",
          "`ns:Employees`.`ns1:Position`.`ns1:Position_ID` as Position_Position_ID",
          "`ns:Employees`.`ns1:Position`.`ns1:Position_Time_Type` as Position_Time_Type",
          "'' as Supervisor_ID",
          "'' as Supervisor_Name",
          "'' as Supervisor_Other_ID",
          "`ns:Employees`.`ns1:Qualifications`.`ns1:Certification_Achievement` as Qualifications_Certification_Achievement",
          "'' as Related_Person_Allowed_for_Tax_Deduction",
          "'' as Related_Person_Annual_Income_Amount",
          "'' as Related_Person_Beneficiary",
          "`relatedPerson`.`ns1:Birth_Date` as Related_Person_Birth_Date",
          "'' as Related_Person_Dependent",
          "`relatedPerson`.`ns1:Dependent_ID` as Related_Person_Dependent_ID",
          "`cast25` as Related_Person_Emergency_Contact",
          "'' as Related_Person_Full_Time_Student",
          "`relatedPerson`.`ns1:Gender` as Related_Person_Gender",
          "'' as Related_Person_Has_Health_Insurance",
          "'' as Related_Person_Is_Dependent_for_Payroll_Purposes",
          "'' as Related_Person_Is_Disabled",
          "'' as Legal_Name_Country",
          "`relatedPerson`.`ns1:Legal_Name`.`ns1:First_Name` as Legal_Name_First_Name",
          "'' as Legal_Name_General_Display_Name",
          "`relatedPerson`.`ns1:Legal_Name`.`ns1:Last_Name` as Legal_Name_Last_Name",
          "`relatedPerson`.`ns1:Legal_Name`.`ns1:Secondary_Last_Name` as Legal_Name_Secondary_Last_Name",
          "'' as Related_Person_Lives_with_Worker",
          "`relatedPerson`.`ns1:Related_Person_ID` as Related_Person_ID",
          "`relatedPerson`.`ns1:Relationship_Type` as Related_Person_Relationship_Type",
          "'' as Related_Person_Relative",
          "'' as Related_Person_Tobacco_Use",
          "`related_communication`.`ns1:Related_Person_ID` as Related_Person_Communication_ID",
          "'' as Government_Identifier",
          "'' as Related_Person_Identification_Country",
          "'' as Related_Person_Identification_ID",
          "'' as Related_Person_Identification_National_ID_Type",
          "'' as Related_Person_Identification_Custom_ID",
          "'' as Related_Person_Identification_Custom_ID_Type",
          "`IDCast` as Employee_ID",
          "`ns:Employees`.`ns1:Summary`.`ns1:Name` as Summary_Name",
          "`ns:Employees`.`ns1:Summary`.`ns1:WID` as WID",
          "`cast33` as Worker_Status_Active",
          "`ns:Employees`.`ns1:Worker_Status`.`ns1:Active_Status_Date` as Worker_Status_Active_Status_Date",
          "`ns:Employees`.`ns1:Worker_Status`.`ns1:Continuous_Service_Date` as Worker_Status_Continuous_Service_Date",
          "'' as Worker_Status_First_Day_of_Work",
          "`ns:Employees`.`ns1:Worker_Status`.`ns1:Hire_Date` as Worker_Status_Hire_Date",
          "'' as Worker_Status_Hire_Rescinded",
          "'' as Worker_Status_Is_Rehire",
          "'' as Worker_Status_Not_Returning",
          "`ns:Employees`.`ns1:Worker_Status`.`ns1:Original_Hire_Date` as Worker_Status_Original_Hire_Date",
          "'' as Worker_Status_Regrettable_Termination",
          "'' as Worker_Status_Return_Unknown",
          "`ns:Employees`.`ns1:Worker_Status`.`ns1:Secondary_Termination_Reason` as Worker_Status_Secondary_Termination_Reason",
          "`ns:Employees`.`ns1:Worker_Status`.`ns1:Seniority_Date` as Worker_Status_Seniority_Date",
          "`ns:Employees`.`ns1:Worker_Status`.`ns1:Status` as Worker_Status_Status",
          "`cast39` as Worker_Status_Terminated",
          "'' as Worker_Status_Worker_Reference_Type",
          s"'$dataDatePart' as data_date_part"
        ).na.fill(" ").distinct()

    }


    else if (dataDatePart == "2021-04-29"){

    }

    else if (dataDatePart == "2021-05-04"){

    }

    else if (dataDatePart == "2021-05-06"){

    }

    else if (dataDatePart == "2021-05-13"){

    }

    else {
            println("La partición insertada no coincide on las que existen actualmente")
    }

    df_WeciFinal.show()


  }
}
