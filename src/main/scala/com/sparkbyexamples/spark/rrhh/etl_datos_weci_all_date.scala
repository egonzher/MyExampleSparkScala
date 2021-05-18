package com.sparkbyexamples.spark.rrhh


import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, explode_outer}

import java.io.File
import scala.collection.mutable.ListBuffer

object etl_datos_weci_all_date {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local[1]")
      .appName("SparkByExample")
      .getOrCreate()


    //Trabajando con la nueva integración de cvdatalake_worker
    //val dataDatePart = "2021-04-26"
    //val dataDatePart = "2021-04-27"
    //val dataDatePart = "2021-04-28"
    //val dataDatePart = "2021-04-29"
    //val dataDatePart = "2021-05-04"
    //val dataDatePart = "2021-05-06"
    //val dataDatePart = "2021-05-13"
    //val dataDatePart = "2021-05-15"
    val dataDatePart = "2021-05-17"

    val dataTimeStampPart = s"src/main/resources/rrhh/example_weci_consolid_wd/data_date_part=$dataDatePart"

    //Buscando las rutas en hdfs
    //val fs = FileSystem.get(new Configuration())
    //val status = fs.listStatus(new Path(YOUR_HDFS_PATH))
    //status.foreach(x=> println(x.getPath))

    def getListOfFiles(dir: String): List[File] = {
      val d = new File(dir)
      if (d.exists && d.isDirectory) {
        d.listFiles.filter(_.isDirectory).toList
      } else {
        List[File]()
      }
    }

    var listFolders = new ListBuffer[String]()

    for (nameFolders <- getListOfFiles(dataTimeStampPart)) {
      listFolders += nameFolders.toString.split("data_timestamp_part=")(1)
    }

    println("Imprimiendo la ruta del dataTimeStampPart")
    println(listFolders)
    println(listFolders.length)
    println(listFolders(0))

    val dataTimeStampPartSplit = listFolders(0)
    val rutaCvDatalakeWorker = s"src/main/resources/rrhh/example_weci_consolid_wd/data_date_part=$dataDatePart/data_timestamp_part=$dataTimeStampPartSplit/*.xml"

    val df_WeciReadXML = spark.read
      .format("com.databricks.spark.xml")
      .option("excludeAttribute", "false")
      .option("rowTag", "ns1:EMPLOYEE_DELTA_INTEGRATION")
      .load(rutaCvDatalakeWorker)

    println("Imprimiendo el esquema de df_WeciReadXML")
    df_WeciReadXML.printSchema()

    var df_WeciFinal = df_WeciReadXML

    if (dataDatePart == "2021-05-13"){

      df_WeciFinal = df_WeciReadXML
        .withColumn("natioId", explode_outer(col("`ns:Employees`.`ns1:Person_Identification`.`ns1:National_Identifier`")))
        .withColumn("relatedPerson", explode_outer(col("`ns:Employees`.`ns1:Related_Person`")))
        .withColumn("relatedNacionalID", explode_outer(col("`ns:Employees`.`ns1:Related_Person_Identification`")))
        //.withColumn("allowance_plan", explode_outer(col("`ns:Employees`.`ns1:Compensation_Plans`.`ns1:Allowance_Plan`")))
        //.withColumn("job_family", explode_outer(col("`ns:Employees`.`ns1:Position`.`ns1:Job_Family`")))
        .withColumn("related_communication", explode_outer(col("`ns:Employees`.`ns1:Related_Person_Communication`")))
        // nuevos withcolumn
        .withColumn("compensationPlan", explode_outer(col("`ns:Employees`.`ns1:Compensation_Plans`")))
        .withColumn("allowance", explode_outer(col("`compensationPlan`.`ns1:Allowance_Plan`")))
        .withColumn("bonus_plan", explode_outer(col("`compensationPlan`.`ns1:Bonus_Plan`")))
        .withColumn("salary_plan", explode_outer(col("`compensationPlan`.`ns1:Salary_and_Hourly_Plan`")))
        .withColumn("other", explode_outer(col("`ns:Employees`.`ns1:Person_Identification`.`ns1:Other_Identifier`")))
        .withColumn("position", explode_outer(col("`ns:Employees`.`ns1:Position`")))
        .withColumn("job_family", explode_outer(col("`position`.`ns1:Job_Family`")))
        .withColumn("compensation", explode_outer(col("`ns:Employees`.`ns1:Compensation`")))
        .withColumn("relatedOther", explode_outer(col("`relatedNacionalID`.`ns1:Other_Identifier`")))
        //
        //.withColumn("PostalCast",col("`ns:Employees`.`ns1:Position`.`ns1:Business_Site`.`ns1:Postal_Code`").cast("String"))
        .withColumn("SuperCast",col("`position`.`ns1:Supervisor`.`ns1:ID`").cast("String"))
        .withColumn("IDCast",col("`ns:Employees`.`ns1:Summary`.`ns1:Employee_ID`").cast("String"))
        .withColumn("cast1",col("`compensation`.`ns1:Compensation_Summary_Based_on_Compensation_Grade`.`ns1:Primary_Compensation_Basis`").cast("String"))
        .withColumn("cast2",col("`compensation`.`ns1:Compensation_Summary_Based_on_Compensation_Grade`.`ns1:Total_Base_Pay`").cast("String"))
        .withColumn("cast3",col("`compensation`.`ns1:Compensation_Summary_in_Annualized_Frequency`.`ns1:Primary_Compensation_Basis`").cast("String"))
        .withColumn("cast4",col("`compensation`.`ns1:Compensation_Summary_in_Annualized_Frequency`.`ns1:Total_Base_Pay`").cast("String"))
        .withColumn("cast5",col("`allowance`.`ns1:Amount`").cast("String"))
        .withColumn("cast6",col("`allowance`.`ns1:Apply_FTE`").cast("String"))
        .withColumn("cast7",col("`bonus_plan`.`ns1:Amount`").cast("String"))
        .withColumn("cast8",col("`bonus_plan`.`ns1:Prorated_Amount`").cast("String"))
        .withColumn("cast9",col("`salary_plan`.`ns1:Amount`").cast("String"))
        .withColumn("cast11",col("`salary_plan`.`ns1:Apply_FTE`").cast("String"))
        .withColumn("cast12",col("`salary_plan`.`ns1:Prorated_Amount`").cast("String"))
        .withColumn("cast13",col("`ns:Employees`.`ns1:Employee_Contract`.`ns1:Maximum_Weekly_Hours`").cast("String"))
        .withColumn("cast14",col("`ns:Employees`.`ns1:Employee_Contract`.`ns1:Minimum_Weekly_Hours`").cast("String"))
        .withColumn("cast15",col("`natioId`.`ns1:Verified_By`").cast("String"))
        .withColumn("cast16",col("`ns:Employees`.`ns1:Personal`.`ns1:Local_Hukou`").cast("String"))
        .withColumn("cast17",col("`ns:Employees`.`ns1:Personal`.`ns1:Number_of_Payroll_Dependents`").cast("String"))
        .withColumn("cast18",col("`ns:Employees`.`ns1:Personal`.`ns1:Uses_Tobacco`").cast("String"))
        .withColumn("cast19",col("`position`.`ns1:Job_Classification`.`ns1:Job_Classification_ID`").cast("String"))
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
        // Nuevos casteos
        .withColumn("cast40",col("`relatedNacionalID`.`_isAdded`").cast("String"))
        .withColumn("cast41",col("`relatedNacionalID`.`_isUpdated`").cast("String"))
        .withColumn("cast42",col("`relatedOther`.`_isAdded`").cast("String"))
        .withColumn("cast43",col("`relatedOther`.`_isUpdated`").cast("String"))
        .selectExpr(
          // Campos nuevos en inicio hasta collective agrement
          "`ns:Employees`.`ns1:Additional_Information`.`ns1:Cash_Range_Max` as Additional_Information_Cash_Range_Max",
          "`ns:Employees`.`ns1:Additional_Information`.`ns1:Cash_Range_Min` as Additional_Information_Cash_Range_Min",
          "`ns:Employees`.`ns1:Additional_Information`.`ns1:Company` as Additional_Information_Company",
          "`ns:Employees`.`ns1:Additional_Information`.`ns1:Country` as Additional_Information_Country",
          "`ns:Employees`.`ns1:Additional_Information`.`ns1:Organizational_Structure_Date` as Additional_Information_Organizational_Structure_Date",
          "`ns:Employees`.`ns1:Additional_Information`.`ns1:Organizational_Structure_ID` as Additional_Information_Organizational_Structure_ID",
          "`ns:Employees`.`ns1:Additional_Information`.`ns1:Organizational_Structure_Name` as Additional_Information_Organizational_Structure_Name",
          "`ns:Employees`.`ns1:Additional_Information`.`ns1:WorkerID` as Additional_Information_WorkerID",
          // Fin de campos nuevos inicio
          "`ns:Employees`.`ns1:Collective_Agreement`.`ns1:Collective_Agreement` as Collective_Agreement",
          "`ns:Employees`.`ns1:Collective_Agreement`.`ns1:Collective_Agreement_Factor`.`ns1:Factor` as Collective_Agreement_Factor",
          "`ns:Employees`.`ns1:Collective_Agreement`.`ns1:Position_ID` as Collective_Agreement_Position_ID",
          "`ns:Employees`.`ns1:Collective_Agreement`.`ns1:Start_Date` as Collective_Agreement_Start_Date",
          "`compensation`.`ns1:Compensation_Change_Reason` as Compensation_Change_Reason",
          "`compensation`.`ns1:Compensation_Grade` as Compensation_Grade",
          "`compensation`.`ns1:Compensation_Summary_Based_on_Compensation_Grade`.`ns1:Currency` as Compensation_Summary_Based_on_Compensation_Grade_Currency", //
          "`compensation`.`ns1:Compensation_Summary_Based_on_Compensation_Grade`.`ns1:Frequency` as Compensation_Summary_Based_on_Compensation_Grade_Frequency", //
          "`cast1` as Compensation_Summary_Based_on_Compensation_Grade_Primary_Compensation_Basis",
          "`cast2` as Compensation_Summary_Based_on_Compensation_Grade_Total_Base_Pay",
          "`compensation`.`ns1:Compensation_Summary_in_Annualized_Frequency`.`ns1:Currency` as Compensation_Summary_in_Annualized_Frequency_Currency",
          "`compensation`.`ns1:Compensation_Summary_in_Annualized_Frequency`.`ns1:Frequency` as Compensation_Summary_in_Annualized_Frequency_Frequency",
          "`cast3` as Compensation_Summary_in_Annualized_Frequency_Primary_Compensation_Basis",
          "`cast4` as Compensation_Summary_in_Annualized_Frequency_Primary_Total_Base_Pay",
          "`compensation`.`ns1:Position_ID` as Compensation_Position_ID",
          "`cast5` as Allowance_Plan_Amount",
          "`cast6` as Allowance_Plan_Apply_FTE",
          "`allowance`.`ns1:Compensation_Plan` as Allowance_Plan_Compensation_Plan",
          "`allowance`.`ns1:Currency` as Allowance_Plan_Currency",
          "'' as Allowance_Plan_End_Date",
          "`allowance`.`ns1:Frequency` as Allowance_Plan_Frequency",
          "`allowance`.`ns1:Start_Date` as Allowance_Plan_Start_Date",
          "`cast7` as Compensation_Plans_Amount",
          "`bonus_plan`.`ns1:Compensation_Plan` as Compensation_Plans_Compensation_Plan",
          "`bonus_plan`.`ns1:Currency` as Compensation_Plans_Currency",
          "'' as Compensation_Plans_End_Date",
          "`bonus_plan`.`ns1:Frequency` as Compensation_Plans_Frequency",
          "`cast8` as Compensation_Plans_Prorated_Amount",
          "`bonus_plan`.`ns1:Start_Date` as Compensation_Plans_Start_Date",
          "'' as Compensation_Plans_Position_End_Date",
          "'' as Compensation_Plans_Position_ID",
          "`cast9` as Salary_and_Hourly_Plan_Amount",
          "`cast11` as Salary_and_Hourly_Plan_Apply_FTE",
          "`salary_plan`.`ns1:Compensation_Plan` as Salary_and_Hourly_Plan_Compensation_Plan",
          "`salary_plan`.`ns1:Currency` as Salary_and_Hourly_Plan_Currency",
          "'' as Salary_and_Hourly_End_Date",
          "`salary_plan`.`ns1:Frequency` as Salary_and_Hourly_Plan_Frequency",
          "`cast12` as Salary_and_Hourly_Plan_Prorated_Amount",
          "`salary_plan`.`ns1:Start_Date` as Salary_and_Hourly_Plan_Start_Date",
          "`ns:Employees`.`ns1:Derived_Event_Code` as Derived_Event_Code",
          "`ns:Employees`.`ns1:Effective_Moment` as Effective_Moment",
          "`ns:Employees`.`ns1:Employee_Contract`.`ns1:Collective_Agreement` as Employee_Contract_Collective_Agreement",
          "'' as Contract_ID",
          "`ns:Employees`.`ns1:Employee_Contract`.`ns1:Contract_Status` as Employee_Contract_Contract_Status",
          "`ns:Employees`.`ns1:Employee_Contract`.`ns1:Contract_Type` as Employee_Contract_Type",
          "'' as Employee_Contract_Description",
          "'' as Employee_Contract_End_Date",
          "`cast13` as Employee_Contract_Maximum_Weekly_Hours",
          "`cast14` as Employee_Contract_Minimum_Weekly_Hours",
          "`ns:Employees`.`ns1:Employee_Contract`.`ns1:Position_ID` as Employee_Contract_Position_ID",
          "`ns:Employees`.`ns1:Employee_Contract`.`ns1:Start_Date` as Employee_Contract_Start_Date",
          "`ns:Employees`.`ns1:Entry_Moment` as Entry_Moment",
          "'' as Leave_of_Absence_Age_of_Dependent",
          "'' as Leave_of_Absence_Child_Birth_Date",
          "'' as Leave_of_Absence_Estimated_Leave_End_Date",
          "'' as Leave_of_Absence_Leave_Entitlement_Override",
          "'' as Leave_of_Absence_Age_Leave_Last_Day_of_Work",
          "'' as Leave_of_Absence_Age_Leave_Percentage",
          "'' as Leave_of_Absence_Age_Leave_Reason",
          "'' as Leave_of_Absence_Age_Leave_Start_Date",
          "'' as Leave_of_Absence_Leave_of_Absence_Type",
          "'' as Leave_of_Absence_Number_of_Babies_or_Adopted_Children",
          "'' as Leave_of_Absence_Age_Number_of_Child_Dependents",
          "'' as Leave_of_Absence_Age_Number_of_Previous_Births",
          "'' as Leave_of_Absence_Number_of_Previous_Maternity_Leaves",
          "'' as Person_Communication_Email",
          "'' as Person_Communication_Phone",
          "`natioId`.`ns1:Country` as National_Identifier_Country",
          // nuevos campos ID
          "`natioId`.`ns1:Expiration_Date` as National_Identifier_Expiration_Date",
          "`natioId`.`ns1:Issued_Date` as National_Identifier_Issued_Date",
          // fin campos ID
          "`natioId`.`ns1:National_ID` as National_Identifier_National_ID",
          "`natioId`.`ns1:National_ID_Type` as National_Identifier_National_ID_Type",
          "`natioId`.`ns1:Verification_Date` as National_Identifier_Verification_Date",
          "`cast15` as National_Identifier_Verified_By",
          "`other`.`ns1:Custom_ID_Type` as Other_Identifier_Custom_ID_Type",
          "'' as Person_Identification_Passport",
          "'' as Person_Identification_Visa",
          "`ns:Employees`.`ns1:Personal`.`ns1:City_of_Birth` as Personal_City_of_Birth",
          "`ns:Employees`.`ns1:Personal`.`ns1:Country_of_Birth` as Personal_Country_of_Birth",
          "`ns:Employees`.`ns1:Personal`.`ns1:Date_of_Birth` as Personal_Date_of_Birth",
          "'' as Personal_Disability_Status",
          "`ns:Employees`.`ns1:Personal`.`ns1:Gender` as Personal_Gender",
          "`cast16` as Personal_Local_Hukou",
          "`ns:Employees`.`ns1:Personal`.`ns1:Name`.`ns1:Country` as Personal_Name_Country",
          "`ns:Employees`.`ns1:Personal`.`ns1:Name`.`ns1:First_Name` as Personal_Name_First_Name",
          "`ns:Employees`.`ns1:Personal`.`ns1:Name`.`ns1:General_Display_Name` as Personal_Name_General_Display_Name",
          "`ns:Employees`.`ns1:Personal`.`ns1:Name`.`ns1:Last_Name` as Personal_Name_Last_Name",
          "'' as Personal_Name_Middle_Name",
          "`ns:Employees`.`ns1:Personal`.`ns1:Name`.`ns1:Secondary_Last_Name` as Personal_Name_Secondary_Last_Name",
          "`ns:Employees`.`ns1:Personal`.`ns1:Name`.`ns1:Title` as Personal_Name_Title",
          "`ns:Employees`.`ns1:Personal`.`ns1:Nationality` as Personal_Nationality",
          "`cast17` as Personal_Number_of_Payroll_Dependents",
          "`ns:Employees`.`ns1:Personal`.`ns1:Preferred_Language` as Personal_Preferred_Language",
          "`ns:Employees`.`ns1:Personal`.`ns1:Preferred_Locale` as Personal_Preferred_Locale",
          "`cast18` as Personal_Uses_Tobacco",
          "`ns:Employees`.`ns1:Personal`.`ns1:Workday_Account` as Personal_Workday_Account",
          "`position`.`ns1:Business_Site`.`ns1:Address_ID` as Address_ID",
          "`position`.`ns1:Business_Site`.`ns1:Address_Line_1` as Address_Line_1",
          "`position`.`ns1:Business_Site`.`ns1:Address_Line_3` as Address_Line_3",
          // nuevos campos Bussines_Site
          "`position`.`ns1:Business_Site`.`ns1:Address_Line_5` as Address_Line_5",
          // fin nuevos campos
          "`position`.`ns1:Business_Site`.`ns1:City` as Business_Site_City",
          "`position`.`ns1:Business_Site`.`ns1:Country` as Business_Site_Country",
          // nuevos campos Bussines_Site
          "`position`.`ns1:Business_Site`.`ns1:Country_Region` as Business_Site_Country_Region",
          // fin nuevos campos
          "`position`.`ns1:Business_Site`.`ns1:Location_ID` as Business_Site_Location_ID",
          "`position`.`ns1:Business_Site`.`ns1:Location_Name` as Business_Site_Location_Name",
          "`position`.`ns1:Business_Site`.`ns1:Postal_Code` as Business_Site_Postal_Code",
          // nuevos campos Bussines_Site
          "`position`.`ns1:Business_Site`.`ns1:State_Province` as Business_Site_State_Province",
          // fin nuevos campos
          "`position`.`ns1:Job_Classification`.`ns1:Job_Classification_Description` as Job_Classification_Description",
          "`position`.`ns1:Job_Classification`.`ns1:Job_Classification_Group` as Job_Classification_Group",
          "`cast19` as Job_Classification_ID",
          "`position`.`ns1:Job_Classification`.`ns1:Mapped_Value` as Job_Classification_Mapped_Value",
          "`cast20` as Job_Family_ID",
          "`job_family`.`ns1:Mapped_Value` as Job_Family_Mapped_Value",
          "`job_family`.`ns1:Name` as Job_Family_Name",
          "`position`.`ns1:Job_Profile` as Position_Job_Profile",
          "`position`.`ns1:Management_Level` as Position_Management_Level",
          "`position`.`ns1:Position_ID` as Position_Position_ID",
          "`position`.`ns1:Position_Time_Type` as Position_Time_Type",
          "'' as Supervisor_isAdded",
          "'' as Supervisor_isDeleted",
          "`SuperCast` as Supervisor_ID",
          "`position`.`ns1:Supervisor`.`ns1:Name` as Supervisor_Name",
          "`position`.`ns1:Supervisor`.`ns1:Other_ID` as Supervisor_Other_ID",
          "`ns:Employees`.`ns1:Qualifications`.`ns1:Certification_Achievement` as Qualifications_Certification_Achievement",
          "`cast21` as Related_Person_Allowed_for_Tax_Deduction",
          "`cast22` as Related_Person_Annual_Income_Amount",
          "`cast23` as Related_Person_Beneficiary",
          // campos nuevos related_person
          "`relatedPerson`.`ns1:Beneficiary_ID` as Related_Person_Beneficiary_ID",
          //fin campos nuevos
          "`relatedPerson`.`ns1:Birth_Date` as Related_Person_Birth_Date",
          "`cast24` as Related_Person_Dependent",
          "`relatedPerson`.`ns1:Dependent_ID` as Related_Person_Dependent_ID",
          "`cast25` as Related_Person_Emergency_Contact",
          // campos nuevos related_person
          "`relatedPerson`.`ns1:Emergency_Contact_ID` as Related_Person_Emergency_Contact_ID",
          //fin campos nuevos
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
          // nuevos campos relatedPerson Identification
          "`cast40` as Related_Person_isAdded",
          "`cast41` as Related_Person_isUpdated",
          // fin nuevos campos
          "`relatedNacionalID`.`ns1:Government_Identifier` as Government_Identifier",
          "`relatedNacionalID`.`ns1:National_Identifier`.`ns1:Country` as Related_Person_Identification_Country",
          // nuevos campos relatedPerson Identification
          "`relatedNacionalID`.`ns1:National_Identifier`.`ns1:Expiration_Date` as Related_Person_Identification_Expiration_Date",
          "`relatedNacionalID`.`ns1:National_Identifier`.`ns1:Issued_Date` as Related_Person_Identification_Issued_Date",
          // fin nuevos campos
          "`relatedNacionalID`.`ns1:National_Identifier`.`ns1:National_ID` as Related_Person_Identification_ID",
          "`relatedNacionalID`.`ns1:National_Identifier`.`ns1:National_ID_Type` as Related_Person_Identification_National_ID_Type",
          // nuevos campos relatedPerson Identification
          "`relatedNacionalID`.`ns1:National_Identifier`.`ns1:Verification_Date` as Related_Person_Identification_Verification_Date",
          // fin nuevos campos
          // nuevos campos relatedPerson Identification other
          "`cast42` as Related_Person_Identification_isAdded",
          "`cast43` as Related_Person_Identification_isUpdated",
          // fin nuevos campos
          "`relatedOther`.`ns1:Custom_ID` as Related_Person_Identification_Custom_ID",
          "`relatedOther`.`ns1:Custom_ID_Type` as Related_Person_Identification_Custom_ID_Type",
          "`IDCast` as Employee_ID",
          "`ns:Employees`.`ns1:Summary`.`ns1:Name` as Summary_Name",
          "`ns:Employees`.`ns1:Summary`.`ns1:WID` as WID",
          "`cast33` as Worker_Status_Active",
          "`ns:Employees`.`ns1:Worker_Status`.`ns1:Active_Status_Date` as Worker_Status_Active_Status_Date",
          // nuevos campos worker_status
          "`ns:Employees`.`ns1:Worker_Status`.`ns1:Benefits_Service_Date` as Worker_Status_Benefits_Service_Date",
          "`ns:Employees`.`ns1:Worker_Status`.`ns1:Company_Service_Date` as Worker_Status_Company_Service_Date",
          // fin worker_status
          "`ns:Employees`.`ns1:Worker_Status`.`ns1:Continuous_Service_Date` as Worker_Status_Continuous_Service_Date",
          "'' as Worker_Status_End_Employment_Date",
          "`ns:Employees`.`ns1:Worker_Status`.`ns1:First_Day_of_Work` as Worker_Status_First_Day_of_Work",
          "`ns:Employees`.`ns1:Worker_Status`.`ns1:Hire_Date` as Worker_Status_Hire_Date",
          // nuevos campos worker_status
          "`ns:Employees`.`ns1:Worker_Status`.`ns1:Hire_Reason` as Worker_Status_Hire_Reason",
          // fin worker_status
          "`cast34` as Worker_Status_Hire_Rescinded",
          "`cast35` as Worker_Status_Is_Rehire",
          "`cast36` as Worker_Status_Not_Returning",
          "`ns:Employees`.`ns1:Worker_Status`.`ns1:Original_Hire_Date` as Worker_Status_Original_Hire_Date",
          "`cast37` as Worker_Status_Regrettable_Termination",
          "`cast38` as Worker_Status_Return_Unknown",
          "'' as Worker_Status_Secondary_Termination_Reason",
          "`ns:Employees`.`ns1:Worker_Status`.`ns1:Seniority_Date` as Worker_Status_Seniority_Date",
          // nuevos campos worker_status
          "`ns:Employees`.`ns1:Worker_Status`.`ns1:Severance_Date` as Worker_Status_Severance_Date",
          // fin worker_status
          "`ns:Employees`.`ns1:Worker_Status`.`ns1:Status` as Worker_Status_Status",
          "`cast39` as Worker_Status_Terminated",
          // nuevos campos worker_status
          "`ns:Employees`.`ns1:Worker_Status`.`ns1:Time_Off_Service_Date` as Worker_Status_Time_Off_Service_Date",
          "`ns:Employees`.`ns1:Worker_Status`.`ns1:Vesting_Date` as Worker_Status_Vesting_Date",
          // fin worker_status
          "`ns:Employees`.`ns1:Worker_Status`.`ns1:Worker_Reference_Type` as Worker_Status_Worker_Reference_Type",
          s"'$dataTimeStampPartSplit' as data_date_part"
        ).na.fill(" ").distinct()

    }

    else if(dataDatePart == "2021-05-15"){
      df_WeciFinal = df_WeciReadXML
        .withColumn("natioId", explode_outer(col("`ns:Employees`.`ns1:Person_Identification`.`ns1:National_Identifier`")))
        //.withColumn("relatedPerson", explode_outer(col("`ns:Employees`.`ns1:Related_Person`")))
        //.withColumn("relatedNacionalID", explode_outer(col("`ns:Employees`.`ns1:Related_Person_Identification`")))
        //.withColumn("allowance_plan", explode_outer(col("`ns:Employees`.`ns1:Compensation_Plans`.`ns1:Allowance_Plan`")))
        //.withColumn("job_family", explode_outer(col("`ns:Employees`.`ns1:Position`.`ns1:Job_Family`")))
        //.withColumn("related_communication", explode_outer(col("`ns:Employees`.`ns1:Related_Person_Communication`")))
        // nuevos withcolumn
        .withColumn("compensationPlan", explode_outer(col("`ns:Employees`.`ns1:Compensation_Plans`")))
        .withColumn("allowance", explode_outer(col("`compensationPlan`.`ns1:Allowance_Plan`")))
        .withColumn("bonus_plan", explode_outer(col("`compensationPlan`.`ns1:Bonus_Plan`")))
        .withColumn("salary_plan", explode_outer(col("`compensationPlan`.`ns1:Salary_and_Hourly_Plan`")))
        .withColumn("other", explode_outer(col("`ns:Employees`.`ns1:Person_Identification`.`ns1:Other_Identifier`")))
        .withColumn("position", explode_outer(col("`ns:Employees`.`ns1:Position`")))
        .withColumn("job_family", explode_outer(col("`position`.`ns1:Job_Family`")))
        .withColumn("compensation", explode_outer(col("`ns:Employees`.`ns1:Compensation`")))
        //.withColumn("relatedOther", explode_outer(col("`relatedNacionalID`.`ns1:Other_Identifier`")))
        // nuevos mas nuevos withcolumns
        .withColumn("collective", explode_outer(col("`ns:Employees`.`ns1:Collective_Agreement`")))
        .withColumn("contract", explode_outer(col("`ns:Employees`.`ns1:Employee_Contract`")))
        .withColumn("supervisor", explode_outer(col("`position`.`ns1:Supervisor`")))
        .withColumn("PostalCast",col("`position`.`ns1:Business_Site`.`ns1:Postal_Code`").cast("String"))
        .withColumn("SuperCast",col("`supervisor`.`ns1:ID`").cast("String"))
        .withColumn("IDCast",col("`ns:Employees`.`ns1:Summary`.`ns1:Employee_ID`").cast("String"))
        .withColumn("cast1",col("`compensation`.`ns1:Compensation_Summary_Based_on_Compensation_Grade`.`ns1:Primary_Compensation_Basis`").cast("String"))
        .withColumn("cast2",col("`compensation`.`ns1:Compensation_Summary_Based_on_Compensation_Grade`.`ns1:Total_Base_Pay`").cast("String"))
        .withColumn("cast3",col("`compensation`.`ns1:Compensation_Summary_in_Annualized_Frequency`.`ns1:Primary_Compensation_Basis`").cast("String"))
        .withColumn("cast4",col("`compensation`.`ns1:Compensation_Summary_in_Annualized_Frequency`.`ns1:Total_Base_Pay`").cast("String"))
        .withColumn("cast5",col("`allowance`.`ns1:Amount`").cast("String"))
        .withColumn("cast6",col("`allowance`.`ns1:Apply_FTE`").cast("String"))
        .withColumn("cast7",col("`bonus_plan`.`ns1:Amount`").cast("String"))
        .withColumn("cast8",col("`bonus_plan`.`ns1:Prorated_Amount`").cast("String"))
        .withColumn("cast9",col("`salary_plan`.`ns1:Amount`").cast("String"))
        .withColumn("cast11",col("`salary_plan`.`ns1:Apply_FTE`").cast("String"))
        .withColumn("cast12",col("`salary_plan`.`ns1:Prorated_Amount`").cast("String"))
        .withColumn("cast13",col("`contract`.`ns1:Maximum_Weekly_Hours`").cast("String"))
        .withColumn("cast14",col("`contract`.`ns1:Minimum_Weekly_Hours`").cast("String"))
        .withColumn("cast15",col("`natioId`.`ns1:Verified_By`").cast("String"))
        .withColumn("cast16",col("`ns:Employees`.`ns1:Personal`.`ns1:Local_Hukou`").cast("String"))
        .withColumn("cast17",col("`ns:Employees`.`ns1:Personal`.`ns1:Number_of_Payroll_Dependents`").cast("String"))
        .withColumn("cast18",col("`ns:Employees`.`ns1:Personal`.`ns1:Uses_Tobacco`").cast("String"))
        .withColumn("cast19",col("`position`.`ns1:Job_Classification`.`ns1:Job_Classification_ID`").cast("String"))
        .withColumn("cast20",col("`job_family`.`ns1:ID`").cast("String"))
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
        .withColumn("cast34",col("`ns:Employees`.`ns1:Worker_Status`.`ns1:Hire_Rescinded`").cast("String"))
        .withColumn("cast35",col("`ns:Employees`.`ns1:Worker_Status`.`ns1:Is_Rehire`").cast("String"))
        .withColumn("cast36",col("`ns:Employees`.`ns1:Worker_Status`.`ns1:Not_Returning`").cast("String"))
        .withColumn("cast37",col("`ns:Employees`.`ns1:Worker_Status`.`ns1:Regrettable_Termination`").cast("String"))
        .withColumn("cast38",col("`ns:Employees`.`ns1:Worker_Status`.`ns1:Return_Unknown`").cast("String"))
        .withColumn("cast39",col("`ns:Employees`.`ns1:Worker_Status`.`ns1:Terminated`").cast("String"))
        // Nuevos casteos
        //.withColumn("cast40",col("`relatedNacionalID`.`_isAdded`").cast("String"))
        //.withColumn("cast41",col("`relatedNacionalID`.`_isUpdated`").cast("String"))
        //.withColumn("cast42",col("`relatedOther`.`_isAdded`").cast("String"))
        //.withColumn("cast43",col("`relatedOther`.`_isUpdated`").cast("String"))
        // Mas nuevos canteos
        .withColumn("cast44",col("`ns:Employees`.`ns1:Leave_of_Absence`.`ns1:Age_of_Dependent`").cast("String"))
        .withColumn("cast45",col("`ns:Employees`.`ns1:Leave_of_Absence`.`ns1:Leave_Entitlement_Override`").cast("String"))
        .withColumn("cast46",col("`ns:Employees`.`ns1:Leave_of_Absence`.`ns1:Leave_Percentage`").cast("String"))
        .withColumn("cast47",col("`ns:Employees`.`ns1:Leave_of_Absence`.`ns1:Number_of_Babies_or_Adopted_Children`").cast("String"))
        .withColumn("cast48",col("`ns:Employees`.`ns1:Leave_of_Absence`.`ns1:Number_of_Child_Dependents`").cast("String"))
        .withColumn("cast49",col("`ns:Employees`.`ns1:Leave_of_Absence`.`ns1:Number_of_Previous_Births`").cast("String"))
        .withColumn("cast50",col("`ns:Employees`.`ns1:Leave_of_Absence`.`ns1:Number_of_Previous_Maternity_Leaves`").cast("String"))
        .withColumn("cast51",col("`supervisor`.`_isAdded`").cast("String"))
        .withColumn("cast52",col("`supervisor`.`_isDeleted`").cast("String"))
        .selectExpr(
          // Campos nuevos en inicio hasta collective agrement
          "`ns:Employees`.`ns1:Additional_Information`.`ns1:Cash_Range_Max` as Additional_Information_Cash_Range_Max",
          "`ns:Employees`.`ns1:Additional_Information`.`ns1:Cash_Range_Min` as Additional_Information_Cash_Range_Min",
          "`ns:Employees`.`ns1:Additional_Information`.`ns1:Company` as Additional_Information_Company",
          "`ns:Employees`.`ns1:Additional_Information`.`ns1:Country` as Additional_Information_Country",
          "`ns:Employees`.`ns1:Additional_Information`.`ns1:Organizational_Structure_Date` as Additional_Information_Organizational_Structure_Date",
          "`ns:Employees`.`ns1:Additional_Information`.`ns1:Organizational_Structure_ID` as Additional_Information_Organizational_Structure_ID",
          "`ns:Employees`.`ns1:Additional_Information`.`ns1:Organizational_Structure_Name` as Additional_Information_Organizational_Structure_Name",
          "`ns:Employees`.`ns1:Additional_Information`.`ns1:WorkerID` as Additional_Information_WorkerID",
          // Fin de campos nuevos inicio
          "`collective`.`ns1:Collective_Agreement` as Collective_Agreement",
          "`collective`.`ns1:Collective_Agreement_Factor`.`ns1:Factor` as Collective_Agreement_Factor",
          "`collective`.`ns1:Position_ID` as Collective_Agreement_Position_ID",
          "`collective`.`ns1:Start_Date` as Collective_Agreement_Start_Date",
          "`compensation`.`ns1:Compensation_Change_Reason` as Compensation_Change_Reason",
          "`compensation`.`ns1:Compensation_Grade` as Compensation_Grade",
          "`compensation`.`ns1:Compensation_Summary_Based_on_Compensation_Grade`.`ns1:Currency` as Compensation_Summary_Based_on_Compensation_Grade_Currency", //
          "`compensation`.`ns1:Compensation_Summary_Based_on_Compensation_Grade`.`ns1:Frequency` as Compensation_Summary_Based_on_Compensation_Grade_Frequency", //
          "`cast1` as Compensation_Summary_Based_on_Compensation_Grade_Primary_Compensation_Basis",
          "`cast2` as Compensation_Summary_Based_on_Compensation_Grade_Total_Base_Pay",
          "`compensation`.`ns1:Compensation_Summary_in_Annualized_Frequency`.`ns1:Currency` as Compensation_Summary_in_Annualized_Frequency_Currency",
          "`compensation`.`ns1:Compensation_Summary_in_Annualized_Frequency`.`ns1:Frequency` as Compensation_Summary_in_Annualized_Frequency_Frequency",
          "`cast3` as Compensation_Summary_in_Annualized_Frequency_Primary_Compensation_Basis",
          "`cast4` as Compensation_Summary_in_Annualized_Frequency_Primary_Total_Base_Pay",
          "`compensation`.`ns1:Position_ID` as Compensation_Position_ID",
          "`cast5` as Allowance_Plan_Amount",
          "`cast6` as Allowance_Plan_Apply_FTE",
          "`allowance`.`ns1:Compensation_Plan` as Allowance_Plan_Compensation_Plan",
          "`allowance`.`ns1:Currency` as Allowance_Plan_Currency",
          "`allowance`.`ns1:End_Date` as Allowance_Plan_End_Date",
          "`allowance`.`ns1:Frequency` as Allowance_Plan_Frequency",
          "`allowance`.`ns1:Start_Date` as Allowance_Plan_Start_Date",
          "`cast7` as Compensation_Plans_Amount",
          "`bonus_plan`.`ns1:Compensation_Plan` as Compensation_Plans_Compensation_Plan",
          "`bonus_plan`.`ns1:Currency` as Compensation_Plans_Currency",
          // nuevos campos end date en estos 3 arrays
          "`bonus_plan`.`ns1:End_Date` as Compensation_Plans_End_Date",
          "`bonus_plan`.`ns1:Frequency` as Compensation_Plans_Frequency",
          "`cast8` as Compensation_Plans_Prorated_Amount",
          "`bonus_plan`.`ns1:Start_Date` as Compensation_Plans_Start_Date",
          // nuevos campos id y end datepositon
          "`compensationPlan`.`ns1:Position_End_Date` as Compensation_Plans_Position_End_Date",
          "`compensationPlan`.`ns1:Position_ID` as Compensation_Plans_Position_ID",
          "`cast9` as Salary_and_Hourly_Plan_Amount",
          "`cast11` as Salary_and_Hourly_Plan_Apply_FTE",
          "`salary_plan`.`ns1:Compensation_Plan` as Salary_and_Hourly_Plan_Compensation_Plan",
          "`salary_plan`.`ns1:Currency` as Salary_and_Hourly_Plan_Currency",
          "`salary_plan`.`ns1:End_Date` as Salary_and_Hourly_End_Date",
          "`salary_plan`.`ns1:Frequency` as Salary_and_Hourly_Plan_Frequency",
          "`cast12` as Salary_and_Hourly_Plan_Prorated_Amount",
          "`salary_plan`.`ns1:Start_Date` as Salary_and_Hourly_Plan_Start_Date",
          "`ns:Employees`.`ns1:Derived_Event_Code` as Derived_Event_Code",
          "`ns:Employees`.`ns1:Effective_Moment` as Effective_Moment",
          "`contract`.`ns1:Collective_Agreement` as Employee_Contract_Collective_Agreement",
          "`contract`.`ns1:Contract_ID` as Employee_Contract_Contract_ID",
          "`contract`.`ns1:Contract_Status` as Employee_Contract_Contract_Status",
          "`contract`.`ns1:Contract_Type` as Employee_Contract_Type",
          "`contract`.`ns1:Description` as Employee_Contract_Description",
          "`contract`.`ns1:End_Date` as Employee_Contract_End_Date",
          "`cast13` as Employee_Contract_Maximum_Weekly_Hours",
          "`cast14` as Employee_Contract_Minimum_Weekly_Hours",
          "`contract`.`ns1:Position_ID` as Employee_Contract_Position_ID",
          "`contract`.`ns1:Start_Date` as Employee_Contract_Start_Date",
          "`ns:Employees`.`ns1:Entry_Moment` as Entry_Moment",
          // MUCHOS CAMPOS NUEVOS CON CASTEOS
          "`cast44` as Leave_of_Absence_Age_of_Dependent",
          "`ns:Employees`.`ns1:Leave_of_Absence`.`ns1:Child_Birth_Date` as Leave_of_Absence_Child_Birth_Date",
          "`ns:Employees`.`ns1:Leave_of_Absence`.`ns1:Estimated_Leave_End_Date` as Leave_of_Absence_Estimated_Leave_End_Date",
          "`cast45` as Leave_of_Absence_Leave_Entitlement_Override",
          "`ns:Employees`.`ns1:Leave_of_Absence`.`ns1:Leave_Last_Day_of_Work` as Leave_of_Absence_Age_Leave_Last_Day_of_Work",
          "`cast46` as Leave_of_Absence_Age_Leave_Percentage",
          "`ns:Employees`.`ns1:Leave_of_Absence`.`ns1:Leave_Reason` as Leave_of_Absence_Age_Leave_Reason",
          "`ns:Employees`.`ns1:Leave_of_Absence`.`ns1:Leave_Start_Date` as Leave_of_Absence_Age_Leave_Start_Date",
          "`ns:Employees`.`ns1:Leave_of_Absence`.`ns1:Leave_of_Absence_Type` as Leave_of_Absence_Leave_of_Absence_Type",
          "`cast47` as Leave_of_Absence_Number_of_Babies_or_Adopted_Children",
          "`cast48` as Leave_of_Absence_Age_Number_of_Child_Dependents",
          "`cast49` as Leave_of_Absence_Age_Number_of_Previous_Births",
          "`cast50` as Leave_of_Absence_Number_of_Previous_Maternity_Leaves",
          // finde los muchos campos nuevos
          "`ns:Employees`.`ns1:Person_Communication` as Person_Communication_Email",
          "'' as Person_Communication_Phone",
          "`natioId`.`ns1:Country` as National_Identifier_Country",
          // nuevos campos ID
          "'' as National_Identifier_Expiration_Date",
          "'' as National_Identifier_Issued_Date",
          // fin campos ID
          "`natioId`.`ns1:National_ID` as National_Identifier_National_ID",
          "`natioId`.`ns1:National_ID_Type` as National_Identifier_National_ID_Type",
          "`natioId`.`ns1:Verification_Date` as National_Identifier_Verification_Date",
          "`cast15` as National_Identifier_Verified_By",
          "`other`.`ns1:Custom_ID_Type` as Other_Identifier_Custom_ID_Type",
          "'' as Person_Identification_Passport",
          "'' as Person_Identification_Visa",
          "`ns:Employees`.`ns1:Personal`.`ns1:City_of_Birth` as Personal_City_of_Birth",
          "`ns:Employees`.`ns1:Personal`.`ns1:Country_of_Birth` as Personal_Country_of_Birth",
          "`ns:Employees`.`ns1:Personal`.`ns1:Date_of_Birth` as Personal_Date_of_Birth",
          "'' as Personal_Disability_Status",
          "`ns:Employees`.`ns1:Personal`.`ns1:Gender` as Personal_Gender",
          "`cast16` as Personal_Local_Hukou",
          "`ns:Employees`.`ns1:Personal`.`ns1:Name`.`ns1:Country` as Personal_Name_Country",
          "`ns:Employees`.`ns1:Personal`.`ns1:Name`.`ns1:First_Name` as Personal_Name_First_Name",
          "`ns:Employees`.`ns1:Personal`.`ns1:Name`.`ns1:General_Display_Name` as Personal_Name_General_Display_Name",
          "`ns:Employees`.`ns1:Personal`.`ns1:Name`.`ns1:Last_Name` as Personal_Name_Last_Name",
          "'' as Personal_Name_Middle_Name",
          "`ns:Employees`.`ns1:Personal`.`ns1:Name`.`ns1:Secondary_Last_Name` as Personal_Name_Secondary_Last_Name",
          "`ns:Employees`.`ns1:Personal`.`ns1:Name`.`ns1:Title` as Personal_Name_Title",
          "`ns:Employees`.`ns1:Personal`.`ns1:Nationality` as Personal_Nationality",
          "`cast17` as Personal_Number_of_Payroll_Dependents",
          "`ns:Employees`.`ns1:Personal`.`ns1:Preferred_Language` as Personal_Preferred_Language",
          "`ns:Employees`.`ns1:Personal`.`ns1:Preferred_Locale` as Personal_Preferred_Locale",
          "`cast18` as Personal_Uses_Tobacco",
          "`ns:Employees`.`ns1:Personal`.`ns1:Workday_Account` as Personal_Workday_Account",
          "`position`.`ns1:Business_Site`.`ns1:Address_ID` as Address_ID",
          "`position`.`ns1:Business_Site`.`ns1:Address_Line_1` as Address_Line_1",
          "`position`.`ns1:Business_Site`.`ns1:Address_Line_3` as Address_Line_3",
          // nuevos campos Bussines_Site
          "`position`.`ns1:Business_Site`.`ns1:Address_Line_5` as Address_Line_5",
          // fin nuevos campos
          "`position`.`ns1:Business_Site`.`ns1:City` as Business_Site_City",
          "`position`.`ns1:Business_Site`.`ns1:Country` as Business_Site_Country",
          // nuevos campos Bussines_Site
          "`position`.`ns1:Business_Site`.`ns1:Country_Region` as Business_Site_Country_Region",
          // fin nuevos campos
          "`position`.`ns1:Business_Site`.`ns1:Location_ID` as Business_Site_Location_ID",
          "`position`.`ns1:Business_Site`.`ns1:Location_Name` as Business_Site_Location_Name",
          "`PostalCast` as Business_Site_Postal_Code",
          // nuevos campos Bussines_Site
          "`position`.`ns1:Business_Site`.`ns1:State_Province` as Business_Site_State_Province",
          // fin nuevos campos
          "`position`.`ns1:Job_Classification`.`ns1:Job_Classification_Description` as Job_Classification_Description",
          "`position`.`ns1:Job_Classification`.`ns1:Job_Classification_Group` as Job_Classification_Group",
          "`cast19` as Job_Classification_ID",
          "`position`.`ns1:Job_Classification`.`ns1:Mapped_Value` as Job_Classification_Mapped_Value",
          "`cast20` as Job_Family_ID",
          "`job_family`.`ns1:Mapped_Value` as Job_Family_Mapped_Value",
          "`job_family`.`ns1:Name` as Job_Family_Name",
          "`position`.`ns1:Job_Profile` as Position_Job_Profile",
          "`position`.`ns1:Management_Level` as Position_Management_Level",
          "`position`.`ns1:Position_ID` as Position_Position_ID",
          "`position`.`ns1:Position_Time_Type` as Position_Time_Type",
          "`cast51` as Supervisor_isAdded",
          "`cast52` as Supervisor_isDeleted",
          "`SuperCast` as Supervisor_ID",
          "`supervisor`.`ns1:Name` as Supervisor_Name",
          "`supervisor`.`ns1:Other_ID` as Supervisor_Other_ID",
          "`ns:Employees`.`ns1:Qualifications`.`ns1:Certification_Achievement` as Qualifications_Certification_Achievement",
          "'' as Related_Person_Allowed_for_Tax_Deduction",
          "'' as Related_Person_Annual_Income_Amount",
          "'' as Related_Person_Beneficiary",
          // campos nuevos related_person
          "'' as Related_Person_Beneficiary_ID",
          //fin campos nuevos
          "'' as Related_Person_Birth_Date",
          "'' as Related_Person_Dependent",
          "'' as Related_Person_Dependent_ID",
          "'' as Related_Person_Emergency_Contact",
          // campos nuevos related_person
          "'' as Related_Person_Emergency_Contact_ID",
          //fin campos nuevos
          "'' as Related_Person_Full_Time_Student",
          "'' as Related_Person_Gender",
          "'' as Related_Person_Has_Health_Insurance",
          "'' as Related_Person_Is_Dependent_for_Payroll_Purposes",
          "'' as Related_Person_Is_Disabled",
          "'' as Legal_Name_Country",
          "'' as Legal_Name_First_Name",
          "'' as Legal_Name_General_Display_Name",
          "'' as Legal_Name_Last_Name",
          "'' as Legal_Name_Secondary_Last_Name",
          "'' as Related_Person_Lives_with_Worker",
          "'' as Related_Person_ID",
          "'' as Related_Person_Relationship_Type",
          "'' as Related_Person_Relative",
          "'' as Related_Person_Tobacco_Use",
          "'' as Related_Person_Communication_ID",
          // nuevos campos relatedPerson Identification
          "'' as Related_Person_isAdded",
          "'' as Related_Person_isUpdated",
          // fin nuevos campos
          "'' as Government_Identifier",
          "'' as Related_Person_Identification_Country",
          // nuevos campos relatedPerson Identification
          "'' as Related_Person_Identification_Expiration_Date",
          "'' as Related_Person_Identification_Issued_Date",
          // fin nuevos campos
          "'' as Related_Person_Identification_ID",
          "'' as Related_Person_Identification_National_ID_Type",
          // nuevos campos relatedPerson Identification
          "'' as Related_Person_Identification_Verification_Date",
          // fin nuevos campos
          // nuevos campos relatedPerson Identification other
          "'' as Related_Person_Identification_isAdded",
          "'' as Related_Person_Identification_isUpdated",
          // fin nuevos campos
          "'' as Related_Person_Identification_Custom_ID",
          "'' as Related_Person_Identification_Custom_ID_Type",
          "`IDCast` as Employee_ID",
          "`ns:Employees`.`ns1:Summary`.`ns1:Name` as Summary_Name",
          "`ns:Employees`.`ns1:Summary`.`ns1:WID` as WID",
          "`cast33` as Worker_Status_Active",
          "`ns:Employees`.`ns1:Worker_Status`.`ns1:Active_Status_Date` as Worker_Status_Active_Status_Date",
          // nuevos campos worker_status
          "`ns:Employees`.`ns1:Worker_Status`.`ns1:Benefits_Service_Date` as Worker_Status_Benefits_Service_Date",
          "`ns:Employees`.`ns1:Worker_Status`.`ns1:Company_Service_Date` as Worker_Status_Company_Service_Date",
          // fin worker_status
          "`ns:Employees`.`ns1:Worker_Status`.`ns1:Continuous_Service_Date` as Worker_Status_Continuous_Service_Date",
          // nuevo
          "'' as Worker_Status_End_Employment_Date",
          "`ns:Employees`.`ns1:Worker_Status`.`ns1:First_Day_of_Work` as Worker_Status_First_Day_of_Work",
          "`ns:Employees`.`ns1:Worker_Status`.`ns1:Hire_Date` as Worker_Status_Hire_Date",
          // nuevos campos worker_status
          "`ns:Employees`.`ns1:Worker_Status`.`ns1:Hire_Reason` as Worker_Status_Hire_Reason",
          // fin worker_status
          "`cast34` as Worker_Status_Hire_Rescinded",
          "`cast35` as Worker_Status_Is_Rehire",
          "`cast36` as Worker_Status_Not_Returning",
          "`ns:Employees`.`ns1:Worker_Status`.`ns1:Original_Hire_Date` as Worker_Status_Original_Hire_Date",
          "`cast37` as Worker_Status_Regrettable_Termination",
          "`cast38` as Worker_Status_Return_Unknown",
          "'' as Worker_Status_Secondary_Termination_Reason",
          "`ns:Employees`.`ns1:Worker_Status`.`ns1:Seniority_Date` as Worker_Status_Seniority_Date",
          // nuevos campos worker_status
          "`ns:Employees`.`ns1:Worker_Status`.`ns1:Severance_Date` as Worker_Status_Severance_Date",
          // fin worker_status
          "`ns:Employees`.`ns1:Worker_Status`.`ns1:Status` as Worker_Status_Status",
          "`cast39` as Worker_Status_Terminated",
          // nuevos campos worker_status
          "'' as Worker_Status_Time_Off_Service_Date",
          "`ns:Employees`.`ns1:Worker_Status`.`ns1:Vesting_Date` as Worker_Status_Vesting_Date",
          // fin worker_status
          "`ns:Employees`.`ns1:Worker_Status`.`ns1:Worker_Reference_Type` as Worker_Status_Worker_Reference_Type",
          s"'$dataTimeStampPartSplit' as data_date_part"
        ).na.fill(" ").distinct()


    }

    else if (dataDatePart == "2021-05-17") {
      df_WeciFinal = df_WeciReadXML
        .withColumn("natioId", explode_outer(col("`ns:Employees`.`ns1:Person_Identification`.`ns1:National_Identifier`")))
        //.withColumn("relatedPerson", explode_outer(col("`ns:Employees`.`ns1:Related_Person`")))
        //.withColumn("relatedNacionalID", explode_outer(col("`ns:Employees`.`ns1:Related_Person_Identification`")))
        //.withColumn("allowance_plan", explode_outer(col("`ns:Employees`.`ns1:Compensation_Plans`.`ns1:Allowance_Plan`")))
        //.withColumn("job_family", explode_outer(col("`ns:Employees`.`ns1:Position`.`ns1:Job_Family`")))
        //.withColumn("related_communication", explode_outer(col("`ns:Employees`.`ns1:Related_Person_Communication`")))
        // nuevos withcolumn
        .withColumn("compensationPlan", explode_outer(col("`ns:Employees`.`ns1:Compensation_Plans`")))
        .withColumn("allowance", explode_outer(col("`compensationPlan`.`ns1:Allowance_Plan`")))
        .withColumn("bonus_plan", explode_outer(col("`compensationPlan`.`ns1:Bonus_Plan`")))
        .withColumn("salary_plan", explode_outer(col("`compensationPlan`.`ns1:Salary_and_Hourly_Plan`")))
        .withColumn("other", explode_outer(col("`ns:Employees`.`ns1:Person_Identification`.`ns1:Other_Identifier`")))
        .withColumn("position", explode_outer(col("`ns:Employees`.`ns1:Position`")))
        .withColumn("job_family", explode_outer(col("`position`.`ns1:Job_Family`")))
        .withColumn("compensation", explode_outer(col("`ns:Employees`.`ns1:Compensation`")))
        //.withColumn("relatedOther", explode_outer(col("`relatedNacionalID`.`ns1:Other_Identifier`")))
        // nuevos mas nuevos withcolumns
        .withColumn("collective", explode_outer(col("`ns:Employees`.`ns1:Collective_Agreement`")))
        .withColumn("contract", explode_outer(col("`ns:Employees`.`ns1:Employee_Contract`")))
        .withColumn("supervisor", explode_outer(col("`position`.`ns1:Supervisor`")))
        .withColumn("PostalCast", col("`position`.`ns1:Business_Site`.`ns1:Postal_Code`").cast("String"))
        .withColumn("SuperCast", col("`supervisor`.`ns1:ID`").cast("String"))
        .withColumn("IDCast", col("`ns:Employees`.`ns1:Summary`.`ns1:Employee_ID`").cast("String"))
        .withColumn("cast1", col("`compensation`.`ns1:Compensation_Summary_Based_on_Compensation_Grade`.`ns1:Primary_Compensation_Basis`").cast("String"))
        .withColumn("cast2", col("`compensation`.`ns1:Compensation_Summary_Based_on_Compensation_Grade`.`ns1:Total_Base_Pay`").cast("String"))
        .withColumn("cast3", col("`compensation`.`ns1:Compensation_Summary_in_Annualized_Frequency`.`ns1:Primary_Compensation_Basis`").cast("String"))
        .withColumn("cast4", col("`compensation`.`ns1:Compensation_Summary_in_Annualized_Frequency`.`ns1:Total_Base_Pay`").cast("String"))
        .withColumn("cast5", col("`allowance`.`ns1:Amount`").cast("String"))
        //.withColumn("cast6", col("`allowance`.`ns1:Apply_FTE`").cast("String"))
        .withColumn("cast7", col("`bonus_plan`.`ns1:Amount`").cast("String"))
        .withColumn("cast8", col("`bonus_plan`.`ns1:Prorated_Amount`").cast("String"))
        .withColumn("cast9", col("`salary_plan`.`ns1:Amount`").cast("String"))
        //.withColumn("cast11", col("`salary_plan`.`ns1:Apply_FTE`").cast("String"))
        .withColumn("cast12", col("`salary_plan`.`ns1:Prorated_Amount`").cast("String"))
        .withColumn("cast13", col("`contract`.`ns1:Maximum_Weekly_Hours`").cast("String"))
        .withColumn("cast14", col("`contract`.`ns1:Minimum_Weekly_Hours`").cast("String"))
        .withColumn("cast15", col("`natioId`.`ns1:Verified_By`").cast("String"))
        //.withColumn("cast16", col("`ns:Employees`.`ns1:Personal`.`ns1:Local_Hukou`").cast("String"))
        .withColumn("cast17", col("`ns:Employees`.`ns1:Personal`.`ns1:Number_of_Payroll_Dependents`").cast("String"))
        .withColumn("cast18", col("`ns:Employees`.`ns1:Personal`.`ns1:Uses_Tobacco`").cast("String"))
        .withColumn("cast19", col("`position`.`ns1:Job_Classification`.`ns1:Job_Classification_ID`").cast("String"))
        .withColumn("cast20", col("`job_family`.`ns1:ID`").cast("String"))
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
        .withColumn("cast33", col("`ns:Employees`.`ns1:Worker_Status`.`ns1:Active`").cast("String"))
        .withColumn("cast34", col("`ns:Employees`.`ns1:Worker_Status`.`ns1:Hire_Rescinded`").cast("String"))
        .withColumn("cast35", col("`ns:Employees`.`ns1:Worker_Status`.`ns1:Is_Rehire`").cast("String"))
        .withColumn("cast36", col("`ns:Employees`.`ns1:Worker_Status`.`ns1:Not_Returning`").cast("String"))
        .withColumn("cast37", col("`ns:Employees`.`ns1:Worker_Status`.`ns1:Regrettable_Termination`").cast("String"))
        .withColumn("cast38", col("`ns:Employees`.`ns1:Worker_Status`.`ns1:Return_Unknown`").cast("String"))
        .withColumn("cast39", col("`ns:Employees`.`ns1:Worker_Status`.`ns1:Terminated`").cast("String"))
        // Nuevos casteos
        //.withColumn("cast40",col("`relatedNacionalID`.`_isAdded`").cast("String"))
        //.withColumn("cast41",col("`relatedNacionalID`.`_isUpdated`").cast("String"))
        //.withColumn("cast42",col("`relatedOther`.`_isAdded`").cast("String"))
        //.withColumn("cast43",col("`relatedOther`.`_isUpdated`").cast("String"))
        // Mas nuevos canteos
        .withColumn("cast44", col("`ns:Employees`.`ns1:Leave_of_Absence`.`ns1:Age_of_Dependent`").cast("String"))
        .withColumn("cast45", col("`ns:Employees`.`ns1:Leave_of_Absence`.`ns1:Leave_Entitlement_Override`").cast("String"))
        .withColumn("cast46", col("`ns:Employees`.`ns1:Leave_of_Absence`.`ns1:Leave_Percentage`").cast("String"))
        .withColumn("cast47", col("`ns:Employees`.`ns1:Leave_of_Absence`.`ns1:Number_of_Babies_or_Adopted_Children`").cast("String"))
        .withColumn("cast48", col("`ns:Employees`.`ns1:Leave_of_Absence`.`ns1:Number_of_Child_Dependents`").cast("String"))
        .withColumn("cast49", col("`ns:Employees`.`ns1:Leave_of_Absence`.`ns1:Number_of_Previous_Births`").cast("String"))
        .withColumn("cast50", col("`ns:Employees`.`ns1:Leave_of_Absence`.`ns1:Number_of_Previous_Maternity_Leaves`").cast("String"))
        .withColumn("cast51", col("`supervisor`.`_isAdded`").cast("String"))
        .withColumn("cast52", col("`supervisor`.`_isDeleted`").cast("String"))
        .selectExpr(
          // Campos nuevos en inicio hasta collective agrement
          "`ns:Employees`.`ns1:Additional_Information`.`ns1:Cash_Range_Max` as Additional_Information_Cash_Range_Max",
          "`ns:Employees`.`ns1:Additional_Information`.`ns1:Cash_Range_Min` as Additional_Information_Cash_Range_Min",
          "`ns:Employees`.`ns1:Additional_Information`.`ns1:Company` as Additional_Information_Company",
          "`ns:Employees`.`ns1:Additional_Information`.`ns1:Country` as Additional_Information_Country",
          "`ns:Employees`.`ns1:Additional_Information`.`ns1:Organizational_Structure_Date` as Additional_Information_Organizational_Structure_Date",
          "`ns:Employees`.`ns1:Additional_Information`.`ns1:Organizational_Structure_ID` as Additional_Information_Organizational_Structure_ID",
          "`ns:Employees`.`ns1:Additional_Information`.`ns1:Organizational_Structure_Name` as Additional_Information_Organizational_Structure_Name",
          "`ns:Employees`.`ns1:Additional_Information`.`ns1:WorkerID` as Additional_Information_WorkerID",
          // Fin de campos nuevos inicio
          "`collective`.`ns1:Collective_Agreement` as Collective_Agreement",
          "`collective`.`ns1:Collective_Agreement_Factor`.`ns1:Factor` as Collective_Agreement_Factor",
          "`collective`.`ns1:Position_ID` as Collective_Agreement_Position_ID",
          "`collective`.`ns1:Start_Date` as Collective_Agreement_Start_Date",
          "`compensation`.`ns1:Compensation_Change_Reason` as Compensation_Change_Reason",
          "`compensation`.`ns1:Compensation_Grade` as Compensation_Grade",
          "`compensation`.`ns1:Compensation_Summary_Based_on_Compensation_Grade`.`ns1:Currency` as Compensation_Summary_Based_on_Compensation_Grade_Currency", //
          "`compensation`.`ns1:Compensation_Summary_Based_on_Compensation_Grade`.`ns1:Frequency` as Compensation_Summary_Based_on_Compensation_Grade_Frequency", //
          "`cast1` as Compensation_Summary_Based_on_Compensation_Grade_Primary_Compensation_Basis",
          "`cast2` as Compensation_Summary_Based_on_Compensation_Grade_Total_Base_Pay",
          "`compensation`.`ns1:Compensation_Summary_in_Annualized_Frequency`.`ns1:Currency` as Compensation_Summary_in_Annualized_Frequency_Currency",
          "`compensation`.`ns1:Compensation_Summary_in_Annualized_Frequency`.`ns1:Frequency` as Compensation_Summary_in_Annualized_Frequency_Frequency",
          "`cast3` as Compensation_Summary_in_Annualized_Frequency_Primary_Compensation_Basis",
          "`cast4` as Compensation_Summary_in_Annualized_Frequency_Primary_Total_Base_Pay",
          "`compensation`.`ns1:Position_ID` as Compensation_Position_ID",
          "`cast5` as Allowance_Plan_Amount",
          "'' as Allowance_Plan_Apply_FTE",
          "`allowance`.`ns1:Compensation_Plan` as Allowance_Plan_Compensation_Plan",
          "`allowance`.`ns1:Currency` as Allowance_Plan_Currency",
          "`allowance`.`ns1:End_Date` as Compensation_Plans_Frequency",
          "`allowance`.`ns1:Frequency` as Allowance_Plan_Frequency",
          "`allowance`.`ns1:Start_Date` as Allowance_Plan_Start_Date",
          "`cast7` as Compensation_Plans_Amount",
          "`bonus_plan`.`ns1:Compensation_Plan` as Compensation_Plans_Compensation_Plan",
          "`bonus_plan`.`ns1:Currency` as Compensation_Plans_Currency",
          // nuevos campos end date en estos 3 arrays
          "`bonus_plan`.`ns1:Frequency` as Compensation_Plans_Frequency",
          "`bonus_plan`.`ns1:End_Date` as Compensation_Plans_Frequency",
          "`cast8` as Compensation_Plans_Prorated_Amount",
          "`bonus_plan`.`ns1:Start_Date` as Compensation_Plans_Start_Date",
          // nuevos campos id y end datepositon
          "`compensationPlan`.`ns1:Position_End_Date` as Compensation_Plans_Position_End_Date",
          "`compensationPlan`.`ns1:Position_ID` as Compensation_Plans_Position_ID",
          "`cast9` as Salary_and_Hourly_Plan_Amount",
          "'' as Salary_and_Hourly_Plan_Apply_FTE",
          "`salary_plan`.`ns1:Compensation_Plan` as Salary_and_Hourly_Plan_Compensation_Plan",
          "`salary_plan`.`ns1:Currency` as Salary_and_Hourly_Plan_Currency",
          "`salary_plan`.`ns1:End_Date` as Compensation_Plans_Frequency",
          "`salary_plan`.`ns1:Frequency` as Salary_and_Hourly_Plan_Frequency",
          "`cast12` as Salary_and_Hourly_Plan_Prorated_Amount",
          "`salary_plan`.`ns1:Start_Date` as Salary_and_Hourly_Plan_Start_Date",
          "`ns:Employees`.`ns1:Derived_Event_Code` as Derived_Event_Code",
          "`ns:Employees`.`ns1:Effective_Moment` as Effective_Moment",
          "`contract`.`ns1:Collective_Agreement` as Employee_Contract_Collective_Agreement",
          "`contract`.`ns1:Contract_ID` as Employee_Contract_Contract_ID",
          "`contract`.`ns1:Contract_Status` as Employee_Contract_Contract_Status",
          "`contract`.`ns1:Contract_Type` as Contract_Type",
          "`contract`.`ns1:Description` as Employee_Contract_Description",
          "`contract`.`ns1:End_Date` as Employee_Contract_End_Date",
          "`cast13` as Employee_Contract_Maximum_Weekly_Hours",
          "`cast14` as Employee_Contract_Minimum_Weekly_Hours",
          "`contract`.`ns1:Position_ID` as Employee_Contract_Position_ID",
          "`contract`.`ns1:Start_Date` as Employee_Contract_Start_Date",
          "`ns:Employees`.`ns1:Entry_Moment` as Entry_Moment",
          // MUCHOS CAMPOS NUEVOS CON CASTEOS
          "`cast44` as Leave_of_Absence_Age_of_Dependent",
          "`ns:Employees`.`ns1:Leave_of_Absence`.`ns1:Child_Birth_Date` as Leave_of_Absence_Child_Birth_Date",
          "`ns:Employees`.`ns1:Leave_of_Absence`.`ns1:Estimated_Leave_End_Date` as Leave_of_Absence_Estimated_Leave_End_Date",
          "`cast45` as Leave_of_Absence_Leave_Entitlement_Override",
          "`ns:Employees`.`ns1:Leave_of_Absence`.`ns1:Leave_Last_Day_of_Work` as Leave_of_Absence_Age_Leave_Last_Day_of_Work",
          "`cast46` as Leave_of_Absence_Age_Leave_Percentage",
          "`ns:Employees`.`ns1:Leave_of_Absence`.`ns1:Leave_Reason` as Leave_of_Absence_Age_Leave_Reason",
          "`ns:Employees`.`ns1:Leave_of_Absence`.`ns1:Leave_Start_Date` as Leave_of_Absence_Age_Leave_Start_Date",
          "`ns:Employees`.`ns1:Leave_of_Absence`.`ns1:Leave_of_Absence_Type` as Leave_of_Absence_Leave_of_Absence_Type",
          "`cast47` as Leave_of_Absence_Number_of_Babies_or_Adopted_Children",
          "`cast48` as Leave_of_Absence_Age_Number_of_Child_Dependents",
          "`cast49` as Leave_of_Absence_Age_Number_of_Previous_Births",
          "`cast50` as Leave_of_Absence_Number_of_Previous_Maternity_Leaves",
          // finde los muchos campos nuevos
          "`ns:Employees`.`ns1:Person_Communication` as Person_Communication_Email",
          "'' as Person_Communication_Phone",
          "`natioId`.`ns1:Country` as National_Identifier_Country",
          // nuevos campos ID
          "'' as National_Identifier_Expiration_Date",
          "'' as National_Identifier_Issued_Date",
          // fin campos ID
          "`natioId`.`ns1:National_ID` as National_Identifier_National_ID",
          "`natioId`.`ns1:National_ID_Type` as National_Identifier_National_ID_Type",
          "`natioId`.`ns1:Verification_Date` as National_Identifier_Verification_Date",
          "`cast15` as National_Identifier_Verified_By",
          "`other`.`ns1:Custom_ID_Type` as Other_Identifier_Custom_ID_Type",
          "'' as Person_Identification_Passport",
          "'' as Person_Identification_Visa",
          "`ns:Employees`.`ns1:Personal`.`ns1:City_of_Birth` as Personal_City_of_Birth",
          "`ns:Employees`.`ns1:Personal`.`ns1:Country_of_Birth` as Personal_Country_of_Birth",
          "`ns:Employees`.`ns1:Personal`.`ns1:Date_of_Birth` as Personal_Date_of_Birth",
          "'' as Personal_Disability_Status",
          "`ns:Employees`.`ns1:Personal`.`ns1:Gender` as Personal_Gender",
          "'' as Personal_Local_Hukou",
          "`ns:Employees`.`ns1:Personal`.`ns1:Name`.`ns1:Country` as Personal_Name_Country",
          "`ns:Employees`.`ns1:Personal`.`ns1:Name`.`ns1:First_Name` as Personal_Name_First_Name",
          "`ns:Employees`.`ns1:Personal`.`ns1:Name`.`ns1:General_Display_Name` as Personal_Name_General_Display_Name",
          "`ns:Employees`.`ns1:Personal`.`ns1:Name`.`ns1:Last_Name` as Personal_Name_Last_Name",
          "'' as Personal_Name_Middle_Name",
          "`ns:Employees`.`ns1:Personal`.`ns1:Name`.`ns1:Secondary_Last_Name` as Personal_Name_Secondary_Last_Name",
          "`ns:Employees`.`ns1:Personal`.`ns1:Name`.`ns1:Title` as Personal_Name_Title",
          "`ns:Employees`.`ns1:Personal`.`ns1:Nationality` as Personal_Nationality",
          "`cast17` as Personal_Number_of_Payroll_Dependents",
          "`ns:Employees`.`ns1:Personal`.`ns1:Preferred_Language` as Personal_Preferred_Language",
          "`ns:Employees`.`ns1:Personal`.`ns1:Preferred_Locale` as Personal_Preferred_Locale",
          "`cast18` as Personal_Uses_Tobacco",
          "`ns:Employees`.`ns1:Personal`.`ns1:Workday_Account` as Personal_Workday_Account",
          "`position`.`ns1:Business_Site`.`ns1:Address_ID` as Address_ID",
          "`position`.`ns1:Business_Site`.`ns1:Address_Line_1` as Address_Line_1",
          "`position`.`ns1:Business_Site`.`ns1:Address_Line_3` as Address_Line_3",
          // nuevos campos Bussines_Site
          "`position`.`ns1:Business_Site`.`ns1:Address_Line_5` as Address_Line_5",
          // fin nuevos campos
          "`position`.`ns1:Business_Site`.`ns1:City` as Business_Site_City",
          "`position`.`ns1:Business_Site`.`ns1:Country` as Business_Site_Country",
          // nuevos campos Bussines_Site
          "`position`.`ns1:Business_Site`.`ns1:Country_Region` as Business_Site_Country_Region",
          // fin nuevos campos
          "`position`.`ns1:Business_Site`.`ns1:Location_ID` as Business_Site_Location_ID",
          "`position`.`ns1:Business_Site`.`ns1:Location_Name` as Business_Site_Location_Name",
          "`PostalCast` as Business_Site_Postal_Code",
          // nuevos campos Bussines_Site
          "`position`.`ns1:Business_Site`.`ns1:State_Province` as Business_Site_State_Province",
          // fin nuevos campos
          "`position`.`ns1:Job_Classification`.`ns1:Job_Classification_Description` as Job_Classification_Description",
          "`position`.`ns1:Job_Classification`.`ns1:Job_Classification_Group` as Job_Classification_Group",
          "`cast19` as Job_Classification_ID",
          "`position`.`ns1:Job_Classification`.`ns1:Mapped_Value` as Job_Classification_Mapped_Value",
          "`cast20` as Job_Family_ID",
          "`job_family`.`ns1:Mapped_Value` as Job_Family_Mapped_Value",
          "`job_family`.`ns1:Name` as Job_Family_Name",
          "`position`.`ns1:Job_Profile` as Position_Job_Profile",
          "`position`.`ns1:Management_Level` as Position_Management_Level",
          "`position`.`ns1:Position_ID` as Position_Position_ID",
          "`position`.`ns1:Position_Time_Type` as Position_Time_Type",
          "`cast51` as Supervisor_ID",
          "`cast52` as Supervisor_ID",
          "`SuperCast` as Supervisor_ID",
          "`supervisor`.`ns1:Name` as Supervisor_Name",
          "`supervisor`.`ns1:Other_ID` as Supervisor_Other_ID",
          "`ns:Employees`.`ns1:Qualifications`.`ns1:Certification_Achievement` as Qualifications_Certification_Achievement",
          "'' as Related_Person_Allowed_for_Tax_Deduction",
          "'' as Related_Person_Annual_Income_Amount",
          "'' as Related_Person_Beneficiary",
          // campos nuevos related_person
          "'' as Related_Person_Beneficiary_ID",
          //fin campos nuevos
          "'' as Related_Person_Birth_Date",
          "'' as Related_Person_Dependent",
          "'' as Related_Person_Dependent_ID",
          "'' as Related_Person_Emergency_Contact",
          // campos nuevos related_person
          "'' as Related_Person_Emergency_Contact_ID",
          //fin campos nuevos
          "'' as Related_Person_Full_Time_Student",
          "'' as Related_Person_Gender",
          "'' as Related_Person_Has_Health_Insurance",
          "'' as Related_Person_Is_Dependent_for_Payroll_Purposes",
          "'' as Related_Person_Is_Disabled",
          "'' as Legal_Name_Country",
          "'' as Legal_Name_First_Name",
          "'' as Legal_Name_General_Display_Name",
          "'' as Legal_Name_Last_Name",
          "'' as Legal_Name_Secondary_Last_Name",
          "'' as Related_Person_Lives_with_Worker",
          "'' as Related_Person_ID",
          "'' as Related_Person_Relationship_Type",
          "'' as Related_Person_Relative",
          "'' as Related_Person_Tobacco_Use",
          "'' as Related_Person_Communication_ID",
          // nuevos campos relatedPerson Identification
          "'' as Related_Person_isAdded",
          "'' as Related_Person_isUpdated",
          // fin nuevos campos
          "'' as Government_Identifier",
          "'' as Related_Person_Identification_Country",
          // nuevos campos relatedPerson Identification
          "'' as Related_Person_Identification_Expiration_Date",
          "'' as Related_Person_Identification_Issued_Date",
          // fin nuevos campos
          "'' as Related_Person_Identification_ID",
          "'' as Related_Person_Identification_National_ID_Type",
          // nuevos campos relatedPerson Identification
          "'' as Related_Person_Identification_Verification_Date",
          // fin nuevos campos
          // nuevos campos relatedPerson Identification other
          "'' as Related_Person_Identification_isAdded",
          "'' as Related_Person_Identification_isUpdated",
          // fin nuevos campos
          "'' as Related_Person_Identification_Custom_ID",
          "'' as Related_Person_Identification_Custom_ID_Type",
          "`IDCast` as Employee_ID",
          "`ns:Employees`.`ns1:Summary`.`ns1:Name` as Summary_Name",
          "`ns:Employees`.`ns1:Summary`.`ns1:WID` as WID",
          "`cast33` as Worker_Status_Active",
          "`ns:Employees`.`ns1:Worker_Status`.`ns1:Active_Status_Date` as Worker_Status_Active_Status_Date",
          // nuevos campos worker_status
          "`ns:Employees`.`ns1:Worker_Status`.`ns1:Benefits_Service_Date` as Worker_Status_Benefits_Service_Date",
          "`ns:Employees`.`ns1:Worker_Status`.`ns1:Company_Service_Date` as Worker_Status_Company_Service_Date",
          // fin worker_status
          "`ns:Employees`.`ns1:Worker_Status`.`ns1:Continuous_Service_Date` as Worker_Status_Continuous_Service_Date",
          // nuevo
          "'' as Worker_Status_End_Employment_Date",
          "`ns:Employees`.`ns1:Worker_Status`.`ns1:First_Day_of_Work` as Worker_Status_First_Day_of_Work",
          "`ns:Employees`.`ns1:Worker_Status`.`ns1:Hire_Date` as Worker_Status_Hire_Date",
          // nuevos campos worker_status
          "`ns:Employees`.`ns1:Worker_Status`.`ns1:Hire_Reason` as Worker_Status_Hire_Reason",
          // fin worker_status
          "`cast34` as Worker_Status_Hire_Rescinded",
          "`cast35` as Worker_Status_Is_Rehire",
          "`cast36` as Worker_Status_Not_Returning",
          "`ns:Employees`.`ns1:Worker_Status`.`ns1:Original_Hire_Date` as Worker_Status_Original_Hire_Date",
          "`cast37` as Worker_Status_Regrettable_Termination",
          "`cast38` as Worker_Status_Return_Unknown",
          "'' as Worker_Status_Secondary_Termination_Reason",
          "`ns:Employees`.`ns1:Worker_Status`.`ns1:Seniority_Date` as Worker_Status_Seniority_Date",
          // nuevos campos worker_status
          "`ns:Employees`.`ns1:Worker_Status`.`ns1:Severance_Date` as Worker_Status_Severance_Date",
          // fin worker_status
          "`ns:Employees`.`ns1:Worker_Status`.`ns1:Status` as Worker_Status_Status",
          "`cast39` as Worker_Status_Terminated",
          // nuevos campos worker_status
          "'' as Worker_Status_Time_Off_Service_Date",
          "`ns:Employees`.`ns1:Worker_Status`.`ns1:Vesting_Date` as Worker_Status_Vesting_Date",
          // fin worker_status
          "`ns:Employees`.`ns1:Worker_Status`.`ns1:Worker_Reference_Type` as Worker_Status_Worker_Reference_Type",
          s"'$dataTimeStampPartSplit' as data_date_part"
        ).na.fill(" ").distinct()

    }

    else {
      println("La partición insertada no coincide on las que existen actualmente")
    }

    println(s"Cantidad de filas en la iteración i=0")
    df_WeciFinal.cache()
    println(df_WeciFinal.count())

    df_WeciFinal.show()

    if (listFolders.length > 0){

      println("Imprimiendo todos los valores del for")
      for (i <- 1 until listFolders.length) {

        val dataTimeStampPartSplit_for = listFolders(i)
        val rutaCvDatalakeWorker_for = s"src/main/resources/rrhh/example_weci_consolid_wd/data_date_part=$dataDatePart/data_timestamp_part=$dataTimeStampPartSplit_for/*.xml"

        var df_WeciReadXML_All = spark.read
          .format("com.databricks.spark.xml")
          .option("excludeAttribute", "false")
          .option("rowTag", "ns1:EMPLOYEE_DELTA_INTEGRATION")
          .load(rutaCvDatalakeWorker_for)

        println("Imprimiendo el esquema de df_WeciReadXML_All")
        df_WeciReadXML_All.printSchema()

        var df_WeciFinal_All = df_WeciReadXML_All

        if (dataDatePart == "2021-05-17") {
          df_WeciFinal_All = df_WeciReadXML_All
            .withColumn("natioId", explode_outer(col("`ns:Employees`.`ns1:Person_Identification`.`ns1:National_Identifier`")))
            //.withColumn("relatedPerson", explode_outer(col("`ns:Employees`.`ns1:Related_Person`")))
            //.withColumn("relatedNacionalID", explode_outer(col("`ns:Employees`.`ns1:Related_Person_Identification`")))
            //.withColumn("allowance_plan", explode_outer(col("`ns:Employees`.`ns1:Compensation_Plans`.`ns1:Allowance_Plan`")))
            //.withColumn("job_family", explode_outer(col("`ns:Employees`.`ns1:Position`.`ns1:Job_Family`")))
            //.withColumn("related_communication", explode_outer(col("`ns:Employees`.`ns1:Related_Person_Communication`")))
            // nuevos withcolumn
            .withColumn("compensationPlan", explode_outer(col("`ns:Employees`.`ns1:Compensation_Plans`")))
            .withColumn("allowance", explode_outer(col("`compensationPlan`.`ns1:Allowance_Plan`")))
            .withColumn("bonus_plan", explode_outer(col("`compensationPlan`.`ns1:Bonus_Plan`")))
            .withColumn("salary_plan", explode_outer(col("`compensationPlan`.`ns1:Salary_and_Hourly_Plan`")))
            .withColumn("other", explode_outer(col("`ns:Employees`.`ns1:Person_Identification`.`ns1:Other_Identifier`")))
            .withColumn("position", explode_outer(col("`ns:Employees`.`ns1:Position`")))
            .withColumn("job_family", explode_outer(col("`position`.`ns1:Job_Family`")))
            .withColumn("compensation", explode_outer(col("`ns:Employees`.`ns1:Compensation`")))
            //.withColumn("relatedOther", explode_outer(col("`relatedNacionalID`.`ns1:Other_Identifier`")))
            // nuevos mas nuevos withcolumns
            .withColumn("collective", explode_outer(col("`ns:Employees`.`ns1:Collective_Agreement`")))
            .withColumn("contract", explode_outer(col("`ns:Employees`.`ns1:Employee_Contract`")))
            .withColumn("supervisor", explode_outer(col("`position`.`ns1:Supervisor`")))
            .withColumn("PostalCast", col("`position`.`ns1:Business_Site`.`ns1:Postal_Code`").cast("String"))
            .withColumn("SuperCast", col("`supervisor`.`ns1:ID`").cast("String"))
            .withColumn("IDCast", col("`ns:Employees`.`ns1:Summary`.`ns1:Employee_ID`").cast("String"))
            .withColumn("cast1", col("`compensation`.`ns1:Compensation_Summary_Based_on_Compensation_Grade`.`ns1:Primary_Compensation_Basis`").cast("String"))
            .withColumn("cast2", col("`compensation`.`ns1:Compensation_Summary_Based_on_Compensation_Grade`.`ns1:Total_Base_Pay`").cast("String"))
            .withColumn("cast3", col("`compensation`.`ns1:Compensation_Summary_in_Annualized_Frequency`.`ns1:Primary_Compensation_Basis`").cast("String"))
            .withColumn("cast4", col("`compensation`.`ns1:Compensation_Summary_in_Annualized_Frequency`.`ns1:Total_Base_Pay`").cast("String"))
            .withColumn("cast5", col("`allowance`.`ns1:Amount`").cast("String"))
            //.withColumn("cast6", col("`allowance`.`ns1:Apply_FTE`").cast("String"))
            .withColumn("cast7", col("`bonus_plan`.`ns1:Amount`").cast("String"))
            .withColumn("cast8", col("`bonus_plan`.`ns1:Prorated_Amount`").cast("String"))
            .withColumn("cast9", col("`salary_plan`.`ns1:Amount`").cast("String"))
            //.withColumn("cast11", col("`salary_plan`.`ns1:Apply_FTE`").cast("String"))
            .withColumn("cast12", col("`salary_plan`.`ns1:Prorated_Amount`").cast("String"))
            .withColumn("cast13", col("`contract`.`ns1:Maximum_Weekly_Hours`").cast("String"))
            .withColumn("cast14", col("`contract`.`ns1:Minimum_Weekly_Hours`").cast("String"))
            .withColumn("cast15", col("`natioId`.`ns1:Verified_By`").cast("String"))
            //.withColumn("cast16", col("`ns:Employees`.`ns1:Personal`.`ns1:Local_Hukou`").cast("String"))
            .withColumn("cast17", col("`ns:Employees`.`ns1:Personal`.`ns1:Number_of_Payroll_Dependents`").cast("String"))
            .withColumn("cast18", col("`ns:Employees`.`ns1:Personal`.`ns1:Uses_Tobacco`").cast("String"))
            .withColumn("cast19", col("`position`.`ns1:Job_Classification`.`ns1:Job_Classification_ID`").cast("String"))
            .withColumn("cast20", col("`job_family`.`ns1:ID`").cast("String"))
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
            .withColumn("cast33", col("`ns:Employees`.`ns1:Worker_Status`.`ns1:Active`").cast("String"))
            .withColumn("cast34", col("`ns:Employees`.`ns1:Worker_Status`.`ns1:Hire_Rescinded`").cast("String"))
            .withColumn("cast35", col("`ns:Employees`.`ns1:Worker_Status`.`ns1:Is_Rehire`").cast("String"))
            .withColumn("cast36", col("`ns:Employees`.`ns1:Worker_Status`.`ns1:Not_Returning`").cast("String"))
            .withColumn("cast37", col("`ns:Employees`.`ns1:Worker_Status`.`ns1:Regrettable_Termination`").cast("String"))
            .withColumn("cast38", col("`ns:Employees`.`ns1:Worker_Status`.`ns1:Return_Unknown`").cast("String"))
            .withColumn("cast39", col("`ns:Employees`.`ns1:Worker_Status`.`ns1:Terminated`").cast("String"))
            // Nuevos casteos
            //.withColumn("cast40",col("`relatedNacionalID`.`_isAdded`").cast("String"))
            //.withColumn("cast41",col("`relatedNacionalID`.`_isUpdated`").cast("String"))
            //.withColumn("cast42",col("`relatedOther`.`_isAdded`").cast("String"))
            //.withColumn("cast43",col("`relatedOther`.`_isUpdated`").cast("String"))
            // Mas nuevos canteos
            .withColumn("cast44", col("`ns:Employees`.`ns1:Leave_of_Absence`.`ns1:Age_of_Dependent`").cast("String"))
            .withColumn("cast45", col("`ns:Employees`.`ns1:Leave_of_Absence`.`ns1:Leave_Entitlement_Override`").cast("String"))
            .withColumn("cast46", col("`ns:Employees`.`ns1:Leave_of_Absence`.`ns1:Leave_Percentage`").cast("String"))
            .withColumn("cast47", col("`ns:Employees`.`ns1:Leave_of_Absence`.`ns1:Number_of_Babies_or_Adopted_Children`").cast("String"))
            .withColumn("cast48", col("`ns:Employees`.`ns1:Leave_of_Absence`.`ns1:Number_of_Child_Dependents`").cast("String"))
            .withColumn("cast49", col("`ns:Employees`.`ns1:Leave_of_Absence`.`ns1:Number_of_Previous_Births`").cast("String"))
            .withColumn("cast50", col("`ns:Employees`.`ns1:Leave_of_Absence`.`ns1:Number_of_Previous_Maternity_Leaves`").cast("String"))
            .withColumn("cast51", col("`supervisor`.`_isAdded`").cast("String"))
            .withColumn("cast52", col("`supervisor`.`_isDeleted`").cast("String"))
            .selectExpr(
              // Campos nuevos en inicio hasta collective agrement
              "`ns:Employees`.`ns1:Additional_Information`.`ns1:Cash_Range_Max` as Additional_Information_Cash_Range_Max",
              "`ns:Employees`.`ns1:Additional_Information`.`ns1:Cash_Range_Min` as Additional_Information_Cash_Range_Min",
              "`ns:Employees`.`ns1:Additional_Information`.`ns1:Company` as Additional_Information_Company",
              "`ns:Employees`.`ns1:Additional_Information`.`ns1:Country` as Additional_Information_Country",
              "`ns:Employees`.`ns1:Additional_Information`.`ns1:Organizational_Structure_Date` as Additional_Information_Organizational_Structure_Date",
              "`ns:Employees`.`ns1:Additional_Information`.`ns1:Organizational_Structure_ID` as Additional_Information_Organizational_Structure_ID",
              "`ns:Employees`.`ns1:Additional_Information`.`ns1:Organizational_Structure_Name` as Additional_Information_Organizational_Structure_Name",
              "`ns:Employees`.`ns1:Additional_Information`.`ns1:WorkerID` as Additional_Information_WorkerID",
              // Fin de campos nuevos inicio
              "`collective`.`ns1:Collective_Agreement` as Collective_Agreement",
              "`collective`.`ns1:Collective_Agreement_Factor`.`ns1:Factor` as Collective_Agreement_Factor",
              "`collective`.`ns1:Position_ID` as Collective_Agreement_Position_ID",
              "`collective`.`ns1:Start_Date` as Collective_Agreement_Start_Date",
              "`compensation`.`ns1:Compensation_Change_Reason` as Compensation_Change_Reason",
              "`compensation`.`ns1:Compensation_Grade` as Compensation_Grade",
              "`compensation`.`ns1:Compensation_Summary_Based_on_Compensation_Grade`.`ns1:Currency` as Compensation_Summary_Based_on_Compensation_Grade_Currency", //
              "`compensation`.`ns1:Compensation_Summary_Based_on_Compensation_Grade`.`ns1:Frequency` as Compensation_Summary_Based_on_Compensation_Grade_Frequency", //
              "`cast1` as Compensation_Summary_Based_on_Compensation_Grade_Primary_Compensation_Basis",
              "`cast2` as Compensation_Summary_Based_on_Compensation_Grade_Total_Base_Pay",
              "`compensation`.`ns1:Compensation_Summary_in_Annualized_Frequency`.`ns1:Currency` as Compensation_Summary_in_Annualized_Frequency_Currency",
              "`compensation`.`ns1:Compensation_Summary_in_Annualized_Frequency`.`ns1:Frequency` as Compensation_Summary_in_Annualized_Frequency_Frequency",
              "`cast3` as Compensation_Summary_in_Annualized_Frequency_Primary_Compensation_Basis",
              "`cast4` as Compensation_Summary_in_Annualized_Frequency_Primary_Total_Base_Pay",
              "`compensation`.`ns1:Position_ID` as Compensation_Position_ID",
              "`cast5` as Allowance_Plan_Amount",
              "'' as Allowance_Plan_Apply_FTE",
              "`allowance`.`ns1:Compensation_Plan` as Allowance_Plan_Compensation_Plan",
              "`allowance`.`ns1:Currency` as Allowance_Plan_Currency",
              "`allowance`.`ns1:End_Date` as Compensation_Plans_Frequency",
              "`allowance`.`ns1:Frequency` as Allowance_Plan_Frequency",
              "`allowance`.`ns1:Start_Date` as Allowance_Plan_Start_Date",
              "`cast7` as Compensation_Plans_Amount",
              "`bonus_plan`.`ns1:Compensation_Plan` as Compensation_Plans_Compensation_Plan",
              "`bonus_plan`.`ns1:Currency` as Compensation_Plans_Currency",
              // nuevos campos end date en estos 3 arrays
              "`bonus_plan`.`ns1:Frequency` as Compensation_Plans_Frequency",
              "`bonus_plan`.`ns1:End_Date` as Compensation_Plans_Frequency",
              "`cast8` as Compensation_Plans_Prorated_Amount",
              "`bonus_plan`.`ns1:Start_Date` as Compensation_Plans_Start_Date",
              // nuevos campos id y end datepositon
              "`compensationPlan`.`ns1:Position_End_Date` as Compensation_Plans_Position_End_Date",
              "`compensationPlan`.`ns1:Position_ID` as Compensation_Plans_Position_ID",
              "`cast9` as Salary_and_Hourly_Plan_Amount",
              "'' as Salary_and_Hourly_Plan_Apply_FTE",
              "`salary_plan`.`ns1:Compensation_Plan` as Salary_and_Hourly_Plan_Compensation_Plan",
              "`salary_plan`.`ns1:Currency` as Salary_and_Hourly_Plan_Currency",
              "`salary_plan`.`ns1:End_Date` as Compensation_Plans_Frequency",
              "`salary_plan`.`ns1:Frequency` as Salary_and_Hourly_Plan_Frequency",
              "`cast12` as Salary_and_Hourly_Plan_Prorated_Amount",
              "`salary_plan`.`ns1:Start_Date` as Salary_and_Hourly_Plan_Start_Date",
              "`ns:Employees`.`ns1:Derived_Event_Code` as Derived_Event_Code",
              "`ns:Employees`.`ns1:Effective_Moment` as Effective_Moment",
              "`contract`.`ns1:Collective_Agreement` as Employee_Contract_Collective_Agreement",
              "`contract`.`ns1:Contract_ID` as Employee_Contract_Contract_ID",
              "`contract`.`ns1:Contract_Status` as Employee_Contract_Contract_Status",
              "`contract`.`ns1:Contract_Type` as Contract_Type",
              "`contract`.`ns1:Description` as Employee_Contract_Description",
              "`contract`.`ns1:End_Date` as Employee_Contract_End_Date",
              "`cast13` as Employee_Contract_Maximum_Weekly_Hours",
              "`cast14` as Employee_Contract_Minimum_Weekly_Hours",
              "`contract`.`ns1:Position_ID` as Employee_Contract_Position_ID",
              "`contract`.`ns1:Start_Date` as Employee_Contract_Start_Date",
              "`ns:Employees`.`ns1:Entry_Moment` as Entry_Moment",
              // MUCHOS CAMPOS NUEVOS CON CASTEOS
              "`cast44` as Leave_of_Absence_Age_of_Dependent",
              "`ns:Employees`.`ns1:Leave_of_Absence`.`ns1:Child_Birth_Date` as Leave_of_Absence_Child_Birth_Date",
              "`ns:Employees`.`ns1:Leave_of_Absence`.`ns1:Estimated_Leave_End_Date` as Leave_of_Absence_Estimated_Leave_End_Date",
              "`cast45` as Leave_of_Absence_Leave_Entitlement_Override",
              "`ns:Employees`.`ns1:Leave_of_Absence`.`ns1:Leave_Last_Day_of_Work` as Leave_of_Absence_Age_Leave_Last_Day_of_Work",
              "`cast46` as Leave_of_Absence_Age_Leave_Percentage",
              "`ns:Employees`.`ns1:Leave_of_Absence`.`ns1:Leave_Reason` as Leave_of_Absence_Age_Leave_Reason",
              "`ns:Employees`.`ns1:Leave_of_Absence`.`ns1:Leave_Start_Date` as Leave_of_Absence_Age_Leave_Start_Date",
              "`ns:Employees`.`ns1:Leave_of_Absence`.`ns1:Leave_of_Absence_Type` as Leave_of_Absence_Leave_of_Absence_Type",
              "`cast47` as Leave_of_Absence_Number_of_Babies_or_Adopted_Children",
              "`cast48` as Leave_of_Absence_Age_Number_of_Child_Dependents",
              "`cast49` as Leave_of_Absence_Age_Number_of_Previous_Births",
              "`cast50` as Leave_of_Absence_Number_of_Previous_Maternity_Leaves",
              // finde los muchos campos nuevos
              "`ns:Employees`.`ns1:Person_Communication` as Person_Communication_Email",
              "'' as Person_Communication_Phone",
              "`natioId`.`ns1:Country` as National_Identifier_Country",
              // nuevos campos ID
              "'' as National_Identifier_Expiration_Date",
              "'' as National_Identifier_Issued_Date",
              // fin campos ID
              "`natioId`.`ns1:National_ID` as National_Identifier_National_ID",
              "`natioId`.`ns1:National_ID_Type` as National_Identifier_National_ID_Type",
              "`natioId`.`ns1:Verification_Date` as National_Identifier_Verification_Date",
              "`cast15` as National_Identifier_Verified_By",
              "`other`.`ns1:Custom_ID_Type` as Other_Identifier_Custom_ID_Type",
              "'' as Person_Identification_Passport",
              "'' as Person_Identification_Visa",
              "`ns:Employees`.`ns1:Personal`.`ns1:City_of_Birth` as Personal_City_of_Birth",
              "`ns:Employees`.`ns1:Personal`.`ns1:Country_of_Birth` as Personal_Country_of_Birth",
              "`ns:Employees`.`ns1:Personal`.`ns1:Date_of_Birth` as Personal_Date_of_Birth",
              "'' as Personal_Disability_Status",
              "`ns:Employees`.`ns1:Personal`.`ns1:Gender` as Personal_Gender",
              "'' as Personal_Local_Hukou",
              "`ns:Employees`.`ns1:Personal`.`ns1:Name`.`ns1:Country` as Personal_Name_Country",
              "`ns:Employees`.`ns1:Personal`.`ns1:Name`.`ns1:First_Name` as Personal_Name_First_Name",
              "`ns:Employees`.`ns1:Personal`.`ns1:Name`.`ns1:General_Display_Name` as Personal_Name_General_Display_Name",
              "`ns:Employees`.`ns1:Personal`.`ns1:Name`.`ns1:Last_Name` as Personal_Name_Last_Name",
              "'' as Personal_Name_Middle_Name",
              "`ns:Employees`.`ns1:Personal`.`ns1:Name`.`ns1:Secondary_Last_Name` as Personal_Name_Secondary_Last_Name",
              "`ns:Employees`.`ns1:Personal`.`ns1:Name`.`ns1:Title` as Personal_Name_Title",
              "`ns:Employees`.`ns1:Personal`.`ns1:Nationality` as Personal_Nationality",
              "`cast17` as Personal_Number_of_Payroll_Dependents",
              "`ns:Employees`.`ns1:Personal`.`ns1:Preferred_Language` as Personal_Preferred_Language",
              "`ns:Employees`.`ns1:Personal`.`ns1:Preferred_Locale` as Personal_Preferred_Locale",
              "`cast18` as Personal_Uses_Tobacco",
              "`ns:Employees`.`ns1:Personal`.`ns1:Workday_Account` as Personal_Workday_Account",
              "`position`.`ns1:Business_Site`.`ns1:Address_ID` as Address_ID",
              "`position`.`ns1:Business_Site`.`ns1:Address_Line_1` as Address_Line_1",
              "`position`.`ns1:Business_Site`.`ns1:Address_Line_3` as Address_Line_3",
              // nuevos campos Bussines_Site
              "`position`.`ns1:Business_Site`.`ns1:Address_Line_5` as Address_Line_5",
              // fin nuevos campos
              "`position`.`ns1:Business_Site`.`ns1:City` as Business_Site_City",
              "`position`.`ns1:Business_Site`.`ns1:Country` as Business_Site_Country",
              // nuevos campos Bussines_Site
              "`position`.`ns1:Business_Site`.`ns1:Country_Region` as Business_Site_Country_Region",
              // fin nuevos campos
              "`position`.`ns1:Business_Site`.`ns1:Location_ID` as Business_Site_Location_ID",
              "`position`.`ns1:Business_Site`.`ns1:Location_Name` as Business_Site_Location_Name",
              "`PostalCast` as Business_Site_Postal_Code",
              // nuevos campos Bussines_Site
              "`position`.`ns1:Business_Site`.`ns1:State_Province` as Business_Site_State_Province",
              // fin nuevos campos
              "`position`.`ns1:Job_Classification`.`ns1:Job_Classification_Description` as Job_Classification_Description",
              "`position`.`ns1:Job_Classification`.`ns1:Job_Classification_Group` as Job_Classification_Group",
              "`cast19` as Job_Classification_ID",
              "`position`.`ns1:Job_Classification`.`ns1:Mapped_Value` as Job_Classification_Mapped_Value",
              "`cast20` as Job_Family_ID",
              "`job_family`.`ns1:Mapped_Value` as Job_Family_Mapped_Value",
              "`job_family`.`ns1:Name` as Job_Family_Name",
              "`position`.`ns1:Job_Profile` as Position_Job_Profile",
              "`position`.`ns1:Management_Level` as Position_Management_Level",
              "`position`.`ns1:Position_ID` as Position_Position_ID",
              "`position`.`ns1:Position_Time_Type` as Position_Time_Type",
              "`cast51` as Supervisor_ID",
              "`cast52` as Supervisor_ID",
              "`SuperCast` as Supervisor_ID",
              "`supervisor`.`ns1:Name` as Supervisor_Name",
              "`supervisor`.`ns1:Other_ID` as Supervisor_Other_ID",
              "`ns:Employees`.`ns1:Qualifications`.`ns1:Certification_Achievement` as Qualifications_Certification_Achievement",
              "'' as Related_Person_Allowed_for_Tax_Deduction",
              "'' as Related_Person_Annual_Income_Amount",
              "'' as Related_Person_Beneficiary",
              // campos nuevos related_person
              "'' as Related_Person_Beneficiary_ID",
              //fin campos nuevos
              "'' as Related_Person_Birth_Date",
              "'' as Related_Person_Dependent",
              "'' as Related_Person_Dependent_ID",
              "'' as Related_Person_Emergency_Contact",
              // campos nuevos related_person
              "'' as Related_Person_Emergency_Contact_ID",
              //fin campos nuevos
              "'' as Related_Person_Full_Time_Student",
              "'' as Related_Person_Gender",
              "'' as Related_Person_Has_Health_Insurance",
              "'' as Related_Person_Is_Dependent_for_Payroll_Purposes",
              "'' as Related_Person_Is_Disabled",
              "'' as Legal_Name_Country",
              "'' as Legal_Name_First_Name",
              "'' as Legal_Name_General_Display_Name",
              "'' as Legal_Name_Last_Name",
              "'' as Legal_Name_Secondary_Last_Name",
              "'' as Related_Person_Lives_with_Worker",
              "'' as Related_Person_ID",
              "'' as Related_Person_Relationship_Type",
              "'' as Related_Person_Relative",
              "'' as Related_Person_Tobacco_Use",
              "'' as Related_Person_Communication_ID",
              // nuevos campos relatedPerson Identification
              "'' as Related_Person_isAdded",
              "'' as Related_Person_isUpdated",
              // fin nuevos campos
              "'' as Government_Identifier",
              "'' as Related_Person_Identification_Country",
              // nuevos campos relatedPerson Identification
              "'' as Related_Person_Identification_Expiration_Date",
              "'' as Related_Person_Identification_Issued_Date",
              // fin nuevos campos
              "'' as Related_Person_Identification_ID",
              "'' as Related_Person_Identification_National_ID_Type",
              // nuevos campos relatedPerson Identification
              "'' as Related_Person_Identification_Verification_Date",
              // fin nuevos campos
              // nuevos campos relatedPerson Identification other
              "'' as Related_Person_Identification_isAdded",
              "'' as Related_Person_Identification_isUpdated",
              // fin nuevos campos
              "'' as Related_Person_Identification_Custom_ID",
              "'' as Related_Person_Identification_Custom_ID_Type",
              "`IDCast` as Employee_ID",
              "`ns:Employees`.`ns1:Summary`.`ns1:Name` as Summary_Name",
              "`ns:Employees`.`ns1:Summary`.`ns1:WID` as WID",
              "`cast33` as Worker_Status_Active",
              "`ns:Employees`.`ns1:Worker_Status`.`ns1:Active_Status_Date` as Worker_Status_Active_Status_Date",
              // nuevos campos worker_status
              "`ns:Employees`.`ns1:Worker_Status`.`ns1:Benefits_Service_Date` as Worker_Status_Benefits_Service_Date",
              "`ns:Employees`.`ns1:Worker_Status`.`ns1:Company_Service_Date` as Worker_Status_Company_Service_Date",
              // fin worker_status
              "`ns:Employees`.`ns1:Worker_Status`.`ns1:Continuous_Service_Date` as Worker_Status_Continuous_Service_Date",
              // nuevo
              "'' as Worker_Status_End_Employment_Date",
              "`ns:Employees`.`ns1:Worker_Status`.`ns1:First_Day_of_Work` as Worker_Status_First_Day_of_Work",
              "`ns:Employees`.`ns1:Worker_Status`.`ns1:Hire_Date` as Worker_Status_Hire_Date",
              // nuevos campos worker_status
              "`ns:Employees`.`ns1:Worker_Status`.`ns1:Hire_Reason` as Worker_Status_Hire_Reason",
              // fin worker_status
              "`cast34` as Worker_Status_Hire_Rescinded",
              "`cast35` as Worker_Status_Is_Rehire",
              "`cast36` as Worker_Status_Not_Returning",
              "`ns:Employees`.`ns1:Worker_Status`.`ns1:Original_Hire_Date` as Worker_Status_Original_Hire_Date",
              "`cast37` as Worker_Status_Regrettable_Termination",
              "`cast38` as Worker_Status_Return_Unknown",
              "'' as Worker_Status_Secondary_Termination_Reason",
              "`ns:Employees`.`ns1:Worker_Status`.`ns1:Seniority_Date` as Worker_Status_Seniority_Date",
              // nuevos campos worker_status
              "`ns:Employees`.`ns1:Worker_Status`.`ns1:Severance_Date` as Worker_Status_Severance_Date",
              // fin worker_status
              "`ns:Employees`.`ns1:Worker_Status`.`ns1:Status` as Worker_Status_Status",
              "`cast39` as Worker_Status_Terminated",
              // nuevos campos worker_status
              "'' as Worker_Status_Time_Off_Service_Date",
              "`ns:Employees`.`ns1:Worker_Status`.`ns1:Vesting_Date` as Worker_Status_Vesting_Date",
              // fin worker_status
              "`ns:Employees`.`ns1:Worker_Status`.`ns1:Worker_Reference_Type` as Worker_Status_Worker_Reference_Type",
              s"'$dataTimeStampPartSplit_for' as data_date_part"
            ).na.fill(" ").distinct()

        }

        else {
          println("La partición insertada no coincide on las que existen actualmente")
        }

        println(s"Cantidad de filas en la iteración i=$i")
        df_WeciFinal_All.cache()
        println(df_WeciFinal_All.count())

        println(s"Realizando la union en la iteración i=$i")

        df_WeciFinal = df_WeciFinal.union(df_WeciFinal_All)

        df_WeciFinal.show()


      } //End for

    } //End if




  } //End Main

} //End Class
