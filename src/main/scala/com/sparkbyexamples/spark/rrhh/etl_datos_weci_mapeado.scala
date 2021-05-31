package com.sparkbyexamples.spark.rrhh

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, StringType, StructField, StructType}
import org.apache.spark.sql.expressions.Window


object etl_datos_weci_mapeado {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local[1]")
      .appName("SparkByExample")
      .getOrCreate()


    //Trabajando con la nueva integración de weci
    //val dataDatePart = "2021-04-26"
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
    //val dataDatePart = "2021-05-19-11-48"

    //XML definitivos con la misma estructura
    //val dataDatePart = "2021-05-20-17-19"
    //val dataDatePart = "2021-05-20-17-19-38"
    //val dataDatePart = "2021-05-21-15-20-06"
    //val dataDatePart = "2021-05-25-05-16-06"
    //val dataDatePart = "2021-05-25-11-22-06"
    val dataDatePart = "2021-05-26-15-33-06"


    val rutaCvDatalakeWorker = s"src/main/resources/rrhh/example_weci_consolid_wd/data_date_part=$dataDatePart/*.xml"
    val rutaWeciAllBetaXml = s"src/main/resources/rrhh/example_weci_consolid_wd/data_date_part=Beta/*.xml"
    val rutaWeciAllPayRollXml = s"src/main/resources/rrhh/example_weci_consolid_wd/data_date_part=EMPLOYEE_DELTA_INTEGRATION/*.xml"

    val schema = StructType(
      Array(
        StructField("Worker", ArrayType(StructType(Array(
          StructField("Worker_Summary", StructType(Seq(
            StructField("WID", StructType(Seq(
              StructField("_VALUE", StringType, true),
            ))),
            StructField("Employee_ID", StructType(Seq(
              StructField("_VALUE", StringType, true),
            )))
          ))),
          StructField("Employees", StructType(Seq(
            StructField("Person_Communication", StructType(Seq(
              StructField("Address", ArrayType(StructType(Array(
                StructField("Address_Line_1", StructType(Seq(
                  StructField("_VALUE", StringType, true),
                ))),
                StructField("Address_Line_3", StructType(Seq(
                  StructField("_VALUE", StringType, true),
                ))),
                StructField("Address_Line_5", StructType(Seq(
                  StructField("_VALUE", StringType, true),
                ))),
                StructField("Address_Line_8", StructType(Seq(
                  StructField("_VALUE", StringType, true),
                ))),
                StructField("Address_Line_9", StructType(Seq(
                  StructField("_VALUE", StringType, true),
                ))),
                StructField("City", StructType(Seq(
                  StructField("_VALUE", StringType, true),
                ))),
                StructField("Postal_Code", StructType(Seq(
                  StructField("_VALUE", StringType, true),
                ))),
                StructField("Country", StructType(Seq(
                  StructField("_VALUE", StringType, true),
                ))),
                StructField("State_Province", StructType(Seq(
                  StructField("_VALUE", StringType, true),
                ))),
                StructField("Usage", StructType(Seq(
                  StructField("Usage_Behavior_ID", StructType(Seq(
                    StructField("_VALUE", StringType, true),
                  ))),
                ))),
              )))),
              StructField("Phone", ArrayType(StructType(Array(
                StructField("Usage_Type", StructType(Seq(
                  StructField("_VALUE", StringType, true),
                ))),
                StructField("Phone_Device_Type", StructType(Seq(
                  StructField("_VALUE", StringType, true),
                ))),
                StructField("International_Phone_Code", StructType(Seq(
                  StructField("_VALUE", StringType, true),
                ))),
                StructField("Phone_Number", StructType(Seq(
                  StructField("_VALUE", StringType, true),
                ))),
              )))),
              StructField("Email", ArrayType(StructType(Array(
                StructField("Usage_Type", StructType(Seq(
                  StructField("_VALUE", StringType, true),
                ))),
                StructField("Email_Address", StructType(Seq(
                  StructField("_VALUE", StringType, true),
                ))),
              )))),
            ))),
            StructField("Employee_Contract", StructType(Seq(
              StructField("Contract_Type", StructType(Seq(
                StructField("_VALUE", StringType, true),
              ))),
              StructField("Contract_Status", StructType(Seq(
                StructField("_VALUE", StringType, true),
              ))),
              StructField("Start_Date", StructType(Seq(
                StructField("_VALUE", StringType, true),
              ))),
              StructField("End_Date", StructType(Seq(
                StructField("_VALUE", StringType, true),
              ))),
            ))),
            StructField("Payment_Election", StructType(Seq(
              StructField("Bank_Account_Name", StructType(Seq(
                StructField("_VALUE", StringType, true),
              ))),
              StructField("IBAN", StructType(Seq(
                StructField("_VALUE", StringType, true),
              ))),
            ))),
            StructField("Compensation", StructType(Seq(
              StructField("Compensation_Grade", StructType(Seq(
                StructField("_VALUE", StringType, true),
              ))),
              StructField("Compensation_Summary_in_Annualized_Frequency", StructType(Seq(
                StructField("Total_Base_Pay", StructType(Seq(
                  StructField("_VALUE", StringType, true),
                ))),
                StructField("Currency", StructType(Seq(
                  StructField("_VALUE", StringType, true),
                ))),
                StructField("Frecuency", StructType(Seq(
                  StructField("_VALUE", StringType, true),
                ))),
              ))),
            ))),
            StructField("Person_Identification", StructType(Seq(
              StructField("Visa_Identifier", StructType(Seq(
                StructField("Country", StructType(Seq(
                  StructField("_VALUE", StringType, true),
                ))),
                StructField("Visa_ID_Type", StructType(Seq(
                  StructField("_VALUE", StringType, true),
                ))),
                StructField("Visa_ID", StructType(Seq(
                  StructField("_VALUE", StringType, true),
                ))),
              ))),
              StructField("Passport_Identifier", StructType(Seq(
                StructField("Country", StructType(Seq(
                  StructField("_VALUE", StringType, true),
                ))),
                StructField("Passport_ID_Type", StructType(Seq(
                  StructField("_VALUE", StringType, true),
                ))),
                StructField("Passport_ID", StructType(Seq(
                  StructField("_VALUE", StringType, true),
                ))),
              ))),
              StructField("National_Identifier", ArrayType(StructType(Array(
                StructField("Country", StructType(Seq(
                  StructField("_VALUE", StringType, true),
                ))),
                StructField("National_ID_Type", StructType(Seq(
                  StructField("_VALUE", StringType, true),
                ))),
                StructField("National_Identifier", StructType(Seq(
                  StructField("_VALUE", StringType, true),
                ))),
                StructField("National_ID", StructType(Seq(
                  StructField("_VALUE", StringType, true),
                ))),
                StructField("First_Name", StructType(Seq(
                  StructField("_VALUE", StringType, true),
                ))),
              )))),
              StructField("Other_Identifier", ArrayType(StructType(Array(
                StructField("Custom_ID", StructType(Seq(
                  StructField("_VALUE", StringType, true),
                ))),
                StructField("Custom_ID_Type", StructType(Seq(
                  StructField("_VALUE", StringType, true),
                ))),
              )))),
            ))),
            StructField("Position", StructType(Seq(
              StructField("Management_Level", StructType(Seq(
                StructField("_VALUE", StringType, true),
              ))),
              StructField("Job_Profile", StructType(Seq(
                StructField("_VALUE", StringType, true),
              ))),
              StructField("Position_ID", StructType(Seq(
                StructField("_VALUE", StringType, true),
              ))),
              StructField("Business_Title", StructType(Seq(
                StructField("_VALUE", StringType, true),
              ))),
              StructField("Probation_Start_Date", StructType(Seq(
                StructField("_VALUE", StringType, true),
              ))),
              StructField("Probation_End_Date", StructType(Seq(
                StructField("_VALUE", StringType, true),
              ))),
              StructField("Position_Time_Type", StructType(Seq(
                StructField("_VALUE", StringType, true),
              ))),
              StructField("Job_Family", ArrayType(StructType(Array(
                StructField("ID", StructType(Seq(
                  StructField("_VALUE", StringType, true),
                ))),
                StructField("Name", StructType(Seq(
                  StructField("_VALUE", StringType, true),
                ))),
              )))),
              StructField("Job_Classification", StructType(Seq(
                StructField("Job_Classiication", StructType(Seq(
                  StructField("_VALUE", StringType, true),
                ))),
              ))),
              StructField("Organization", StructType(Seq(
                StructField("Organization_Type", StructType(Seq(
                  StructField("_VALUE", StringType, true),
                ))),
                StructField("Organization_Code", StructType(Seq(
                  StructField("_VALUE", StringType, true),
                ))),
                StructField("Organization_Name", StructType(Seq(
                  StructField("_VALUE", StringType, true),
                ))),
              ))),
              StructField("Supervisor", StructType(Seq(
                StructField("ID", StructType(Seq(
                  StructField("_VALUE", StringType, true),
                ))),
                StructField("Name", StructType(Seq(
                  StructField("_VALUE", StringType, true),
                ))),
              ))),
            ))),
            StructField("Personal", StructType(Seq(
              StructField("Workday_Account", StructType(Seq(
                StructField("_VALUE", StringType, true),
              ))),
              StructField("Date_of_Birth", StructType(Seq(
                StructField("_VALUE", StringType, true),
              ))),
              StructField("Date_of_Death", StructType(Seq(
                StructField("_VALUE", StringType, true),
              ))),
              StructField("Country_of_Birth", StructType(Seq(
                StructField("_VALUE", StringType, true),
              ))),
              StructField("City_of_Birth", StructType(Seq(
                StructField("_VALUE", StringType, true),
              ))),
              StructField("Region_of_Birth", StructType(Seq(
                StructField("_VALUE", StringType, true),
              ))),
              StructField("Nationality", StructType(Seq(
                StructField("_VALUE", StringType, true),
              ))),
              StructField("Preferred_Lenguage", StructType(Seq(
                StructField("_VALUE", StringType, true),
              ))),
              StructField("Gender", StructType(Seq(
                StructField("_VALUE", StringType, true),
              ))),
              StructField("Marital_Status", StructType(Seq(
                StructField("_VALUE", StringType, true),
              ))),
              StructField("Marital_Status_Date", StructType(Seq(
                StructField("_VALUE", StringType, true),
              ))),
              StructField("Name", StructType(Seq( // Name == legal_name en xml
                StructField("First_Name", StructType(Seq(
                  StructField("_VALUE", StringType, true),
                ))),
                StructField("Middle_Name", StructType(Seq(
                  StructField("_VALUE", StringType, true),
                ))),
                StructField("Last_Name", StructType(Seq(
                  StructField("_VALUE", StringType, true),
                ))),
                StructField("Secondary_Last_Name", StructType(Seq(
                  StructField("_VALUE", StringType, true),
                ))),
              ))),
              StructField("Disability_Status", StructType(Seq(
                StructField("Grade", StructType(Seq(
                  StructField("_VALUE", StringType, true),
                ))),
                StructField("Disability", StructType(Seq(
                  StructField("_VALUE", StringType, true),
                ))),
                StructField("Certification_Authority", StructType(Seq(
                  StructField("_VALUE", StringType, true),
                ))),
                StructField("Disability_Status_Date", StructType(Seq(
                  StructField("_VALUE", StringType, true),
                ))),
                StructField("End_Date", StructType(Seq(
                  StructField("_VALUE", StringType, true),
                ))),
                StructField("Date_Known", StructType(Seq(
                  StructField("_VALUE", StringType, true),
                ))),
              ))),
            ))),
            StructField("Worker_Status", StructType(Seq(
              StructField("Active", StructType(Seq(
                StructField("_VALUE", StringType, true),
              ))),
              StructField("Status", StructType(Seq(
                StructField("_VALUE", StringType, true),
              ))),

              StructField("Company_Service_Date", StructType(Seq(
                StructField("_VALUE", StringType, true),
              ))),
              StructField("Hire_Date", StructType(Seq(
                StructField("_VALUE", StringType, true),
              ))),
              StructField("Hire_Reason", StructType(Seq(
                StructField("_VALUE", StringType, true),
              ))),
              StructField("Seniority_Date", StructType(Seq(
                StructField("_VALUE", StringType, true),
              ))),
              StructField("Original_Hire_Date", StructType(Seq(
                StructField("_VALUE", StringType, true),
              ))),
              StructField("Continuous_Service_Date", StructType(Seq(
                StructField("_VALUE", StringType, true),
              ))),
              StructField("Termination_Date", StructType(Seq(
                StructField("_VALUE", StringType, true),
              ))),
              StructField("Primary_Termination_Reason", StructType(Seq(
                StructField("_VALUE", StringType, true),
              ))),
              StructField("Local_Termination_Reason", StructType(Seq(
                StructField("_VALUE", StringType, true),
              ))),
              StructField("Retirement_Date", StructType(Seq(
                StructField("_VALUE", StringType, true),
              ))),
            ))),
            StructField("Collective_Agreement", StructType(Seq(
              StructField("Collective_Agreement", StructType(Seq(
                StructField("_VALUE", StringType, true),
              ))),
              StructField("Collective_Agreement_Factor", StructType(Seq(
                StructField("_VALUE", StringType, true),
              ))),
              StructField("Start_Date", StructType(Seq(
                StructField("_VALUE", StringType, true),
              ))),
            ))),
            StructField("Summary", StructType(Seq(
              StructField("WID", StructType(Seq(
                StructField("_VALUE", StringType, true),
              ))),
            )))
          )))
        )))
        )))

    val schema2 = StructType(
      Array(
        StructField("Worker", ArrayType(StructType(Array(
          StructField("Worker_Summary",StructType(Seq(
            StructField("WID",StructType(Seq(
              StructField("_VALUE",StringType,true),
              StructField("_priorValue",StringType,true),
              StructField("_isAdded",StringType,true),
              StructField("_IsDeleted",StringType,true),
            ))),
            StructField("Employee_ID",StructType(Seq(
              StructField("_VALUE",StringType,true),
              StructField("_priorValue",StringType,true),
              StructField("_isAdded",StringType,true),
              StructField("_IsDeleted",StringType,true),
            ))),
          ))),
          StructField("Employees",StructType(Seq(
            StructField("Person_Communication",StructType(Seq(
              StructField("Address", ArrayType(StructType(Array(
                StructField("Address_Line_1",StructType(Seq(
                  StructField("_VALUE",StringType,true),
                  StructField("_priorValue",StringType,true),
                  StructField("_isAdded",StringType,true),
                  StructField("_IsDeleted",StringType,true),
                ))),
                StructField("Address_Line_3",StructType(Seq(
                  StructField("_VALUE",StringType,true),
                  StructField("_priorValue",StringType,true),
                  StructField("_isAdded",StringType,true),
                  StructField("_IsDeleted",StringType,true),
                ))),
                StructField("Address_Line_5",StructType(Seq(
                  StructField("_VALUE",StringType,true),
                  StructField("_priorValue",StringType,true),
                  StructField("_isAdded",StringType,true),
                  StructField("_IsDeleted",StringType,true),
                ))),
                StructField("Address_Line_8",StructType(Seq(
                  StructField("_VALUE",StringType,true),
                  StructField("_priorValue",StringType,true),
                  StructField("_isAdded",StringType,true),
                  StructField("_IsDeleted",StringType,true),
                ))),
                StructField("Address_Line_9",StructType(Seq(
                  StructField("_VALUE",StringType,true),
                  StructField("_priorValue",StringType,true),
                  StructField("_isAdded",StringType,true),
                  StructField("_IsDeleted",StringType,true),
                ))),
                StructField("City",StructType(Seq(
                  StructField("_VALUE",StringType,true),
                  StructField("_priorValue",StringType,true),
                  StructField("_isAdded",StringType,true),
                  StructField("_IsDeleted",StringType,true),
                ))),
                StructField("Postal_Code",StructType(Seq(
                  StructField("_VALUE",StringType,true),
                  StructField("_priorValue",StringType,true),
                  StructField("_isAdded",StringType,true),
                  StructField("_IsDeleted",StringType,true),
                ))),
                StructField("Country",StructType(Seq(
                  StructField("_VALUE",StringType,true),
                  StructField("_priorValue",StringType,true),
                  StructField("_isAdded",StringType,true),
                  StructField("_IsDeleted",StringType,true),
                ))),
                StructField("State_Province",StructType(Seq(
                  StructField("_VALUE",StringType,true),
                  StructField("_priorValue",StringType,true),
                  StructField("_isAdded",StringType,true),
                  StructField("_IsDeleted",StringType,true),
                ))),
                StructField("Usage",StructType(Seq(
                  StructField("Usage_Behavior_ID",StructType(Seq(
                    StructField("_VALUE",StringType,true),
                    StructField("_priorValue",StringType,true),
                    StructField("_isAdded",StringType,true),
                    StructField("_IsDeleted",StringType,true),
                  ))),
                ))),
              )))),
              StructField("Phone", ArrayType(StructType(Array(
                StructField("Usage_Type",StructType(Seq(
                  StructField("_VALUE",StringType,true),
                  StructField("_priorValue",StringType,true),
                  StructField("_isAdded",StringType,true),
                  StructField("_IsDeleted",StringType,true),
                ))),
                StructField("Phone_Device_Type",StructType(Seq(
                  StructField("_VALUE",StringType,true),
                  StructField("_priorValue",StringType,true),
                  StructField("_isAdded",StringType,true),
                  StructField("_IsDeleted",StringType,true),
                ))),
                StructField("International_Phone_Code",StructType(Seq(
                  StructField("_VALUE",StringType,true),
                  StructField("_priorValue",StringType,true),
                  StructField("_isAdded",StringType,true),
                  StructField("_IsDeleted",StringType,true),
                ))),
                StructField("Phone_Number",StructType(Seq(
                  StructField("_VALUE",StringType,true),
                  StructField("_priorValue",StringType,true),
                  StructField("_isAdded",StringType,true),
                  StructField("_IsDeleted",StringType,true),
                ))),
              )))),
              StructField("Email", ArrayType(StructType(Array(
                StructField("Usage_Type",StructType(Seq(
                  StructField("_VALUE",StringType,true),
                  StructField("_priorValue",StringType,true),
                  StructField("_isAdded",StringType,true),
                  StructField("_IsDeleted",StringType,true),
                ))),
                StructField("Email_Address",StructType(Seq(
                  StructField("_VALUE",StringType,true),
                  StructField("_priorValue",StringType,true),
                  StructField("_isAdded",StringType,true),
                  StructField("_IsDeleted",StringType,true),
                ))),
              )))),
            ))),
            StructField("Employee_Contract",StructType(Seq(
              StructField("Contract_Type",StructType(Seq(
                StructField("_VALUE",StringType,true),
                StructField("_priorValue",StringType,true),
                StructField("_isAdded",StringType,true),
                StructField("_IsDeleted",StringType,true),
              ))),
              StructField("Contract_Status",StructType(Seq(
                StructField("_VALUE",StringType,true),
                StructField("_priorValue",StringType,true),
                StructField("_isAdded",StringType,true),
                StructField("_IsDeleted",StringType,true),
              ))),
              StructField("Start_Date",StructType(Seq(
                StructField("_VALUE",StringType,true),
                StructField("_priorValue",StringType,true),
                StructField("_isAdded",StringType,true),
                StructField("_IsDeleted",StringType,true),
              ))),
              StructField("End_Date",StructType(Seq(
                StructField("_VALUE",StringType,true),
                StructField("_priorValue",StringType,true),
                StructField("_isAdded",StringType,true),
                StructField("_IsDeleted",StringType,true),
              ))),
            ))),
            StructField("Payment_Election",StructType(Seq(
              StructField("Bank_Account_Name",StructType(Seq(
                StructField("_VALUE",StringType,true),
                StructField("_priorValue",StringType,true),
                StructField("_isAdded",StringType,true),
                StructField("_IsDeleted",StringType,true),
              ))),
              StructField("IBAN",StructType(Seq(
                StructField("_VALUE",StringType,true),
                StructField("_priorValue",StringType,true),
                StructField("_isAdded",StringType,true),
                StructField("_IsDeleted",StringType,true),
              ))),
            ))),
            StructField("Compensation",StructType(Seq(
              StructField("Compensation_Grade",StructType(Seq(
                StructField("_VALUE",StringType,true),
                StructField("_priorValue",StringType,true),
                StructField("_isAdded",StringType,true),
                StructField("_IsDeleted",StringType,true),
              ))),
              StructField("Compensation_Summary_in_Annualized_Frequency",StructType(Seq(
                StructField("Total_Base_Pay",StructType(Seq(
                  StructField("_VALUE",StringType,true),
                  StructField("_priorValue",StringType,true),
                  StructField("_isAdded",StringType,true),
                  StructField("_IsDeleted",StringType,true),
                ))),
                StructField("Currency",StructType(Seq(
                  StructField("_VALUE",StringType,true),
                  StructField("_priorValue",StringType,true),
                  StructField("_isAdded",StringType,true),
                  StructField("_IsDeleted",StringType,true),
                ))),
                StructField("Frecuency",StructType(Seq(
                  StructField("_VALUE",StringType,true),
                  StructField("_priorValue",StringType,true),
                  StructField("_isAdded",StringType,true),
                  StructField("_IsDeleted",StringType,true),
                ))),
              ))),
            ))),
            StructField("Person_Identification",StructType(Seq(
              StructField("Visa_Identifier",StructType(Seq(
                StructField("Country",StructType(Seq(
                  StructField("_VALUE",StringType,true),
                  StructField("_priorValue",StringType,true),
                  StructField("_isAdded",StringType,true),
                  StructField("_IsDeleted",StringType,true),
                ))),
                StructField("Visa_ID_Type",StructType(Seq(
                  StructField("_VALUE",StringType,true),
                  StructField("_priorValue",StringType,true),
                  StructField("_isAdded",StringType,true),
                  StructField("_IsDeleted",StringType,true),
                ))),
                StructField("Visa_ID",StructType(Seq(
                  StructField("_VALUE",StringType,true),
                  StructField("_priorValue",StringType,true),
                  StructField("_isAdded",StringType,true),
                  StructField("_IsDeleted",StringType,true),
                ))),
              ))),
              StructField("Passport_Identifier",StructType(Seq(
                StructField("Country",StructType(Seq(
                  StructField("_VALUE",StringType,true),
                  StructField("_priorValue",StringType,true),
                  StructField("_isAdded",StringType,true),
                  StructField("_IsDeleted",StringType,true),
                ))),
                StructField("Passport_ID_Type",StructType(Seq(
                  StructField("_VALUE",StringType,true),
                  StructField("_priorValue",StringType,true),
                  StructField("_isAdded",StringType,true),
                  StructField("_IsDeleted",StringType,true),
                ))),
                StructField("Passport_ID",StructType(Seq(
                  StructField("_VALUE",StringType,true),
                  StructField("_priorValue",StringType,true),
                  StructField("_isAdded",StringType,true),
                  StructField("_IsDeleted",StringType,true),
                ))),
              ))),
              StructField("National_Identifier", ArrayType(StructType(Array(
                StructField("Country",StructType(Seq(
                  StructField("_VALUE",StringType,true),
                  StructField("_priorValue",StringType,true),
                  StructField("_isAdded",StringType,true),
                  StructField("_IsDeleted",StringType,true),
                ))),
                StructField("National_ID_Type",StructType(Seq(
                  StructField("_VALUE",StringType,true),
                  StructField("_priorValue",StringType,true),
                  StructField("_isAdded",StringType,true),
                  StructField("_IsDeleted",StringType,true),
                ))),
                StructField("National_Identifier",StructType(Seq(
                  StructField("_VALUE",StringType,true),
                  StructField("_priorValue",StringType,true),
                  StructField("_isAdded",StringType,true),
                  StructField("_IsDeleted",StringType,true),
                ))),
                StructField("National_ID",StructType(Seq(
                  StructField("_VALUE",StringType,true),
                  StructField("_priorValue",StringType,true),
                  StructField("_isAdded",StringType,true),
                  StructField("_IsDeleted",StringType,true),
                ))),
                StructField("First_Name",StructType(Seq(
                  StructField("_VALUE",StringType,true),
                  StructField("_priorValue",StringType,true),
                  StructField("_isAdded",StringType,true),
                  StructField("_IsDeleted",StringType,true),
                ))),
              )))),
              StructField("Other_Identifier", ArrayType(StructType(Array(
                StructField("Custom_ID",StructType(Seq(
                  StructField("_VALUE",StringType,true),
                  StructField("_priorValue",StringType,true),
                  StructField("_isAdded",StringType,true),
                  StructField("_IsDeleted",StringType,true),
                ))),
                StructField("Custom_ID_Type",StructType(Seq(
                  StructField("_VALUE",StringType,true),
                  StructField("_priorValue",StringType,true),
                  StructField("_isAdded",StringType,true),
                  StructField("_IsDeleted",StringType,true),
                ))),
              )))),
            ))),
            StructField("Position", ArrayType(StructType(Array(
              StructField("Business_Site",StructType(Seq(
                StructField("Location_ID",StructType(Seq(
                  StructField("_VALUE",StringType,true),
                  StructField("_priorValue",StringType,true),
                  StructField("_isAdded",StringType,true),
                  StructField("_IsDeleted",StringType,true),
                ))),
                StructField("Location_Name",StructType(Seq(
                  StructField("_VALUE",StringType,true),
                  StructField("_priorValue",StringType,true),
                  StructField("_isAdded",StringType,true),
                  StructField("_IsDeleted",StringType,true),
                ))),
              ))),
              StructField("Management_Level",StructType(Seq(
                StructField("_VALUE",StringType,true),
                StructField("_priorValue",StringType,true),
                StructField("_isAdded",StringType,true),
                StructField("_IsDeleted",StringType,true),
              ))),
              StructField("Job_Profile",StructType(Seq(
                StructField("_VALUE",StringType,true),
                StructField("_priorValue",StringType,true),
                StructField("_isAdded",StringType,true),
                StructField("_IsDeleted",StringType,true),
              ))),
              StructField("Position_ID",StructType(Seq(
                StructField("_VALUE",StringType,true),
                StructField("_priorValue",StringType,true),
                StructField("_isAdded",StringType,true),
                StructField("_IsDeleted",StringType,true),
              ))),
              StructField("Business_Title",StructType(Seq(
                StructField("_VALUE",StringType,true),
                StructField("_priorValue",StringType,true),
                StructField("_isAdded",StringType,true),
                StructField("_IsDeleted",StringType,true),
              ))),
              StructField("Probation_Start_Date",StructType(Seq(
                StructField("_VALUE",StringType,true),
                StructField("_priorValue",StringType,true),
                StructField("_isAdded",StringType,true),
                StructField("_IsDeleted",StringType,true),
              ))),
              StructField("Probation_End_Date",StructType(Seq(
                StructField("_VALUE",StringType,true),
                StructField("_priorValue",StringType,true),
                StructField("_isAdded",StringType,true),
                StructField("_IsDeleted",StringType,true),
              ))),
              StructField("Position_Time_Type",StructType(Seq(
                StructField("_VALUE",StringType,true),
                StructField("_priorValue",StringType,true),
                StructField("_isAdded",StringType,true),
                StructField("_IsDeleted",StringType,true),
              ))),
              StructField("Job_Family", ArrayType(StructType(Array(
                StructField("ID",StructType(Seq(
                  StructField("_VALUE",StringType,true),
                  StructField("_priorValue",StringType,true),
                  StructField("_isAdded",StringType,true),
                  StructField("_IsDeleted",StringType,true),
                ))),
                StructField("Name",StructType(Seq(
                  StructField("_VALUE",StringType,true),
                  StructField("_priorValue",StringType,true),
                  StructField("_isAdded",StringType,true),
                  StructField("_IsDeleted",StringType,true),
                ))),
              )))),
              StructField("Job_Classification",StructType(Seq(
                StructField("Job_Classiication",StructType(Seq(
                  StructField("_VALUE",StringType,true),
                  StructField("_priorValue",StringType,true),
                  StructField("_isAdded",StringType,true),
                  StructField("_IsDeleted",StringType,true),
                ))),
              ))),
              StructField("Organization",StructType(Seq(
                StructField("Organization_Type",StructType(Seq(
                  StructField("_VALUE",StringType,true),
                  StructField("_priorValue",StringType,true),
                  StructField("_isAdded",StringType,true),
                  StructField("_IsDeleted",StringType,true),
                ))),
                StructField("Organization_Code",StructType(Seq(
                  StructField("_VALUE",StringType,true),
                  StructField("_priorValue",StringType,true),
                  StructField("_isAdded",StringType,true),
                  StructField("_IsDeleted",StringType,true),
                ))),
                StructField("Organization_Name",StructType(Seq(
                  StructField("_VALUE",StringType,true),
                  StructField("_priorValue",StringType,true),
                  StructField("_isAdded",StringType,true),
                  StructField("_IsDeleted",StringType,true),
                ))),
              ))),
              StructField("Supervisor",StructType(Seq(
                StructField("ID",StructType(Seq(
                  StructField("_VALUE",StringType,true),
                  StructField("_priorValue",StringType,true),
                  StructField("_isAdded",StringType,true),
                  StructField("_IsDeleted",StringType,true),
                ))),
                StructField("Name",StructType(Seq(
                  StructField("_VALUE",StringType,true),
                  StructField("_priorValue",StringType,true),
                  StructField("_isAdded",StringType,true),
                  StructField("_IsDeleted",StringType,true),
                ))),
              ))),
            )))),
            StructField("Personal",StructType(Seq(
              StructField("Workday_Account",StructType(Seq(
                StructField("_VALUE",StringType,true),
                StructField("_priorValue",StringType,true),
                StructField("_isAdded",StringType,true),
                StructField("_IsDeleted",StringType,true),
              ))),
              StructField("Date_of_Birth",StructType(Seq(
                StructField("_VALUE",StringType,true),
                StructField("_priorValue",StringType,true),
                StructField("_isAdded",StringType,true),
                StructField("_IsDeleted",StringType,true),
              ))),
              StructField("Date_of_Death",StructType(Seq(
                StructField("_VALUE",StringType,true),
                StructField("_priorValue",StringType,true),
                StructField("_isAdded",StringType,true),
                StructField("_IsDeleted",StringType,true),
              ))),
              StructField("Country_of_Birth",StructType(Seq(
                StructField("_VALUE",StringType,true),
                StructField("_priorValue",StringType,true),
                StructField("_isAdded",StringType,true),
                StructField("_IsDeleted",StringType,true),
              ))),
              StructField("City_of_Birth",StructType(Seq(
                StructField("_VALUE",StringType,true),
                StructField("_priorValue",StringType,true),
                StructField("_isAdded",StringType,true),
                StructField("_IsDeleted",StringType,true),
              ))),
              StructField("Region_of_Birth",StructType(Seq(
                StructField("_VALUE",StringType,true),
                StructField("_priorValue",StringType,true),
                StructField("_isAdded",StringType,true),
                StructField("_IsDeleted",StringType,true),
              ))),
              StructField("Nationality",StructType(Seq(
                StructField("_VALUE",StringType,true),
                StructField("_priorValue",StringType,true),
                StructField("_isAdded",StringType,true),
                StructField("_IsDeleted",StringType,true),
              ))),
              StructField("Preferred_Lenguage",StructType(Seq(
                StructField("_VALUE",StringType,true),
                StructField("_priorValue",StringType,true),
                StructField("_isAdded",StringType,true),
                StructField("_IsDeleted",StringType,true),
              ))),
              StructField("Gender",StructType(Seq(
                StructField("_VALUE",StringType,true),
                StructField("_priorValue",StringType,true),
                StructField("_isAdded",StringType,true),
                StructField("_IsDeleted",StringType,true),
              ))),
              StructField("Marital_Status",StructType(Seq(
                StructField("_VALUE",StringType,true),
                StructField("_priorValue",StringType,true),
                StructField("_isAdded",StringType,true),
                StructField("_IsDeleted",StringType,true),
              ))),
              StructField("Marital_Status_Date",StructType(Seq(
                StructField("_VALUE",StringType,true),
                StructField("_priorValue",StringType,true),
                StructField("_isAdded",StringType,true),
                StructField("_IsDeleted",StringType,true),
              ))),
              StructField("Name",StructType(Seq(// Name == legal_name en xml
                StructField("First_Name",StructType(Seq(
                  StructField("_VALUE",StringType,true),
                  StructField("_priorValue",StringType,true),
                  StructField("_isAdded",StringType,true),
                  StructField("_IsDeleted",StringType,true),
                ))),
                StructField("Middle_Name",StructType(Seq(
                  StructField("_VALUE",StringType,true),
                  StructField("_priorValue",StringType,true),
                  StructField("_isAdded",StringType,true),
                  StructField("_IsDeleted",StringType,true),
                ))),
                StructField("Last_Name",StructType(Seq(
                  StructField("_VALUE",StringType,true),
                  StructField("_priorValue",StringType,true),
                  StructField("_isAdded",StringType,true),
                  StructField("_IsDeleted",StringType,true),
                ))),
                StructField("Secondary_Last_Name",StructType(Seq(
                  StructField("_VALUE",StringType,true),
                  StructField("_priorValue",StringType,true),
                  StructField("_isAdded",StringType,true),
                  StructField("_IsDeleted",StringType,true),
                ))),
              ))),
              StructField("Disability_Status",StructType(Seq(
                StructField("Grade",StructType(Seq(
                  StructField("_VALUE",StringType,true),
                  StructField("_priorValue",StringType,true),
                  StructField("_isAdded",StringType,true),
                  StructField("_IsDeleted",StringType,true),
                ))),
                StructField("Disability",StructType(Seq(
                  StructField("_VALUE",StringType,true),
                  StructField("_priorValue",StringType,true),
                  StructField("_isAdded",StringType,true),
                  StructField("_IsDeleted",StringType,true),
                ))),
                StructField("Certification_Authority",StructType(Seq(
                  StructField("_VALUE",StringType,true),
                  StructField("_priorValue",StringType,true),
                  StructField("_isAdded",StringType,true),
                  StructField("_IsDeleted",StringType,true),
                ))),
                StructField("Disability_Status_Date",StructType(Seq(
                  StructField("_VALUE",StringType,true),
                  StructField("_priorValue",StringType,true),
                  StructField("_isAdded",StringType,true),
                  StructField("_IsDeleted",StringType,true),
                ))),
                StructField("End_Date",StructType(Seq(
                  StructField("_VALUE",StringType,true),
                  StructField("_priorValue",StringType,true),
                  StructField("_isAdded",StringType,true),
                  StructField("_IsDeleted",StringType,true),
                ))),
                StructField("Date_Known",StructType(Seq(
                  StructField("_VALUE",StringType,true),
                  StructField("_priorValue",StringType,true),
                  StructField("_isAdded",StringType,true),
                  StructField("_IsDeleted",StringType,true),
                ))),
              ))),
            ))),
            StructField("Worker_Status",StructType(Seq(
              StructField("Active",StructType(Seq(
                StructField("_VALUE",StringType,true),
                StructField("_priorValue",StringType,true),
                StructField("_isAdded",StringType,true),
                StructField("_IsDeleted",StringType,true),
              ))),
              StructField("Status",StructType(Seq(
                StructField("_VALUE",StringType,true),
                StructField("_priorValue",StringType,true),
                StructField("_isAdded",StringType,true),
                StructField("_IsDeleted",StringType,true),
              ))),

              StructField("Company_Service_Date",StructType(Seq(
                StructField("_VALUE",StringType,true),
                StructField("_priorValue",StringType,true),
                StructField("_isAdded",StringType,true),
                StructField("_IsDeleted",StringType,true),
              ))),
              StructField("Hire_Date",StructType(Seq(
                StructField("_VALUE",StringType,true),
                StructField("_priorValue",StringType,true),
                StructField("_isAdded",StringType,true),
                StructField("_IsDeleted",StringType,true),
              ))),
              StructField("Hire_Reason",StructType(Seq(
                StructField("_VALUE",StringType,true),
                StructField("_priorValue",StringType,true),
                StructField("_isAdded",StringType,true),
                StructField("_IsDeleted",StringType,true),
              ))),
              StructField("Seniority_Date",StructType(Seq(
                StructField("_VALUE",StringType,true),
                StructField("_priorValue",StringType,true),
                StructField("_isAdded",StringType,true),
                StructField("_IsDeleted",StringType,true),
              ))),
              StructField("Original_Hire_Date",StructType(Seq(
                StructField("_VALUE",StringType,true),
                StructField("_priorValue",StringType,true),
                StructField("_isAdded",StringType,true),
                StructField("_IsDeleted",StringType,true),
              ))),
              StructField("Continuous_Service_Date",StructType(Seq(
                StructField("_VALUE",StringType,true),
                StructField("_priorValue",StringType,true),
                StructField("_isAdded",StringType,true),
                StructField("_IsDeleted",StringType,true),
              ))),
              StructField("Termination_Date",StructType(Seq(
                StructField("_VALUE",StringType,true),
                StructField("_priorValue",StringType,true),
                StructField("_isAdded",StringType,true),
                StructField("_IsDeleted",StringType,true),
              ))),
              StructField("Primary_Termination_Reason",StructType(Seq(
                StructField("_VALUE",StringType,true),
                StructField("_priorValue",StringType,true),
                StructField("_isAdded",StringType,true),
                StructField("_IsDeleted",StringType,true),
              ))),
              StructField("Local_Termination_Reason",StructType(Seq(
                StructField("_VALUE",StringType,true),
                StructField("_priorValue",StringType,true),
                StructField("_isAdded",StringType,true),
                StructField("_IsDeleted",StringType,true),
              ))),
              StructField("Retirement_Date",StructType(Seq(
                StructField("_VALUE",StringType,true),
                StructField("_priorValue",StringType,true),
                StructField("_isAdded",StringType,true),
                StructField("_IsDeleted",StringType,true),
              ))),
            ))),
            StructField("Collective_Agreement",StructType(Seq(
              StructField("Collective_Agreement",StructType(Seq(
                StructField("_VALUE",StringType,true),
                StructField("_priorValue",StringType,true),
                StructField("_isAdded",StringType,true),
                StructField("_IsDeleted",StringType,true),
              ))),
              StructField("Collective_Agreement_Factor",StructType(Seq(
                StructField("_VALUE",StringType,true),
                StructField("_priorValue",StringType,true),
                StructField("_isAdded",StringType,true),
                StructField("_IsDeleted",StringType,true),
              ))),
              StructField("Start_Date",StructType(Seq(
                StructField("_VALUE",StringType,true),
                StructField("_priorValue",StringType,true),
                StructField("_isAdded",StringType,true),
                StructField("_IsDeleted",StringType,true),
              ))),
            ))),
          )))
        )))
        )))


    var df_WeciReadXML = spark.read
      .schema(schema)
      .format("com.databricks.spark.xml")
      .option("excludeAttribute", "false")
      .option("inferSchema", "false")
      .option("ignoreNamespace", "true")
      .option("rowTag", "ns2:EMPLOYEE_DELTA_INTEGRATION")
      .load(rutaWeciAllPayRollXml)


    //println("Imprimiendo el df de df_WeciReadXML")
    //df_WeciReadXML.printSchema()
    //df_WeciReadXML.show()

    var df_WeciReadXML_FieldAll = spark.read
      .schema(schema2)
      .format("com.databricks.spark.xml")
      .option("excludeAttribute", "false")
      .option("inferSchema", "false")
      .option("ignoreNamespace", "true")
      .option("rowTag", "ns2:EMPLOYEE_DELTA_INTEGRATION")
      .load(rutaWeciAllPayRollXml)


    //println("Imprimiendo el df de df_WeciReadXML_FieldAll")
    //df_WeciReadXML_FieldAll.printSchema()
    //df_WeciReadXML_FieldAll.show()


    //////////////////////////////////////////////////////////////////////////////

    //En esta lógica tengo todos los campos weci del DFC con el esquema 1

    var df_final = df_WeciReadXML
      .withColumn("Worker", explode_outer(col("`Worker`")))
      .withColumn("Other_Identifier", explode_outer(col("`Worker`.`Employees`.`Person_Identification`.`Other_Identifier`")))
      .withColumn("National_Identifier", explode_outer(col("`Worker`.`Employees`.`Person_Identification`.`National_Identifier`")))
      .withColumn("Job_Family", explode_outer(col("`Worker`.`Employees`.`Position`.`Job_Family`")))
      // Estos 3 arrays de abajo son para las operaciones que tenemos que hacer luego
      //.withColumn("Address", explode_outer(col("`Person_Communication`.`Address`")))
      //.withColumn("Phone", explode_outer(col("`Address`.`Phone`")))
      //.withColumn("Email", explode_outer(col("`Address`.`Email`")))
      .selectExpr(
        //Los dos campos que no deben faltar
        s"'$dataDatePart' as data_date_part",
        "`Worker`.`Worker_Summary`.`Employee_ID`.`_VALUE` as EMPLID_Worker_Summary",

        //Todos los campos mapeados de weci
        //"`Other_Identifier`.`Custom_ID`.`_VALUE` as EMPLID",
        //"`Other_Identifier`.`Custom_ID_Type`.`_VALUE` as ID_CORP_EXTERNAL_SYSTEM_ID", // preguntar duda
        "`Worker`.`Worker_Summary`.`WID`.`_VALUE` as ID_WKD",
        "`Worker`.`Employees`.`Personal`.`Workday_Account`.`_VALUE` as WD_ACC",
        "'' as EFFDT", // N/A
        "'' as EFFSEQ", // Calculado
        "'' as ACTION", // N/A
        "'' as ACTION_DESCR", // null
        "'' as ACTION_EFFDT", // N/A
        "'' as ACTION_REASON", // null
        "`Worker`.`Employees`.`Worker_Status`.`Active`.`_VALUE` as ACTIVE_STATUS",
        "`Worker`.`Employees`.`Worker_Status`.`Status`.`_VALUE` as EMPLOYEE_STATUS",
        "'' as EMPL_STATUS_EFFDT", // N/A
        "'' as WORKER_TYPE", // n/A ID
        "'' as EMPLOYEE_TYPE", // null
        "'' as CONTINGENT_WORKER_TYPE", // null
        "`Worker`.`Employees`.`Personal`.`Name`.`First_Name`.`_VALUE` as FIRST_NAME",
        "`Worker`.`Employees`.`Personal`.`Name`.`Middle_Name`.`_VALUE` as MIDDLE_NAME",
        "`Worker`.`Employees`.`Personal`.`Name`.`Last_Name`.`_VALUE` as LAST_NAME",
        "`Worker`.`Employees`.`Personal`.`Name`.`Secondary_Last_Name`.`_VALUE` as SECOND_LAST_NAME",
        "`National_Identifier`.`Country` as NATIONAL_ID_COUNTRY",
        "`National_Identifier`.`National_ID_Type` as NATIONAL_ID_TYPE",
        "`National_Identifier`.`National_ID_Type` as NATIONAL_ID_TYPE_DESCR", // campo es un array ? Pocho
        "`National_Identifier`.`National_ID` as NATIONAL_ID",
        "`Worker`.`Employees`.`Person_Identification`.`Visa_Identifier`.`Country`.`_VALUE` as VISA_ID_COUNTRY",
        "`Worker`.`Employees`.`Person_Identification`.`Visa_Identifier`.`Visa_ID_Type`.`_VALUE` as VISA_ID_TYPE",
        "`Worker`.`Employees`.`Person_Identification`.`Visa_Identifier`.`Visa_ID`.`_VALUE` as VISA_ID",
        "`Worker`.`Employees`.`Person_Identification`.`Passport_Identifier`.`Country`.`_VALUE` as PASSPORT_ID_COUNTRY",
        "`Worker`.`Employees`.`Person_Identification`.`Passport_Identifier`.`Passport_ID_Type`.`_VALUE` as PASSPORT_ID_TYPE",
        "`Worker`.`Employees`.`Person_Identification`.`Passport_Identifier`.`Passport_ID`.`_VALUE` as PASSPORT_ID",
        "`Worker`.`Employees`.`Personal`.`Date_of_Birth`.`_VALUE` as BIRTHDATE",
        "'' as AGE", // CALCULATED
        "'' as ACT_AGE", // CALCULATED
        "`Worker`.`Employees`.`Personal`.`Date_of_Death`.`_VALUE` as DT_OF_DEATH",
        "`Worker`.`Employees`.`Personal`.`Country_of_Birth`.`_VALUE` as BIRTHCOUNTRY",
        "`Worker`.`Employees`.`Personal`.`City_of_Birth`.`_VALUE` as BIRTHCITY_BIRTHPLACE",
        "`Worker`.`Employees`.`Personal`.`Region_of_Birth`.`_VALUE` as BIRTHSTATE",
        "`Worker`.`Employees`.`Personal`.`Nationality`.`_VALUE` as Nationality",
        "'' as NATIONALITY_DESCR", // N/A
        "`Worker`.`Employees`.`Personal`.`Preferred_Lenguage`.`_VALUE` as PREF_LANG_LANG_CD",
        "'' as LANG_CD_DESCR",
        "`Worker`.`Employees`.`Personal`.`Gender`.`_VALUE` as GENDER",
        "'' as GENDER_DESCR",
        "`Worker`.`Employees`.`Personal`.`Marital_Status`.`_VALUE` as MAR_STATUS",
        "'' as MAR_STATUS_DESCR",
        "`Worker`.`Employees`.`Personal`.`Marital_Status_Date`.`_VALUE` as MAR_STATUS_EFFDT",
        "`Worker`.`Employees`.`Personal`.`Disability_Status`.`Grade`.`_VALUE` as DISABLE_DEGREE",
        "`Worker`.`Employees`.`Personal`.`Disability_Status`.`Disability`.`_VALUE` as DISABLE_DEGREE_DESCR",
        "`Worker`.`Employees`.`Personal`.`Disability_Status`.`Certification_Authority`.`_VALUE` as DISABLE_CERT_AUTHORITY",
        "`Worker`.`Employees`.`Personal`.`Disability_Status`.`Disability_Status_Date`.`_VALUE` as DISABLE_EFFDT", // comprobar cuando tengamos datos reales
        "`Worker`.`Employees`.`Personal`.`Disability_Status`.`End_Date`.`_VALUE` as DISABLE_END_EFFDT",
        "`Worker`.`Employees`.`Personal`.`Disability_Status`.`Date_Known`.`_VALUE` as DISABLE_KNOW_EFFDT",
        "'' as EMPLID_CONYUGE", // null
        "'' as NUM_DISABLE_HIJOS_33_65", // null
        "'' as NUM_DISABLE_HIJOS_MAS_65", // null
        "'' as HIGHEST_EDUC_LVL", // null
        "`Worker`.`Employees`.`Employee_Contract`.`Contract_Type`.`_VALUE` as CONTRACT_TYPE",
        "'' as CONTRACT_TYPE_LOCAL", // Aditional 5 ?
        "`Worker`.`Employees`.`Employee_Contract`.`Contract_Status`.`_VALUE` as CONTRACT_STATUS",
        "`Worker`.`Employees`.`Employee_Contract`.`Start_Date`.`_VALUE` as CONTRACT_START_DT",
        "`Worker`.`Employees`.`Employee_Contract`.`End_Date`.`_VALUE` as CONTRACT_END_DT",
        "'' as CONTRACT_TYPE_DESCR", // null
        "'' as PROBATION_N_DAYS", // Operacion ? datos start // end
        "`Worker`.`Employees`.`position`.`Position_Time_Type`.`_VALUE` as TYME_TYPE",
        "'' as JORNADA_EFFDT", // null
        "'' as COMPANY", // operacion
        "'' as COMPANY_DESCR", // operacion
        "'' as COUNTRY_COMPANY", // null
        "'' as COUNTRY_COMPANY_DESCR", // null
        "`Worker`.`Employees`.`Worker_Status`.`Company_Service_Date`.`_VALUE` as COMPANY_EFFDT",
        "`Worker`.`Employees`.`Worker_Status`.`Hire_Date`.`_VALUE` as HIRE_DT",
        "`Worker`.`Employees`.`Worker_Status`.`Hire_Reason`.`_VALUE` as HIRE_REASON",
        "`Worker`.`Employees`.`Worker_Status`.`Seniority_Date`.`_VALUE` as CMPNY_SENIORITY_DT",
        "`Worker`.`Employees`.`Worker_Status`.`Original_Hire_Date`.`_VALUE` as ORIGINAL_HIRE_DT",
        "`Worker`.`Employees`.`Worker_Status`.`Continuous_Service_Date`.`_VALUE` as CONTINUOUS_SERV_DT",
        "'' as ZINGRESOBANCA_FI", // null
        "`Worker`.`Employees`.`Worker_Status`.`Termination_Date`.`_VALUE` as TERMINATION_DT",
        "`Worker`.`Employees`.`Worker_Status`.`Primary_Termination_Reason`.`_VALUE` as TERMINATION_REASON",
        "`Worker`.`Employees`.`Worker_Status`.`Local_Termination_Reason`.`_VALUE` as TERMINATION_REASON_LOCAL",
        "`Worker`.`Employees`.`Worker_Status`.`Retirement_Date`.`_VALUE` as RETIREMENT_DT",
        "'' as GBL_MRT", // aditional field
        "'' as GBL_MRT_START_DT", // aditional field
        "'' as GBL_MRT_END_DT", // aditional field
        "'' as LCL_MRT", // aditional field
        "'' as LCL_MRT_START_DT", // aditional field
        "'' as LCL_MRT_END_DT", // aditional field
        "'' as DEPTID", // operacion ??
        "'' as DEPTID_DESCR", // operacion  ??
        "'' as DEPT_TYPE", // aditional field
        "'' as DEPT_ENTRY_DT", // revisar en WD
        "'' as DEPTID_SUP", // null
        /*
        "'' as POS_LVL_1_CODE", // inicio JOIN
        "'' as POS_LVL_1_NAME",
        "'' as POS_LVL_2_CODE",
        "'' as POS_LVL_2_NAME",
        "'' as POS_LVL_3_CODE",
        "'' as POS_LVL_3_NAME",
        "'' as POS_LVL_4_CODE",
        "'' as POS_LVL_4_NAME",
        "'' as POS_LVL_5_CODE",
        "'' as POS_LVL_5_NAME",
        "'' as POS_LVL_6_CODE",
        "'' as POS_LVL_6_NAME",
        "'' as POS_LVL_7_CODE",
        "'' as POS_LVL_7_NAME",
        "'' as POS_LVL_8_CODE",
        "'' as POS_LVL_8_NAME",
        "'' as POS_LVL_9_CODE",
        "'' as POS_LVL_9_NAME",
        "'' as POS_LVL_10_CODE",
        "'' as POS_LVL_10_NAME",
        "'' as POS_LVL_11_CODE",
        "'' as POS_LVL_11_NAME",
        "'' as POS_LVL_12_CODE",
        "'' as POS_LVL_12_NAME",
        "'' as POS_LVL_13_CODE",
        "'' as POS_LVL_13_NAME",
        "'' as POS_LVL_14_CODE",
        "'' as POS_LVL_14_NAME",
        "'' as POS_LVL_15_CODE",
        "'' as POS_LVL_15_NAME",
        "'' as POS_LVL_16_CODE",
        "'' as POS_LVL_16_NAME",
        "'' as POS_LVL_17_CODE",
        "'' as POS_LVL_17_NAME",
        "'' as POS_LVL_18_CODE",
        "'' as POS_LVL_18_NAME",
        "'' as POS_LVL_19_CODE",
        "'' as POS_LVL_19_NAME",
        "'' as POS_LVL_20_CODE",
        "'' as POS_LVL_20_NAME", // Fin JOIN
        */
        "'' as JOB_FAMILY_GROUP", // adicional field
        "'' as JOB_FAMILY", // operation job famility
        "`Worker`.`Employees`.`Position`.`Management_Level`.`_VALUE` as MANAGEMENT_LEVEL",
        "'' as MANAGEMENT_LEVEL_DESCR", // N/A
        "`Worker`.`Employees`.`position`.`Job_Profile`.`_VALUE` as JOBCODE", // WECI432 (code&&descr??)
        "`Worker`.`Employees`.`position`.`Job_Profile`.`_VALUE` as JOBCODE_DESCR", // WECI432 (code&&descr??)
        "'' as JOB_ENTRY_DT", // null
        "`Worker`.`Employees`.`Position`.`Position_ID`.`_VALUE` as POSITION_NBR",
        "`Worker`.`Employees`.`Position`.`Business_Title`.`_VALUE` as POSITION_NBR_DESCR",
        "'' as POSITION_ENTRY_DT", // N/A
        "'' as BUSINESS_UNIT", // operation with position organization
        "'' as BUSINESS_UNIT_DESCR", // operation with position organization
        "'' as PRODUCT", // operation with position organization
        "'' as AGILE_ROLE", // operation with position organization
        "`Worker`.`Employees`.`Position`.`Supervisor`.`ID`.`_VALUE` as MANAGER_ID",
        "'' as MANAGER_NAME", // calculated field N/a
        "'' as MANAGER_MAIL", // calculated field N/a
        "'' as MANAGER_PHONE", // calculated field N/a
        "'' as MNGR_FUNCT_ID", // aditional  field aditional 4
        "`Worker`.`Employees`.`Position`.`Supervisor`.`ID`.`_VALUE` as SUPERVISOR_EFFDT", // null ID Supervisor ?
        "`Worker`.`Employees`.`Position`.`Supervisor`.`Name`.`_VALUE` as SUPERVISORY", // null Name Supervisor?
        /*
         "'' as MANAGEMENT_CHAIN_LEVEL1", // inicio Join
         "'' as MANAGEMENT_CHAIN_LEVEL2",
         "'' as MANAGEMENT_CHAIN_LEVEL3",
         "'' as MANAGEMENT_CHAIN_LEVEL4",
         "'' as MANAGEMENT_CHAIN_LEVEL5",
         "'' as MANAGEMENT_CHAIN_LEVEL6",
         "'' as MANAGEMENT_CHAIN_LEVEL7",
         "'' as MANAGEMENT_CHAIN_LEVEL8",
         "'' as MANAGEMENT_CHAIN_LEVEL9",
         "'' as MANAGEMENT_CHAIN_LEVEL10",
         "'' as MANAGEMENT_CHAIN_LEVEL11",
         "'' as MANAGEMENT_CHAIN_LEVEL12",
         "'' as MANAGEMENT_CHAIN_LEVEL13",
         "'' as MANAGEMENT_CHAIN_LEVEL14",
         "'' as MANAGEMENT_CHAIN_LEVEL15",
         "'' as MANAGEMENT_CHAIN_LEVEL16",
         "'' as MANAGEMENT_CHAIN_LEVEL17",
         "'' as MANAGEMENT_CHAIN_LEVEL18",
         "'' as MANAGEMENT_CHAIN_LEVEL19",
         "'' as MANAGEMENT_CHAIN_LEVEL20", // fin join
         */
        "'' as COMP_GRADE_PROFILE", // aditional field  7
        "`Worker`.`Employees`.`Compensation`.`Compensation_Grade`.`_VALUE` as COMPENSATION_GRADE",
        "'' as MIN_SALARY_RANGE", // N/A ID
        "'' as MAX_SALARY_RANGE", // N/A ID
        "`Worker`.`Employees`.`Position`.`Job_Classification`.`Job_Classiication`.`_VALUE` as JOB_CLASSIFICATION", // errata
        "'' as JOB_CLASSIFICATION_EFFDT", // null
        "'' as PAY_GROUP", // operation
        "'' as ADDITIONAL_JOB_CLASSIFICATION", // aditional field 3
        "`Worker`.`Employees`.`Collective_Agreement`.`Collective_Agreement`.`_VALUE` as COLLECTIVE_AGREEMENT",
        "'' as COLLECTIVE_AGREEMENT_DESCR", // null
        //"`Worker`.`Employees`.`Collective_Agreement`.`Collective_Agreement_Factor`.`_VALUE` as COLLECTIVE_AGREEMENT_FACTOR",
        "`Worker`.`Employees`.`Collective_Agreement`.`Start_Date`.`_VALUE` as COLLECTIVE_AGREEMENT_DT",
        "'' as EMPL_CTG_DESCR", // null
        "'' as EMPL_CTG_EFFDT", // null
        "'' as ZGADP_GRADOEMPL", // null
        "'' as ZGADP_GRADOEMPL_EFFDT", // null
        "'' as COST_CENTER", // operation
        "'' as COST_CENTER_NAME", // operation
        "'' as ZCCOSTE_FENTRADA_EFFDT", // null
        "'' as ZGADP_BANCOPROC", // null
        "'' as CORP_SEGMENT", // operation
        "'' as SETID_DT_ALTA", // null
        "'' as CORP_SEGMENT_DT", // operation
        "'' as IND_PODERES", // bloque null
        "'' as IND_PODERES_EFFDT",
        "'' as Z02_DIVISION",
        "'' as MOTIVO_ALTA_HR",
        "'' as MOTIVO_ALTA_HR_DESCR",
        "'' as MOTIVO_ALTA_HR_EFFDT",
        "'' as ESCALA_JEFATURA_DT",
        "'' as ESCALA_JEFATURA_EFFDT",
        "'' as TRIENIOS_DT",
        "'' as TRIENIOS_EFFDT",
        "'' as TRIENIOS_JEFATURA_DT",
        "'' as TRIENIOS_JEFATURA_EFFDT",
        "'' as ESCALA_ADMIN_DT",
        "'' as ESCALA_ADMIN_EFFDT",
        "'' as HORAS_SEMANALES",
        "'' as HORAS_JORNADA_EFFDT",
        "'' as PORC_REDUC_JORNADA",
        "'' as REDUC_JORNADA_FINI",
        "'' as REDUC_JORNADA_FESTIMADA",
        "'' as REDUC_JORNADA_FCONFIRMADA",
        "'' as REDUC_JORNADA_TYPE",
        "'' as REDUC_JORNADA_REASON", // Fin bloque null
        "`Worker`.`Employees`.`Compensation`.`Compensation_Summary_in_Annualized_Frequency`.`Total_Base_Pay`.`_VALUE` as TOTAL_BASE_PAY", // seguramente haya que pasarlo a array ( futuro )
        "`Worker`.`Employees`.`Compensation`.`Compensation_Summary_in_Annualized_Frequency`.`Currency`.`_VALUE` as TOTAL_BASE_PAY_CURRENCY", // seguramente haya que pasarlo a array ( futuro )
        "'' as ANNUAL_BASE_SALARY_EFFDT", // null
        "'' as TOTAL_FIXED_COMP", // operation
        "'' as TOTAL_VAR_COMP", // aditional field 8
        "'' as TOTAL_REWARDS", // N/A operation
        "`Worker`.`Employees`.`Payment_Election`.`Bank_Account_Name`.`_VALUE` as BANK_ACC",
        "`Worker`.`Employees`.`Payment_Election`.`IBAN`.`_VALUE`as BANK_IBAN",
        "'' as ANTICIPO_PDTE_IMPORTE", // inicio bloque null
        "'' as ANTICIPO_PDTE_MONEDA",
        "'' as ANTICIPO_MOTIVO",
        "'' as ANTICIPO_MOTIVO_DESCR",
        "'' as ANTICIPO_TIPO",
        "'' as ANTICIPO_TIPO_DESCR",
        "'' as ANTICIPO_PDTE_EFFDT",
        "'' as PRESTAMO_VIV_PDTE",
        "'' as PRESTAMO_VIV_PDTE_EFFDT",
        "'' as CREDITO_REDZP_TIPO",
        "'' as CREDITO_REDZP_TIPO_DESCR",
        "'' as CREDITO_REDZP_TIPO_EFFDT",
        "'' as CREDITO_REDZP_IMP",
        "'' as CREDITO_REDZP_IMP_EFFDT",
        "'' as IND_CR_REDZV",
        "'' as IND_CR_REDZV_EFFDT",
        "'' as IND_PR_REDZV",
        "'' as IND_PR_REDZV_EFFDT",
        "'' as PLURIEMPLEO_PORC",
        "'' as PLURIEMPLEO_EFFDT",
        "'' as IND_RETEN_JUD",
        "'' as IND_RETEN_JUD_EFFDT",
        "'' as AMBITO",
        "'' as AMBITO_DESCR",
        "'' as Z_PART_TIME_REASON",
        "'' as ZBANK_SENIORITY_DT", // fin bloque null
        "'' as LOCATION",
        "'' as LOCATION_NAME",
        "'' as LOCATION_ADDR", // null
        "'' as POSTAL", // null
        "'' as CITY", // null
        "'' as STATE", // null
        "'' as PHONE_PERSLAND_PREF", // calculo hacen falta weci 218-19-20-22-25-26
        "'' as PHONE_PERSLAND_NUM", // calculo
        "'' as PHONE_BUSSLAND_PREF", // calculo
        "'' as PHONE_BUSSLAND_NUM", // calculo
        "'' as PHONE_PERSMOB_PREF", // calculo
        "'' as PHONE_PERSMOB_NUM", // calculo
        "'' as PHONE_BUSSMOB_PREF", // calculo
        "'' as PHONE_BUSSMOB_NUM", // calculo
        "'' as MAIL_PW", // calculo
        "'' as MAIL_PERS", // calculo
        "'' as HRBP", // null
        "'' as HRBP_Name", // null
        "'' as HRBP_MAIL", // null
        "'' as HRBP_PHONE", // null
        "'' as PERS_ADDR_COUNTRY", // operaciones hasta el final  217 + 209
        "'' as PERS_ADDR_TYPE", // 217 + 185
        "'' as PERS_ADDR_NAME", // 217 + 183
        "'' as PERS_ADDR_NUMBER", // 217 + 187
        "'' as PERS_ADDR_FLOOR", // 217 + 190
        "'' as PERS_ADDR_DOOR", // 217 + 191
        "'' as PERS_ADDR_STAIRWELL", // 217 + 189
        "'' as PERS_ADDR_CITY", // 217 + 192
        "'' as PERS_ADDR_POSTAL", // 217 + 208
        "'' as PERS_ADDR_STATE_ID", // 217 + 211
        "'' as PERS_ADDR_STATE_DESCR", // 217 + 211 +  (Verificar code&&descr)
        "'' as FEC_PROC" // null
      ).na.fill(" ").distinct()


    //println("Imprimiendo el df de df_final")
    //df_final.printSchema()
    //df_final.show()


    //////////////////////////////////////////////////////////////////////////////

    //En esta lógica tengo todos los campos weci del DFC con el esquema 2

    var df_final2 = df_WeciReadXML_FieldAll
      .withColumn("Worker", explode_outer(col("`Worker`")))
      .withColumn("Other_Identifier", explode_outer(col("`Worker`.`Employees`.`Person_Identification`.`Other_Identifier`")))
      .withColumn("National_Identifier", explode_outer(col("`Worker`.`Employees`.`Person_Identification`.`National_Identifier`")))
      .withColumn("Position", explode_outer(col("`Worker`.`Employees`.`Position`")))
      .withColumn("Job_Family", explode_outer(col("`Position`.`Job_Family`")))

      // Estos 3 arrays de abajo son para las operaciones que tenemos que hacer luego
      //.withColumn("Address", explode_outer(col("`Person_Communication`.`Address`")))
      //.withColumn("Phone", explode_outer(col("`Address`.`Phone`")))
      //.withColumn("Email", explode_outer(col("`Address`.`Email`")))
      .selectExpr(
        //Los dos campos que no deben faltar
        s"'$dataDatePart' as data_date_part",
        "`Worker`.`Worker_Summary`.`Employee_ID`.`_VALUE` as ID_Empleado_Joins",
        "`Other_Identifier`.`Custom_ID_Type`.`_VALUE` as afterLogical", //Se utiliza para aplicar la lógica después

        //Todos los campos de weci
        "`Other_Identifier`.`Custom_ID`.`_VALUE` as EMPLID", //Mete ruido reguntar duda

        "`Other_Identifier`.`Custom_ID`.`_priorValue` as EMPLID_priorValue",
        "`Other_Identifier`.`Custom_ID`.`_isAdded` as EMPLID_isAdded",
        "`Other_Identifier`.`Custom_ID`.`_isDeleted` as EMPLID_isDeleted",

        "`Other_Identifier`.`Custom_ID_Type`.`_VALUE` as ID_CORP_EXTERNAL_SYSTEM_ID", //Mete ruido reguntar duda
        "`Other_Identifier`.`Custom_ID_Type`.`_priorValue` as ID_CORP_EXTERNAL_SYSTEM_ID_priorValue", // preguntar duda
        "`Other_Identifier`.`Custom_ID_Type`.`_isAdded` as ID_CORP_EXTERNAL_SYSTEM_ID_isAdded", // preguntar duda
        "`Other_Identifier`.`Custom_ID_Type`.`_isDeleted` as ID_CORP_EXTERNAL_SYSTEM_ID_isDeleted", // preguntar duda

        "`Worker`.`Worker_Summary`.`Employee_ID`.`_VALUE` as ID_WKD",
        "`Worker`.`Worker_Summary`.`Employee_ID`.`_priorValue` as ID_WKD_priorValue",
        "`Worker`.`Worker_Summary`.`Employee_ID`.`_isAdded` as ID_WKD_isAdded",
        "`Worker`.`Worker_Summary`.`Employee_ID`.`_isDeleted` as ID_WKD_isDeleted",

        "`Worker`.`Employees`.`Personal`.`Workday_Account`.`_VALUE` as WD_ACC",
        "`Worker`.`Employees`.`Personal`.`Workday_Account`.`_priorValue` as WD_ACC_priorValue",
        "`Worker`.`Employees`.`Personal`.`Workday_Account`.`_isAdded` as WD_ACC_isAdded",
        "`Worker`.`Employees`.`Personal`.`Workday_Account`.`_isDeleted` as WD_ACC_isDeleted",

        "`Worker`.`Employees`.`Worker_Status`.`Active`.`_VALUE` as ACTIVE_STATUS",
        "`Worker`.`Employees`.`Worker_Status`.`Active`.`_priorValue` as ACTIVE_STATUS_priorValue",
        "`Worker`.`Employees`.`Worker_Status`.`Active`.`_isAdded` as ACTIVE_STATUS_isAdded",
        "`Worker`.`Employees`.`Worker_Status`.`Active`.`_isDeleted` as ACTIVE_STATUS_isDeleted",

        "`Worker`.`Employees`.`Worker_Status`.`Status`.`_VALUE` as EMPLOYEE_STATUS",
        "`Worker`.`Employees`.`Worker_Status`.`Status`.`_priorValue` as EMPLOYEE_STATUS_priorValue",
        "`Worker`.`Employees`.`Worker_Status`.`Status`.`_isAdded` as EMPLOYEE_STATUS_isAdded",
        "`Worker`.`Employees`.`Worker_Status`.`Status`.`_isDeleted` as EMPLOYEE_STATUS_isDeleted",

        "`Worker`.`Employees`.`Personal`.`Name`.`First_Name`.`_VALUE` as FIRST_NAME",
        "`Worker`.`Employees`.`Personal`.`Name`.`First_Name`.`_priorValue` as FIRST_NAME_priorValue",
        "`Worker`.`Employees`.`Personal`.`Name`.`First_Name`.`_isAdded` as FIRST_NAME_isAdded",
        "`Worker`.`Employees`.`Personal`.`Name`.`First_Name`.`_isDeleted` as FIRST_NAME_isDeleted",

        "`Worker`.`Employees`.`Personal`.`Name`.`Middle_Name`.`_VALUE` as MIDDLE_NAME",
        "`Worker`.`Employees`.`Personal`.`Name`.`Middle_Name`.`_priorValue` as MIDDLE_NAME_priorValue",
        "`Worker`.`Employees`.`Personal`.`Name`.`Middle_Name`.`_isAdded` as MIDDLE_NAME_isAdded",
        "`Worker`.`Employees`.`Personal`.`Name`.`Middle_Name`.`_isDeleted` as MIDDLE_NAME_isDeleted",

        "`Worker`.`Employees`.`Personal`.`Name`.`Last_Name`.`_VALUE` as LAST_NAME",
        "`Worker`.`Employees`.`Personal`.`Name`.`Last_Name`.`_priorValue` as LAST_NAME_priorValue",
        "`Worker`.`Employees`.`Personal`.`Name`.`Last_Name`.`_isAdded` as LAST_NAME_isAdded",
        "`Worker`.`Employees`.`Personal`.`Name`.`Last_Name`.`_isDeleted` as LAST_NAME_isDeleted",

        "`Worker`.`Employees`.`Personal`.`Name`.`Secondary_Last_Name`.`_VALUE` as SECOND_LAST_NAME",
        "`Worker`.`Employees`.`Personal`.`Name`.`Secondary_Last_Name`.`_priorValue` as SECOND_LAST_NAME_priorValue",
        "`Worker`.`Employees`.`Personal`.`Name`.`Secondary_Last_Name`.`_isAdded` as SECOND_LAST_NAME_isAdded",
        "`Worker`.`Employees`.`Personal`.`Name`.`Secondary_Last_Name`.`_isDeleted` as SECOND_LAST_NAME_isDeleted",

        "`National_Identifier`.`Country`.`_VALUE` as NATIONAL_ID_COUNTRY",
        "`National_Identifier`.`Country`.`_priorValue` as NATIONAL_ID_COUNTRY_priorValue",
        "`National_Identifier`.`Country`.`_isAdded` as NATIONAL_ID_COUNTRY_isAdded",
        "`National_Identifier`.`Country`.`_isDeleted` as NATIONAL_ID_COUNTRY_isDeleted",

        "`National_Identifier`.`National_ID_Type`.`_VALUE` as NATIONAL_ID_TYPE",
        "`National_Identifier`.`National_ID_Type`.`_priorValue` as NATIONAL_ID_TYPE_priorValue",
        "`National_Identifier`.`National_ID_Type`.`_isAdded` as NATIONAL_ID_TYPE_isAdded",
        "`National_Identifier`.`National_ID_Type`.`_isDeleted` as NATIONAL_ID_TYPE_isDeleted",

        "`National_Identifier`.`National_ID_Type`.`_VALUE` as NATIONAL_ID_TYPE_DESCR", // campo es un array ? Pocho
        "`National_Identifier`.`National_ID_Type`.`_priorValue` as NATIONAL_ID_TYPE_DESCR_priorValue", // campo es un array ? Pocho
        "`National_Identifier`.`National_ID_Type`.`_isAdded` as NATIONAL_ID_TYPE_DESCR_isAdded", // campo es un array ? Pocho
        "`National_Identifier`.`National_ID_Type`.`_isDeleted` as NATIONAL_ID_TYPE_DESCR_isDeleted", // campo es un array ? Pocho

        "`National_Identifier`.`National_ID`.`_VALUE` as NATIONAL_ID",
        "`National_Identifier`.`National_ID`.`_priorValue` as NATIONAL_ID_priorValue",
        "`National_Identifier`.`National_ID`.`_isAdded` as NATIONAL_ID_isAdded",
        "`National_Identifier`.`National_ID`.`_isDeleted` as NATIONAL_ID_isDeleted",

        "`Worker`.`Employees`.`Person_Identification`.`Visa_Identifier`.`Country`.`_VALUE` as VISA_ID_COUNTRY",
        "`Worker`.`Employees`.`Person_Identification`.`Visa_Identifier`.`Country`.`_priorValue` as VISA_ID_COUNTRY_priorValue",
        "`Worker`.`Employees`.`Person_Identification`.`Visa_Identifier`.`Country`.`_isAdded` as VISA_ID_COUNTRY_isAdded",
        "`Worker`.`Employees`.`Person_Identification`.`Visa_Identifier`.`Country`.`_isDeleted` as VISA_ID_COUNTRY_isDeleted",

        "`Worker`.`Employees`.`Person_Identification`.`Visa_Identifier`.`Visa_ID_Type`.`_VALUE` as VISA_ID_TYPE",
        "`Worker`.`Employees`.`Person_Identification`.`Visa_Identifier`.`Visa_ID_Type`.`_priorValue` as VISA_ID_TYPE_priorValue",
        "`Worker`.`Employees`.`Person_Identification`.`Visa_Identifier`.`Visa_ID_Type`.`_isAdded` as VISA_ID_TYPE_isAdded",
        "`Worker`.`Employees`.`Person_Identification`.`Visa_Identifier`.`Visa_ID_Type`.`_isDeleted` as VISA_ID_TYPE_isDeleted",

        "`Worker`.`Employees`.`Person_Identification`.`Visa_Identifier`.`Visa_ID`.`_VALUE` as VISA_ID",
        "`Worker`.`Employees`.`Person_Identification`.`Visa_Identifier`.`Visa_ID`.`_priorValue` as VISA_ID_priorValue",
        "`Worker`.`Employees`.`Person_Identification`.`Visa_Identifier`.`Visa_ID`.`_isAdded` as VISA_ID_isAdded",
        "`Worker`.`Employees`.`Person_Identification`.`Visa_Identifier`.`Visa_ID`.`_isDeleted` as VISA_ID_isDeleted",

        "`Worker`.`Employees`.`Person_Identification`.`Passport_Identifier`.`Country`.`_VALUE` as PASSPORT_ID_COUNTRY",
        "`Worker`.`Employees`.`Person_Identification`.`Passport_Identifier`.`Country`.`_priorValue` as PASSPORT_ID_COUNTRY_priorValue",
        "`Worker`.`Employees`.`Person_Identification`.`Passport_Identifier`.`Country`.`_isAdded` as PASSPORT_ID_COUNTRY_isAdded",
        "`Worker`.`Employees`.`Person_Identification`.`Passport_Identifier`.`Country`.`_isDeleted` as PASSPORT_ID_COUNTRY_isDeleted",

        "`Worker`.`Employees`.`Person_Identification`.`Passport_Identifier`.`Passport_ID_Type`.`_VALUE` as PASSPORT_ID_TYPE",
        "`Worker`.`Employees`.`Person_Identification`.`Passport_Identifier`.`Passport_ID_Type`.`_priorValue` as PASSPORT_ID_TYPE_priorValue",
        "`Worker`.`Employees`.`Person_Identification`.`Passport_Identifier`.`Passport_ID_Type`.`_isAdded` as PASSPORT_ID_TYPE_isAdded",
        "`Worker`.`Employees`.`Person_Identification`.`Passport_Identifier`.`Passport_ID_Type`.`_isDeleted` as PASSPORT_ID_TYPE_isDeleted",

        "`Worker`.`Employees`.`Person_Identification`.`Passport_Identifier`.`Passport_ID`.`_VALUE` as PASSPORT_ID",
        "`Worker`.`Employees`.`Person_Identification`.`Passport_Identifier`.`Passport_ID`.`_priorValue` as PASSPORT_ID_priorValue",
        "`Worker`.`Employees`.`Person_Identification`.`Passport_Identifier`.`Passport_ID`.`_isAdded` as PASSPORT_ID_isAdded",
        "`Worker`.`Employees`.`Person_Identification`.`Passport_Identifier`.`Passport_ID`.`_isDeleted` as PASSPORT_ID_isDeleted",

        "`Worker`.`Employees`.`Personal`.`Date_of_Birth`.`_VALUE` as BIRTHDATE",
        "`Worker`.`Employees`.`Personal`.`Date_of_Birth`.`_priorValue` as BIRTHDATE_priorValue",
        "`Worker`.`Employees`.`Personal`.`Date_of_Birth`.`_isAdded` as BIRTHDATE_isAdded",
        "`Worker`.`Employees`.`Personal`.`Date_of_Birth`.`_isDeleted` as BIRTHDATE_isDeleted",

        "`Worker`.`Employees`.`Personal`.`Date_of_Death`.`_VALUE` as DT_OF_DEATH",
        "`Worker`.`Employees`.`Personal`.`Date_of_Death`.`_priorValue` as DT_OF_DEATH_priorValue",
        "`Worker`.`Employees`.`Personal`.`Date_of_Death`.`_isAdded` as DT_OF_DEATH_isAdded",
        "`Worker`.`Employees`.`Personal`.`Date_of_Death`.`_isDeleted` as DT_OF_DEATH_isDeleted",

        "`Worker`.`Employees`.`Personal`.`Country_of_Birth`.`_VALUE` as BIRTHCOUNTRY",
        "`Worker`.`Employees`.`Personal`.`Country_of_Birth`.`_priorValue` as BIRTHCOUNTRY_priorValue",
        "`Worker`.`Employees`.`Personal`.`Country_of_Birth`.`_isAdded` as BIRTHCOUNTRY_isAdded",
        "`Worker`.`Employees`.`Personal`.`Country_of_Birth`.`_isDeleted` as BIRTHCOUNTRY_isDeleted",

        "`Worker`.`Employees`.`Personal`.`City_of_Birth`.`_VALUE` as BIRTHCITY_BIRTHPLACE",
        "`Worker`.`Employees`.`Personal`.`City_of_Birth`.`_priorValue` as BIRTHCITY_BIRTHPLACE_priorValue",
        "`Worker`.`Employees`.`Personal`.`City_of_Birth`.`_isAdded` as BIRTHCITY_BIRTHPLACE_isAdded",
        "`Worker`.`Employees`.`Personal`.`City_of_Birth`.`_isDeleted` as BIRTHCITY_BIRTHPLACE_isDeleted",

        "`Worker`.`Employees`.`Personal`.`Region_of_Birth`.`_VALUE` as BIRTHSTATE",
        "`Worker`.`Employees`.`Personal`.`Region_of_Birth`.`_priorValue` as BIRTHSTATE_priorValue",
        "`Worker`.`Employees`.`Personal`.`Region_of_Birth`.`_isAdded` as BIRTHSTATE_isAdded",
        "`Worker`.`Employees`.`Personal`.`Region_of_Birth`.`_isDeleted` as BIRTHSTATE_isDeleted",

        "`Worker`.`Employees`.`Personal`.`Nationality`.`_VALUE` as Nationality",
        "`Worker`.`Employees`.`Personal`.`Nationality`.`_priorValue` as Nationality_priorValue",
        "`Worker`.`Employees`.`Personal`.`Region_of_Birth`.`_isAdded` as BIRTHSTATE_isAdded",
        "`Worker`.`Employees`.`Personal`.`Nationality`.`_isDeleted` as Nationality_isDeleted",

        "`Worker`.`Employees`.`Personal`.`Preferred_Lenguage`.`_VALUE` as PREF_LANG_LANG_CD",
        "`Worker`.`Employees`.`Personal`.`Preferred_Lenguage`.`_priorValue` as PREF_LANG_LANG_CD_priorValue",
        "`Worker`.`Employees`.`Personal`.`Preferred_Lenguage`.`_isAdded` as PREF_LANG_LANG_CD_isAdded",
        "`Worker`.`Employees`.`Personal`.`Preferred_Lenguage`.`_isDeleted` as PREF_LANG_LANG_CD_isDeleted",

        "`Worker`.`Employees`.`Personal`.`Gender`.`_VALUE` as GENDER",
        "`Worker`.`Employees`.`Personal`.`Gender`.`_priorValue` as GENDER_priorValue",
        "`Worker`.`Employees`.`Personal`.`Gender`.`_isAdded` as GENDER_isAdded",
        "`Worker`.`Employees`.`Personal`.`Gender`.`_isDeleted` as GENDER_isDeleted",

        "`Worker`.`Employees`.`Personal`.`Marital_Status`.`_VALUE` as MAR_STATUS",
        "`Worker`.`Employees`.`Personal`.`Marital_Status`.`_priorValue` as MAR_STATUS_priorValue",
        "`Worker`.`Employees`.`Personal`.`Marital_Status`.`_isAdded` as MAR_STATUS_isAdded",
        "`Worker`.`Employees`.`Personal`.`Marital_Status`.`_isDeleted` as MAR_STATUS_isDeleted",

        "`Worker`.`Employees`.`Personal`.`Marital_Status_Date`.`_VALUE` as MAR_STATUS_EFFDT",
        "`Worker`.`Employees`.`Personal`.`Marital_Status_Date`.`_priorValue` as MAR_STATUS_EFFDT_priorValue",
        "`Worker`.`Employees`.`Personal`.`Marital_Status_Date`.`_isAdded` as MAR_STATUS_EFFDT_isAdded",
        "`Worker`.`Employees`.`Personal`.`Marital_Status_Date`.`_isDeleted` as MAR_STATUS_EFFDT_isDeleted",

        "`Worker`.`Employees`.`Personal`.`Disability_Status`.`Grade`.`_VALUE` as DISABLE_DEGREE",
        "`Worker`.`Employees`.`Personal`.`Disability_Status`.`Grade`.`_priorValue` as DISABLE_DEGREE_priorValue",
        "`Worker`.`Employees`.`Personal`.`Disability_Status`.`Grade`.`_isAdded` as DISABLE_DEGREE_isAdded",
        "`Worker`.`Employees`.`Personal`.`Disability_Status`.`Grade`.`_isDeleted` as DISABLE_DEGREE_isDeleted",

        "`Worker`.`Employees`.`Personal`.`Disability_Status`.`Disability`.`_VALUE` as DISABLE_DEGREE_DESCR",
        "`Worker`.`Employees`.`Personal`.`Disability_Status`.`Disability`.`_priorValue` as DISABLE_DEGREE_DESCR_priorValue",
        "`Worker`.`Employees`.`Personal`.`Disability_Status`.`Disability`.`_isAdded` as DISABLE_DEGREE_DESCR_isAdded",
        "`Worker`.`Employees`.`Personal`.`Disability_Status`.`Disability`.`_isDeleted` as DISABLE_DEGREE_DESCR_isDeleted",

        "`Worker`.`Employees`.`Personal`.`Disability_Status`.`Certification_Authority`.`_VALUE` as DISABLE_CERT_AUTHORITY",
        "`Worker`.`Employees`.`Personal`.`Disability_Status`.`Certification_Authority`.`_priorValue` as DISABLE_CERT_AUTHORITY_priorValue",
        "`Worker`.`Employees`.`Personal`.`Disability_Status`.`Certification_Authority`.`_isAdded` as DISABLE_CERT_AUTHORITY_isAdded",
        "`Worker`.`Employees`.`Personal`.`Disability_Status`.`Certification_Authority`.`_isDeleted` as DISABLE_CERT_AUTHORITY_isDeleted",

        "`Worker`.`Employees`.`Personal`.`Disability_Status`.`Disability_Status_Date`.`_VALUE` as DISABLE_EFFDT", // comprobar cuando tengamos datos reales
        "`Worker`.`Employees`.`Personal`.`Disability_Status`.`Disability_Status_Date`.`_priorValue` as DISABLE_EFFDT_priorValue", // comprobar cuando tengamos datos reales
        "`Worker`.`Employees`.`Personal`.`Disability_Status`.`Disability_Status_Date`.`_isAdded` as DISABLE_EFFDT_isAdded", // comprobar cuando tengamos datos reales
        "`Worker`.`Employees`.`Personal`.`Disability_Status`.`Disability_Status_Date`.`_isDeleted` as DISABLE_EFFDT_isDeleted", // comprobar cuando tengamos datos reales

        "`Worker`.`Employees`.`Personal`.`Disability_Status`.`End_Date`.`_VALUE` as DISABLE_END_EFFDT",
        "`Worker`.`Employees`.`Personal`.`Disability_Status`.`End_Date`.`_priorValue` as DISABLE_END_EFFDT_priorValue",
        "`Worker`.`Employees`.`Personal`.`Disability_Status`.`End_Date`.`_isAdded` as DISABLE_END_EFFDT_isAdded",
        "`Worker`.`Employees`.`Personal`.`Disability_Status`.`End_Date`.`_isDeleted` as DISABLE_END_EFFDT_isDeleted",

        "`Worker`.`Employees`.`Personal`.`Disability_Status`.`Date_Known`.`_VALUE` as DISABLE_KNOW_EFFDT",
        "`Worker`.`Employees`.`Personal`.`Disability_Status`.`Date_Known`.`_priorValue` as DISABLE_KNOW_EFFDT_priorValue",
        "`Worker`.`Employees`.`Personal`.`Disability_Status`.`Date_Known`.`_isAdded` as DISABLE_KNOW_EFFDT_isAdded",
        "`Worker`.`Employees`.`Personal`.`Disability_Status`.`Date_Known`.`_isDeleted` as DISABLE_KNOW_EFFDT_isDeleted",

        "`Worker`.`Employees`.`Employee_Contract`.`Contract_Type`.`_VALUE` as CONTRACT_TYPE",
        "`Worker`.`Employees`.`Employee_Contract`.`Contract_Type`.`_priorValue` as CONTRACT_TYPE_priorValue",
        "`Worker`.`Employees`.`Employee_Contract`.`Contract_Type`.`_isAdded` as CONTRACT_TYPE_isAdded",
        "`Worker`.`Employees`.`Employee_Contract`.`Contract_Type`.`_isDeleted` as CONTRACT_TYPE_isDeleted",

        "`Worker`.`Employees`.`Employee_Contract`.`Contract_Status`.`_VALUE` as CONTRACT_STATUS",
        "`Worker`.`Employees`.`Employee_Contract`.`Contract_Status`.`_priorValue` as CONTRACT_STATUS_priorValue",
        "`Worker`.`Employees`.`Employee_Contract`.`Contract_Status`.`_isAdded` as CONTRACT_STATUS_isAdded",
        "`Worker`.`Employees`.`Employee_Contract`.`Contract_Status`.`_isDeleted` as CONTRACT_STATUS_isDeleted",


        "`Worker`.`Employees`.`Employee_Contract`.`Start_Date`.`_VALUE` as CONTRACT_START_DT",
        "`Worker`.`Employees`.`Employee_Contract`.`Start_Date`.`_priorValue` as CONTRACT_START_DT_priorValue",
        "`Worker`.`Employees`.`Employee_Contract`.`Start_Date`.`_isAdded` as CONTRACT_START_DT_isAdded",
        "`Worker`.`Employees`.`Employee_Contract`.`Start_Date`.`_isDeleted` as CONTRACT_START_DT_isDeleted",

        "`Worker`.`Employees`.`Employee_Contract`.`End_Date`.`_VALUE` as CONTRACT_END_DT",
        "`Worker`.`Employees`.`Employee_Contract`.`End_Date`.`_priorValue` as CONTRACT_END_DT_priorValue",
        "`Worker`.`Employees`.`Employee_Contract`.`End_Date`.`_isAdded` as CONTRACT_END_DT_isAdded",
        "`Worker`.`Employees`.`Employee_Contract`.`End_Date`.`_isDeleted` as CONTRACT_END_DT_isDeleted",

        "`Position`.`Position_Time_Type`.`_VALUE` as TYME_TYPE",
        "`Position`.`Position_Time_Type`.`_priorValue` as TYME_TYPE_priorValue",
        "`Position`.`Position_Time_Type`.`_isAdded` as TYME_TYPE_isAdded",
        "`Position`.`Position_Time_Type`.`_isDeleted` as TYME_TYPE_isDeleted",

        "`Worker`.`Employees`.`Worker_Status`.`Company_Service_Date`.`_VALUE` as COMPANY_EFFDT",
        "`Worker`.`Employees`.`Worker_Status`.`Company_Service_Date`.`_priorValue` as COMPANY_EFFDT_priorValue",
        "`Worker`.`Employees`.`Worker_Status`.`Company_Service_Date`.`_isAdded` as COMPANY_EFFDT_isAdded",
        "`Worker`.`Employees`.`Worker_Status`.`Company_Service_Date`.`_isDeleted` as COMPANY_EFFDT_isDeleted",

        "`Worker`.`Employees`.`Worker_Status`.`Hire_Date`.`_VALUE` as HIRE_DT",
        "`Worker`.`Employees`.`Worker_Status`.`Hire_Date`.`_priorValue` as HIRE_DT_priorValue",
        "`Worker`.`Employees`.`Worker_Status`.`Hire_Date`.`_isAdded` as HIRE_DT_isAdded",
        "`Worker`.`Employees`.`Worker_Status`.`Hire_Date`.`_isDeleted` as HIRE_DT_isDeleted",

        "`Worker`.`Employees`.`Worker_Status`.`Hire_Reason`.`_VALUE` as HIRE_REASON",
        "`Worker`.`Employees`.`Worker_Status`.`Hire_Reason`.`_priorValue` as HIRE_REASON_priorValue",
        "`Worker`.`Employees`.`Worker_Status`.`Hire_Reason`.`_isAdded` as HIRE_REASON_isAdded",
        "`Worker`.`Employees`.`Worker_Status`.`Hire_Reason`.`_isDeleted` as HIRE_REASON_isDeleted",

        "`Worker`.`Employees`.`Worker_Status`.`Seniority_Date`.`_VALUE` as CMPNY_SENIORITY_DT",
        "`Worker`.`Employees`.`Worker_Status`.`Seniority_Date`.`_priorValue` as CMPNY_SENIORITY_DT_priorValue",
        "`Worker`.`Employees`.`Worker_Status`.`Seniority_Date`.`_isAdded` as CMPNY_SENIORITY_DT_isAdded",
        "`Worker`.`Employees`.`Worker_Status`.`Seniority_Date`.`_isDeleted` as CMPNY_SENIORITY_DT_isDeleted",

        "`Worker`.`Employees`.`Worker_Status`.`Original_Hire_Date`.`_VALUE` as ORIGINAL_HIRE_DT",
        "`Worker`.`Employees`.`Worker_Status`.`Original_Hire_Date`.`_priorValue` as ORIGINAL_HIRE_DT_priorValue",
        "`Worker`.`Employees`.`Worker_Status`.`Original_Hire_Date`.`_isAdded` as ORIGINAL_HIRE_DT_isAdded",
        "`Worker`.`Employees`.`Worker_Status`.`Original_Hire_Date`.`_isDeleted` as ORIGINAL_HIRE_DT_isDeleted",

        "`Worker`.`Employees`.`Worker_Status`.`Continuous_Service_Date`.`_VALUE` as CONTINUOUS_SERV_DT",
        "`Worker`.`Employees`.`Worker_Status`.`Continuous_Service_Date`.`_priorValue` as CONTINUOUS_SERV_DT_priorValue",
        "`Worker`.`Employees`.`Worker_Status`.`Continuous_Service_Date`.`_isAdded` as CONTINUOUS_SERV_DT_isAdded",
        "`Worker`.`Employees`.`Worker_Status`.`Continuous_Service_Date`.`_isDeleted` as CONTINUOUS_SERV_DT_isDeleted",

        "`Worker`.`Employees`.`Worker_Status`.`Termination_Date`.`_VALUE` as TERMINATION_DT",
        "`Worker`.`Employees`.`Worker_Status`.`Termination_Date`.`_priorValue` as TERMINATION_DT_priorValue",
        "`Worker`.`Employees`.`Worker_Status`.`Termination_Date`.`_isAdded` as TERMINATION_DT_isAdded",
        "`Worker`.`Employees`.`Worker_Status`.`Termination_Date`.`_isDeleted` as TERMINATION_DT_isDeleted",

        "`Worker`.`Employees`.`Worker_Status`.`Primary_Termination_Reason`.`_VALUE` as TERMINATION_REASON",
        "`Worker`.`Employees`.`Worker_Status`.`Primary_Termination_Reason`.`_priorValue` as TERMINATION_REASON_priorValue",
        "`Worker`.`Employees`.`Worker_Status`.`Primary_Termination_Reason`.`_isAdded` as TERMINATION_REASON_isAdded",
        "`Worker`.`Employees`.`Worker_Status`.`Primary_Termination_Reason`.`_isDeleted` as TERMINATION_REASON_isDeleted",

        "`Worker`.`Employees`.`Worker_Status`.`Local_Termination_Reason`.`_VALUE` as TERMINATION_REASON_LOCAL",
        "`Worker`.`Employees`.`Worker_Status`.`Local_Termination_Reason`.`_priorValue` as TERMINATION_REASON_LOCAL_priorValue",
        "`Worker`.`Employees`.`Worker_Status`.`Local_Termination_Reason`.`_isAdded` as TERMINATION_REASON_LOCAL_isAdded",
        "`Worker`.`Employees`.`Worker_Status`.`Local_Termination_Reason`.`_isDeleted` as TERMINATION_REASON_LOCAL_isDeleted",

        "`Worker`.`Employees`.`Worker_Status`.`Retirement_Date`.`_VALUE` as RETIREMENT_DT",
        "`Worker`.`Employees`.`Worker_Status`.`Retirement_Date`.`_priorValue` as RETIREMENT_DT_priorValue",
        "`Worker`.`Employees`.`Worker_Status`.`Retirement_Date`.`_isAdded` as RETIREMENT_DT_isAdded",
        "`Worker`.`Employees`.`Worker_Status`.`Retirement_Date`.`_isDeleted` as RETIREMENT_DT_isDeleted",

        "`Position`.`Management_Level`.`_VALUE` as MANAGEMENT_LEVEL",
        "`Position`.`Management_Level`.`_priorValue` as MANAGEMENT_LEVEL_priorValue",
        "`Position`.`Management_Level`.`_isAdded` as MANAGEMENT_LEVEL_isAdded",
        "`Position`.`Management_Level`.`_isDeleted` as MANAGEMENT_LEVEL_isDeleted",

        "`Position`.`Job_Profile`.`_VALUE` as JOBCODE", // WECI432 (code&&descr??)
        "`Position`.`Job_Profile`.`_priorValue` as JOBCODE_priorValue", // WECI432 (code&&descr??)
        "`Position`.`Job_Profile`.`_isAdded` as JOBCODE_isAdded", // WECI432 (code&&descr??)
        "`Position`.`Job_Profile`.`_isDeleted` as JOBCODE_isDeleted", // WECI432 (code&&descr??)

        "`Position`.`Job_Profile`.`_VALUE` as JOBCODE_DESCR", // WECI432 (code&&descr??)
        "`Position`.`Job_Profile`.`_priorValue` as JOBCODE_DESCR_priorValue", // WECI432 (code&&descr??)
        "`Position`.`Job_Profile`.`_isAdded` as JOBCODE_DESCR_isAdded", // WECI432 (code&&descr??)
        "`Position`.`Job_Profile`.`_isDeleted` as JOBCODE_DESCR_isDeleted", // WECI432 (code&&descr??)

        "`Position`.`Position_ID`.`_VALUE` as POSITION_NBR",
        "`Position`.`Position_ID`.`_priorValue` as POSITION_NBR_priorValue",
        "`Position`.`Position_ID`.`_isAdded` as POSITION_NBR_isAdded",
        "`Position`.`Position_ID`.`_isDeleted` as POSITION_NBR_isDeleted",

        "`Position`.`Business_Title`.`_VALUE` as POSITION_NBR_DESCR",
        "`Position`.`Business_Title`.`_priorValue` as POSITION_NBR_DESCR_priorValue",
        "`Position`.`Business_Title`.`_isAdded` as POSITION_NBR_DESCR_isAdded",
        "`Position`.`Business_Title`.`_isDeleted` as POSITION_NBR_DESCR_isDeleted",

        "`Position`.`Supervisor`.`ID`.`_VALUE` as MANAGER_ID",
        "`Position`.`Supervisor`.`ID`.`_priorValue` as MANAGER_ID_priorValue",
        "`Position`.`Supervisor`.`ID`.`_isAdded` as MANAGER_ID_isAdded",
        "`Position`.`Supervisor`.`ID`.`_isDeleted` as MANAGER_ID_isDeleted",

        "`Position`.`Supervisor`.`ID`.`_VALUE` as SUPERVISOR_EFFDT", // null ID Supervisor ?
        "`Position`.`Supervisor`.`ID`.`_priorValue` as SUPERVISOR_EFFDT_priorValue", // null ID Supervisor ?
        "`Position`.`Supervisor`.`ID`.`_isAdded` as SUPERVISOR_EFFDT_isAdded", // null ID Supervisor ?
        "`Position`.`Supervisor`.`ID`.`_isDeleted` as SUPERVISOR_EFFDT_isDeleted", // null ID Supervisor ?

        "`Position`.`Supervisor`.`Name`.`_VALUE` as SUPERVISORY", // null Name Supervisor?
        "`Position`.`Supervisor`.`Name`.`_priorValue` as SUPERVISORY_priorValue", // null Name Supervisor?
        "`Position`.`Supervisor`.`Name`.`_isAdded` as SUPERVISORY_isAdded", // null Name Supervisor?
        "`Position`.`Supervisor`.`Name`.`_isDeleted` as SUPERVISORY_isDeleted", // null Name Supervisor?

        "`Worker`.`Employees`.`Compensation`.`Compensation_Grade`.`_VALUE` as COMPENSATION_GRADE",
        "`Worker`.`Employees`.`Compensation`.`Compensation_Grade`.`_priorValue` as COMPENSATION_GRADE_priorValue",
        "`Worker`.`Employees`.`Compensation`.`Compensation_Grade`.`_isAdded` as COMPENSATION_GRADE_isAdded",
        "`Worker`.`Employees`.`Compensation`.`Compensation_Grade`.`_isDeleted` as COMPENSATION_GRADE_isDeleted",

        "`Position`.`Job_Classification`.`Job_Classiication`.`_VALUE` as JOB_CLASSIFICATION", // errata
        "`Position`.`Job_Classification`.`Job_Classiication`.`_priorValue` as JOB_CLASSIFICATION_priorValue", // errata
        "`Position`.`Job_Classification`.`Job_Classiication`.`_isAdded` as JOB_CLASSIFICATION_isAdded", // errata
        "`Position`.`Job_Classification`.`Job_Classiication`.`_isDeleted` as JOB_CLASSIFICATION_isDeleted", // errata

        "`Worker`.`Employees`.`Collective_Agreement`.`Collective_Agreement`.`_VALUE` as COLLECTIVE_AGREEMENT",
        "`Worker`.`Employees`.`Collective_Agreement`.`Collective_Agreement`.`_priorValue` as COLLECTIVE_AGREEMENT_priorValue",
        "`Worker`.`Employees`.`Collective_Agreement`.`Collective_Agreement`.`_isAdded` as COLLECTIVE_AGREEMENT_isAdded",
        "`Worker`.`Employees`.`Collective_Agreement`.`Collective_Agreement`.`_isDeleted` as COLLECTIVE_AGREEMENT_isDeleted",

        //"`Worker`.`Employees`.`Collective_Agreement`.`Collective_Agreement_Factor`.`_VALUE` as COLLECTIVE_AGREEMENT_FACTOR",
        //"`Worker`.`Employees`.`Collective_Agreement`.`Collective_Agreement_Factor`.`_priorValue` as COLLECTIVE_AGREEMENT_FACTOR_priorValue",
        //"`Worker`.`Employees`.`Collective_Agreement`.`Collective_Agreement_Factor`.`_isAdded` as COLLECTIVE_AGREEMENT_FACTOR_isAdded",
        //"`Worker`.`Employees`.`Collective_Agreement`.`Collective_Agreement_Factor`.`_isDeleted` as COLLECTIVE_AGREEMENT_FACTOR_isDeleted",

        "`Worker`.`Employees`.`Collective_Agreement`.`Start_Date`.`_VALUE` as COLLECTIVE_AGREEMENT_DT",
        "`Worker`.`Employees`.`Collective_Agreement`.`Start_Date`.`_priorValue` as COLLECTIVE_AGREEMENT_DT_priorValue",
        "`Worker`.`Employees`.`Collective_Agreement`.`Start_Date`.`_isAdded` as COLLECTIVE_AGREEMENT_DT_isAdded",
        "`Worker`.`Employees`.`Collective_Agreement`.`Start_Date`.`_isDeleted` as COLLECTIVE_AGREEMENT_DT_isDeleted",

        "`Worker`.`Employees`.`Compensation`.`Compensation_Summary_in_Annualized_Frequency`.`Total_Base_Pay`.`_VALUE` as TOTAL_BASE_PAY", // seguramente haya que pasarlo a array ( futuro )
        "`Worker`.`Employees`.`Compensation`.`Compensation_Summary_in_Annualized_Frequency`.`Total_Base_Pay`.`_priorValue` as TOTAL_BASE_PAY_priorValue", // seguramente haya que pasarlo a array ( futuro )
        "`Worker`.`Employees`.`Compensation`.`Compensation_Summary_in_Annualized_Frequency`.`Total_Base_Pay`.`_isAdded` as TOTAL_BASE_PAY_isAdded", // seguramente haya que pasarlo a array ( futuro )
        "`Worker`.`Employees`.`Compensation`.`Compensation_Summary_in_Annualized_Frequency`.`Total_Base_Pay`.`_isDeleted` as TOTAL_BASE_PAY_isDeleted", // seguramente haya que pasarlo a array ( futuro )

        "`Worker`.`Employees`.`Compensation`.`Compensation_Summary_in_Annualized_Frequency`.`Currency`.`_VALUE` as TOTAL_BASE_PAY_CURRENCY", // seguramente haya que pasarlo a array ( futuro )
        "`Worker`.`Employees`.`Compensation`.`Compensation_Summary_in_Annualized_Frequency`.`Currency`.`_priorValue` as TOTAL_BASE_PAY_CURRENCY_priorValue", // seguramente haya que pasarlo a array ( futuro )
        "`Worker`.`Employees`.`Compensation`.`Compensation_Summary_in_Annualized_Frequency`.`Currency`.`_isAdded` as TOTAL_BASE_PAY_CURRENCY_isAdded", // seguramente haya que pasarlo a array ( futuro )
        "`Worker`.`Employees`.`Compensation`.`Compensation_Summary_in_Annualized_Frequency`.`Currency`.`_isDeleted` as TOTAL_BASE_PAY_CURRENCY_isDeleted", // seguramente haya que pasarlo a array ( futuro )

        "`Worker`.`Employees`.`Payment_Election`.`Bank_Account_Name`.`_VALUE` as BANK_ACC",
        "`Worker`.`Employees`.`Payment_Election`.`Bank_Account_Name`.`_priorValue` as BANK_ACC_priorValue",
        "`Worker`.`Employees`.`Payment_Election`.`Bank_Account_Name`.`_isAdded` as BANK_ACC_isAdded",
        "`Worker`.`Employees`.`Payment_Election`.`Bank_Account_Name`.`_isDeleted` as BANK_ACC_isDeleted",

        "`Worker`.`Employees`.`Payment_Election`.`IBAN`.`_VALUE`as BANK_IBAN",
        "`Worker`.`Employees`.`Payment_Election`.`IBAN`.`_priorValue` as BANK_IBAN_priorValue",
        "`Worker`.`Employees`.`Payment_Election`.`IBAN`.`_isAdded`as BANK_IBAN_isAdded",
        "`Worker`.`Employees`.`Payment_Election`.`IBAN`.`_isDeleted`as BANK_IBAN_isDeleted",

        "`Position`.`Business_Site`.`Location_ID`.`_VALUE` as LOCATION",
        "`Position`.`Business_Site`.`Location_ID`.`_priorValue` as LOCATION_priorValue",
        "`Position`.`Business_Site`.`Location_ID`.`_isAdded` as LOCATION_isAdded",
        "`Position`.`Business_Site`.`Location_ID`.`_isDeleted` as LOCATION_isDeleted",

        "`Position`.`Business_Site`.`Location_Name`.`_VALUE` as LOCATION_NAME",
        "`Position`.`Business_Site`.`Location_Name`.`_priorValue` as LOCATION_NAME_priorValue",
        "`Position`.`Business_Site`.`Location_Name`.`_isAdded` as LOCATION_NAME_isAdded",
        "`Position`.`Business_Site`.`Location_Name`.`_isDeleted` as LOCATION_NAME_isDeleted",

      ).na.fill(" ").distinct()

    println("Imprimiendo el df de df_final2")
    //df_final2.printSchema()
    //df_final2.show(100)

    var df_Local = df_final2
      .selectExpr(
      "ID_WKD",
      "if(ID_CORP_EXTERNAL_SYSTEM_ID Like '%Local_ID%',EMPLID,'N/A-XML') as ID_CORP_EXTERNAL_SYSTEM_ID")
      .na.fill(" ").distinct()

    //df_Local.show(100)

    var df_Payroll = df_final2
      .selectExpr(
      "ID_WKD",
      "if(ID_CORP_EXTERNAL_SYSTEM_ID Like '%Payroll_ID%',EMPLID,'N/A-XML') as EMPLID")
      .na.fill(" ").distinct()

    //df_Payroll.show(100)

    var df_primerMapeo_payroll_local = df_final2.as("df_final2")
      .join(df_Local.as("df_Local"), expr("df_final2.ID_WKD=df_Local.ID_WKD") ,"left")
      .join(df_Payroll.as("df_Payroll"), expr("df_final2.ID_WKD=df_Payroll.ID_WKD") ,"left")

    df_primerMapeo_payroll_local
      .selectExpr(
        "df_final2.data_date_part",
        "df_final2.ID_Empleado_Joins",
        "df_final2.ID_WKD",
        "df_Payroll.EMPLID",
        "df_Local.ID_CORP_EXTERNAL_SYSTEM_ID")
      .na.fill(" ").distinct()
      .show(100)


  } //End Main

} //End Class
