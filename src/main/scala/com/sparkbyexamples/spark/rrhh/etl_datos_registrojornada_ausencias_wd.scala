package com.sparkbyexamples.spark.rrhh

import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, explode, expr}

object etl_datos_registrojornada_ausencias_wd {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local[1]")
      .appName("SparkByExample")
      .getOrCreate()

    //Primeros Ejemplos Reales sobre SGT
    val registroJornadaXML = spark.sqlContext.read
      .format("com.databricks.spark.xml")
      .option("rowTag", "wd:Report_Entry")
      .load("src/main/resources/rrhh/registrojornada_ausencias.xml")

    println("PrintSchema de registroJornadaXML")
    registroJornadaXML.printSchema()


      val xmlRegisters = registroJornadaXML
        .withColumnRenamed("wd:WD_ID","wd_id")
        .withColumn("wd_id",col("wd_id").cast(StringType))
        .withColumnRenamed("wd:Payroll_ID","payroll_id")
        //extravalues of payroll_id
        .withColumn("payroll_id_id",col("payroll_id.`wd:ID`.`_VALUE`"))
        .withColumn("payroll_id_type",col("payroll_id.`wd:ID`.`_type`"))

        .withColumn("payroll_id",col("payroll_id.`_Descriptor`").cast(StringType))
        .withColumn("last_day_employee",col("`wd:Employee_group`.`wd:Last_Day_Employee_Actual`"))
        .withColumn("unit_of_time_for_leave_tracking_aux",explode_outer(col("`wd:Leave_of_Absence_Requests_group`")))
        .withColumn("amount_in_hours_of_leave",col("unit_of_time_for_leave_tracking_aux.`wd:Amount_In_hours_of_Leave`"))
        .withColumn("first_date_of_absence",col("unit_of_time_for_leave_tracking_aux.`wd:First_Date_of_Absence`"))
        .withColumn("last_day_request_actual",col("unit_of_time_for_leave_tracking_aux.`wd:Last_Day_Request_Actual`"))
        .withColumn("leave_percentage",col("unit_of_time_for_leave_tracking_aux.`wd:Leave_Percentage`"))
        .withColumn("leave_of_absence_type_id",col("unit_of_time_for_leave_tracking_aux.`wd:Leave_of_Absence_Type_ID`"))
        .withColumn("unit_of_time_for_leave_tracking_id", col("unit_of_time_for_leave_tracking_aux.`wd:Unit_of_Time_for_Leave_Tracking`.`_Descriptor`"))
        .withColumn("units_requested_for_current_leave",col("unit_of_time_for_leave_tracking_aux.`wd:Units_Requested_for_Current_Leave`"))

        .withColumn("Time_Off_Requests_group_array",explode_outer(col("`wd:Time_Off_Requests_group`")))
        .withColumn("approval_date",col("Time_Off_Requests_group_array.`wd:Approval_Date`"))
        .withColumn("approved",col("Time_Off_Requests_group_array.`wd:Approved`"))
        .withColumn("assignment_date",col("Time_Off_Requests_group_array.`wd:Assignment_Date`"))
        .withColumn("Awaiting_Persons_aux",explode_outer(col("Time_Off_Requests_group_array.`wd:Awaiting_Persons`")))
        .withColumn("awaiting_persons",col("Awaiting_Persons_aux.`_Descriptor`"))
        //exttravalues of awaiting persons
        .withColumn("Awaiting_Persons_id",col("Awaiting_Persons_aux.`wd:ID`.`_VALUE`"))
        .withColumn("Awaiting_Persons_type",col("Awaiting_Persons_aux.`wd:ID`.`_type`"))


        .withColumn("cf_time_off_reason_aux",explode_outer(col("Time_Off_Requests_group_array.`wd:CF_Time_Off_Reason`.`wd:ID`")))
        .withColumn("cf_time_off_reason",col("Time_Off_Requests_group_array.`wd:CF_Time_Off_Reason`.`_Descriptor`"))
        //extradatos del CF_TIME_OFF_REASON
        .withColumn("explode_time_off_reason",explode_outer((col("Time_Off_Requests_group_array.`wd:CF_Time_Off_Reason`.`wd:ID`"))))
        .withColumn("Awaiting_Persons_aux_id_id",col("explode_time_off_reason.`_VALUE`"))
        .withColumn("Awaiting_Persons_aux_id_type",col("explode_time_off_reason.`_type`"))

        .withColumn("dates_time_off_aux",explode_outer(col("Time_Off_Requests_group_array.`wd:Dates_Time_Off`.`wd:ID`")))
        .withColumn("dates_time_off",col("Time_Off_Requests_group_array.`wd:Dates_Time_Off`.`_Descriptor`"))
        //dates_time off extradatos
        .withColumn("explode_dates_time_off", explode(col("dates_time_off_aux.`wd:ID`")))
        .withColumn("dates_time_off_id",col("explode_dates_time_off.`_VALUE`"))
        .withColumn("dates_time_off_type",col("explode_dates_time_off.`_type`"))

        .withColumn("first_date_of_time_off",col("Time_Off_Requests_group_array.`wd:First_Date_of_Time_Off`"))
        .withColumn("time_off_total_units_days",col("Time_Off_Requests_group_array.`wd:Time_Off_Total_Units_Days`"))
        .withColumn("time_off_total_units_hours",col("Time_Off_Requests_group_array.`wd:Time_Off_Total_Units_Hours`"))

        .withColumn("Time_Off_Types_for_Time_Off_Event_aux",explode_outer(col("Time_Off_Requests_group_array.`wd:Time_Off_Types_for_Time_Off_Event`.`wd:ID`")))
        .withColumn("dates_time_off_event",col("Time_Off_Requests_group_array.`wd:Time_Off_Types_for_Time_Off_Event`.`_Descriptor`"))
        // extradatos time_off_types-for_time_off_event
        .withColumn("explode_Time_Off_Types_for_Time_Off_Event", col("dates_time_off_aux.`wd:ID`"))
        .withColumn("time_off_types_for_time_off_id",col("explode_Time_Off_Types_for_Time_Off_Event.`_VALUE`"))
        .withColumn("time_off_types_for_time_off_type",col("explode_Time_Off_Types_for_Time_Off_Event.`_type`"))

        /* .withColumn("dates_time_off_event",
           when(col("Time_Off_Requests_group_array.`wd:Approved`") === 0, concat_ws(sep =" ",col("Time_Off_Requests_group_array.`wd:Time_Off_Types_for_Time_Off_Event`.`_Descriptor`"),lit(" (PENDING)"))).otherwise(col("Time_Off_Requests_group_array.`wd:Time_Off_Types_for_Time_Off_Event`.`_Descriptor`"))))
  */
        .withColumn("total_units",col("Time_Off_Requests_group_array.`wd:Total_Units`"))
        .withColumn("approvedByWorkers",col("Time_Off_Requests_group_array.`wd:approvedByWorkers`.`_Descriptor`"))
        // extradatos approvedbyworkers
        .withColumn("explode_approvedByWorkers", explode_outer(col("Time_Off_Requests_group_array.`wd:approvedByWorkers`.`wd:ID`")))
        .withColumn("time_off_types_for_time_off_id",col("explode_approvedByWorkers.`_VALUE`"))
        .withColumn("time_off_types_for_time_off_type",col("explode_approvedByWorkers.`_type`"))

        .withColumn("timeOffRequest",col("Time_Off_Requests_group_array.`wd:timeOffRequest`.`_Descriptor`"))
        // extradatos time off request
        .withColumn("explode_time_off_request", explode_outer(col("Time_Off_Requests_group_array.`wd:timeOffRequest`.`wd:ID`")))
        .withColumn("time_off_types_for_time_off_id",col("explode_time_off_request.`_VALUE`"))
        .withColumn("time_off_types_for_time_off_type",col("explode_time_off_request.`_type`"))

        //.withColumn("data_date_part",lit(s"$dataDatePart"))
        /* .select("wd_id","payroll_id","last_day_employee","amount_in_hours_of_leave","first_date_of_absence", "last_day_request_actual", "leave_percentage","leave_of_absence_type_id","unit_of_time_for_leave_tracking_id",
           "units_requested_for_current_leave", /*"approval_date","approved","assignment_date",*/"awaiting_persons", "data_date_part")*/

        /*
        .select("wd_id","payroll_id","last_day_employee","amount_in_hours_of_leave","first_date_of_absence", "last_day_request_actual", "leave_percentage","leave_of_absence_type_id","unit_of_time_for_leave_tracking_id",
          "units_requested_for_current_leave", "approval_date","approved","assignment_date","awaiting_persons","cf_time_off_reason","dates_time_off","first_date_of_time_off","time_off_total_units_days","time_off_total_units_hours",
          "dates_time_off_event","total_units","approvedByWorkers","timeOffRequest", "data_date_part")
        */

        .na.fill(" ")
        .na.fill(0)
        .na.fill(0.0)


      //println("new df => " + xmlRegisters.count)


      //xmlRegisters.show(truncate = false)
      //leaveOfAbsenceRequestsGroupsArray.show(numRows = 10,truncate = false)
      //xmlRegisters.write.format(format).mode(saveMode).insertInto(PARAM_BU_WORKDAY.concat(table))






  }

}
