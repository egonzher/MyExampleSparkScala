#! /bin/ksh

export DIR_HOME=${param.RUTA_BUSINESS_WD}

cd $DIR_HOME

export JAVA_HOME=${JAVA_HOME}
export SPARK_HOME=${SPARK_HOME}

export PROPERTIES_CONSOLID=consolid.prop
export PROPERTIES=DM_RRHH_ETL_WD.prop
properties_lectura=$(cat $PROPERTIES)
export properties_wd="$properties_lectura
PARAM_FECHA_DATA_DATE=$1"
echo "$properties_wd" > $PROPERTIES_CONSOLID


export EXEC=${EXEC}
export CORES=${CORES}
export DRIVER=${DRIVER}
export MEM=${MEM}
export BROADCAST=${BROADCAST}
export OVERHEAD=${OVERHEAD}

export BBDD=${param.PARAM_BU_WORKDAY}
export NODO=${impala.nodo}
export PORT=${impala.port}

export CLASE=etl_datos_consolid_wd


spark2-submit --master yarn-client --num-executors $EXEC --executor-memory $MEM --executor-cores $CORES --driver-memory $DRIVER --conf spark.yarn.executor.memoryOverhead=$OVERHEAD \
--conf spark.sql.autoBroadcastJoinThreshold=$BROADCAST \
--class com.santander.rrhh.$CLASE   ${param.RUTA_BUSINESS_WD}/rrhh-spark-shade.jar  $PROPERTIES_CONSOLID > salida_$CLASE.log 2>error_$CLASE.log;


RESULT=$?
if [ ${RESULT} -ne 0 ]; then
  echo echo "Error en la llamada al script de hive job_consolid_wd.sh: $RESULT"
  i=1
  until [ $i -gt 2 ]
  do
   spark2-submit --master yarn-client --num-executors $EXEC --executor-memory $MEM --executor-cores $CORES --driver-memory $DRIVER --conf spark.yarn.executor.memoryOverhead=$OVERHEAD \
            --conf spark.sql.autoBroadcastJoinThreshold=$BROADCAST \
            --class com.santander.rrhh.$CLASE  ${param.RUTA_BUSINESS_WD}/rrhh-spark-shade.jar  $PROPERTIES_CONSOLID > salida_$CLASE.log 2>error_$CLASE.log;
   RESULT=$?
   if [ ${RESULT} -ne 0 ]; then
   echo echo "Error en la llamada al script de hive job_consolid_wd.sh: $RESULT"
   ((i=i+1))
   else
   exit $RESULT
   fi
  done
fi

    table=datos_consolid
    validateTable=$(impala-shell -k -i $NODO:$PORT -d $BBDD -q "SHOW TABLES LIKE '$table'")
    if [[ -z $validateTable ]]; then
        echo "invalidate metadata"
        impala-shell -k -i $NODO:$PORT -q "invalidate metadata $BBDD.$table"
    else
        echo "refresh"
        impala-shell -k -i $NODO:$PORT -q "refresh $BBDD.$table"
        sleep 60
        impala-shell -k -i $NODO:$PORT -q "COMPUTE STATS $BBDD.$table"
    fi

exit $RESULT
