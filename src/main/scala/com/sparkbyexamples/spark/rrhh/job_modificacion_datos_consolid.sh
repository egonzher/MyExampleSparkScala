#! /bin/ksh

###############################################################################
#                                                                             #
# Nombre.....: job_modificacion_datos_consolid.sh                             #
#                                                                             #
# Descripcion..: Actualiza los campos de una tabla                            #
#                                                                             #
#                                                                             #
# Version: 1.0                                                                #
#                                                                             #
# Creacion...:  14/02/2019                                                    #
#                                                                             #
# Fecha......:  14/02/2019                                                    #
#                                                                             #
#                                                                             #
# Modificaciones                                                              #
#                                                                             #
# Autor       Fecha       Version   Motivo                                    #
# ----------  ----------  -------   ---------------------------------------   #
# ISBAN       14/02/2019  1.0       Creacion                                  #
#                                                                             #
###############################################################################

. ${PATH_INITIATIVES_SHARE}/DM_RRHH_ETL.cfg
. ${param.RUTA_BUSINESS_WD}/DM_RRHH_ETL_WD.cfg

kinit ${kerberos.user} -k -t ${kerberos.keytab};

beeline -u $PARAM_CONEXION_BEELINE -hiveconf PARAM_BU_WORKDAY=$PARAM_BU_WORKDAY -hiveconf HDFS_BUSINESS_WORKDAY=$HDFS_BUSINESS_WORKDAY -f ${PATH_INITIATIVES_SHARE}/datos_vacaciones_cib.hql  2> ${RUTA_EXTRACTOR_WD}/modificacion_tablas.log
beeline -u $PARAM_CONEXION_BEELINE -hiveconf PARAM_BU_WORKDAY=$PARAM_BU_WORKDAY -hiveconf HDFS_BUSINESS_WORKDAY=$HDFS_BUSINESS_WORKDAY -f ${PATH_INITIATIVES_SHARE}/wd/datos_supervisory.hql  2> ${RUTA_EXTRACTOR_WD}/modificacion_tablas.log
beeline -u $PARAM_CONEXION_BEELINE -hiveconf PARAM_BU_WORKDAY=$PARAM_BU_WORKDAY -hiveconf HDFS_BUSINESS_WORKDAY=$HDFS_BUSINESS_WORKDAY -f ${PATH_INITIATIVES_SHARE}/wd/datos_organization.hql  2> ${RUTA_EXTRACTOR_WD}/modificacion_tablas.log


exit 0
