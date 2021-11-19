#! /bin/sh
#
# $Id: logReport.sh 421 2007-04-05 15:46:55Z dhill $
#
if [ $1 ] ; then
        MODULE=$1
else
        MODULE="pm1"
fi

if [ $2 ] ; then
        OUT_FILE=$2
else
        OUT_FILE=${MODULE}_logReport.txt
fi

#get temp directory
tmpDir=`mcsGetConfig SystemConfig SystemTempFileDir`

rm -f ${tmpDir}/${MODULE}_logReport.tar.gz
tar -zcf ${tmpDir}/${MODULE}_logReport.tar.gz /var/log/mariadb/columnstore > /dev/null 2>&1
cp ${tmpDir}/${MODULE}_logReport.tar.gz .
tar -zcf ${MODULE}_mysqllogReport.tar.gz /var/log/mysql/*.err 2>/dev/null

echo '******************** Log Configuration  ********************' >> $OUT_FILE
echo '' >> $OUT_FILE
echo 'MariaDB ColumnStore System Log Configuration Data' >> $OUT_FILE
echo '' >> $OUT_FILE
exit 0

