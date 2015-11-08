
SH=/D/scala-2.9.1.final

PARAH=D:/workspace/parabond

JARS=$PARAH/casbah-commons_2.9.0-1-2.1.5.0.jar
JARS="$JARS;$PARAH/casbah-core_2.9.0-1-2.1.5.0.jar"
JARS="$JARS;$PARAH/casbah-gridfs_2.9.0-1-2.1.5.0.jar"
JARS="$JARS;$PARAH/casbah-query_2.9.0-1-2.1.5.0.jar"
JARS="$JARS;$PARAH/junit-4.9b3.jar"
JARS="$JARS;$PARAH/log4j-over-slf4j-1.6.2.jar"
JARS="$JARS;$PARAH/mongo-2.6.5.jar"
JARS="$JARS;$PARAH/scalaj-collection_2.9.1-1.2.jar"
JARS="$JARS;$PARAH/scalatest-1.6.1.jar"
JARS="$JARS;$PARAH/slf4j-api-1.6.2.jar"
JARS="$JARS;$PARAH/slf4j-simple-1.6.2.jar"

export CLASSPATH="$PARAH/bin;$JARS"

PATH=$SH/bin:$PATH

export JAVA_OPTS

TRIALS="1 2 4 8 16 32 64 128 256 512 1024"


DATE=`date "+%Y-%m-%d"`
HOST=`hostname -s`

TRIALS_OUT=trials-${HOST}-${DATE}.txt

echo $HOST > $TRIALS_OUT

for TR in $TRIALS
do
  T0=`date -f "%s"`
  JAVA_OPTS=-Dn=$TR
  
  $SH/bin/scala.bat org.junit.runner.JUnitCore scaly.parabond.test.Mr01 | grep class >> $TRIALS_OUT
  
  $SH/bin/scala.bat org.junit.runner.JUnitCore scaly.parabond.test.Mr03 | grep class >> $TRIALS_OUT

  $SH/bin/scala.bat org.junit.runner.JUnitCore scaly.parabond.test.NPortfolio00 | grep class >> $TRIALS_OUT

  $SH/bin/scala.bat org.junit.runner.JUnitCore scaly.parabond.test.NPortfolio01 | grep class >> $TRIALS_OUT
  
  T1=`date -f "%s"`
  
  DT=`echo $T0 $T1 | awk '{print ($2 - $1)}'`
  echo $TR $DT
done