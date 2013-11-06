# $1 = File path for input data
# $2 = File path for output data
mvn clean install
hadoop jar target/finalproject-1.0-SNAPSHOT.jar edu.boisestate.cs597.RequestsByDay $1 $2 $3
#hadoop fs -rmr $2
#hadoop fs -copyToLocal $2 .
#sort -t$'\t' -k2 -nr `basename $2`/part-r-00000 > `basename $2`/output.txt
#hadoop fs -put `basename $2`/output.txt $2
#echo "data can be found in `basename $2`/output.txt"