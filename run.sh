# $1 = File path for input
# $2 = File path for output 
# $3 = File path for kml 

if [ $# -le 3 ]
then
	echo  "Usage: $0 <input path> <output path> <community areas kml file path> [rebuild]"
else
	if [ $# -eq 4 ]
	then
		mvn package;
	fi
	hadoop jar target/finalproject-1.0-SNAPSHOT.jar edu.boisestate.cs597.MapLonLatCommunity $1 `dirname $2`/part0 $3
	hadoop jar target/finalproject-1.0-SNAPSHOT.jar edu.boisestate.cs597.TopCrimes `dirname $2`/part0 `dirname $2`/part1 $3
	hadoop fs -copyToLocal `dirname $2`/part1 .
	hadoop fs -rmr `dirname $2`/part1
	head -n 50 part1/part-r-00000 | sort -t$'\t' -k2 -nr > part1/top50.csv
	hadoop fs -mkdir `dirname $2`/part2
	hadoop fs -put part1/top50.csv `dirname $2`/part2
	rm -rf part1
	hadoop jar target/finalproject-1.0-SNAPSHOT.jar edu.boisestate.cs597.CrimesByDay `dirname $2`/part0 `dirname $2`/part3 `dirname $2`/part2/top50.csv
	hadoop fs -rmr `dirname $2`/part2 
	#hadoop jar target/finalproject-1.0-SNAPSHOT.jar edu.boisestate.cs597.CalculateCorrelation `dirname $2`/part0 `dirname $2`/part4
fi
