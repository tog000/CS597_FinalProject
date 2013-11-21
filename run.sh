# $1 = File path for input
# $2 = File path for output 
# $3 = File path for kml 

if [ $# -le 2 ]
then
	echo  "Usage: $0 <input path> <output path> <community areas kml file path> [rebuild]"
else
	if [ $# -eq 4 ]
	then
		mvn package;
	fi
	echo "************ MAP CRIMES TO COMMUNITIES ************"
	hadoop jar target/finalproject-1.0-SNAPSHOT.jar edu.boisestate.cs597.MapLonLatCommunity $1 mapped_crimes $3
	echo "************ FIND TOP CRIME TYPES ************"
	hadoop jar target/finalproject-1.0-SNAPSHOT.jar edu.boisestate.cs597.TopCrimes mapped_crimes top_crimes
	rm -r top_crimes
	hadoop fs -copyToLocal top_crimes .
	number_of_crimes=`java -cp target/finalproject-1.0-SNAPSHOT.jar edu.boisestate.cs597.TopCrimes getNumber`
	top_filename="top$number_of_crimes.csv"
	head -n $number_of_crimes top_crimes/part* | sort -t$'\t' -k2 -nr > top_crimes/$top_filename
	hadoop fs -put top_crimes/$top_filename top_crimes
	echo "************ CALCULATE TOP CRIMES FOR EVERY DAY ************"
	hadoop jar target/finalproject-1.0-SNAPSHOT.jar edu.boisestate.cs597.CrimesByDay mapped_crimes daily_crimes top_crimes/$top_filename
	#hadoop fs -rmr `dirname $2`/part2 
	#hadoop jar target/finalproject-1.0-SNAPSHOT.jar edu.boisestate.cs597.CalculateCorrelation `dirname $2`/part0 `dirname $2`/part4
fi
