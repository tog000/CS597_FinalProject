# $1 = File path for input data
# $2 = File path for output data
mvn package
#hadoop jar target/finalproject-1.0-SNAPSHOT.jar edu.boisestate.cs597.CalculateCorrelation $1 $2
hadoop jar target/finalproject-1.0-SNAPSHOT-jar-with-dependencies.jar edu.boisestate.cs597.CalculateCorrelation $1 $2
hadoop dfs -cat output/part* > output.txt
sort -r output.txt > output.txt.sorted
nano output.txt.sorted
