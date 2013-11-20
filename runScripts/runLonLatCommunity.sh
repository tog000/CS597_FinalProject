# $0 = File path for KML
# $1 = File path for input data
# $2 = File path for output data
mvn package
hadoop jar target/finalproject-1.0-SNAPSHOT-jar-with-dependencies.jar edu.boisestate.cs597.MapLonLatCommunity $@
hadoop dfs -cat output/part* > output.txt
nano output.txt
