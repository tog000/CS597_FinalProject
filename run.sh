# $input = File path for input
# $output = File path for output 
# $kml = File path for kml 

if [ $# -le 2 ]
then
        echo  "Usage: $0 <input path> <output path> <community areas kml file path> [rebuild]"
else
        if [ $# -eq 4 ]
        then
                mvn package;
        fi

	project_path=/home/projects/eucleia

	input=$project_path/$1;
	output=$project_path/$2;
	kml=$project_path/$3;

        echo "************ MAP CRIMES TO COMMUNITIES ************"
        hadoop jar target/finalproject-1.0-SNAPSHOT.jar edu.boisestate.cs597.MapLonLatCommunity $input/crime* $project_path/mapped_crimes $kml
        echo "************ FIND TOP CRIME TYPES ************"
        hadoop jar target/finalproject-1.0-SNAPSHOT.jar edu.boisestate.cs597.TopCrimes $project_path/mapped_crimes $project_path/top_crimes
        rm -r top_crimes
        hadoop fs -copyToLocal $project_path/top_crimes .
        number_of_crimes=`java -cp target/finalproject-1.0-SNAPSHOT.jar edu.boisestate.cs597.TopCrimes getNumber`
        top_filename="top$number_of_crimes.csv"
        head -n $number_of_crimes top_crimes/part* | sort -t$'\t' -k2 -nr > top_crimes/$top_filename
        hadoop fs -put top_crimes/$top_filename $project_path/top_crimes
        echo "************ CALCULATE TOP CRIMES FOR EVERY DAY ************"
        hadoop jar target/finalproject-1.0-SNAPSHOT.jar edu.boisestate.cs597.CrimesByDay $project_path/mapped_crimes $project_path/daily_crimes $project_path/top_crimes/$top_filename
        hadoop fs -rmr $project_path/correlation/
        hadoop fs -mkdir $project_path/correlation/
        hadoop fs -cp $project_path/daily_crimes/* $project_path/correlation
        echo "************ CALCULATE CORRELATION BETWEEN WEATHER, SOCIOECONOMIC, HEALTH and CRIME ************"
        #hadoop jar target/finalproject-1.0-SNAPSHOT.jar edu.boisestate.cs597.CalculateCorrelation $input/weather* $input/health* $input/economy* $project_path/correlation $output
        hadoop jar target/finalproject-1.0-SNAPSHOT-jar-with-dependencies.jar edu.boisestate.cs597.CalculateCorrelation $input/weather* $input/health* $input/economy* $project_path/correlation $output
        hadoop fs -cat $output/part* > output.txt
        sort -t$'\t' -k1 -nr output.txt > sorted_output.txt
fi

