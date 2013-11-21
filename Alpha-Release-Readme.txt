Build and run instructions:

1. Download the project (commit 1f0df929128912bd069a89624646072115f1d343)

2. Place the following files on HDFS: 

input/
	crime_chicago.csv
	weather_chicago.csv
	economy_chicago.csv
	health_chicago.csv

data/
	community_areas.kml

3. Invoke the main run script "run.sh" by: $ ./run.sh input output data/communityareas.kml
