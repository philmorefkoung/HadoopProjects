# MapReduce Word Count
## To Run
input this into your terminal to run the hadoop program: 'hadoop jar MyWordCount3.jar (the .java you want to run so WordCount1 WordCount2 or WordCount3) (input path of file in HDFS) (directory you want to output)' <br>
To get the output file input: "hdfs dfs -get (directory you want to output)/part-r-00000 (directory you want to place it)" <br>
Feel free to run it against the input_txt text file provided, it will run with the jar and java files assuming all the appropriate libraries for hadoop MapReduce are installed 
## Each java file (they are all stored in MyWordCount3.jar)
**WordCount** : <br>
Returns the word count of every word in the text file <br>
**WordCount2** : <br>
Returns the occurrances of specific words (ignoring punctuation and case) you can change these in the java file to whichever word you want in the if statement<br>
**WordCount3** : <br>
Returns top 10 words in the file regardless (ignoring punctuation and case) you can change it to top k words in the source file by changing the threshold  <br>

# MapReduce Average Temperature 
**Average Temperature** : <br>
Finds the average temperature by "Region" <br>
**Average Temperature 2** : <br>
Finds the average temperature by "Year" for countries located in the "Asia" "Region" <br>
**Average Temperature 3** : <br>
Finds the average temperature by "City" located in "Spain" <br>
**Average Temperature 4** : <br>
Finds the average temperature of each "Capital" using Reduce-side join of the files city_temperature.csv and country-list.csv <br>

## To Run  <br>
to run the jar files for 1-3:  <br>
hadoop jar AverageTemperature.jar AverageTemperature /directory/of/city_temperature.csv /outputFileName    <br>
hadoop jar AverageTemperature2.jar AverageTemperature2 /directory/of/city_temperature.csv /outputFileName  <br>
hadoop jar AverageTemperature3.jar AverageTemperature3 /directory/of/city_temperature.csv /outputFileName   <br>
  
to run far file for 4:    <br>
hadoop jar AverageTemperature4.jar AverageTemperature4 /directory/of/city_temperature.csv /directory/of/country-list.csv /outputFileName   <br>

to retrive the file for part 1-4:    <br>
hdfs dfs -get /outputFileName/part-r-00000 /directory/of/where/you/want   <br>
