# Word Count
## To Run
input this into your terminal to run the hadoop program: 'hadoop jar MyWordCount3.jar (the .java you want to run so WordCount1 WordCount2 or WordCount3) (input path of file in HDFS) (directory you want to output)' <br>
To get the output file input: "hdfs dfs -get (directory you want to output)/part-r-00000 (directory you want to place it)" <br>
## Each java file (they are all stored in MyWordCount3.jar)
**WordCount** : <br>
Returns the word count of every word in the text file <br>
**WordCount2** : <br>
Returns the occurrances of specific words (ignoring punctuation and case) you can change these in the java file to whichever word you want in the if statement<br>
**WordCount3** : <br>
Returns top 10 words in the file regardless (ignoring punctuation and case) you can change it to top k words in the source file by changing the threshold  <br>
