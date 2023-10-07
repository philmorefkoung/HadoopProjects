//import java.io.BufferedReader;
//import java.io.File;
//import java.io.FileReader;
import java.io.IOException;
//import java.net.URI;
//import java.util.ArrayList;
//import java.util.Collections;
//import java.util.Comparator;
//import java.util.HashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
//import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
//import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
//import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import java.util.TreeMap;
import java.util.Map;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class WordCount3 {

    public static class Map2
            extends Mapper<LongWritable, Text, Text, IntWritable>{

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text(); // type of output key

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] mydata = value.toString().split(" ");
            for (String data : mydata) {
            	String result = data.replaceAll("\\p{Punct}","");
            	word.set(result.toLowerCase()); // set word as each input keyword
                context.write(word, one); // create a pair <keyword, 1>
            	}
            }
        }
    

    public static class Reduce
            extends Reducer<Text,IntWritable,Text,IntWritable> {

        //private IntWritable result = new IntWritable();
        
        private TreeMap<Integer, Text> tmap2 = new TreeMap<>();
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0; // initialize the sum for each keyword
            for (IntWritable val : values) {
                sum += val.get();
            }
           tmap2.put(sum,  new Text(key));
            
            if(tmap2.size() > 10) {
            	tmap2.remove(tmap2.firstKey());
            }
        }
    @Override
    public void cleanup(Context context) throws IOException, InterruptedException
    {
    	for(Map.Entry<Integer, Text> entry : tmap2.descendingMap().entrySet()) {
    		context.write(entry.getValue(), new IntWritable(entry.getKey()));
    	}
    }
    }
    // Driver program
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        // get all args
        if (otherArgs.length != 2) {
            System.err.println("Usage: WordCount2 <in> <out>");
            System.exit(2);
        }

        // create a job with name "wordcount"
        Job job = new Job(conf, "wordcount");
        job.setJarByClass(WordCount3.class);
        job.setMapperClass(Map2.class);
        job.setReducerClass(Reduce.class);

        job.setCombinerClass(Reduce.class);

        // set output key type
        job.setOutputKeyClass(Text.class);
        // set output value type
        job.setOutputValueClass(IntWritable.class);
        //set the HDFS path of the input data
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        // set the HDFS path for the output
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        //Wait till job completion
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}