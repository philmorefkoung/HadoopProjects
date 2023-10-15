import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AverageTemperature3{

    public static class TokenizerMapper extends Mapper<Object, Text, Text, DoubleWritable> {

        private Text city = new Text();
        private DoubleWritable temperature = new DoubleWritable();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = value.toString().split(",");
            if (tokens.length >= 8 && tokens[0].equals("Region") && 
            		tokens[1].equals("Country") && tokens[2].equals("State") 
            		&& tokens[3].equals("City") && tokens[4].equals("Month") 
            		&& tokens[5].equals("Day") && tokens[6].equals("Year") 
            		&& tokens[7].equals("AvgTemperature")) { 
            	return; 
            	}
            if (tokens.length >= 8) {
                String countryName = tokens[1];
                String cityName = tokens[3];
                double avgTemp = Double.parseDouble(tokens[7]);
                if(countryName.equals("Spain") && avgTemp != -99) {
                	city.set(cityName);
                	temperature.set(avgTemp);
                	context.write(city, temperature);
                }
            }
            
        
        }
    }

    public static class AverageReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

        private DoubleWritable result = new DoubleWritable();

        public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {
            double sum = 0;
            int count = 0;
            for (DoubleWritable val : values) {
                sum += val.get();
                count++;
            }
            double avg = sum / count;
            result.set(avg);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "average temperature by year");
        job.setJarByClass(AverageTemperature3.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(AverageReducer.class);
        job.setReducerClass(AverageReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

