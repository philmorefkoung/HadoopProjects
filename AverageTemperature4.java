import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import java.io.IOException;
import java.util.*;

public class AverageTemperature4 {

    public static class TemperatureMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // Skip the header row
            if (key.get() == 0) {
                return;
            }

            String[] parts = value.toString().split(",");
            String country = parts[1];
            String city = parts[3];
            double temperature = Double.parseDouble(parts[7]);
            if(temperature != -99) {
            context.write(new Text(country), new Text(city + ":" + temperature));
            }
        }
    }

    public static class CapitalMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // Skip the header row
            if (key.get() == 0) {
                return;
            }

            String[] parts = value.toString().split(",");
            String country = parts[0];
            String capital = parts[1];
            String type = parts[2];

            if (type.equals("capitalType")) {
                context.write(new Text(country), new Text(capital));
            }
        }
    }

    public static class AvgTempReducer extends Reducer<Text, Text, Text, DoubleWritable> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String capital = "";
            List<Double> temperatures = new ArrayList<>();

            for (Text value : values) {
                String[] parts = value.toString().split(":");
                if (parts.length == 2) {
                    capital = parts[0];
                    double temperature = Double.parseDouble(parts[1]);
                    temperatures.add(temperature);
                }
            }

            if (!capital.isEmpty()) {
                double sum = 0.0;
                int count = 0;
                for (Double temperature : temperatures) {
                    sum += temperature;
                    count++;
                }

                if (count > 0) {
                    double avgTemperature = sum / count;
                    context.write(new Text(key.toString() + " " + capital), new DoubleWritable(avgTemperature));
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "AverageTemperature4");

        job.setJarByClass(AverageTemperature4.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        job.setReducerClass(AvgTempReducer.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, TemperatureMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, CapitalMapper.class);

        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

