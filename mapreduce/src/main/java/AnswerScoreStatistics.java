import model.AnswersTuple;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class AnswerScoreStatistics {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "answer score statistics");
        job.setJarByClass(AnswerScoreStatistics.class);
        job.setMapperClass(AnswerScoreMapper.class);
        job.setReducerClass(AnswerScoreReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class AnswerScoreMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final Text statsKey = new Text("statistics");

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            AnswersTuple tuple = AnswersTuple.parseCsvLine(key.get(), value);
            IntWritable scoreValue = new IntWritable();

            // Emit the score value
            scoreValue.set(tuple.getScore());
            context.write(statsKey, scoreValue);
        }
    }

    public static class AnswerScoreReducer extends Reducer<Text, IntWritable, Text, Text> {

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            List<Integer> scores = new ArrayList<>();
            Text result = new Text();

            // Collect all the scores
            for (IntWritable value : values) {
                scores.add(value.get());
            }

            // Calculate statistics
            int count = scores.size();
            double sum = 0;
            double sumOfSquares = 0;

            for (int score : scores) {
                sum += score;
                sumOfSquares += Math.pow(score, 2);
            }

            // Sort the scores to calculate median
            Collections.sort(scores);

            // Calculate statistics
            double average = sum / count;
            double variance = (sumOfSquares / count) - Math.pow(average, 2);
            double standardDeviation = Math.sqrt(variance);
            int minimum = scores.get(0);
            int maximum = scores.get(count - 1);
            int median = count % 2 == 0 ? (scores.get(count / 2 - 1) + scores.get(count / 2)) / 2 : scores.get(count / 2);

            // Prepare the result string
            String statistics = "\n" +
                    "Count: " + count + "\n" +
                    "Average: " + average + "\n" +
                    "Median: " + median + "\n" +
                    "Min: " + minimum + "\n" +
                    "Max: " + maximum + "\n" +
                    "Standard Deviation: " + standardDeviation + "\n";

            // Output the result
            result.set(statistics);
            context.write(key, result);
        }
    }
}
