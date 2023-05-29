import model.QuestionsTuple;
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

public class QuestionScoreStatistics {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "question score statistics");
        job.setJarByClass(QuestionScoreStatistics.class);
        job.setMapperClass(QuestionScoreMapper.class);
        job.setReducerClass(QuestionScoreReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class QuestionScoreMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final Text statsKey = new Text("statistics");

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            QuestionsTuple tuple = QuestionsTuple.parseCsvLine(key.get(), value);
            IntWritable scoreValue = new IntWritable();

            // Emit the score value
            scoreValue.set(tuple.getScore());
            context.write(statsKey, scoreValue);
        }
    }

    public static class QuestionScoreReducer extends Reducer<Text, IntWritable, Text, Text> {
        private final Text result = new Text();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            List<Integer> scores = new ArrayList<>();

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
            StringBuilder statistics = new StringBuilder();
            statistics.append("\n");
            statistics.append("Count: ").append(count).append("\n");
            statistics.append("Average: ").append(average).append("\n");
            statistics.append("Median: ").append(median).append("\n");
            statistics.append("Min: ").append(minimum).append("\n");
            statistics.append("Max: ").append(maximum).append("\n");
            statistics.append("Standard Deviation: ").append(standardDeviation).append("\n");

            // Output the result
            result.set(statistics.toString());
            context.write(key, result);
        }
    }
}
