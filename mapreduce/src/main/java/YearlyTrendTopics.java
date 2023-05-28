/**
 * This MapReduce job reads Questions.csv file and constructs QuestionsTuple objects by parsing lines.
 * Then the resulting objects are passed to the reducer to be serialized using QuestionsTupleOutputFormat.
 * Another Mapper can de-serialize objects from this file using the QuestionsTupleInputFormat.
 */

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
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.*;
import java.util.logging.Logger;

public class YearlyTrendTopics {
    private static final Logger log = Logger.getLogger(YearlyTrendTopics.class.getName());

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        // Job Config
        // Reduce number of splits to reduce Map node overhead
        conf.setInt("mapreduce.input.lineinputformat.linespermap", 80000);
        Job job = Job.getInstance(conf, "yearly trend topics");
        job.setJarByClass(YearlyTrendTopics.class);

        // Mapper Config
        job.setInputFormatClass(NLineInputFormat.class);
        job.setMapperClass(YearMapper.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(QuestionsTuple.class);

        // Reducer Config
        // Note: If you do not set Reducer, then Mapper output is the final output
        job.setReducerClass(TrendReducer.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class YearMapper extends Mapper<LongWritable, Text, IntWritable, QuestionsTuple> {

        public void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException {
            IntWritable year = new IntWritable();
            QuestionsTuple tuple;

            // Parse line into object fields
            tuple = QuestionsTuple.parseCsvLine(key.get(), values);
            if (tuple.getCreationDate().isPresent()) {
                year.set(tuple.getCreationDate().get().getYear());
                context.write(year, tuple);
            }
        }
    }

    public static class TrendReducer
            extends Reducer<IntWritable, QuestionsTuple, IntWritable, Text> {

        // Reducer value must be defined as Iterable<>
        public void reduce(IntWritable key, Iterable<QuestionsTuple> tuples,
                           Context context
        ) throws IOException, InterruptedException {
            HashMap<String, Integer> tagScores = new HashMap<>();

            for (QuestionsTuple tuple :
                    tuples) {
                
                for (String tag :
                        tuple.getTags()) {
                    tagScores.put(tag, tagScores.getOrDefault(tag, 0) + tuple.getScore());
                }
            }
            
            int max = Collections.max(tagScores.values());

            for (Map.Entry<String, Integer> entry : tagScores.entrySet()) {
                if (entry.getValue() == max) {
                    Text trendTag = new Text();
                    
                    trendTag.set(entry.getKey() + ", " + entry.getValue());
                    context.write(key, trendTag);
                }
            }
        }
    }
}
