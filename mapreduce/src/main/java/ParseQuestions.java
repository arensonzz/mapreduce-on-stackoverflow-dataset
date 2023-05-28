/**
 * This MapReduce job reads Questions.csv file and constructs QuestionsTuple objects by parsing lines.
 * Then the resulting objects are passed to the reducer to be serialized using QuestionsTupleOutputFormat.
 * Another Mapper can de-serialize objects from this file using the QuestionsTupleInputFormat.
 */

import model.QuestionsTuple;
import model.customFileIOFormat.QuestionsTupleOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.logging.Logger;

public class ParseQuestions {
    private static final Logger log = Logger.getLogger(ParseQuestions.class.getName());

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        // Job Config
        // Reduce number of splits to reduce Map node overhead
        conf.setInt("mapreduce.input.lineinputformat.linespermap", 80000);
        Job job = Job.getInstance(conf, "parse questions");
        job.setJarByClass(ParseQuestions.class);

        // Mapper Config
        job.setInputFormatClass(NLineInputFormat.class);
        job.setMapperClass(ParseMapper.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(QuestionsTuple.class);

        // Reducer Config
        // Note: If you do not set Reducer, then Mapper output is the final output
        job.setReducerClass(ParseReducer.class);
        // Optional: Set number of reduce tasks
        //job.setNumReduceTasks(5);
        // Optional: Set output format from TextOutputFormat to the custom defined serialization format
        //job.setOutputFormatClass(QuestionsTupleOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class ParseMapper extends Mapper<LongWritable, Text, LongWritable, QuestionsTuple> {

        public void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException {
            LongWritable id = new LongWritable();
            QuestionsTuple tuple;

            // Parse line into object fields
            tuple = QuestionsTuple.parseCsvLine(key.get(), values);
            id.set(tuple.getId());

            context.write(id, tuple);
        }
    }

    public static class ParseReducer
            extends Reducer<LongWritable, QuestionsTuple, LongWritable, QuestionsTuple> {

        // Reducer value must be defined as Iterable<>
        public void reduce(LongWritable key, Iterable<QuestionsTuple> tuples,
                           Context context
        ) throws IOException, InterruptedException {
            for (QuestionsTuple tuple :
                    tuples) {
                context.write(key, tuple);
            }
        }
    }
}
