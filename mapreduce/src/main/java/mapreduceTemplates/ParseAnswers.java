package mapreduceTemplates; 

import model.AnswersTuple;
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

/**
 * This MapReduce job reads Answers.csv file and constructs AnswersTuple objects by parsing lines.
 * Then the resulting objects are passed to the reducer to be serialized using default FileOutputFormat.
 */
public class ParseAnswers {
    private static final Logger log = Logger.getLogger(ParseAnswers.class.getName());

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        // Job Config
        // Reduce number of splits to reduce Map node overhead
        conf.setInt("mapreduce.input.lineinputformat.linespermap", 250000);
        Job job = Job.getInstance(conf, "parse answers");
        job.setJarByClass(ParseAnswers.class);

        // Mapper Config
        job.setInputFormatClass(NLineInputFormat.class);
        job.setMapperClass(ParseMapper.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(AnswersTuple.class);

        // Reducer Config
        // Note: If you do not set Reducer, then Mapper output is the final output
        job.setReducerClass(ParseReducer.class);
        // Optional: Set number of reduce tasks
        //job.setNumReduceTasks(5);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class ParseMapper extends Mapper<LongWritable, Text, LongWritable, AnswersTuple> {

        public void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException {
            LongWritable id = new LongWritable();
            AnswersTuple tuple;

            // Parse line into object fields
            tuple = AnswersTuple.parseCsvLine(key.get(), values);
            id.set(tuple.getId());

            context.write(id, tuple);
        }
    }

    public static class ParseReducer
            extends Reducer<LongWritable, AnswersTuple, LongWritable, AnswersTuple> {

        // Reducer value must be defined as Iterable<>
        public void reduce(LongWritable key, Iterable<AnswersTuple> tuples,
                           Context context
        ) throws IOException, InterruptedException {
            for (AnswersTuple tuple :
                    tuples) {
                context.write(key, tuple);
            }
        }
    }
}