import model.QuestionsTuple;
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
//import java.util.logging.Logger;

public class UsersQuestionScoreSum {
    //private static final Logger log = Logger.getLogger(UsersQuestionScoreSum.class.getName());

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        // Job Config
        // Reduce number of splits to reduce Map node overhead
        conf.setInt("mapreduce.input.lineinputformat.linespermap", 80000);
        Job job = Job.getInstance(conf, "users question score sum");
        job.setJarByClass(UsersQuestionScoreSum.class);

        // Mapper Config
        job.setInputFormatClass(NLineInputFormat.class);
        job.setMapperClass(UsersScoreMapper.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(LongWritable.class);

        // Reducer Config
        // Note: If you do not set Reducer, then Mapper output is the final output
        job.setReducerClass(ScoreSumReducer.class);
        //job.setNumReduceTasks(5);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class UsersScoreMapper extends Mapper<LongWritable, Text, LongWritable, LongWritable> {

        public void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException {
            QuestionsTuple tuple;
            LongWritable ownerUserId = new LongWritable();
            LongWritable score = new LongWritable();

            tuple = QuestionsTuple.parseCsvLine(key.get(), values);
            // Parse line into object fields

            ownerUserId.set(tuple.getOwnerUserId());
            score.set(tuple.getScore());

            context.write(ownerUserId, score);
        }
    }

    public static class ScoreSumReducer
            extends Reducer<LongWritable, LongWritable, LongWritable, LongWritable> {

        public void reduce(LongWritable key, Iterable<LongWritable> scores,
                           Context context
        ) throws IOException, InterruptedException {
            LongWritable result = new LongWritable();
            int sum = 0;
            for (LongWritable score :
                    scores) {
                sum += score.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }
}
