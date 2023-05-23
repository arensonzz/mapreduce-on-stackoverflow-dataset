import model.QuestionsTuple;
import model.customFileIOFormat.QuestionsTupleInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.logging.Logger;

public class UsersQuestionScoreSum {
    private static final Logger log = Logger.getLogger(UsersQuestionScoreSum.class.getName());

    public static class UserScoreMapper extends Mapper<LongWritable, QuestionsTuple, LongWritable, LongWritable> {

        public void map(LongWritable key, QuestionsTuple tuple, Context context) throws IOException, InterruptedException {
            LongWritable ownerUserId = new LongWritable();
            LongWritable score = new LongWritable();

            log.info("Mapper: " + tuple);
            ownerUserId.set(tuple.getOwnerUserId());
            score.set(tuple.getScore());

            context.write(ownerUserId, score);
        }
    }

    public static class ScoreSumReducer
            extends Reducer<LongWritable, Iterable<LongWritable>, LongWritable, LongWritable> {

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

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "users question score sum");
        job.setJarByClass(UsersQuestionScoreSum.class);
        job.setMapperClass(UserScoreMapper.class);
        job.setInputFormatClass(QuestionsTupleInputFormat.class);
        job.setReducerClass(ScoreSumReducer.class);
        // Set mapper key-value class types
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(LongWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
