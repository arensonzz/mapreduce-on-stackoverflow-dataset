import com.opencsv.CSVParser;
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
import java.time.ZonedDateTime;
import java.time.format.DateTimeParseException;
import java.util.Optional;
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
        //job.setNumReduceTasks(5);
        // Set output format from TextOutputFormat to the custom defined serialization format
        job.setOutputFormatClass(QuestionsTupleOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class ParseMapper extends Mapper<LongWritable, Text, LongWritable, QuestionsTuple> {
        private final CSVParser parser = new CSVParser();

        private QuestionsTuple parseCsvLine(long key, Text line) throws IOException {
            // Skip if the input is csv header
            if (key == 0 && line.toString().contains("CreationDate")) {
                return null;
            }
            QuestionsTuple tuple = new QuestionsTuple();
            String[] fields = parser.parseLine(line.toString());

            tuple.setId(Long.parseLong(fields[0]));
            tuple.setOwnerUserId(Long.parseLong(fields[1]));
            try {
                tuple.setCreationDate(Optional.of(ZonedDateTime.parse(fields[2])));
            } catch (DateTimeParseException e) {
                tuple.setCreationDate(Optional.empty());
            }
            tuple.setScore(Integer.parseInt(fields[3]));
            tuple.setTitle(fields[4]);
            tuple.setBody(fields[5]);
            tuple.setTags(fields[6]);
            return tuple;
        }

        public void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException {
            LongWritable id = new LongWritable();
            QuestionsTuple tuple;

            // Parse line into object fields
            if ((tuple = parseCsvLine(key.get(), values)) == null) {
                return;
            }
            id.set(tuple.getId());

            context.write(id, tuple);
        }
    }

    public static class ParseReducer
            extends Reducer<LongWritable, QuestionsTuple, LongWritable, QuestionsTuple> {

        public void reduce(LongWritable key, QuestionsTuple tuple,
                           Context context
        ) throws IOException, InterruptedException {
            context.write(key, tuple);
        }
    }
}
