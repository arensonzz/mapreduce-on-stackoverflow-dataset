import csvInputFormat.CSVLineRecordReader;
import csvInputFormat.CSVNLineInputFormat;
import model.QuestionsTuple;
import model.customFileIOFormat.QuestionsTupleOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.time.ZonedDateTime;
import java.time.format.DateTimeParseException;
import java.util.List;
import java.util.Optional;
import java.util.logging.Logger;

public class ParseQuestions {
    private static final Logger log = Logger.getLogger(ParseQuestions.class.getName());

    public static class ParseMapper extends Mapper<LongWritable, List<Text>, LongWritable, QuestionsTuple> {

        public void map(LongWritable key, List<Text> values, Context context) throws IOException, InterruptedException {
            LongWritable id = new LongWritable();
            QuestionsTuple tuple = new QuestionsTuple();

            // Skip if the input is csv header
            if (key.get() == 0 && values.toString().contains("ClosedDate")) {
                return;
            }
            String[] fields = new String[values.size()];
            for (int i = 0; i < fields.length; i++) {
                fields[i] = values.get(i).toString();
            }

            tuple.setId(Long.parseLong(fields[0]));
            tuple.setOwnerUserId(Long.parseLong(fields[1]));
            try {
                tuple.setCreationDate(Optional.of(ZonedDateTime.parse(fields[2])));
            } catch (DateTimeParseException e) {
                tuple.setCreationDate(Optional.empty());
            }
            try {
                tuple.setClosedDate(Optional.of(ZonedDateTime.parse(fields[3])));
            } catch (DateTimeParseException e) {
                tuple.setClosedDate(Optional.empty());
            }
            tuple.setScore(Integer.parseInt(fields[4]));
            tuple.setTitle(fields[5]);
            tuple.setBody(fields[6]);
            id.set(tuple.getId());

            context.write(id, tuple);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set(CSVLineRecordReader.FORMAT_DELIMITER, "\"");
        conf.set(CSVLineRecordReader.FORMAT_SEPARATOR, ",");
        conf.set(CSVLineRecordReader.IS_ZIPFILE, "false");
        Job job = Job.getInstance(conf, "parse questions");
        job.setInputFormatClass(CSVNLineInputFormat.class);
        job.setJarByClass(ParseQuestions.class);
        // If you do not set Reducer, then Mapper output is the final output
        job.setMapperClass(ParseMapper.class);

        // Set mapper key-value class types
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(QuestionsTuple.class);
        // Set output format from TextOutputFormat to the custom defined serialization format
        job.setOutputFormatClass(QuestionsTupleOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
