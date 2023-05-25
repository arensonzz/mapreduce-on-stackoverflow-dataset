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
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.logging.Logger;

public class ParseQuestions {
    private static final Logger log = Logger.getLogger(ParseQuestions.class.getName());
    private static int linesPerMap;

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set(CSVLineRecordReader.FORMAT_DELIMITER, "\"");
        conf.set(CSVLineRecordReader.FORMAT_SEPARATOR, ",");
        conf.set(CSVLineRecordReader.IS_ZIPFILE, "false");

        // Reduce number of map jobs
        linesPerMap = 200;
        conf.setInt("mapreduce.input.lineinputformat.linespermap", linesPerMap);
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

    public static class ParseMapper extends Mapper<LongWritable, List<Text>, LongWritable, QuestionsTuple> {

        public void map(LongWritable key, List<Text> values, Context context) throws IOException, InterruptedException {
            LongWritable id = new LongWritable();
            QuestionsTuple tuple = new QuestionsTuple();
            //ArrayList<String> valuesStr = new ArrayList<String>();
            //
            //log.info("key: " + key.get());
            //log.info("values.size(): " + values.size());
            //values.forEach((value) -> valuesStr.add(value.toString()));
            //log.info("values: " + String.join("\t|||\t", valuesStr));
            //log.info("getInputSplit: " + context.getInputSplit().toString());
            
            // Skip if the input is csv header
            if (key.get() == 0 && values.toString().contains("CreationDate")) {
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
            tuple.setScore(Integer.parseInt(fields[3]));
            tuple.setTitle(fields[4]);
            tuple.setBody(fields[5]);
            tuple.setTags(fields[6]);
            
            id.set(tuple.getId());

            context.write(id, tuple);
        }
    }
}
