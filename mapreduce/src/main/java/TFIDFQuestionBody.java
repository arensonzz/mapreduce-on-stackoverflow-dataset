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
import java.util.HashMap;
import java.util.Map;

public class TFIDFQuestionBody {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        // Job Config
        conf.setInt("mapreduce.input.lineinputformat.linespermap", 80000);
        Job job = Job.getInstance(conf, "tfidf questin body");
        job.setJarByClass(TFIDFQuestionBody.class);

        // Mapper Config
        job.setInputFormatClass(NLineInputFormat.class);
        job.setMapperClass(TFIDFMapper.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);

        // Reducer Config
        job.setReducerClass(TFIDFReducer.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class TFIDFMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            QuestionsTuple tuple = QuestionsTuple.parseCsvLine(key.get(), value);

            LongWritable documentId = new LongWritable(tuple.getId());
            String[] terms = tuple.getBody().split("\\s+");  // Split the body into individual terms

            // Emit each term with the document ID as key and term count as value
            for (String term : terms) {
                context.write(documentId, new Text(term));
            }
        }
    }

    public static class TFIDFReducer extends Reducer<LongWritable, Text, LongWritable, Text> {
        public void reduce(LongWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            Map<String, Integer> termFrequency = new HashMap<>();

            // Count the term frequency in the document
            for (Text term : values) {
                String termStr = term.toString();
                termFrequency.put(termStr, termFrequency.getOrDefault(termStr, 0) + 1);
            }

            // Calculate TF-IDF score for each term
            for (Map.Entry<String, Integer> entry : termFrequency.entrySet()) {
                String term = entry.getKey();
                int tf = entry.getValue();

                // Calculate inverse document frequency (IDF)
                int totalDocuments = 585888; // Set the total number of documents in the corpus
                int documentFrequency = termFrequency.containsKey(term) ? 1 : 0;
                double idf = Math.log((double) totalDocuments / (documentFrequency + 1));

                // Calculate TF-IDF
                double tfidf = tf * idf;

                // Output term and its TF-IDF score
                context.write(key, new Text(term + ":" + tfidf));
            }
        }
    }
}
