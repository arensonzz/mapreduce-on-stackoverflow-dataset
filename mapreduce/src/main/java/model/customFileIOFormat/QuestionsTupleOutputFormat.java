/**
 * Custom FileOutputFormat for serializing QuestionsTuple instances.
 * <p>
 * Some parts taken from: https://stackoverflow.com/q/40891164
 * Also check: https://www.infoq.com/articles/HadoopOutputFormat/
 */
package model.customFileIOFormat;

import model.QuestionsTuple;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.DataOutputStream;
import java.io.IOException;

public class QuestionsTupleOutputFormat extends FileOutputFormat<LongWritable, QuestionsTuple> {

    @Override
    public RecordWriter<LongWritable, QuestionsTuple> getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException {
        Path file = getDefaultWorkFile(job, "");
        FileSystem fs = file.getFileSystem(job.getConfiguration());
        FSDataOutputStream fileOut = fs.create(file, false);
        return new QuestionsTupleRecordWriter(fileOut);
    }

    protected static class QuestionsTupleRecordWriter extends RecordWriter<LongWritable, QuestionsTuple> {
        private DataOutputStream out;

        public QuestionsTupleRecordWriter(DataOutputStream out) throws IOException {
            this.out = out;
        }

        public synchronized void write(LongWritable key, QuestionsTuple value) throws IOException {
            value.write(out);
        }

        public synchronized void close(TaskAttemptContext job) throws IOException {
            out.close();
        }
    }
}
