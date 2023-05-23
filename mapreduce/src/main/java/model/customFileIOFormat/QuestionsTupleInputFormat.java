/**
 * Custom FileInputFormat for de-serializing QuestionsTuple instances.
 */
package model.customFileIOFormat;

import model.QuestionsTuple;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.EOFException;
import java.io.IOException;
import java.util.Arrays;
import java.util.logging.Logger;

public class QuestionsTupleInputFormat extends FileInputFormat<LongWritable, QuestionsTuple> {
    private static final Logger log = Logger.getLogger(QuestionsTupleInputFormat.class.getName());

    protected static class QuestionsTupleRecordReader extends RecordReader<LongWritable, QuestionsTuple> {
        private FSDataInputStream filein;
        private LongWritable key;
        private QuestionsTuple value = new QuestionsTuple();
        private long start = 0;
        private long end = 0;
        private long pos = 0;

        @Override
        public void close() throws IOException {
            if (filein != null) {
                filein.close();
            }
        }

        @Override
        public LongWritable getCurrentKey() throws IOException, InterruptedException {
            return key;
        }

        @Override
        public QuestionsTuple getCurrentValue() throws IOException, InterruptedException {
            return value;
        }

        @Override
        public float getProgress() throws IOException, InterruptedException {
            if (start == end) {
                return 0.0f;
            } else {
                return Math.min(1.0f, (pos - start) / (float) (end - start));
            }
        }

        @Override
        public void initialize(InputSplit genericSplit, TaskAttemptContext context) throws IOException, InterruptedException {
            FileSplit split = (FileSplit) genericSplit;
            final Path file = split.getPath();
            Configuration conf = context.getConfiguration();
            FileSystem fs = file.getFileSystem(conf);
            
            start = split.getStart();
            end = start + split.getLength();
            filein = fs.open(split.getPath());
            filein.seek(start);
            this.pos = start;
        }

        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            if (key == null) {
                key = new LongWritable();
            }
            if (value == null) {
                value = new QuestionsTuple();
            }
            // Read until EOF
            try {
                value.readFields(filein);
                pos = filein.getPos();
            } catch (EOFException e) {
                log.info(Arrays.toString(e.getStackTrace()));
                return false;
            }
            key.set(value.getId());
            return true;
        }
    }

    @Override
    public RecordReader<LongWritable, QuestionsTuple> createRecordReader(InputSplit inputSplit, TaskAttemptContext job) throws IOException, InterruptedException {
        return new QuestionsTupleRecordReader();
    }
}