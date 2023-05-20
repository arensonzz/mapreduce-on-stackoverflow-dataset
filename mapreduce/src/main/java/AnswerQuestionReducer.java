import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.List;
import java.util.ArrayList;
import java.util.Collections;
import org.apache.hadoop.io.DoubleWritable;


public class AnswerQuestionReducer extends Reducer<Text, LongWritable, Text, DoubleWritable> {

    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        List<Long> upvotesList = new ArrayList<>();
        long sum = 0;
        int count = 0;

        for (LongWritable value : values) {
            long upvotes = value.get();
            upvotesList.add(upvotes);
            sum += upvotes;
            count++;
        }

        double average = (double) sum / count;

        Collections.sort(upvotesList);
        double median;
        if (count % 2 == 0) {
            int mid = count / 2;
            median = (upvotesList.get(mid - 1) + upvotesList.get(mid)) / 2.0;
        } else {
            median = upvotesList.get(count / 2);
        }

        long min = Collections.min(upvotesList);
        long max = Collections.max(upvotesList);

        double sumOfSquaredDifferences = 0;
        for (Long upvotes : upvotesList) {
            double difference = upvotes - average;
            sumOfSquaredDifferences += difference * difference;
        }
        double standardDeviation = Math.sqrt(sumOfSquaredDifferences / count);

        context.write(key, new DoubleWritable(average));
        context.write(key, new DoubleWritable(count));
        context.write(key, new DoubleWritable(median));
        context.write(key, new DoubleWritable(min));
        context.write(key, new DoubleWritable(max));
        context.write(key, new DoubleWritable(standardDeviation));
    }
}
