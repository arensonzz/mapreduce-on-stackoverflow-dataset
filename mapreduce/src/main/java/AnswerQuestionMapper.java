import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class AnswerQuestionMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

    private Text word = new Text();
    private LongWritable count = new LongWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split(",");

        String parentId = fields[3];
        String answerUpvotes = fields[4];

        if (!parentId.isEmpty() && !answerUpvotes.isEmpty()) {
            word.set("question_" + parentId);
            count.set(Long.parseLong(answerUpvotes));
            context.write(word, count);
        }
    }
}
