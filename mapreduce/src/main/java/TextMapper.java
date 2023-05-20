import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class TextMapper extends Mapper<LongWritable, Text, Text, Text> {

    private Text word = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split(",");

        String parentId = fields[3];
        String body = fields[5];

        if (!parentId.isEmpty() && !body.isEmpty()) {
            word.set("question_" + parentId);
            context.write(word, new Text(body));
        }
    }
}
