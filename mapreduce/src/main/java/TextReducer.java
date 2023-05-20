import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.util.List;
import java.util.ArrayList;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;


public class TextReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        List<String> processedTexts = new ArrayList<>();

        for (Text value : values) {
            String body = value.toString();
            String preprocessedText = preprocessText(body);
            processedTexts.add(preprocessedText);
        }

        // Perform statistical analysis on processedTexts

        // Example statistical information:
        int totalCount = processedTexts.size();
        int uniqueCount = getUniqueCount(processedTexts);
        int averageLength = getAverageLength(processedTexts);

        context.write(key, new Text("Total Count: " + totalCount));
        context.write(key, new Text("Unique Count: " + uniqueCount));
        context.write(key, new Text("Average Length: " + averageLength));
    }

    private String preprocessText(String text) {
        // Implement your text preprocessing steps using TextPreprocessingUtils class
        String preprocessedText = TextPreprocessingUtils.removeHtmlTags(text);
        preprocessedText = TextPreprocessingUtils.removePunctuation(preprocessedText);
        preprocessedText = TextPreprocessingUtils.removeHashtags(preprocessedText);
        preprocessedText = TextPreprocessingUtils.removeStopwords(preprocessedText);
        preprocessedText = TextPreprocessingUtils.removeDigits(preprocessedText);

        return preprocessedText;
    }

    private int getUniqueCount(List<String> texts) {
        // Implement logic to count unique preprocessed texts
        Set<String> uniqueTexts = new HashSet<>(texts);
        return uniqueTexts.size();
    }

    private int getAverageLength(List<String> texts) {
        // Implement logic to calculate average length of preprocessed texts
        int totalLength = 0;
        for (String text : texts) {
            totalLength += text.length();
        }
        return totalLength / texts.size();
    }
}
