import java.util.regex.Pattern;
import java.util.Set;
import java.util.HashSet;
import java.util.Arrays;

public class TextPreprocessingUtils {

    private static final Pattern HTML_TAGS_PATTERN = Pattern.compile("<.*?>");
    private static final Pattern PUNCTUATION_PATTERN = Pattern.compile("[^a-zA-Z0-9 ]");
    private static final Pattern HASHTAG_PATTERN = Pattern.compile("#\\w+");
    private static final Set<String> STOPWORDS = new HashSet<>(Arrays.asList("the", "and", "to", "of", "a", "in", "is", "that"));
    private static final Pattern DIGIT_PATTERN = Pattern.compile("\\d");

    public static String removeHtmlTags(String text) {
        return HTML_TAGS_PATTERN.matcher(text).replaceAll("");
    }

    public static String removePunctuation(String text) {
        return PUNCTUATION_PATTERN.matcher(text).replaceAll("");
    }

    public static String removeHashtags(String text) {
        return HASHTAG_PATTERN.matcher(text).replaceAll("");
    }

    public static String removeStopwords(String text) {
        String[] words = text.toLowerCase().split("\\s+");
        StringBuilder result = new StringBuilder();

        for (String word : words) {
            if (!STOPWORDS.contains(word)) {
                result.append(word).append(" ");
            }
        }

        return result.toString().trim();
    }

    public static String removeDigits(String text) {
        return DIGIT_PATTERN.matcher(text).replaceAll("");
    }
}
