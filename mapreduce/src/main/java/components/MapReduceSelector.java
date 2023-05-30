package components;

import javax.swing.*;

public class MapReduceSelector extends JComboBox<String> {

    private static final String[] mapReduceJobs = new String[]{
            "Question Score Sum of Each User",
            "TF-IDF Scores of Questions' Body",
            "Word Count of Answers' Body",
            "Word Count of Questions' Body",
            "Word Count of Questions' Title",
            "Statistics from Question Scores",
            "Statistics from Answer Scores",
            "Yearly Trend Topics from Question Tags",
    };

    public MapReduceSelector() {
        super(mapReduceJobs);

        this.setVisible(true);
        this.addActionListener(this);
    }
}
