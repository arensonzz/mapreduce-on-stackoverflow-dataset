package components;

import command.DockerCommand;
import command.HadoopDFS;

import javax.swing.*;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;

public class AppContainer extends JFrame {

    private final JButton btnUpload = new JButton("Upload Data");
    private final JButton btnStart = new JButton("Start");

    private final MapReduceSelector selector = new MapReduceSelector();
    private final JTextField txtB = new JTextField("/inputs/");
    private final JTextField txtC = new JTextField("/outputs/");
    private final JTextField txtD = new JTextField("../data/");
    private final JTextField txtE = new JTextField("/inputs/");

    private final JLabel lblA = new JLabel("MapReduce Job:");
    private final JLabel lblB = new JLabel("Input Data:");
    private final JLabel lblC = new JLabel("Output Path:");
    private final JLabel lblD = new JLabel("Source Path:");
    private final JLabel lblE = new JLabel("Destination Path:");

    public AppContainer() {
        setTitle("Big Data Project");
        setSize(550, 300);
        setLocation(new Point(300, 200));
        setLayout(null);
        setResizable(false);

        initComponent();
        initEvent();
    }

    private void initComponent() {

        lblA.setBounds(20, 10, 200, 30);
        selector.setBounds(128, 10, 400, 30);
        add(selector);

        lblB.setBounds(20, 35, 200, 30);
        txtB.setBounds(128, 35, 400, 30);

        lblC.setBounds(20, 60, 200, 30);
        txtC.setBounds(128, 60, 400, 30);

        btnStart.setBounds(128, 90, 400, 30);
        add(btnStart);

        lblD.setBounds(20, 140, 200, 30);
        txtD.setBounds(128, 140, 400, 30);

        lblE.setBounds(20, 165, 200, 30);
        txtE.setBounds(128, 165, 400, 30);

        btnUpload.setBounds(128, 195, 400, 30);
        add(btnUpload);

        add(lblA);
        add(lblB);
        add(lblC);
        add(lblD);
        add(lblE);

        add(txtB);
        add(txtC);
        add(txtD);
        add(txtE);
    }

    private void initEvent() {

        this.addWindowListener(new WindowAdapter() {
            public void windowClosing(WindowEvent e) {
                System.exit(1);
            }
        });

        btnUpload.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent e) {
                btnUpload.setEnabled(false);
                btnUpload.setText("Uploading...");
                new Thread(new Runnable() {
                    public void run() {
                        HadoopDFS.uploadData(txtD.getText(), txtE.getText());
                        btnUpload.setEnabled(true);
                        btnUpload.setText("Upload");
                    }
                }).start();
            }
        });

        btnStart.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent e) {
                btnStart.setEnabled(false);
                btnStart.setText("Running...");
                new Thread(new Runnable() {
                    public void run() {
                        btnStartClick();
                    }
                }).start();
            }
        });
    }

    private void btnStartClick() {
        switch (selector.getSelectedIndex()) {
            case 0:
                DockerCommand.RunMapReduce("UsersQuestionScoreSum", txtB.getText(), txtC.getText());
                break;
            case 1:
                DockerCommand.RunMapReduce("TFIDFQuestionBody", txtB.getText(), txtC.getText());
                break;
            case 2:
                DockerCommand.RunMapReduce("AnswerWordCount", txtB.getText(), txtC.getText());
                break;
            case 3:
                DockerCommand.RunMapReduce("QuestionBodyWordCount", txtB.getText(), txtC.getText());
                break;
            case 4:
                DockerCommand.RunMapReduce("QuestionTitleWordCount", txtB.getText(), txtC.getText());
                break;
            case 5:
                DockerCommand.RunMapReduce("QuestionScoreStatistics", txtB.getText(), txtC.getText());
                break;
            case 6:
                DockerCommand.RunMapReduce("AnswerScoreStatistics", txtB.getText(), txtC.getText());
                break;
            case 7:
                DockerCommand.RunMapReduce("YearlyTrendTopics", txtB.getText(), txtC.getText());
                break;
        }
        btnStart.setEnabled(true);
        btnStart.setText("Start");
    }
}