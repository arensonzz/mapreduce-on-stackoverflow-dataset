package components;

import command.DockerCommand;

import javax.swing.*;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;
import java.io.File;
import java.io.IOException;

public class AppContainer extends JFrame {

    private final JButton btnStart = new JButton("Start Job");

    private final MapReduceSelector selector = new MapReduceSelector();
    private final JTextField txtB = new JTextField("/input/");
    private final JTextField txtC = new JTextField("/output/");
    
    //private final JTextField txtD = new JTextField("../data/");
    private final JFileChooser fileChooser = new JFileChooser();
    
    private final JLabel showSelectedFile = new JLabel();
    private final JButton btnSelect = new JButton("Open a File...");
    private final JButton btnUpload = new JButton("Upload Selected File");
    private File selectedFile = null;
    private final JTextField txtE = new JTextField("/input/");

    private final JLabel lblA = new JLabel("MapReduce Job:");
    private final JLabel lblB = new JLabel("HDFS Input Path:");
    private final JLabel lblC = new JLabel("HDFS Output Path:");
    private final JLabel lblD = new JLabel("Selected File:");
    private final JLabel lblE = new JLabel("Destination Path:");

    public AppContainer() {
        setTitle("Big Data Project");
        setSize(700, 600);
        setLocation(new Point(300, 200));
        setLayout(null);
        setResizable(false);

        initComponent();
        initEvent();
    }

    private void initComponent() {

        lblA.setBounds(20, 10, 200, 30);
        selector.setBounds(160, 10, 400, 30);
        add(selector);

        lblB.setBounds(20, 35, 200, 30);
        txtB.setBounds(160, 35, 400, 30);

        lblC.setBounds(20, 60, 200, 30);
        txtC.setBounds(160, 60, 400, 30);

        btnStart.setBounds(160, 90, 400, 30);
        add(btnStart);

        lblD.setBounds(20, 180, 200, 30);
        //fileChooser.setBounds(160, 140, 400, 300);
        btnSelect.setBounds(20, 140, 200, 30);
        btnUpload.setBounds(160, 225, 400, 30);
        showSelectedFile.setBounds(20, 180, 200, 30);

        lblE.setBounds(20, 395, 200, 30);
        txtE.setBounds(160, 395, 400, 30);

        add(lblA);
        add(lblB);
        add(lblC);
        add(lblD);
        add(btnSelect);
        add(btnUpload);
        add(showSelectedFile);
        add(lblE);

        add(txtB);
        add(txtC);
        add(fileChooser);
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
                        //HadoopDFS.uploadData(txtD.getText(), txtE.getText());
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
        
        btnSelect.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                int returnVal = fileChooser.showOpenDialog(null);

                if (returnVal == JFileChooser.APPROVE_OPTION) {
                    selectedFile = fileChooser.getSelectedFile();
                    //This is where a real application would open the file.
                    try {
                        
                        System.out.println(file.getCanonicalPath());
                    } catch (IOException ex) {
                        throw new RuntimeException(ex);
                    }
                }
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