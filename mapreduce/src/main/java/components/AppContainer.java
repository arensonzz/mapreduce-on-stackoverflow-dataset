package components;

import command.DockerCommand;
import command.HadoopDFS;

import javax.swing.*;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;
import java.io.File;
import java.io.IOException;

public class AppContainer extends JFrame {
    // MapReduce Job Section
    private final JLabel lblJobHeader = new JLabel("MapReduce Job");
    private final JLabel lblA = new JLabel("Select Job:");
    private final JLabel lblB = new JLabel("HDFS Input Path:");
    private final JLabel lblC = new JLabel("HDFS Output Path:");
    private final JTextField txtB = new JTextField("/input/");
    private final JTextField txtC = new JTextField("/output/");
    private final MapReduceSelector selector = new MapReduceSelector();
    private final JButton btnStart = new JButton("Start Job");
    private final JFrame rootFrame = this;
    
    private  final JSeparator sectionSeparator = new JSeparator();
    
    // File Upload Section
    private final JLabel lblFileHeader = new JLabel("File Upload");
    private final JLabel lblE = new JLabel("Destination Path:");
    private final JLabel lblD = new JLabel("Selected File:");
    private final JLabel lblF = new JLabel("Selected File:");
    private final JTextField uploadSelectedFile = new JTextField();
    private final JTextField txtE = new JTextField("/input/");
    private final JFileChooser fileChooser = new JFileChooser();
    private final JButton btnSelect = new JButton("Open a File...");
    private final JButton btnUpload = new JButton("Upload Selected File");
    private File selectedFile = null;

    private  final JSeparator sectionSeparator2 = new JSeparator();
    
    // File Delete Section
    private final JLabel lblFileDeleteHeader = new JLabel("File Delete");
    private final JButton btnDelete = new JButton("Delete Selected File");
    private final JTextField deleteSelectedFile = new JTextField("/input/");


    public AppContainer() {
        setTitle("MapReduce on Stackoverflow Dataset");
        setSize(700, 700);
        setLocation(new Point(300, 200));
        setLayout(null);
        setResizable(false);

        initComponent();
        initEvent();
    }

    private void initComponent() {
        // MapReduce Job Section
        lblJobHeader.setBounds(150, 10, 400, 40);
        lblJobHeader.setHorizontalAlignment(SwingConstants.CENTER);
        add(lblJobHeader);
        
        lblA.setBounds(20, 70, 200, 30);
        selector.setBounds(160, 70, 400, 30);
        add(selector);

        lblB.setBounds(20, 95, 200, 30);
        txtB.setBounds(160, 95, 400, 30);

        lblC.setBounds(20, 120, 200, 30);
        txtC.setBounds(160, 120, 400, 30);

        btnStart.setBounds(160, 160, 400, 30);
        add(btnStart);
        
        sectionSeparator.setBounds(0, 210, 700, 5);
        add(sectionSeparator);
        
        // File Upload Section
        lblFileHeader.setBounds(150, 230, 400, 40);
        lblFileHeader.setHorizontalAlignment(SwingConstants.CENTER);
        add(lblFileHeader);
         
        btnSelect.setBounds(160, 290, 400, 30);
        add(btnSelect);
        lblD.setBounds(20, 330, 200, 30);
        uploadSelectedFile.setBounds(160, 330, 400, 30);
        uploadSelectedFile.setEditable(false);
        add(uploadSelectedFile);
        
        lblE.setBounds(20, 370, 200, 30);
        txtE.setBounds(160, 370, 400, 30);
        
        btnUpload.setBounds(160, 410, 400, 30);
        add(btnUpload);

        sectionSeparator2.setBounds(0, 460, 700, 5);
        add(sectionSeparator2);
        // File Delete Section
        lblFileDeleteHeader.setBounds(150, 480, 400, 40);
        lblFileDeleteHeader.setHorizontalAlignment(SwingConstants.CENTER);
        add(lblFileDeleteHeader);
        lblF.setBounds(20, 540, 200, 30);
        deleteSelectedFile.setBounds(160, 540, 400, 30);
        add(deleteSelectedFile);
        btnDelete.setBounds(160, 580, 400, 30);
        add(btnDelete);
        

        add(lblA);
        add(lblB);
        add(lblC);
        add(lblD);
        add(lblE);
        add(lblF);

        add(txtB);
        add(txtC);
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
                if (selectedFile == null || txtE.getText().trim().isEmpty()) {
                    JOptionPane.showMessageDialog(rootFrame, "Input file or destination directory not selected!");
                    return;
                }
                
                btnUpload.setEnabled(false);
                btnUpload.setText("Uploading...");
                
                new Thread(new Runnable() {
                    public void run() {
                        try {
                            HadoopDFS.uploadData(selectedFile.getCanonicalPath(), txtE.getText(), rootFrame);
                        } catch (IOException ex) {
                            throw new RuntimeException(ex);
                        }
                        btnUpload.setEnabled(true);
                        btnUpload.setText("Upload Selected File");
                    }
                }).start();
            }
        });

        btnDelete.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent e) {
                if (deleteSelectedFile.getText().trim().isEmpty()) {
                    JOptionPane.showMessageDialog(rootFrame, "Input file not selected!");
                    return;
                }

                btnDelete.setEnabled(false);
                btnDelete.setText("Deleting...");

                new Thread(new Runnable() {
                    public void run() {
                        HadoopDFS.deleteData(deleteSelectedFile.getText(), rootFrame);
                        btnDelete.setEnabled(true);
                        btnDelete.setText("Delete Selected File");
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
                    uploadSelectedFile.setText(selectedFile.getName());
                }
            }
        });
        
    }

    private void btnStartClick() {
        switch (selector.getSelectedIndex()) {
            case 0:
                DockerCommand.RunMapReduce("users-question-score-sum", txtB.getText(), txtC.getText());
                break;
            case 1:
                DockerCommand.RunMapReduce("question-tfidf", txtB.getText(), txtC.getText());
                break;
            case 2:
                DockerCommand.RunMapReduce("wc-answers", txtB.getText(), txtC.getText());
                break;
            case 3:
                DockerCommand.RunMapReduce("wc-questions-body", txtB.getText(), txtC.getText());
                break;
            case 4:
                DockerCommand.RunMapReduce("wc-questions-title", txtB.getText(), txtC.getText());
                break;
            case 5:
                DockerCommand.RunMapReduce("question-statistics", txtB.getText(), txtC.getText());
                break;
            case 6:
                DockerCommand.RunMapReduce("answer-statistics", txtB.getText(), txtC.getText());
                break;
            case 7:
                DockerCommand.RunMapReduce("yearly-trend-topics", txtB.getText(), txtC.getText());
                break;
        }
        btnStart.setEnabled(true);
        btnStart.setText("Start");
    }
    
}
