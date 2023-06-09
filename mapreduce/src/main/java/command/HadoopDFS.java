package command;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import javax.swing.*;
import java.awt.*;
import java.io.IOException;

public class HadoopDFS {

    public static void uploadData(String input, String output,  JFrame frame) {
        try {
            Configuration conf = new Configuration();
            conf.set("fs.defaultFS", "hdfs://localhost:9000/");
            FileSystem fs = FileSystem.get(conf);
            fs.copyFromLocalFile(new Path(input), new Path(output));
            JOptionPane.showMessageDialog(frame, "Upload Completed");
        } catch (IOException e) {
            JOptionPane.showMessageDialog(frame, "Could not upload file: " + e.getMessage());
            e.printStackTrace();
        }
    }

    public static void deleteData(String input, JFrame frame) {
        try {
            Configuration conf = new Configuration();
            conf.set("fs.defaultFS", "hdfs://localhost:9000/");
            FileSystem fs = FileSystem.get(conf);
            fs.delete(new Path(input), true);
            JOptionPane.showMessageDialog(frame, "Delete Operation Completed");
        } catch (IOException e) {
            JOptionPane.showMessageDialog(frame, "Could not upload file: " + e.getMessage());
            e.printStackTrace();
        }
    }
}