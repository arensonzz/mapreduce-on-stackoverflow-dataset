package command;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

public class HadoopDFS {

    public static void uploadData(String input, String output) {
        try {
            Configuration conf = new Configuration();
            conf.set("fs.defaultFS", "hdfs://localhost:9000/");
            FileSystem fs = FileSystem.get(conf);
            fs.copyFromLocalFile(new Path(input), new Path(output));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}