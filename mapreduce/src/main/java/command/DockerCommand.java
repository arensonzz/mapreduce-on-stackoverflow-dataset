package command;

import components.TextAreaOutputStream;
import org.apache.commons.io.IOUtils;

import javax.swing.*;
import java.awt.*;
import java.io.IOException;
import java.util.HashMap;

public class DockerCommand {

    public static void RunMapReduce(String type, String input, String output) {
        ProcessBuilder builder = new ProcessBuilder();
        HashMap<String, String> envs = new HashMap<>();
        
        builder.environment().put("INPUT_PATH", input);
        builder.environment().put("OUTPUT_PATH", output);
        builder.command("make", type);
        builder.redirectErrorStream(true);

        try {
            Process process = builder.start();
           
            JTextArea txtAreaConsole = new JTextArea();
            JFrame frameConsole = new JFrame();

            frameConsole.setTitle("Running " + type);
            frameConsole.setSize(700, 600);
            frameConsole.setLocation(new Point(300, 200));
            frameConsole.setLayout(null);
            frameConsole.setResizable(false);

            txtAreaConsole.setEditable(false);
            txtAreaConsole.setVisible(true);
            
            JScrollPane scroll = new JScrollPane (txtAreaConsole,
                    JScrollPane.VERTICAL_SCROLLBAR_ALWAYS, JScrollPane.HORIZONTAL_SCROLLBAR_ALWAYS);

            scroll.setBounds(10, 10, 680, 540);
            scroll.setVisible(true);
            frameConsole.add(scroll);
            frameConsole.setVisible(true);

            TextAreaOutputStream textAreaOutputStream = new TextAreaOutputStream(txtAreaConsole, "");
            IOUtils.copy(process.getInputStream(), textAreaOutputStream);

            int exitVal = process.waitFor();
            if (exitVal == 0) {
                System.out.println("MapReduce Job Successfully Finished: " + type);
                textAreaOutputStream.write(("MapReduce Job Successfully Finished: " + type + "\n").getBytes());
            } else {
                System.err.println("MapReduce Job Failed: " + type);
                textAreaOutputStream.write(("MapReduce Job Failed: " + type + "\n").getBytes());
            }
            textAreaOutputStream.close();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
