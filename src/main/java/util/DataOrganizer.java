package util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.*;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by yifeiliu on 3/14/17.
 */
public class DataOrganizer {

    public static void splitData(Configuration conf, String trainingPath, String inputPath, int treeNum) throws IOException {

        List<String> rawData = readRawData(conf, trainingPath);
        int chunkSize = rawData.size()/treeNum;

        FileSystem fs = FileSystem.get(conf);
        fs.delete(new Path(inputPath), true);

        BufferedWriter bWriter = null;

        for (int i = 0; i < treeNum; i++) {
            Path tempFile = new Path(inputPath + "/" + i + ".txt");

            System.out.println("Writing training file: " + tempFile);

            OutputStream os = fs.create(tempFile);
            bWriter = new BufferedWriter(new OutputStreamWriter(os, "UTF-8"));

            int start = i * chunkSize, stop = (i + 1) * chunkSize;
            for (int j = start; j < stop; j++) {
                bWriter.write(rawData.get(j));
                bWriter.newLine();
            }
            bWriter.close();
        }
    }

    public static List<String> readRawData(Configuration conf, String inputPath) throws IOException {
        List<String> ret = new LinkedList<>();
        FileSystem fs = FileSystem.get(conf);
        FileStatus status = FileSystem.get(conf).listStatus(new Path(inputPath))[0];

        try {
            BufferedReader bReader = new BufferedReader(
                    new InputStreamReader(fs.open(new Path(status.getPath().toString()))));

            String line = bReader.readLine();   //skip column header line
            while ((line = bReader.readLine()) != null) {
                ret.add(line);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return ret;
    }

}
