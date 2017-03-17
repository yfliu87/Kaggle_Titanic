package knn;

import dataStructure.FeatureLabel;
import dataStructure.Record;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import util.Calculator;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by yifeiliu on 3/11/17.
 */
public class KNNMapper extends Mapper<Object, Text, Text, Text> {

    Set<Record> trainingRecords = new HashSet<>();

    @Override
    public void setup(Context context) throws IOException {
        Configuration conf = context.getConfiguration();
        FileSystem fs = FileSystem.get(conf);

        try {
            BufferedReader bReader = new BufferedReader(new InputStreamReader(
                    fs.open(new Path(conf.get("trainingPath")))));

            String line = bReader.readLine();   // skip column line
            while((line = bReader.readLine()) != null) {
                trainingRecords.add(new Record(line));
            }
            bReader.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        // skip column line for test file
        if (value.toString().startsWith("PassengerId"))
            return;

        Record testRec = new Record(value.toString());

        for (Record trainingRec : trainingRecords) {
            double dist = Calculator.euclideanDistance(testRec, trainingRec);
            context.write(new Text(String.valueOf((int)testRec.getFeature(FeatureLabel.PASSENGERID))),
                          new Text(dist + "," + trainingRec.getLabel()));

        }
    }
}
