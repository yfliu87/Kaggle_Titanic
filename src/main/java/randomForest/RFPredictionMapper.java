package randomForest;

import dataStructure.FeatureLabel;
import dataStructure.Record;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 * Created by yifeiliu on 3/11/17.
 */
public class RFPredictionMapper extends Mapper<Object, Text, Text, Text> {

    private RandomForest forest;

    @Override
    public void setup(Context context) throws IOException {

        FileSystem fs = FileSystem.get(context.getConfiguration());
        int treeCount = Integer.parseInt(context.getConfiguration().get("treeNum"));
        String rf = "";

        try {
            BufferedReader bReader = new BufferedReader(new InputStreamReader(
                    fs.open(new Path(context.getConfiguration().get("modelPath") + "/part-r-00000"))));

            String line = null;
            while ((line = bReader.readLine()) != null) {
                rf += (line + ";");
            }
            bReader.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

        forest = new RandomForest(treeCount);
        forest.restore(rf.substring(0, rf.length() - 1));
    }

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        String original = value.toString();
        if (original.startsWith("PassengerId"))
            return;

        Record testRec = new Record(original);
        String prediction = forest.predict(testRec);
        context.write(new Text(String.valueOf((int)testRec.getFeature(FeatureLabel.PASSENGERID))),
                      new Text(prediction));
    }
}
