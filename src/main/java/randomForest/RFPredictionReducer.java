package randomForest;

import dataStructure.FeatureLabel;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by yifeiliu on 3/11/17.
 */
public class RFPredictionReducer extends Reducer<Text, Text, Text, IntWritable> {

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String result = "";

        for (Text t : values) {
            context.write(key, new IntWritable(t.toString().equals(FeatureLabel.SURVIVED) ? 1 : 0));
        }
    }
}
