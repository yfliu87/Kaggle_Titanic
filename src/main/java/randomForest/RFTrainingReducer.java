package randomForest;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by yifeiliu on 3/11/17.
 */
public class RFTrainingReducer extends Reducer<Text, Text, Text, Text> {

    private String randomForest = "";

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        for (Text t : values) {
            randomForest += (t.toString() + "\n");
        }

        context.write(new Text(randomForest), new Text());
    }
}
