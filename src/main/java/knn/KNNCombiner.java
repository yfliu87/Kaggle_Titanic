package knn;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.PriorityQueue;

/**
 * Created by yifeiliu on 3/11/17.
 */
public class KNNCombiner extends Reducer<Text, Text, Text, Text> {

    PriorityQueue<String> maxPQ = new PriorityQueue<String>(
            (t1, t2) -> Double.compare(Double.parseDouble(t2.split(",")[0]), Double.parseDouble(t1.split(",")[0])));

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        int knnSize = Integer.parseInt(context.getConfiguration().get("knnSize"));

        for (Text t : values) {
            maxPQ.add(t.toString());

            if (maxPQ.size() > knnSize)
                maxPQ.poll();
        }

        while(!maxPQ.isEmpty()) {
            context.write(key, new Text(maxPQ.poll()));
        }
    }
}
