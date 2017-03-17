package knn;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;

/**
 * Created by yifeiliu on 3/11/17.
 */
public class KNNReducer extends Reducer<Text,Text,Text,Text> {

    PriorityQueue<String> pq = new PriorityQueue<>(
            (t1,t2) -> Double.compare(Double.parseDouble(t2.split(",")[0]), Double.parseDouble(t1.split(",")[0])));

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        int knnSize = Integer.parseInt(context.getConfiguration().get("knnSize"));

        for (Text t : values) {
            pq.add(t.toString());

            if (pq.size() > knnSize)
                pq.poll();
        }

        context.write(key, new Text(typeVote(pq)));
    }

    private String typeVote(PriorityQueue<String> pq) {

        Map<String, Integer> typeCount = new HashMap<>();
        while(!pq.isEmpty()) {
            String type = pq.poll().split(",")[1];
            typeCount.put(type, typeCount.getOrDefault(type, 0) + 1);
        }
        return typeCount.entrySet().stream()
                .max((e1, e2) -> e2.getValue() - e1.getValue())
                .get().getKey().equalsIgnoreCase("true") ? "1" : "0";
    }
}
