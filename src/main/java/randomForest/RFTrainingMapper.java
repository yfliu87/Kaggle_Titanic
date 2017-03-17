package randomForest;

import com.google.gson.Gson;
import dataStructure.Record;
import dataStructure.RecordTable;
import decisionTree.DecisionTree;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by yifeiliu on 3/11/17.
 */
public class RFTrainingMapper extends Mapper<Object, Text, Text, Text> {

    private RecordTable table = new RecordTable();

    public void map(Object key, Text value, Context context) {
        String line = value.toString();
        if (line.startsWith("PassengerId"))
            return;

        if (line.endsWith(","))
            line += "S";    // training passengerId=62, missing embarked field

        table.addRecord(new Record(line));
    }

    public void cleanup(Context context) throws IOException, InterruptedException {

        DecisionTree dt = DecisionTree.fit(table);
        String json = new Gson().toJson(dt);
        context.write(new Text("dt"), new Text(json));
    }
}
