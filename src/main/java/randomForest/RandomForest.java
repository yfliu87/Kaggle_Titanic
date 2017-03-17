package randomForest;

import com.google.gson.Gson;
import dataStructure.Record;
import decisionTree.DecisionTree;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Created by yifeiliu on 3/11/17.
 */
public class RandomForest {

    private int numberOfTrees;
    private List<DecisionTree> trees;
    private List<Double> treeAccuracy;

    public RandomForest(int dtNum) {
        this.numberOfTrees = dtNum;
        trees = new LinkedList<>();
        treeAccuracy = new LinkedList<>();
    }

    public void restore(String json) {
        Gson gs = new Gson();
        String[] treeJsons = json.split(";");

        for (String treeJson : treeJsons) {
            if (treeJson == null || treeJson.length() == 0)
                continue;;

            DecisionTree dt = gs.fromJson(treeJson, DecisionTree.class);

            if (dt == null)
                continue;

            trees.add(dt);
        }
    }

    public String predict(Record rec) {
        Map<String, Integer> prediction = new HashMap<>();

        for (DecisionTree dt : trees) {
            String type = dt.predict(rec);

            prediction.put(type, prediction.getOrDefault(type, 0) + 1);
        }
        return voteForResult(prediction);
    }

    private String voteForResult(Map<String, Integer> prediction) {
        return prediction.entrySet().parallelStream()
                .sorted((a,b) -> b.getValue() - a.getValue())
                .collect(Collectors.toList())
                .get(0).getKey();
    }
}
