package util;

import dataStructure.Record;
import dataStructure.RecordTable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import scala.Tuple2;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by yifeiliu on 3/11/17.
 */
public class Calculator {

    /**
     * Calculate the euclidean distance between the input records
     * @param r1
     * @param r2
     * @return the euclidean distance
     */
    public static double euclideanDistance(Record r1, Record r2) {
        double sum = 0.0;
        // Calculate similarity using features not skipped
        for (int i : r1.getFeatureIndexes()) {
            sum += diffSquare(r1.getFeature(i), r2.getFeature(i));
        }
        return Math.sqrt(sum);
    }

    private static double diffSquare(double d1, double d2) {
        return (d1 - d2) * (d1 - d2);
    }

    /**
     * Find out the majority label among all the records of current table
     *
     * @param table
     * @return the majority label
     */
    public static int majority(RecordTable table) {
        return table.getTypeCounts().entrySet().parallelStream()
                .sorted((a, b) -> b.getValue() - a.getValue())
                .collect(Collectors.toList()).get(0).getKey();
    }

    /**
     * Calculate entropy of multiclass labels
     * @param counts <flower type, counts>
     * @return entropy of multiclass labels
     */
    public static double entropy(Map<Integer,Integer> counts) {
        double entropy = 0.0;
        double total = counts.entrySet().parallelStream().mapToDouble(e -> e.getValue()).sum();

        for (Map.Entry<Integer,Integer> entry : counts.entrySet()) {
            entropy -= ((entry.getValue()/total) * log2(entry.getValue()/total));
        }
        return entropy + 0.0;
    }

    private static double log2(double num) {
        return (Double.compare(num, 0.0) == 0 ? 0.0 : Math.log(num)/Math.log(2));
    }

    /**
     * Get the unique values in given collection
     * @param featureCollection
     * @return set of unique values
     */
    public static Set<Double> uniqueVals(List<Double> featureCollection) {
        return new HashSet<>(featureCollection);
    }

    public static double infoGain(double baseEntropy, RecordTable[] subRecordTable, int size) {

        double entropy = subRecordTable[0].size()/(double)size * entropy(subRecordTable[0].getTypeCounts());
        entropy += subRecordTable[1].size()/(double)size * entropy(subRecordTable[1].getTypeCounts());
        return baseEntropy - entropy;
    }

    /**
     * Calculate the precision, recall and F1Score
     * @param predictionPath
     * @param referencePath
     * @return metric
     */
    public static String calculateMetrics(Configuration conf, Path predictionPath, Path referencePath) {

        Map<String, Tuple2<Integer,Integer>> survival = new HashMap<>();

        try {
            FileSystem fs = FileSystem.get(conf);

            readReference(fs, survival, referencePath);
            readPrediction(fs, survival, predictionPath);

            return "\nMetrics: " + getMetrics(survival);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return "";
    }

    private static void readReference(FileSystem fs, Map<String, Tuple2<Integer,Integer>> survival, Path referencePath) {
        try {
            BufferedReader bReader = new BufferedReader(new InputStreamReader(
                    fs.open(new Path(referencePath.toString() + "/reference.csv"))));

            String line = bReader.readLine();   //skip column line
            while ((line = bReader.readLine()) != null) {
                String[] items = line.split(",");
                survival.put(items[0],
                        new Tuple2<>(Integer.parseInt(items[items.length - 1].trim()), Integer.MAX_VALUE));
            }
            bReader.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void readPrediction(FileSystem fs, Map<String, Tuple2<Integer,Integer>> survival, Path predictionPath) {
        try {
            BufferedReader bReader = new BufferedReader(new InputStreamReader(
                    fs.open(new Path(predictionPath.toString() + "/part-r-00000"))));

            String line = null;
            while ((line = bReader.readLine()) != null) {
                String passengerId = line.split(",")[0];
                assert(survival.containsKey(passengerId));

                int prediction = Integer.parseInt(line.split(",")[1].trim());
                survival.put(passengerId, new Tuple2<>(survival.get(passengerId)._1, prediction));
            }
            bReader.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static String getMetrics(Map<String, Tuple2<Integer,Integer>> survival) {
        long tp = getTruePositive(survival);
        long fp = getFalsePositive(survival);
        long fn = getFalseNegative(survival);
        long tn = getTrueNegative(survival);

        double precision = (tp + fp) == 0 ? 0.0 : (double) tp / (tp + fp);
        double recall = (tp + fp) == 0 ? 0.0 : (double) tp / (tp + fn);
        double f1Score = (precision + recall) == 0.0 ? 0.0 : 2*precision*recall/(precision + recall);

        StringBuilder sb = new StringBuilder();
        sb.append(String.format("\nPrecision: %f, recall: %f, F1 score: %f", precision, recall, f1Score));
        sb.append(getConfusionMatrix(tp, fp, fn, tn));
        return sb.toString();
    }

    private static String getConfusionMatrix(long tp, long fp, long fn, long tn) {
        StringBuilder sb = new StringBuilder();
        sb.append("\n\n====== confusion matrix ======\n");
        sb.append("\t\tPrediction\n");
        sb.append("Actual\t\tSurvival\tNot Survival\n");
        sb.append(String.format("Survival\t%d\t\t%d\n", tp, fn));
        sb.append(String.format("Not survival\t%d\t\t%d", fp, tn));
        return sb.toString();
    }

    private static long getTruePositive(Map<String, Tuple2<Integer, Integer>> survival) {
        return survival.entrySet().parallelStream()
                .filter(i -> i.getValue()._1 == 1 && i.getValue()._2 == 1).count();
    }

    private static long getFalseNegative(Map<String, Tuple2<Integer, Integer>> survival) {
        return survival.entrySet().parallelStream()
                .filter(i -> i.getValue()._1 == 1 && i.getValue()._2 == 0).count();
    }

    private static long getFalsePositive(Map<String, Tuple2<Integer, Integer>> survival) {
        return survival.entrySet().parallelStream()
                .filter(i -> i.getValue()._1 == 0 && i.getValue()._2 == 1).count();
    }

    private static long getTrueNegative(Map<String, Tuple2<Integer, Integer>> survival) {
        return survival.entrySet().parallelStream()
                .filter(i -> i.getValue()._1 == 0 && i.getValue()._2 == 0).count();
    }
}
