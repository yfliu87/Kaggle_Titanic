package decisionTree;

import dataStructure.Record;
import dataStructure.RecordTable;
import util.Calculator;

import java.util.Set;

/**
 * Created by yifeiliu on 3/11/17.
 */
public class DecisionTree {

    private TreeNode root;

    private DecisionTree(RecordTable table) {
        root = buildTree(table);
    }

    //build decision tree from training data
    public static DecisionTree fit(RecordTable table) {
        return new DecisionTree(table);
    }

    private TreeNode buildTree(RecordTable table) {

        System.out.println("\nBuild tree table size: " + table.size());

        TreeNode root = null;

        if (shouldStop(table)) {
            System.out.println("\nShould stop table size: " + table.size());
            root = new TreeNode(table);
            root.setSize(table.size());
            return root;
        }

        double[] bestSplit = findBestSplitAttribute(table);
        int bestFeatureIndex = (int) bestSplit[0];
        double splitValue = bestSplit[1];

        if (bestFeatureIndex == -1) {
            root = new TreeNode(table);
            root.setSize(table.size());
            return root;
        }

        RecordTable[] splitTable = splitTable(table, bestFeatureIndex, splitValue);

        if (splitTable[0].size() == 0 || splitTable[1].size() == 0) {
            root = new TreeNode(table);
            root.setSize(table.size());
            return root;
        }

        root = new TreeNode(bestFeatureIndex, splitValue, table);
        root.setSize(table.size());

        splitTable[0].removeFeatureByIndex(bestFeatureIndex);
        splitTable[1].removeFeatureByIndex(bestFeatureIndex);

        root.setLeftSubtreeSize(splitTable[0].size());
        root.setRightSubtreeSize(splitTable[1].size());

        root.setLeft(buildTree(splitTable[0]));
        root.setRight(buildTree(splitTable[1]));

        return root;
    }

    private boolean shouldStop(RecordTable table) {
        return (table.size() <= 1 || Double.compare(Calculator.entropy(table.getTypeCounts()), 0.0) == 0);
    }

    /**
     * Find the best feature & corresponding value to split the data table.
     * @param table
     * @return the split table
     */
    private double[] findBestSplitAttribute(RecordTable table) {
        double bestInfoGain = Double.MIN_VALUE, bestValue = Double.MAX_VALUE;
        double baseEntropy = Calculator.entropy(table.getTypeCounts());
        int bestFeatureIndex = -1;

        for (int i : table.getFeatureIndex()) {

            Set<Double> uniqueVals = Calculator.uniqueVals(table.getFeatureCollection(i));

            for (double value : uniqueVals) {
                RecordTable[] subRecordTable = splitTable(table, i, value);

                double infoGain = Calculator.infoGain(baseEntropy, subRecordTable, table.size());

                if (Double.compare(infoGain, bestInfoGain) > 0) {
                    bestInfoGain = infoGain;
                    bestFeatureIndex = i;
                    bestValue = value;
                }
            }
        }
        return new double[] {bestFeatureIndex, bestValue};
    }

    /**
     * Split the table according to feature index and best split value
     * @param table
     * @param featureIndex
     * @param splitValue
     * @return the split tables
     */
    private RecordTable[] splitTable(RecordTable table, int featureIndex, double splitValue) {
        RecordTable left = new RecordTable();
        RecordTable right = new RecordTable();

        for (int i = 0; i < table.size(); i++) {
            Record rec = table.getRecord(i);
            double featureValue = rec.getFeature(featureIndex);

            if (Double.compare(featureValue, splitValue) < 0)
                left.addRecord(rec);
            else
                right.addRecord(rec);
        }

        return new RecordTable[]{left, right};
    }

    /**
     * Predict the survival status of given record
     * @param rec
     * @return "Survived" or "Not Survived"
     */
    public String predict(Record rec) {
        StringBuilder sb = new StringBuilder();
        match(root, rec, sb);
        return sb.toString();
    }

    private void match(TreeNode root, Record rec, StringBuilder sb) {
        if (root.getLeft() == null && root.getRight() == null)
            sb.append(root.getLabel());
        else {
            int featureIdx = root.getFeatureIndex();

            if (!rec.getFeatureIndexes().contains(featureIdx))
                return;

            if (rec.getFeature(featureIdx) < root.getValue())
                match(root.getLeft(), rec, sb);
            else
                match(root.getRight(), rec, sb);
        }
    }
}
