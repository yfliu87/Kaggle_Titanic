package decisionTree;

import dataStructure.FeatureLabel;
import dataStructure.RecordTable;

import java.util.HashMap;
import java.util.Map;

import static util.Calculator.majority;

/**
 * Created by yifeiliu on 3/11/17.
 */
public class TreeNode {

    private int featureIndex = -1;
    private String feature = null;
    private int size = 0, leftSubtreeSize = 0, rightSubtreeSize = 0;
    private double value = Double.MAX_VALUE;
    private String label = null;
    private TreeNode left = null, right = null;
    private Map<String,Integer> distribution = new HashMap<>();

    public TreeNode(RecordTable table) {

        this.value = majority(table);
        this.label = FeatureLabel.getName((int)this.value);
        updateDistribution(table);
    }

    public TreeNode(int featureIndex, double val, RecordTable table) {
        this.featureIndex = featureIndex;
        this.feature = FeatureLabel.getFeatureName(featureIndex);
        this.value = val;
        this.label = FeatureLabel.getName((int) majority(table));
        updateDistribution(table);
    }

    private void updateDistribution(RecordTable table) {
        Map<Integer, Integer> typeCounts = table.getTypeCounts();
        for (int label : typeCounts.keySet()) {
            distribution.put(FeatureLabel.getName(label), typeCounts.get(label));
        }
    }

    public TreeNode getLeft() {
        return this.left;
    }

    public void setLeft(TreeNode node) {
        this.left = node;
    }

    public TreeNode getRight() {
        return this.right;
    }

    public void setRight(TreeNode node) {
        this.right = node;
    }

    public void setSize(int size) {
        this.size = size;
    }

    public void setLeftSubtreeSize(int size) {
        this.leftSubtreeSize = size;
    }

    public void setRightSubtreeSize(int size) {
        this.rightSubtreeSize = size;
    }

    public int getFeatureIndex() {
        return featureIndex;
    }

    public double getValue() {
        return this.value;
    }

    public String getLabel() {
        return label;
    }
}
