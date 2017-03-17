package dataStructure;

import java.util.LinkedList;
import java.util.List;

/**
 * Created by yifeiliu on 3/11/17.
 */
public class Record {

    private List<Integer> featureIndexes = new LinkedList<>();
    private List<Double> features = new LinkedList<>();
    private boolean survived = false;

    // User defined skipped features
    private List<Integer> skipFeatures = new LinkedList<Integer>(){{
        //add(FeatureLabel.getFeatureIndex("SibSp"));
        //add(FeatureLabel.getFeatureIndex("Parch"));
        add(FeatureLabel.getFeatureIndex("Ticket"));
        add(FeatureLabel.getFeatureIndex("Cabin"));
        //add(FeatureLabel.getFeatureIndex("Embarked"));
    }};

    public Record() {}

    public Record(String line) {
        String str = removeName(line);
        String[] items = str.split(",");

        if (items.length == 11) { // training data
            this.survived = items[1].equals("1") ? true : false;
            str = removeSurvivedColumn(str);

            buildRecord(str.split(","));

        } else if (items.length == 10) { // test data

            buildRecord(items);

        } else
            System.err.println("Record length error: " + str);
    }

    public boolean getLabel() {
        return survived;
    }

    public void setLabel(boolean b) {
        this.survived = b;
    }

    public List<Integer> getFeatureIndexes() {
        return this.featureIndexes;
    }

    public double getFeature(int idx) {
        return this.features.get(featureIndexes.indexOf(new Integer(idx)));
    }

    public double getFeature(String name) {
        return this.getFeature(FeatureLabel.getFeatureIndex(name));
    }

    public void addFeature(int idx, double value) {
        this.featureIndexes.add(idx);
        this.features.add(value);
    }

    public Record removeFeatureByIndex(int featureIndex) {
        List<Integer> indexes = this.getFeatureIndexes();
        indexes.remove(new Integer(featureIndex));

        Record rec = new Record();
        for (int idx : indexes) {
            rec.addFeature(idx, this.getFeature(idx));
        }
        rec.setLabel(this.getLabel());
        return rec;
    }

    private String removeName(String str) {
        int start = str.indexOf("\""), stop = str.lastIndexOf("\"");
        String pre = str.substring(0, start - 1);
        String post = str.substring(stop + 1);
        return pre + post;
    }

    private String removeSurvivedColumn(String line) {
        String pre = line.substring(0, line.indexOf(","));
        String post = line.substring(line.indexOf(",") + 1);
        pre += post.substring(post.indexOf(","));
        return pre;
    }

    private void buildRecord(String[] columns) {

        for (int idx = 0; idx < columns.length; idx++) {
            if (skipFeatures.contains(idx))
                continue;

            this.featureIndexes.add(idx);

            if (idx == FeatureLabel.getFeatureIndex("Sex")) {
                this.features.add(columns[idx].equalsIgnoreCase("male") ? 0.0 : 1.0);
            }
            else if (idx == FeatureLabel.getFeatureIndex("Embarked")) {
                this.features.add(FeatureLabel.getEmbark(columns[idx]));
            }
            else
                this.features.add(columns[idx].isEmpty() ? 0.0 : Double.parseDouble(columns[idx]));
        }
    }
}
