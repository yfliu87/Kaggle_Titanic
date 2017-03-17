package dataStructure;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by yifeiliu on 3/11/17.
 */
public class RecordTable {

    private List<Record> records = new LinkedList<>();
    private Map<Integer,Integer> typeCounts = null;

    public RecordTable() {}

    public RecordTable(List<Record> records) {
        this.records = records;
    }

    public int size() {
        return this.records.size();
    }

    public Record getRecord(int index) {
        assert(index < records.size());

        return this.records.get(index);
    }

    public void addRecord(Record rec) {
        this.records.add(rec);
    }

    public void shuffle() {
        Collections.shuffle(records);
    }

    public RecordTable[] randomSelectSubset(double percentage) {
        int threshold = (int) (10 * percentage);
        Random rand = new Random();

        List<Record> train = new LinkedList<>();
        List<Record> test = new LinkedList<>();

        for (int i = 0; i < this.size(); i++) {
            if (rand.nextInt(10) < threshold)
                train.add(this.getRecord(i));
            else
                test.add(this.getRecord(i));
        }
        return new RecordTable[]{new RecordTable(train), new RecordTable(test)};
    }

    /**
     * Get the count of each class type.
     * @return Map<Integer,Integer> class type count
     */
    public Map<Integer,Integer> getTypeCounts() {
        if (typeCounts != null)
            return this.typeCounts;

        Map<Integer, Integer> typeCounts = new HashMap<>();
        for (Record rec : this.records) {
            int label = rec.getLabel() ? 1 : 0;
            typeCounts.put(label, typeCounts.getOrDefault(label, 0) + 1);
        }

        this.typeCounts = typeCounts;
        return typeCounts;
    }

    /**
     * Remove the specific feature column from the table
     * @param featureIndex
     */
    public void removeFeatureByIndex(int featureIndex) {
        this.records = this.records.parallelStream()
                .map(i -> i.removeFeatureByIndex(featureIndex))
                .collect(Collectors.toList());
    }

    /**
     * Get all the feature indexes of current record table.
     * @return
     */
    public List<Integer> getFeatureIndex() {
        return this.records.get(0).getFeatureIndexes();
    }

    /**
    * Get the feature index value from all records.
    * @param index
    * @return RecordTable
    */
    public List<Double> getFeatureCollection(int index) {
        return this.records.parallelStream()
                    .map(e -> e.getFeature(index))
                    .collect(Collectors.toList());
    }
}
