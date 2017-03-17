package dataStructure;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Created by yifeiliu on 3/11/17.
 */
public class FeatureLabel {

    public static final String PASSENGERID = "PassengerId";
    public static final String PCLASS = "Pclass";
    public static final String SEX = "Sex";
    public static final String AGE = "Age";
    public static final String SIBSP = "Sibsp";
    public static final String PARCH = "Parch";
    public static final String TICKET = "Ticket";
    public static final String FARE = "Fare";
    public static final String CABIN = "Cabin";
    public static final String EMBARKED = "Embarked";

    public static final String SURVIVED = "Survived";
    public static final String NOTSURVIVED = "Not Survived";

    private static Map<String, Integer> predictionTypes = new HashMap<String, Integer>() {{
        put(NOTSURVIVED, 0);
        put(SURVIVED, 1);
    }};

    private static Map<Integer, String> featureIndex = new HashMap<Integer, String>() {{
        put(0, PASSENGERID); put(1, PCLASS); put(2, SEX);
        put(3, AGE); put(4, SIBSP); put(5, PARCH); put(6, TICKET);
        put(7, FARE); put(8, CABIN); put(9, EMBARKED);
    }};

    private static Map<String,Double> embarkMapping = new HashMap<String,Double>(){{
        put("S",1.0);
        put("C",2.0);
        put("Q",3.0);
    }};


    /**
     * Get label name according to label value
     * @param type
     * @return the label "Survived" or "Not Survived"
     */
    public static String getName(int type) {
        return predictionTypes.entrySet()
                .parallelStream().filter(i -> i.getValue() == type)
                .collect(Collectors.toList()).get(0).getKey();
    }

    /**
     * Get corresponding index given a feature name
     * @param feature
     * @return the index of the feature
     */
    public static int getFeatureIndex(String feature) {
        return featureIndex.entrySet().parallelStream()
                .filter(item -> item.getValue().equalsIgnoreCase(feature))
                .collect(Collectors.toList()).get(0).getKey();
    }

    /**
     * Get the corresponding feature name given index
     * @param index
     * @return the feature name
     */
    public static String getFeatureName(int index) {
        return featureIndex.get(index);
    }

    /**
     * Get the corresponding int mapping of embark location
     * @param s
     * @return the integer mapping
     */
    public static Double getEmbark(String s) {
        return embarkMapping.get(s);
    }
}
