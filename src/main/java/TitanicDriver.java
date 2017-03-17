import knn.KNNDriver;
import randomForest.RFDriver;

import java.io.IOException;

/**
 * Created by yifeiliu on 3/11/17.
 */
public class TitanicDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        int jobCode = 0;
        if (args[0].equalsIgnoreCase("knn")) {
            if (args.length == 6)
                jobCode = KNNDriver.run(args);
            else {
                System.out.println("KNN input format: hadoop jar xxx.jar knn " +
                        "training_path test_path output_path reference_path " +
                        "knnSize1,knnSize2,knnSize3...");
                jobCode = 1;
            }
        }
        else if (args[0].equalsIgnoreCase("rf")) {
            if (args.length == 6)
                jobCode = RFDriver.run(args);
            else {
                System.out.println("RF input format: hadoop jar xxx.jar rf " +
                        "training_path test_path output_path reference_path " +
                        "treeSize1,treeSize2,treeSize3...");
                jobCode = 1;
            }
        }
        else {
            System.out.println("Input format error. Please select algorithm to use: knn or rf");
            jobCode = 1;
        }
        System.exit(jobCode);
    }
}
