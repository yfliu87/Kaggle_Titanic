package knn;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static util.Calculator.calculateMetrics;

/**
 * Created by yifeiliu on 3/11/17.
 */
public class KNNDriver {

    private static Map<Integer, String> metrics = new HashMap<>();

    public static int run(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();
        conf.set("mapreduce.output.textoutputformat.separator", ",");

        Path trainingPath = new Path(args[1]);
        Path testPath = new Path(args[2]);
        Path referencePath = new Path(args[4]);
        String[] kSizes = args[5].split(",");

        for (FileStatus fs : FileSystem.get(conf).listStatus(trainingPath)) {
            conf.set("trainingPath", fs.getPath().toString());

            // grid search to find optimal k
            for (String size : kSizes) {

                System.out.println("\n=============================");
                System.out.println("\tKNN with k = " + size);
                System.out.println("=============================");

                Path predictionPath = new Path(args[3] + "/KNN_" + size);
                int res = launchJob(conf, testPath, predictionPath, size);

                if (res != 0)
                    return res;

                metrics.put(Integer.parseInt(size), calculateMetrics(conf, predictionPath, referencePath));
            }
        }

        for (Map.Entry<Integer,String> entry : metrics.entrySet()) {
            System.out.println("\n\nK size: " + entry.getKey() + entry.getValue());
        }

        return 0;
    }

    private static int launchJob(Configuration conf, Path testPath, Path predictionPath, String size)
            throws IOException, ClassNotFoundException, InterruptedException {

        Job job = Job.getInstance(conf, "KNN" + size);

        job.setJarByClass(KNNDriver.class);
        job.setMapperClass(KNNMapper.class);
        job.setCombinerClass(KNNCombiner.class);
        job.setReducerClass(KNNReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileSystem.get(conf).delete(predictionPath, true);

        FileInputFormat.addInputPath(job, testPath);
        FileOutputFormat.setOutputPath(job, predictionPath);
        job.getConfiguration().set("knnSize", size);

        return job.waitForCompletion(true) ? 0 : 1;
    }

}
