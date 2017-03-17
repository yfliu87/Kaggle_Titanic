package randomForest;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static util.Calculator.calculateMetrics;
import static util.DataOrganizer.splitData;

/**
 * Created by yifeiliu on 3/11/17.
 */
public class RFDriver {

    public static int run(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

        Path trainingPath = new Path(args[1]);
        Path testPath = new Path(args[2]);
        Path referencePath = new Path(args[4]);
        Path inputPath = new Path("/input");
        String[] treeNums = args[5].split(",");

        int jobCode = 0;
        Map<String, String> metrics = new HashMap<>();

        // grid search according to given tree numbers
        for (String treeNum : treeNums) {

            System.out.println("\n=======================================");
            System.out.println("\tRandom Forest with " + treeNum + " trees");
            System.out.println("=======================================");

            Configuration conf = new Configuration();
            Path modelPath = new Path(args[3] + "/Model_" + treeNum + "_trees");

            splitData(conf, trainingPath.toString(), inputPath.toString(), Integer.parseInt(treeNum));

            jobCode = trainingJob(conf, inputPath, modelPath, Integer.parseInt(treeNum));
            if (jobCode != 0)
                return jobCode;

            Path predictionPath = new Path(args[3] + "/Predict_" + treeNum + "_trees");
            jobCode = predictionJob(conf, testPath, modelPath, predictionPath, treeNum);
            if (jobCode != 0)
                return jobCode;

            metrics.put(treeNum, calculateMetrics(conf, predictionPath, referencePath));
        }

        for (Map.Entry<String,String> entry : metrics.entrySet()) {
            System.out.println("\n\nTree number: " + entry.getKey() + entry.getValue());
        }
        return jobCode;
    }

    private static int trainingJob(Configuration trainingConf, Path trainingPath, Path modelPath, int treeNum) throws IOException, ClassNotFoundException, InterruptedException {

        trainingConf = new Configuration();
        Job trainingJob = Job.getInstance(trainingConf, "Random Forest Training");

        trainingJob.setJarByClass(RFDriver.class);
        trainingJob.setMapperClass(RFTrainingMapper.class);
        trainingJob.setReducerClass(RFTrainingReducer.class);
        trainingJob.setOutputKeyClass(Text.class);
        trainingJob.setOutputValueClass(Text.class);

        trainingJob.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.addInputPath(trainingJob, trainingPath);

        trainingJob.setOutputFormatClass(TextOutputFormat.class);
        FileSystem.get(trainingConf).delete(modelPath, true);
        FileOutputFormat.setOutputPath(trainingJob, modelPath);

        return trainingJob.waitForCompletion(true) ? 0 : 1;
    }

    private static int predictionJob(Configuration predictionConf, Path testPath, Path modelPath, Path predictionPath, String treeNum) throws IOException, ClassNotFoundException, InterruptedException {

        predictionConf = new Configuration();
        predictionConf.set("modelPath", modelPath.toString());
        predictionConf.set("treeNum", treeNum);
        predictionConf.set("mapreduce.output.textoutputformat.separator", ",");

        Job predictJob = Job.getInstance(predictionConf, "Random Forest Prediction");

        predictJob.setJarByClass(RFDriver.class);
        predictJob.setMapperClass(RFPredictionMapper.class);
        predictJob.setReducerClass(RFPredictionReducer.class);
        predictJob.setOutputKeyClass(Text.class);
        predictJob.setOutputValueClass(Text.class);

        predictJob.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(predictJob, testPath);

        predictJob.setOutputFormatClass(TextOutputFormat.class);
        FileSystem.get(predictionConf).delete(predictionPath, true);
        TextOutputFormat.setOutputPath(predictJob, predictionPath);

        return predictJob.waitForCompletion(true) ? 0 : 1;
    }
}
