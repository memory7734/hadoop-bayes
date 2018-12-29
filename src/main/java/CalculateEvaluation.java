import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.reduce.IntSumReducer;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class CalculateEvaluation extends Configured implements Tool {
    private static HashMap<String, String> realClass = new HashMap<String, String>();
    private static HashMap<String, String> predictClass = new HashMap<String, String>();

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new CalculateEvaluation(), args);
        System.exit(exitCode);
    }

    public HashMap<String, String> getRealClass(Configuration conf, String[] args) throws IOException {
        String filePath = args[0];
        SequenceFile.Reader reader = null;
        SequenceFile.Reader.Option pathOption = SequenceFile.Reader.file(new Path(filePath));
        try {
            reader = new SequenceFile.Reader(conf, pathOption);
            Text key = (Text) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
            Text value = (Text) ReflectionUtils.newInstance(reader.getValueClass(), conf);
            while (reader.next(key, value)) {
                String[] keys = key.toString().split(":");
                realClass.put(keys[1], keys[0]);
            }
        } finally {
            IOUtils.closeStream(reader);
        }
        return realClass;
    }

    public HashMap<String, String> getPredictClass(Configuration conf, String[] args) throws IOException {
        String filePath = args[1] + "/part-r-00000";
        SequenceFile.Reader reader = null;
        SequenceFile.Reader.Option pathOption = SequenceFile.Reader.file(new Path(filePath));
        try {
            reader = new SequenceFile.Reader(conf, pathOption);
            Text key = (Text) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
            Text value = (Text) ReflectionUtils.newInstance(reader.getValueClass(), conf);
            while (reader.next(key, value)) {
                predictClass.put(key.toString(), value.toString());
            }
        } finally {
            IOUtils.closeStream(reader);
        }
        return predictClass;
    }

    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();
        getRealClass(conf, args);
        getPredictClass(conf, args);
        Job job = JobBuilder.parseInputAndOutput(this, conf, args);
        if (job == null) {
            return -1;
        }
        job.setMapperClass(CalculateEvaluationMapper.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    static class CalculateEvaluationMapper extends Mapper<Text, IntWritable, Text, IntWritable> {
        private static final String tp = ":TP";
        private static final String tn = ":TN";
        private static final String fp = ":FP";
        private static final String fn = ":FN";
        private static final IntWritable one = new IntWritable(1);
        private Text text = new Text();

        @Override
        protected void map(Text key, IntWritable value, Context context) throws IOException, InterruptedException {
            String className = key.toString();
            System.out.println(className);
            for (Map.Entry<String, String> entry : predictClass.entrySet()) {
                if (realClass.get(entry.getKey()).equals(className) && entry.getValue().equals(className)) {
                    text.set(className + tp);
                    context.write(text, one);
                } else if (realClass.get(entry.getKey()).equals(className) && !entry.getValue().equals(className)) {
                    text.set(className + fn);
                    context.write(text, one);
                } else if (!realClass.get(entry.getKey()).equals(className) && entry.getValue().equals(className)) {
                    text.set(className + fp);
                    context.write(text, one);
                } else {
                    text.set(className + tn);
                    context.write(text, one);
                }
            }
        }
    }
}
