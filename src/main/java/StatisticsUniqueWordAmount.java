import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class StatisticsUniqueWordAmount extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new StatisticsUniqueWordAmount(), args);
        System.exit(exitCode);
    }

    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = JobBuilder.parseInputAndOutput(this, conf, args);
        if (job == null) {
            return -1;
        }
        job.setMapperClass(StatisticsUniqueWordAmountMapper.class);
        job.setReducerClass(StatisticsUniqueWordAmountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    /*
     * 第三个MapReduce在第一个MapReduce的计算基础上得到训练集中不重复的单词<word,one>
     * 输入:args[1],输入格式为<<class,word>,counts>
     * 输出:args[3],输出key为不重复单词,value为1.格式为<word,one>
     */
    static class StatisticsUniqueWordAmountMapper extends Mapper<Text, IntWritable, Text, IntWritable> {
        private final IntWritable one = new IntWritable(1);
        private Text newKey = new Text();

        @Override
        protected void map(Text key, IntWritable value, Context context) throws IOException, InterruptedException {
            String name = key.toString().split(":", 2)[1];
            newKey.set(name);
            context.write(newKey, value);
        }
    }

    static class StatisticsUniqueWordAmountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private static final IntWritable one = new IntWritable(1);

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            context.write(key, one);
        }
    }
}