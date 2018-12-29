import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.reduce.IntSumReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.StringTokenizer;

public class StatisticsClassAmount extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new StatisticsClassAmount(), args);
        System.exit(exitCode);
    }

    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = JobBuilder.parseInputAndOutput(this, conf, args);
        if (job == null) {
            return -1;
        }
        job.setMapperClass(StatisticsClassAmountMapper.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    /*
     * 第一个MapReduce用于处理序列化的文件，得到<<类名:单词>,单词出现次数>,即<<Class:word>,TotalCounts>
     * 输入:args[0],序列化的训练集,key为<类名:文档名>,value为文档中对应的单词.形式为<<ClassName:Doc>,word1 word2...>
     * 输出:args[1],key为<类名:单词>,value为单词出现次数,即<<Class:word>,TotalCounts>
     */
    static class StatisticsClassAmountMapper extends Mapper<Text, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text newKey = new Text();

        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            String name = key.toString().split(":", 2)[0];
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                newKey.set(name + ":" + itr.nextToken());
                context.write(newKey, one);
            }
        }
    }
}
