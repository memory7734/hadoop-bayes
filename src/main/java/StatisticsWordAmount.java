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

public class StatisticsWordAmount extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new StatisticsWordAmount(), args);
        System.exit(exitCode);
    }

    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = JobBuilder.parseInputAndOutput(this, conf, args);
        if (job == null) {
            return -1;
        }
        job.setMapperClass(StatisticsWordAmountMapper.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    /*
     * 第二个MapReduce在第一个MapReduce计算的基础上进一步得到每个类的单词总数<class,TotalWords>
     * 输入:args[1],输入格式为<<class,word>,counts>
     * 输出:args[2],输出key为类名,value为单词总数.格式为<class,Totalwords>
     */
    static class StatisticsWordAmountMapper extends Mapper<Text, IntWritable, Text, IntWritable> {
        private final IntWritable one = new IntWritable(1);
        private Text newKey = new Text();

        @Override
        protected void map(Text key, IntWritable value, Context context) throws IOException, InterruptedException {
            String name = key.toString().split(":", 2)[0];
            newKey.set(name);
            context.write(newKey, value);
        }
    }
}