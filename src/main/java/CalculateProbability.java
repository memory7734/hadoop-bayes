import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

public class CalculateProbability extends Configured implements Tool {


    private static HashMap<String, Double> classProbably = new HashMap<String, Double>();

    private static HashMap<String, Double> wordsProbably = new HashMap<String, Double>();

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new CalculateProbability(), args);
        System.exit(exitCode);
    }

    /* 计算先验概率
     * 先验概率P(c)=类c下的单词总数/整个训练样本的单词总数
     * 输入:对应第二个MapReduce的输出,格式为<class,totalWords>
     * 输出:得到HashMap<String,Double>,即<类名,概率>
     */
    public HashMap<String, Double> getPriorProbably(Configuration conf, String[] args) throws IOException {
        String filePath = args[1];
        SequenceFile.Reader reader = null;
        SequenceFile.Reader.Option pathOption = SequenceFile.Reader.file(new Path(filePath));
        double totalWords = 0;
        try {
            reader = new SequenceFile.Reader(conf, pathOption);
            Text key = (Text) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
            IntWritable value = (IntWritable) ReflectionUtils.newInstance(reader.getValueClass(), conf);
            long position = reader.getPosition();// 获取当前读取的字节位置。设置标记点，标记文档起始位置，方便后面再回来遍历
            while (reader.next(key, value)) {
                totalWords += value.get();// 得到训练集文档总数
            }
            reader.seek(position);// 重置到前面定位的标记点
            while (reader.next(key, value)) {
                classProbably.put(key.toString(), value.get() / totalWords);// 各类文档的概率 = 各类文档数目/总文档数目
//                System.out.println(key + ":" + value.get() + "/" + totalWords + "\t" + value.get() / totalWords);
            }
        } finally {
            IOUtils.closeStream(reader);
        }
        return classProbably;
    }

    /* 计算条件概率
     * 条件概率P(tk|c)=(类c下单词tk在各个文档中出现过的次数之和+1)/（类c下单词总数+训练样本中不重复特征词总数）
     * 输入:对应第一个MapReduce的输出<<class,word>,counts>,第二个MapReduce的输出<class,totalWords>,第三个MapReduce的输出<class,diffTotalWords>
     * 输出:得到HashMap<String,Double>,即<<类名:单词>,概率>
     */
    public HashMap<String, Double> getConditionProbably(Configuration conf, String[] args) throws IOException {
        HashMap<String, Double> classTotalWord = new HashMap<String, Double>();
        String classWordAmountFilePath = args[0];
        String classTotalWordAmountFilePath = args[1];
        String uniqueWordAmountFilePath = args[2];

        SequenceFile.Reader reader = null;
        Text key = null;
        IntWritable value = null;
        double totalDiffWords = 0.0;
        try {
            reader = new SequenceFile.Reader(conf, SequenceFile.Reader.file(new Path(classTotalWordAmountFilePath)));
            key = (Text) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
            value = (IntWritable) ReflectionUtils.newInstance(reader.getValueClass(), conf);
            while (reader.next(key, value)) {
                classTotalWord.put(key.toString(), value.get() * 1.0);//得到每个类及类对应的文件数目
//                System.out.println(key.toString() + "\t" + classTotalWord.get(key.toString()));
            }
        } finally {
            IOUtils.closeStream(reader);
        }
        try {
            reader = new SequenceFile.Reader(conf, SequenceFile.Reader.file(new Path(uniqueWordAmountFilePath)));
            key = (Text) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
            value = (IntWritable) ReflectionUtils.newInstance(reader.getValueClass(), conf);
            while (reader.next(key, value)) {
                totalDiffWords += value.get();
            }
        } finally {
            IOUtils.closeStream(reader);
        }
        try {
            reader = new SequenceFile.Reader(conf, SequenceFile.Reader.file(new Path(classWordAmountFilePath)));
            key = (Text) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
            value = (IntWritable) ReflectionUtils.newInstance(reader.getValueClass(), conf);
            while (reader.next(key, value)) {
                String name = key.toString().split(":", 2)[0];
                wordsProbably.put(key.toString(), (value.get() + 1) / (classTotalWord.get(name) + totalDiffWords));
//                System.out.println(key.toString() + "\t" + (value.get() + 1) / (classTotalWord.get(name) + totalDiffWords));
            }
            // 对于同一个类别没有出现过的单词的概率一样，1/(classTotalWord.get(newKey.toString())+2)
            // 遍历类，每个类别中再加一个没有出现单词的概率，其格式为<class,probably>
            for (Map.Entry<String, Double> entry : classTotalWord.entrySet()) {
                wordsProbably.put(entry.getKey(), 1.0 / (classTotalWord.get(entry.getKey()) + totalDiffWords));
//                System.out.println(entry.getKey().toString() + "\t" + 1.0 / (classTotalWord.get(entry.getKey()) + totalDiffWords));
            }
        } finally {
            IOUtils.closeStream(reader);
        }
        return wordsProbably;
    }

    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();
        getPriorProbably(conf, args);
        getConditionProbably(conf, args);
        Job job = JobBuilder.parseInputAndOutput(this, conf, args);
        if (job == null) {
            return -1;
        }
        job.setMapperClass(CalculateProbabilityMapper.class);
        job.setReducerClass(CalculateProbabilityReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    /*
     * 第四个MapReduce进行贝叶斯测试
     * 输入:args[4],测试文件的路径,测试数据格式<<class:doc>,word1 word2 ...>
     *      HashMap<String,Double> classProbably先验概率
     *      HashMap<String,Double> wordsProbably条件概率
     * 输出:args[5],输出每一份文档经贝叶斯分类后所对应的类,格式为<doc,class>
     */
    static class CalculateProbabilityMapper extends Mapper<Text, Text, Text, Text> {
        private Text newKey = new Text();
        private Text newValue = new Text();

        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            String docID = key.toString().split(":", 2)[1];

            for (Map.Entry<String, Double> entry : classProbably.entrySet()) {//外层循环遍历所有类别
                String mykey = entry.getKey();
                newKey.set(docID);//新的键值的key为<文档名>
                double tempValue = Math.log(entry.getValue());//构建临时键值对的value为各概率相乘,转化为各概率取对数再相加
                StringTokenizer itr = new StringTokenizer(value.toString());
                while (itr.hasMoreTokens()) {//内层循环遍历一份测试文档中的所有单词
                    String tempkey = mykey + ":" + itr.nextToken();//构建临时键值对<class:word>,在wordsProbably表中查找对应的概率
                    if (wordsProbably.containsKey(tempkey)) {
                        //如果测试文档的单词在训练集中出现过，则直接加上之前计算的概率
                        tempValue += Math.log(wordsProbably.get(tempkey));
                    } else {//如果测试文档中出现了新单词则加上之前计算新单词概率
                        tempValue += Math.log(wordsProbably.get(mykey));
                    }
                }
                newValue.set(mykey + ":" + tempValue);///新的value为<类名:概率>,即<class:probably>
                context.write(newKey, newValue);//一份文档遍历在一个类中遍历完毕,则将结果写入文件,即<docID,<class:probably>>
//                System.out.println(newKey + "\t" + newValue);
            }
        }
    }

    static class CalculateProbabilityReducer extends Reducer<Text, Text, Text, Text> {
        Text newValue = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String tempClass = null;
            double tempProbably = -Double.MAX_VALUE;
            for (Text value : values) {//对于每份文档找出最大的概率所对应的类
                String valueInfo[] = value.toString().split(":");
                if (Double.parseDouble(valueInfo[1]) > tempProbably) {
                    tempClass = valueInfo[0];
                    tempProbably = Double.parseDouble(valueInfo[1]);
                }
            }
            newValue.set(tempClass);
            context.write(key, newValue);
            // System.out.println(key + "\t" + newValue);
        }
    }
}
