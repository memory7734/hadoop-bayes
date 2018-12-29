import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Random;

/**
 * ConvertToSequenceFile:
 * 序列化小文件，格式为<<dir:doc>,contents>
 * eg:I01002:484619newsML.txt	national oilseed processorsassociation weekly soybean crushings reporting members...
 */

public class ConvertToSequenceFile {

    /**
     * 读取File的内容 将一个文件合并成空格分隔的一行
     *
     * @param file txt文件
     * @return 转换后的结果
     * @throws IOException
     */
    private static String fileToString(File file) throws IOException {
        BufferedReader reader = new BufferedReader(new FileReader(file));
        String line;
        StringBuilder result = new StringBuilder();
        while ((line = reader.readLine()) != null)
            // 过滤掉以数字开头的词
            if (line.matches("[a-zA-Z]+"))
                // 单词之间以空格符隔开
                result.append(line).append(" ");
        reader.close();
        return result.toString();
    }

    /**
     * 将Country或者Industry文件夹下面的文件全部序列化
     *
     * @param args args[0] 文件夹路径
     * @throws IOException
     */
    public static void main(String[] args) throws IOException {
        File[] dirs = new File(args[0]).listFiles();
        Text key = new Text();
        Text value = new Text();
        Random random = new Random();
        Configuration configuration = new Configuration();
        FileSystem system = FileSystem.get(configuration);
        Path trainSet = new Path(args[1]);
        Path testSet = new Path(args[2]);
        if (system.exists(trainSet)) {
            system.delete(trainSet, true);
        }
        if (system.exists(testSet)) {
            system.delete(testSet, true);
        }
        SequenceFile.Writer trainSetWriter = null;
        SequenceFile.Writer testSetWriter = null;
        SequenceFile.Writer.Option trainSetPath = SequenceFile.Writer.file(new Path(args[1]));
        SequenceFile.Writer.Option testSetPath = SequenceFile.Writer.file(new Path(args[2]));
        SequenceFile.Writer.Option keyClass = SequenceFile.Writer.keyClass(key.getClass());
        SequenceFile.Writer.Option valueClass = SequenceFile.Writer.valueClass(value.getClass());
        try {
            trainSetWriter = SequenceFile.createWriter(new Configuration(), trainSetPath, keyClass, valueClass);
            testSetWriter = SequenceFile.createWriter(new Configuration(), testSetPath, keyClass, valueClass);
            assert dirs != null;
            for (File dir : dirs) {
                File[] files = dir.listFiles();
                // 如果文件个数小于300个则不作为数据集使用，跳过
                if (files == null || files.length <= 300) continue;
                for (File file : files) {
                    // key：目录名+":"+文件名
                    key.set(dir.getName() + ":" + file.getName());
                    // value：文件内容
                    value.set(fileToString(file));
                    if (random.nextInt(100) < 75)
                        trainSetWriter.append(key, value);
                    else
                        testSetWriter.append(key, value);
                    // System.out.println(key + "\t" + value);
                }
            }
        } finally {
            IOUtils.closeStream(trainSetWriter);
            IOUtils.closeStream(testSetWriter);
        }
    }

}
