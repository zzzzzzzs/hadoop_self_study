package expandcase.friends;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TwoShareFriendsDriver {

    public static void main(String[] args) throws Exception {
        // 输入输出路径需要根据自己电脑上实际的输入输出路径设置
        args = new String[]{"./testFile/inputoutputfriends", "./testFile/output4"};
        // 1 获取job对象
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        // 2 指定jar包运行的路径
        job.setJarByClass(TwoShareFriendsDriver.class);

        // 3 指定map/reduce使用的类
        job.setMapperClass(TwoShareFriendsMapper.class);
        job.setReducerClass(TwoShareFriendsReducer.class);

        // 4 指定map输出的数据类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // 5 指定最终输出的数据类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // 6 指定job的输入原始所在目录
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 7 提交
        boolean result = job.waitForCompletion(true);
        System.exit(result?0:1);
    }
}