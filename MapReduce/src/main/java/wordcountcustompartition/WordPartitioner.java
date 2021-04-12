package wordcountcustompartition;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class WordPartitioner extends Partitioner<Text, IntWritable> {

    @Override
    public int getPartition(Text text, IntWritable intWritable, int numPartitions) {
        String line = text.toString();
        if(line.charAt(0) >= 'a' && line.charAt(0) <= 'p') return 0;
        else return 1;
    }
}
