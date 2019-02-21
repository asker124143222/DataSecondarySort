import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @Author: xu.dm
 * @Date: 2019/2/21 11:56
 * @Description:根据key进行分区，确保同一个key.first进入相同的分区
 * Partitioner的泛型类型和mapper输出的一致
 */
public class FirstPartitioner extends Partitioner<IntPair,IntWritable> {
    /**
     * Get the partition number for a given key (hence record) given the total
     * number of partitions i.e. number of reduce-tasks for the job.
     * <p>
     * <p>Typically a hash function on a all or a subset of the key.</p>
     *
     * @param key       the key to be partioned.
     * @param value  the entry value.
     * @param numPartitions the total number of partitions.
     * @return the partition number for the <code>key</code>.
     */
    @Override
    public int getPartition(IntPair key, IntWritable value, int numPartitions) {
        return Math.abs(key.getFirst() * 127) % numPartitions;
    }
}
