import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @Author: xu.dm
 * @Date: 2019/2/21 12:44
 * @Description:
 */
public class DataSecondarySort extends Configured implements Tool {
    /**
     * Execute the command with the given arguments.
     *
     * @param args command specific arguments.
     * @return exit code.
     * @throws Exception
     */
    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();

        Job job = Job.getInstance(conf,"Secondary Sort");
//        conf.set("mapreduce.job.ubertask.enable","true");

        if(conf==null){
            return -1;
        }

        job.setJarByClass(DataSecondarySort.class);
        job.setMapperClass(DataMapper.class);
        job.setPartitionerClass(FirstPartitioner.class);
        job.setSortComparatorClass(KeyComparator.class);
        //决定如何分组
//        job.setGroupingComparatorClass(GroupComparator.class);
        job.setReducerClass(DataReducer.class);
//        job.setNumReduceTasks(2);

        job.setOutputKeyClass(IntPair.class);
        //如果只求最大数，前面的mapper，reducer和这里的输出都可以设置成NullWritable
        job.setOutputValueClass(IntWritable.class);


        FileInputFormat.addInputPath(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        Path outPath = new Path(args[1]);
        FileSystem fileSystem = outPath.getFileSystem(conf);
        //删除输出路径
        if(fileSystem.exists(outPath))
        {
            fileSystem.delete(outPath,true);
        }

        return job.waitForCompletion(true) ? 0:1;
    }

    public static void main(String[] args) throws Exception{
        int exitCode = ToolRunner.run(new DataSecondarySort(),args);
        System.exit(exitCode);
    }
}
