import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Author: xu.dm
 * @Date: 2019/2/21 10:43
 * @Description:
 */
public class DataMapper extends Mapper<LongWritable,Text,IntPair,IntWritable> {
    private RecordParser parser = new RecordParser();
    /**
     * Called once for each key/value pair in the input split. Most applications
     * should override this, but the default is the identity function.
     *
     * @param key
     * @param value
     * @param context
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        parser.parse(value.toString());
        if(parser.isValid()){
            context.write(new IntPair(parser.getYear(),parser.getData()),new IntWritable(1));
            context.getCounter("MapValidData","dataCounter").increment(1);
        }
    }
}
