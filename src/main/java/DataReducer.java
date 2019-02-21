import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Author: xu.dm
 * @Date: 2019/2/21 11:54
 * @Description:
 */
public class DataReducer extends Reducer<IntPair,IntWritable,IntPair,IntWritable> {
    /**
     * This method is called once for each key. Most applications will define
     * their reduce class by overriding this method. The default implementation
     * is an identity function.
     *
     * @param key
     * @param values
     * @param context
     */
    @Override
    protected void reduce(IntPair key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        //因为分组器[1990,100]和[1990,00]会被认为是相同的分组
        //这里的计数就会混淆。如果需要年份下各数据的正确的计数结果，则需要注销分组器
        for(IntWritable val:values){
            sum+=val.get();
        }

        context.write(key,new IntWritable(sum));
    }
}
