import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * @Author: xu.dm
 * @Date: 2019/2/21 11:59
 * @Description: key比较器
 * 对IntPair的first升序，second降序，在mapper排序的时候被应用
 * 最终同样年份的数据第一条是最大的。
 */
public class KeyComparator extends WritableComparator {
    protected KeyComparator() {
        super(IntPair.class,true);
    }

    /**
     * Compare two WritableComparables.
     * <p>
     * <p> The default implementation uses the natural ordering, calling {@link
     * Comparable#compareTo(Object)}.
     *
     * @param a
     * @param b
     */
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        IntPair p1=(IntPair)a;
        IntPair p2=(IntPair)b;
        int result = IntPair.compare(p1.getFirst(),p2.getFirst());
        if(result==0){
            result = -IntPair.compare(p1.getSecond(),p2.getSecond()); //前面加一个减号求反
        }
        return result;
    }
}
