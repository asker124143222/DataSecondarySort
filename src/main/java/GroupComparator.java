import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * @Author: xu.dm
 * @Date: 2019/2/21 12:16
 * @Description: 分组比较器，确保同一个年份的数据在同一个组里
 * 这个是本例最特殊的地方，之前key比较器使得key值中的年份升序，数据降序排列。
 * 那么这个分组比较器只按年进行比较，意味着，[1990,100]和[1990,00]会被认为是相同的分组，
 * 而，reduce阶段，相同的KEY只取第一个，哦也，这个时候，reduce阶段后，年份中最大的数据就被保存下来，其他数据都被kickout
 * 所以，用这种方式变相的达到取最大值得效果。
 */
public class GroupComparator extends WritableComparator {
    public GroupComparator() {
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
        return IntPair.compare(p1.getFirst(),p2.getFirst());
    }
}
