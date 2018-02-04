package movieratings;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 *
 * @author hdadmin
 */
public class MovieRatingsGroupingComparator extends WritableComparator {
    
    protected MovieRatingsGroupingComparator() {
        super(MovieRatingsCompositeKey.class, true);
    }
    
    @Override
    public int compare(WritableComparable w1, WritableComparable w2){
        // The grouping comparator is the joinKey (item_id)
        MovieRatingsCompositeKey key1 = (MovieRatingsCompositeKey) w1;
        MovieRatingsCompositeKey key2 = (MovieRatingsCompositeKey) w2;
        return key1.getjoinKey().compareTo(key2.getjoinKey());
    }
    
}
