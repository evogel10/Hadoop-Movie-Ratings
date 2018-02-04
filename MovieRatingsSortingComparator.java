package movieratings;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 *
 * @author hdadmin
 */
public class MovieRatingsSortingComparator extends WritableComparator{
    
    protected MovieRatingsSortingComparator() {
        super(MovieRatingsCompositeKey.class, true);
    }
    
    @Override
    public int compare(WritableComparable w1, WritableComparable w2) {
        // Sort on all attributes of composite key
        MovieRatingsCompositeKey key1 = (MovieRatingsCompositeKey) w1;
        MovieRatingsCompositeKey key2 = (MovieRatingsCompositeKey) w2;
        
        int cmpResult = key1.getjoinKey().compareTo(key2.getjoinKey());
        // same joinKey
        if (cmpResult == 0) {
            return Double.compare(key1.getsourceIndex(), key2.getsourceIndex());
        }
        return cmpResult;
        
    }
    
}
