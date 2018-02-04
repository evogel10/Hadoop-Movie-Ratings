package movieratings;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author hdadmin
 */
public class MovieRatingsCombiner extends Reducer<MovieRatingsCompositeKey, Text, MovieRatingsCompositeKey, Text> {
    
//    private Text joinedText = new Text();
    private StringBuilder data = new StringBuilder();
    private StringBuilder item = new StringBuilder();
//    private NullWritable nullKey = NullWritable.get();
    
    private ArrayList<Integer> ratings = new ArrayList<Integer>();
    private ArrayList<String> users = new ArrayList<String>();
    
    
    //NEW CODE
    private Text joinedText = new Text();
    private StringBuilder builder = new StringBuilder();
    private NullWritable nullKey = NullWritable.get();
    MovieRatingsCompositeKey compKey = new MovieRatingsCompositeKey();
    Text txtValue = new Text("");

    @Override
    protected void reduce(MovieRatingsCompositeKey key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        
 
        builder.append(key.getjoinKey()).append(",");
        for (Text value : values) {
            builder.append(value.toString()).append(",");
        }
        builder.setLength(builder.length()-1);
        joinedText.set(builder.toString());
        context.write(key, joinedText);
        builder.setLength(0);
   
    }
    
}
