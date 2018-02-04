package movieratings;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author Eric Vogel
 * 
 */

public class MovieRatingsReducer extends Reducer<MovieRatingsCompositeKey, Text, NullWritable, Text> {
    
    private Text joinedText = new Text();
    private StringBuilder data = new StringBuilder();
    private StringBuilder item = new StringBuilder();
    private NullWritable nullKey = NullWritable.get(); 
    private ArrayList<Integer> ratings = new ArrayList<Integer>();
    private ArrayList<String> users = new ArrayList<String>();

    @Override
    protected void reduce(MovieRatingsCompositeKey key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        
        Integer numRatings = 0;
        Integer uniqUsers = 0;
        
        switch (key.getsourceIndex()) {
            case 1:
                for (Text value : values) {
                    String line = value.toString();
                    // Split line on commas
                    String newLine[] = line.split(",");
                    users.add(newLine[0]);
                    ratings.add(Integer.parseInt(newLine[1]));
                                       
                }
                
                Double total = 0.0;
                Double avg = 0.0;
                
                // Average rating
                for(int i = 0; i < ratings.size(); i++)
                    {
                        total += ratings.get(i);
                        avg = (double)total / (double)ratings.size();
                    }
                
                Set<String> uniqueUsers = new HashSet<String>(users);
                
                uniqUsers = uniqueUsers.size();
                numRatings = ratings.size();
                
                data.append(String.format("%.1f", avg)).append(",");
                data.append(uniqUsers.toString()).append(",");
                data.append(numRatings.toString());

                users.clear();
                uniqueUsers.clear();
                uniqUsers = 0;
                ratings.clear();
                numRatings = 0;
                break;

            case 2:
                for (Text value : values) {
                    item.append(value.toString()).append(",");
                }   
;
                item.append(data.toString());
                joinedText.set(item.toString());
                context.write(nullKey, joinedText);
                item.setLength(0);
                data.setLength(0);
                
                Counter counter = context.getCounter(MovieRatingsCounter.Total_Unique_Movies);
                counter.increment(1);

                break;
            default:
                break;    
                
        }     
    }
}