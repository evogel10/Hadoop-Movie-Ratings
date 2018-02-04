package movieratings;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class MovieRatingsPartitioner extends Partitioner<MovieRatingsCompositeKey, Text> {
    
    @Override
    public int getPartition(MovieRatingsCompositeKey key, Text value, int numReducers) {
        return (key.getjoinKey().hashCode() % numReducers);
    }
}
