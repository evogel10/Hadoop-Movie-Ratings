package movieratings;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 *
 * @author Eric Vogel
 * October 2017
 * 
 * This MapReduce program takes in two separate compressed dataset files and joins
 * them together uses a reduce side join.
 */
public class MovieRatings {

    /**
     * @param args the command line arguments for input file/directory and output directory
     */
    public static void main(String[] args) {
        
        Job ratingsCountJob = null;
        
        Configuration conf = new Configuration();
        
        conf.setBoolean("mapred.output.compress", true);
        
        System.out.println ("======");
        try {
            ratingsCountJob = Job.getInstance(conf, "MovieRatings");
        } catch (IOException ex) {
            Logger.getLogger(MovieRatings.class.getName()).log(Level.SEVERE, null, ex);
            return;
        }
        
        // Specify the Input path
        try {
            FileInputFormat.addInputPath(ratingsCountJob, new Path(args[0]));
        } catch (IOException ex) {
            Logger.getLogger(MovieRatings.class.getName()).log(Level.SEVERE, null, ex);
            return;
        }
        
        // Set the Input Data Format
        ratingsCountJob.setInputFormatClass(TextInputFormat.class);
        
        // Set the Mapper, Reducer, Combiner and Partitioner Classes
        ratingsCountJob.setMapperClass(MovieRatingsMapper.class);
        ratingsCountJob.setReducerClass(MovieRatingsReducer.class);
//        ratingsCountJob.setCombinerClass(MovieRatingsCombiner.class);
        ratingsCountJob.setPartitionerClass(MovieRatingsPartitioner.class);
//        ratingsCountJob.setSortComparatorClass(MovieRatingsSortingComparator.class);
          
        // Set the Jar file 
        ratingsCountJob.setJarByClass(movieratings.MovieRatings.class);
        
        // Set the Output path
        FileOutputFormat.setOutputPath(ratingsCountJob, new Path(args[1]));
        
        // Set the Output Data Format
        ratingsCountJob.setOutputFormatClass(TextOutputFormat.class);
        
        // Set the Output Key and Value Class
        ratingsCountJob.setOutputKeyClass(MovieRatingsCompositeKey.class);
        ratingsCountJob.setOutputValueClass(Text.class);
        
        // Set the Number of Reducers
        ratingsCountJob.setNumReduceTasks(2);
        
        // Submit the job
        try {
            ratingsCountJob.waitForCompletion(true);
        } catch (IOException ex) {
            Logger.getLogger(MovieRatings.class.getName()).log(Level.SEVERE, null, ex);
        } catch (InterruptedException ex) {
            Logger.getLogger(MovieRatings.class.getName()).log(Level.SEVERE, null, ex);
        } catch (ClassNotFoundException ex) {
            Logger.getLogger(MovieRatings.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
}