package movieratings;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.log4j.Logger;

/**
 *
 * @author Eric Vogel
 * 
 */
//public class MovieRatingsMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

public class MovieRatingsMapper extends Mapper<LongWritable, Text, MovieRatingsCompositeKey, Text> {
    
    Logger logger = Logger.getLogger(MovieRatingsMapper.class);
    IntWritable one = new IntWritable(1);
    Text item_id = new Text();
    
    //NEW CODE HERE
    MovieRatingsCompositeKey compKey = new MovieRatingsCompositeKey();
    Text txtValue = new Text("");
    int intSrcIndex = 0;
    StringBuilder strMapValueBuilder = new StringBuilder("");
    List<Integer> lstRequiredAttribList = new ArrayList<Integer>();
    
    @Override
    protected void setup(Mapper.Context context) throws IOException, InterruptedException {

        String fName = ((FileSplit) context.getInputSplit()).getPath().getName();
        
        if (fName.contains("data")) {
            intSrcIndex = 1;
            lstRequiredAttribList.add(0); // user_id
            lstRequiredAttribList.add(2); // rating
            
        }
        else {
            intSrcIndex = 2;
            lstRequiredAttribList.add(0); // movie_id
            lstRequiredAttribList.add(1); // movie_title
            lstRequiredAttribList.add(2); // release_date
            lstRequiredAttribList.add(4); // IMDB_URL
        }
    }
    
    private String buildMapValue(String arrEntityAttributesList[]) {
        
        strMapValueBuilder.setLength(0); // Initialize
        
        // Build list of attributes to output based on source - u.data/u.item
        for (int i = 0; i < arrEntityAttributesList.length; i++) {
            // If the field is in the list of required output, append to stringbuilder
            if (lstRequiredAttribList.contains(i)) {
                strMapValueBuilder.append(arrEntityAttributesList[i]).append(",");
            }
        }
        if (strMapValueBuilder.length() > 0) {
            // Drop last comma
            strMapValueBuilder.setLength(strMapValueBuilder.length() - 1);
        }
        
        return strMapValueBuilder.toString();
    }
    
    
    @Override
    protected void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException {
        
        if (value.toString().length() > 0) {
            if (intSrcIndex == 1) {
                String line = value.toString();
                line = line.replaceAll("[,.;?\"!\\/]", "");
                // Split line on tabs
                String arrEntityAttributes[] = line.split("\\t");
                
                compKey.setjoinKey(arrEntityAttributes[1].toString());
                compKey.setsourceIndex(intSrcIndex);
                txtValue.set(buildMapValue(arrEntityAttributes));
                
                context.write(compKey, txtValue);
                
            }
            else {
                String line = value.toString();
                line = line.replaceAll("[,.;?\"!\\/]", "");
                // Split line on tabs
                String arrEntityAttributes[] = line.split("\\|");
                
                compKey.setjoinKey(arrEntityAttributes[0].toString());
                compKey.setsourceIndex(intSrcIndex);
                txtValue.set(buildMapValue(arrEntityAttributes));
                
                context.write(compKey, txtValue);
                
            }
        }
        Counter counter = context.getCounter(MovieRatingsCounter.Total_Map_Records);
        counter.increment(1);
    }  
}
