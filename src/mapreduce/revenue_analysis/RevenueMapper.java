import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class RevenueMapper extends Mapper<LongWritable, Text, Text, Text> {
    
    private Text publisherKey = new Text();
    private Text valueOut = new Text();
    
    @Override
    public void map(LongWritable key, Text value, Context context) 
            throws IOException, InterruptedException {
        
        // Skip header line
        if (key.get() == 0) {
            return;
        }
        
        String line = value.toString();
        String[] fields = line.split(",");
        
        // CSV format: timestamp,publisher_id,advertiser_id,campaign_id,
        //             device_type,user_id,region,clicked,converted,bid_price,revenue,is_suspicious
        
        if (fields.length >= 11) {
            try {
                String publisherId = fields[1].trim();
                String clicked = fields[7].trim();
                String converted = fields[8].trim();
                String revenue = fields[10].trim();
                
                // Emit: publisher_id -> "clicked,converted,revenue"
                publisherKey.set(publisherId);
                valueOut.set(clicked + "," + converted + "," + revenue);
                
                context.write(publisherKey, valueOut);
                
            } catch (Exception e) {
                // Skip malformed lines
                System.err.println("Error processing line: " + line);
            }
        }
    }
}