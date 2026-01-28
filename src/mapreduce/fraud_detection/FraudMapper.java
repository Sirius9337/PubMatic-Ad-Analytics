import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FraudMapper extends Mapper<LongWritable, Text, Text, Text> {
    
    private Text userKey = new Text();
    private Text valueOut = new Text();
    
    @Override
    public void map(LongWritable key, Text value, Context context) 
            throws IOException, InterruptedException {
        
        // Skip header
        if (key.get() == 0) {
            return;
        }
        
        String line = value.toString();
        String[] fields = line.split(",");
        
        // CSV: timestamp,publisher_id,advertiser_id,campaign_id,
        //      device_type,user_id,region,clicked,converted,bid_price,revenue,is_suspicious
        
        if (fields.length >= 12) {
            try {
                String userId = fields[5].trim();
                String publisherId = fields[1].trim();
                String clicked = fields[7].trim();
                String converted = fields[8].trim();
                String isSuspicious = fields[11].trim();
                
                // Emit: user_id -> "publisher_id,clicked,converted,is_suspicious"
                userKey.set(userId);
                valueOut.set(publisherId + "," + clicked + "," + converted + "," + isSuspicious);
                
                context.write(userKey, valueOut);
                
            } catch (Exception e) {
                System.err.println("Error processing line: " + line);
            }
        }
    }
}