import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DeviceMapper extends Mapper<LongWritable, Text, Text, Text> {
    
    private Text deviceKey = new Text();
    private Text valueOut = new Text();
    
    @Override
    public void map(LongWritable key, Text value, Context context) 
            throws IOException, InterruptedException {
        
        if (key.get() == 0) return;
        
        String line = value.toString();
        String[] fields = line.split(",");
        
        // CSV: timestamp,publisher_id,advertiser_id,campaign_id,
        //      device_type,user_id,region,clicked,converted,bid_price,revenue,is_suspicious
        
        if (fields.length >= 11) {
            try {
                String deviceType = fields[4].trim();
                String clicked = fields[7].trim();
                String converted = fields[8].trim();
                String bidPrice = fields[9].trim();
                String revenue = fields[10].trim();
                
                // Emit: device_type -> "clicked,converted,bid_price,revenue"
                deviceKey.set(deviceType);
                valueOut.set(clicked + "," + converted + "," + bidPrice + "," + revenue);
                
                context.write(deviceKey, valueOut);
                
            } catch (Exception e) {
                System.err.println("Error: " + e.getMessage());
            }
        }
    }
}