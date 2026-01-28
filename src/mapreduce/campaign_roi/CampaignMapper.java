import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CampaignMapper extends Mapper<LongWritable, Text, Text, Text> {
    
    private Text campaignKey = new Text();
    private Text valueOut = new Text();
    
    @Override
    public void map(LongWritable key, Text value, Context context) 
            throws IOException, InterruptedException {
        
        if (key.get() == 0) return;
        
        String line = value.toString();
        String[] fields = line.split(",");
        
        if (fields.length >= 11) {
            try {
                String campaignId = fields[3].trim();
                String advertiserId = fields[2].trim();
                String clicked = fields[7].trim();
                String converted = fields[8].trim();
                String bidPrice = fields[9].trim();
                String revenue = fields[10].trim();
                
                // Emit: campaign_id -> "advertiser_id,clicked,converted,bid_price,revenue"
                campaignKey.set(campaignId);
                valueOut.set(advertiserId + "," + clicked + "," + converted + "," + bidPrice + "," + revenue);
                
                context.write(campaignKey, valueOut);
                
            } catch (Exception e) {
                System.err.println("Error: " + e.getMessage());
            }
        }
    }
}