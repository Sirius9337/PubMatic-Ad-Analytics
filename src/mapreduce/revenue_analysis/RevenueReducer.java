import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class RevenueReducer extends Reducer<Text, Text, Text, Text> {
    
    private Text result = new Text();
    
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) 
            throws IOException, InterruptedException {
        
        long totalImpressions = 0;
        long totalClicks = 0;
        long totalConversions = 0;
        double totalRevenue = 0.0;
        
        // Aggregate all values for this publisher
        for (Text value : values) {
            String[] parts = value.toString().split(",");
            
            if (parts.length == 3) {
                totalImpressions++;
                totalClicks += Integer.parseInt(parts[0]);
                totalConversions += Integer.parseInt(parts[1]);
                totalRevenue += Double.parseDouble(parts[2]);
            }
        }
        
        // Calculate metrics
        double ctr = (totalImpressions > 0) ? 
            (totalClicks * 100.0 / totalImpressions) : 0;
        double conversionRate = (totalClicks > 0) ? 
            (totalConversions * 100.0 / totalClicks) : 0;
        double avgRevenue = (totalImpressions > 0) ? 
            (totalRevenue / totalImpressions) : 0;
        
        // Output format: impressions,clicks,conversions,total_revenue,ctr,cvr,avg_revenue
        String output = String.format("%d,%d,%d,%.4f,%.2f,%.2f,%.4f", 
            totalImpressions, 
            totalClicks, 
            totalConversions, 
            totalRevenue, 
            ctr, 
            conversionRate,
            avgRevenue);
        
        result.set(output);
        context.write(key, result);
    }
}