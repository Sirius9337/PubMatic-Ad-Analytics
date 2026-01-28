import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CampaignReducer extends Reducer<Text, Text, Text, Text> {
    
    private Text result = new Text();
    
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) 
            throws IOException, InterruptedException {
        
        String advertiserId = "";
        long totalImpressions = 0;
        long totalClicks = 0;
        long totalConversions = 0;
        double totalSpend = 0.0;
        double totalRevenue = 0.0;
        
        for (Text value : values) {
            String[] parts = value.toString().split(",");
            
            if (parts.length == 5) {
                if (advertiserId.isEmpty()) {
                    advertiserId = parts[0];
                }
                totalImpressions++;
                totalClicks += Integer.parseInt(parts[1]);
                totalConversions += Integer.parseInt(parts[2]);
                totalSpend += Double.parseDouble(parts[3]);
                totalRevenue += Double.parseDouble(parts[4]);
            }
        }
        
        // Calculate ROI metrics
        double ctr = (totalImpressions > 0) ? 
            (totalClicks * 100.0 / totalImpressions) : 0;
        double conversionRate = (totalClicks > 0) ? 
            (totalConversions * 100.0 / totalClicks) : 0;
        double costPerImpression = (totalImpressions > 0) ? 
            (totalSpend / totalImpressions) : 0;
        double costPerClick = (totalClicks > 0) ? 
            (totalSpend / totalClicks) : 0;
        double costPerConversion = (totalConversions > 0) ? 
            (totalSpend / totalConversions) : 0;
        double roi = (totalSpend > 0) ? 
            ((totalRevenue - totalSpend) / totalSpend * 100) : 0;
        
        String output = String.format("%s,%d,%d,%d,%.4f,%.4f,%.2f,%.2f,%.4f,%.4f,%.4f,%.2f", 
            advertiserId,
            totalImpressions,
            totalClicks,
            totalConversions,
            totalSpend,
            totalRevenue,
            ctr,
            conversionRate,
            costPerImpression,
            costPerClick,
            costPerConversion,
            roi);
        
        result.set(output);
        context.write(key, result);
    }
}