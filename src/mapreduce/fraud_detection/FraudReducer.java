import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.util.HashSet;

public class FraudReducer extends Reducer<Text, Text, Text, Text> {
    
    private Text result = new Text();
    
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) 
            throws IOException, InterruptedException {
        
        int totalImpressions = 0;
        int totalClicks = 0;
        int totalConversions = 0;
        int suspiciousFlags = 0;
        HashSet<String> publishers = new HashSet<>();
        
        // Aggregate user behavior
        for (Text value : values) {
            String[] parts = value.toString().split(",");
            
            if (parts.length == 4) {
                publishers.add(parts[0]);
                totalImpressions++;
                totalClicks += Integer.parseInt(parts[1]);
                totalConversions += Integer.parseInt(parts[2]);
                suspiciousFlags += Integer.parseInt(parts[3]);
            }
        }
        
        // Fraud detection logic
        boolean isFraudulent = false;
        String fraudReason = "NORMAL";
        
        // Pattern 1: High click rate (>10 clicks) but zero conversions
        if (totalClicks > 10 && totalConversions == 0) {
            isFraudulent = true;
            fraudReason = "HIGH_CLICKS_NO_CONVERSION";
        }
        
        // Pattern 2: Very high impression count from single user (>50)
        if (totalImpressions > 50) {
            isFraudulent = true;
            fraudReason = "EXCESSIVE_IMPRESSIONS";
        }
        
        // Pattern 3: Multiple suspicious flags
        if (suspiciousFlags > 3) {
            isFraudulent = true;
            fraudReason = "MULTIPLE_SUSPICIOUS_FLAGS";
        }
        
        // Pattern 4: User active across many publishers (>10) with many clicks
        if (publishers.size() > 10 && totalClicks > 15) {
            isFraudulent = true;
            fraudReason = "SUSPICIOUS_MULTI_PUBLISHER";
        }
        
        // Only output potentially fraudulent users
        if (isFraudulent) {
            double clickRate = (totalImpressions > 0) ? 
                (totalClicks * 100.0 / totalImpressions) : 0;
            
            String output = String.format("%d,%d,%d,%d,%.2f,%s", 
                totalImpressions,
                totalClicks,
                totalConversions,
                publishers.size(),
                clickRate,
                fraudReason);
            
            result.set(output);
            context.write(key, result);
        }
    }
}