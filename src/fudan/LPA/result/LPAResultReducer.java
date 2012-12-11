package fudan.LPA.result;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

@SuppressWarnings("deprecation")
public class LPAResultReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
    
    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
    	
    	String pages = "";
    	
    	boolean first = true;
    	while(values.hasNext()) {
        	String page = values.next().toString();
        	
			if (!first)
				pages += ",";
			
			pages += page;
			
			first = false;
        }
    	
    	if (pages == "")
    		return;

		output.collect(key, new Text(pages));
		
    }
}
