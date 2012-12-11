package fudan.LPA.Initialize;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

@SuppressWarnings("deprecation")
public class WikiLPAInitReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
    
    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
    	
    	String linkto = "";
    	String labels = "";
    	
    	boolean first = true;
    	while(values.hasNext()) {
        	String list = values.next().toString();
        	
        	if (list.startsWith("<c>")) {
        		// list is categorie
        		labels = list+"</c>";
        	} else {
        		// list is page
        		String[] pages = list.split(",");
        		for (String page : pages) {
        			if (!linkto.contains(page)) {
        				if (!first)
        					linkto += ",";
        				linkto += page;
        				first = false;
        			}
        		}
        	}
        }
    	
    	if (linkto == null || linkto.isEmpty())
    		return;
    	if (labels == null || labels.isEmpty())
    		return;

		// output: <page> <label1,label2,...> <linkto1,linkto2,...> 
		output.collect(key, new Text(labels+"\t"+linkto));
		
    }
}
