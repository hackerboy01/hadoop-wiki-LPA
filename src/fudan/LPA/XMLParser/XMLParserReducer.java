package fudan.LPA.XMLParser;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

@SuppressWarnings("deprecation")
public class XMLParserReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
    
    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        String categorie = "<c>";

        boolean first = true;
        while(values.hasNext()) {
        	if(!first)
        		categorie += "\t";
        	
            categorie += values.next().toString();
            
            first = false;
        }
        
        // output: <page> <category1,category2,...>
        output.collect(key, new Text(categorie));
    }
}
