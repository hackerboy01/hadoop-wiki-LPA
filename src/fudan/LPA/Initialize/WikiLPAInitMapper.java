package fudan.LPA.Initialize;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

@SuppressWarnings("deprecation")
public class WikiLPAInitMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text>{

	public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
		int tabIndex = value.find("\t");
		
		String page = Text.decode(value.getBytes(), 0, tabIndex);
		String list = Text.decode(value.getBytes(), tabIndex+1, value.getLength()-(tabIndex+1));
		
		if (list.startsWith("<c>")) {
			// list is categorie
			output.collect(new Text(page), new Text(list));
		} else {
			// list is linkin/linkout
			if (list.startsWith("|"))
				list = list.substring(1);
			String[] neighbors = list.split(",");
			if (neighbors.length == 0)
				return;
			for (String neighbor : neighbors) {
				output.collect(new Text(neighbor), new Text(page));
			}
		}
    }
	
}