package fudan.LPA.Calculate;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

@SuppressWarnings("deprecation")
public class LPACalculateMapper extends MapReduceBase implements
		Mapper<LongWritable, Text, Text, Text> {

	public void map(LongWritable key, Text value,
			OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {
		int pageTabIndex = value.find("\t");
		int labelStartIndex = value.find("<c>", pageTabIndex + 1);
		int labelEndIndex = value.find("</c>", labelStartIndex + 3);

		if (pageTabIndex == -1 || labelStartIndex == -1 || labelEndIndex == -1)
			return;

		String page = Text.decode(value.getBytes(), 0, pageTabIndex);
		String pageWithLabel = Text.decode(value.getBytes(), 0, labelEndIndex);
		String linktoList = Text.decode(value.getBytes(), labelEndIndex + 4,
				value.getLength() - (labelEndIndex + 4));

		String[] links = linktoList.split(",");
		if (links.length == 0)
			return;

		for (String linkin : links) {
			output.collect(new Text(linkin), new Text(pageWithLabel));
		}

		// Put the original links of the page for the reduce output
		output.collect(new Text(page), new Text(linktoList));
	}
}
