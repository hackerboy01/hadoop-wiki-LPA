package fudan.LPA.Calculate;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

@SuppressWarnings("deprecation")
public class LPACalculateReducer extends MapReduceBase implements
		Reducer<Text, Text, Text, Text> {

	public void reduce(Text page, Iterator<Text> values,
			OutputCollector<Text, Text> out, Reporter reporter)
			throws IOException {

		String linktoList = "";
		HashMap<String, Integer> labelRank = new HashMap<String, Integer>();
		String pageLabel = "";

		String nodeString = "";
		while (values.hasNext()) {
			nodeString = values.next().toString();

			int labelTabIndex = nodeString.indexOf("<c>");

			if (labelTabIndex == -1) {
				// nodeString is linkto-list
				linktoList = nodeString;
			} else {
				// nodeString is pageWithLabel
				String[] labels = nodeString.substring(labelTabIndex+3).split("\t");
				for (String label : labels) {
					if (labelRank.containsKey(label))
						labelRank.put(label, labelRank.get(label)+1);
					else
						labelRank.put(label, 1);
				}
			}
		}
		
		if (linktoList == null || linktoList.isEmpty())
			return;
		if (labelRank.isEmpty())
			return;
		
		int maxNum = 0;
		Iterator<Entry<String, Integer>> itr = labelRank.entrySet().iterator();
		while (itr.hasNext()) {
			Entry<String, Integer> et = (Entry<String, Integer>) itr.next();
			String key = et.getKey();
			int value = et.getValue();
			if (value > maxNum) {
				maxNum = value;
				pageLabel = key;
			}
		}
		
		out.collect(page, new Text("<c>"+pageLabel+"</c>"+"\t"+linktoList));
	}
}
