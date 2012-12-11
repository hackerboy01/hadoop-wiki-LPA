package fudan.LPA.result;

import java.io.IOException;
import java.nio.charset.CharacterCodingException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

@SuppressWarnings("deprecation")
public class LPAResultMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
    
    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter arg3) throws IOException {
        String[] pageAndRank = getPageAndLabel(key, value);
        
        Text page = new Text(pageAndRank[0]);
        Text label = new Text(pageAndRank[1]);
        
        output.collect(label, page);
    }
    
    private String[] getPageAndLabel(LongWritable key, Text value) throws CharacterCodingException {
        String[] pageAndLabel = new String[2];
        int tabPageIndex = value.find("\t");
        int labelStartIdx = value.find("<c>", tabPageIndex + 1) + 3;
        int labelEndIndex = value.find("</c>", labelStartIdx);
        
        pageAndLabel[0] = Text.decode(value.getBytes(), 0, tabPageIndex);
        pageAndLabel[1] = Text.decode(value.getBytes(), labelStartIdx, labelEndIndex-labelStartIdx);
        
        return pageAndLabel;
    }
    
}
