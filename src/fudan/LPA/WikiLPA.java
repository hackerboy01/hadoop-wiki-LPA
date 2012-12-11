package fudan.LPA;

import java.io.IOException;
import java.text.DecimalFormat;
import java.text.NumberFormat;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

import fudan.LPA.Calculate.LPACalculateMapper;
import fudan.LPA.Calculate.LPACalculateReducer;
import fudan.LPA.Initialize.WikiLPAInitMapper;
import fudan.LPA.Initialize.WikiLPAInitReducer;
import fudan.LPA.XMLParser.XMLInputFormat;
import fudan.LPA.XMLParser.XMLParserMapper;
import fudan.LPA.XMLParser.XMLParserReducer;
import fudan.LPA.result.LPAResultMapper;
import fudan.LPA.result.LPAResultReducer;

@SuppressWarnings("deprecation")
public class WikiLPA {

	private static NumberFormat nf = new DecimalFormat("00");
	private static int iterations = 5;

	/**
	 * @param args
	 */    
    public static void main(String[] args) throws Exception {
        WikiLPA pageLPA = new WikiLPA();
        
        // get categorie
        pageLPA.runXmlParsing(args[0], "wiki/categorie");
        
        // initialization
        pageLPA.initialize("wiki/categorie", "wiki/linkout", "wiki/linkin", "wiki/LPA/iter00");
        
        if (args.length == 2)
        	iterations = Integer.parseInt(args[1]);
        int runs = 0;
        for (; runs < iterations; runs++) {
        	pageLPA.runLPA("wiki/LPA/iter"+nf.format(runs), "wiki/LPA/iter"+nf.format(runs + 1));
        }
        
        pageLPA.getResult("wiki/LPA/iter"+nf.format(runs), "wiki/LPA/result");
    }

	public void runXmlParsing(String inputPath, String outputPath) throws IOException {
        JobConf conf = new JobConf(WikiLPA.class);
        
        conf.set(XMLInputFormat.START_TAG_KEY, "<page>");
        conf.set(XMLInputFormat.END_TAG_KEY, "</page>");
        
        // Input / Mapper
        FileInputFormat.setInputPaths(conf, new Path(inputPath));
        conf.setInputFormat(XMLInputFormat.class);
        conf.setMapperClass(XMLParserMapper.class);
        
        // Output / Reducer
        FileOutputFormat.setOutputPath(conf, new Path(outputPath));
        conf.setOutputFormat(TextOutputFormat.class);
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);
        conf.setReducerClass(XMLParserReducer.class);
        
        JobClient.runJob(conf);
    }
	
	private void initialize(String inputPath1, String inputPath2, String inputPath3, String outputPath) throws IOException {
        JobConf conf = new JobConf(WikiLPA.class);
        
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);
        
        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);
        
        FileInputFormat.addInputPath(conf, new Path(inputPath1));
        FileInputFormat.addInputPath(conf, new Path(inputPath2));
        FileInputFormat.addInputPath(conf, new Path(inputPath3));
        FileOutputFormat.setOutputPath(conf, new Path(outputPath));
        
        conf.setMapperClass(WikiLPAInitMapper.class);
        conf.setReducerClass(WikiLPAInitReducer.class);
        
        JobClient.runJob(conf);
    }
	
	private void runLPA(String inputPath, String outputPath) throws IOException {
        JobConf conf = new JobConf(WikiLPA.class);
        
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);
        
        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);
        
        FileInputFormat.setInputPaths(conf, new Path(inputPath));
        FileOutputFormat.setOutputPath(conf, new Path(outputPath));
        
        conf.setMapperClass(LPACalculateMapper.class);
        conf.setReducerClass(LPACalculateReducer.class);
        
        JobClient.runJob(conf);
    }
	
	private void getResult(String inputPath, String outputPath) throws IOException {
        JobConf conf = new JobConf(WikiLPA.class);
        
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);
        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);
        
        FileInputFormat.setInputPaths(conf, new Path(inputPath));
        FileOutputFormat.setOutputPath(conf, new Path(outputPath));
        
        conf.setMapperClass(LPAResultMapper.class);
        conf.setReducerClass(LPAResultReducer.class);
        
        JobClient.runJob(conf);
    }

}
