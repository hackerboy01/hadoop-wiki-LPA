package fudan.LPA.XMLParser;

import java.io.IOException;
import java.nio.charset.CharacterCodingException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

@SuppressWarnings("deprecation")
public class XMLParserMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
    
    private static final Pattern categoriePattern = Pattern.compile("\\[\\[Categorie:.+?\\]\\]");
    
    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        
        // Returns  String[0] = <title>[TITLE]</title>
        //          String[1] = <text>[CONTENT]</text>
        // !! without the <tags>.
        String[] titleAndText = parseTitleAndText(value);
        
        String pageString = titleAndText[0];
        if(notValidPage(pageString))
            return;
        pageString = sweetify(pageString);
        
        Text page = new Text(pageString.replace(' ', '_'));

        Matcher matcher = categoriePattern.matcher(titleAndText[1]);
        
        //Loop through the matched links in [CONTENT]
        while (matcher.find()) {
            String categorie = matcher.group();
            //Filter only wiki pages.
            //- some have [[realPage|linkName]], some single [realPage]
            //- some link to files or external pages.
            //- some link to paragraphs into other pages.
            categorie = getCategorieFromLink(categorie);
            if(categorie == null || categorie.isEmpty()) 
                continue;
            
            // add valid otherPages to the map.
            output.collect(page, new Text(categorie));
        }
    }
    
    private boolean notValidPage(String pageString) {
        return pageString.contains(":");
    }

    private String getCategorieFromLink(String aLink){
        if(isNotCateLink(aLink)) return null;
        
        int start = aLink.indexOf(":")+1;
        int endLink = aLink.indexOf("]]");

        int pipePosition = aLink.indexOf("|");
        if(pipePosition > 0){
            endLink = pipePosition;
        }
        
        int part = aLink.indexOf("#");
        if(part > 0){
            endLink = part;
        }
        
        aLink =  aLink.substring(start, endLink);
        aLink = aLink.replaceAll("\\s", "_");
        aLink = aLink.replaceAll(",", "");
        aLink = sweetify(aLink);
        
        return aLink;
    }
    
    private String sweetify(String aLinkText) {
    	if(aLinkText.contains("&amp;"))
            aLinkText.replace("&amp;", "&");
        if(aLinkText.contains("&quot;"))
        	aLinkText.replace("&quot;", "\"");

        return aLinkText;
    }

    private String[] parseTitleAndText(Text value) throws CharacterCodingException {
        String[] titleAndText = new String[2];
        
        int start = value.find("<title>");
        int end = value.find("</title>", start);
        start += 7; //add <title> length.
        
        titleAndText[0] = Text.decode(value.getBytes(), start, end-start);

        start = value.find("<text");
        start = value.find(">", start);
        end = value.find("</text>", start);
        start += 1;
        
        if(start == -1 || end == -1) {
            return new String[]{"",""};
        }
        
        titleAndText[1] = Text.decode(value.getBytes(), start, end-start);
        
        return titleAndText;
    }

    private boolean isNotCateLink(String aLink) {
        
        if(!aLink.startsWith("[[Categorie:")){
            return true;
        }
        
        int start = "[[Categorie:".length();
        if( aLink.length() < start || aLink.length() > 100) return true;
        char firstChar = aLink.charAt(start);
        
        if( firstChar == '#') return true;
        if( firstChar == ',') return true;
        if( firstChar == '.') return true;
        if( firstChar == '&') return true;
        if( firstChar == '\'') return true;
        if( firstChar == '-') return true;
        if( firstChar == '{') return true;
        
        if( aLink.contains("&")) return true;
        
        return false;
    }
}

