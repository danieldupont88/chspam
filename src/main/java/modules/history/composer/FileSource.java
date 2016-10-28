package modules.history.composer;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

import org.apache.commons.lang.StringUtils;

import context.Context;
import context.Entity;

public class FileSource {

	static String fileToRead = "C:/Dev/context/context-saple1.txt";
	
	public static void main(String[] args) {
		generateContext();
		
		
		HistoryComposer hc = new HistoryComposer();
		//hc.clearHistoryCollection();
		hc.printSavedContexts();
	}
	
	public static void generateContext(){
	File file = new File(fileToRead);
	
		try {
			Scanner scr = new Scanner(file);
			while (scr.hasNext()) {				
				String line = scr.next();
				System.out.println("line : " + line);
				
				List<String> splitedLine = Arrays.asList(StringUtils.split(line, "|"));
				Entity entity = new Entity(splitedLine.get(0));
				Context c = new Context();
				c.setEntity(entity);
				
				List<String> contexts = Arrays.asList(StringUtils.split(splitedLine.get(1), ","));
				for (String context : contexts) {
					List<String> ctxComponent = Arrays.asList(StringUtils.split(context, ":"));
					
					//System.out.println("ctxComponent : " + ctxComponent);
						
						Map<String, String> ctxMap = new HashMap<String, String>();
						ctxMap.put("PLACE", ctxComponent.get(1));
						
						if (ctxComponent.get(0).equals("LOCATION")) { 
							c.setLocation(ctxMap); 
						} else c.setSituation(ctxMap);						
				}
				
				HistoryComposer hc = new HistoryComposer();
				hc.saveContext(c);
				System.out.println("c : " + c.toString());
				
			}
			scr.close();
		} catch (FileNotFoundException ex) {
			System.out.println("File not found" + ex.getMessage());
		}
		
	}
}
