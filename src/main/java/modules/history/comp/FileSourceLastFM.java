package modules.history.comp;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import config.ChspamConfig;
import context.Context;
import context.Entity;
import kernel.Kernel;

public class FileSourceLastFM {
	final static Logger logger = Logger.getLogger(FileSourceLastFM.class);
	static HistoryComposer hc = new HistoryComposer();

	public static void processFile(File fileToProcess) {
		
		HistoryComposer hc = new HistoryComposer();
		
		/*
		 * Se a importação não é incremental, deleta todos os registros de históricos anteriormente importados
		 */
		if(ChspamConfig.getConfig().isIncrementalImport()) {
			logger.info("IMPORT IS INCREMENTAL - DROPING HISTORY COLLECTION");
			hc.clearHistoryCollection();
		}
		
		importHistory(fileToProcess);
		
		if(ChspamConfig.getConfig().isDeleteFileAfterImport()) {
			fileToProcess.delete();
		}
	}
	
	public static void importHistory(File fileToRead){
		
		int lineCount = 0;
		
		try {
			Scanner scr = new Scanner(fileToRead);
			
			while (scr.hasNext()) {				
				String line = scr.next();
				logger.info("line : " + line);
				lineCount++;
				
				List<String> splitedLine = Arrays.asList(StringUtils.split(line, "|"));
				Entity entity = new Entity(splitedLine.get(0));
				Context c = new Context();
				c.setEntity(entity);
				
				List<String> contexts = Arrays.asList(StringUtils.split(splitedLine.get(1), ","));
				for (String context : contexts) {
					List<String> ctxComponent = Arrays.asList(StringUtils.split(context, ":"));
					
					//System.out.println("ctxComponent : " + ctxComponent);
						
						Map<String, String> ctxMap = new HashMap<String, String>();
						
						if (ctxComponent.get(0).equals("LOCATION")) { 
							ctxMap.put("PLACE", ctxComponent.get(1));
							c.setLocation(ctxMap); 
						} else {
							ctxMap.put("ACTION", ctxComponent.get(1));
							c.setSituation(ctxMap);			
						}
				}
				
				
				hc.saveContext(c);
				System.out.println("c : " + c.toString());
				
			}
			scr.close();
		} catch (FileNotFoundException ex) {
			System.out.println("File not found" + ex.getMessage());
		}
		
	}
}
