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

public class FileSourceUCI {
	final static Logger logger = Logger.getLogger(FileSourceUCI.class);
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
		
		try {
			Scanner scr = new Scanner(fileToRead);
			
			while (scr.hasNext()) {				
				String line = scr.next();
				logger.info("line : " + line);
				
				List<String> splitedLine = Arrays.asList(StringUtils.split(line, ";"));
				Entity entity = new Entity(splitedLine.get(0));
				Context c = new Context();
				c.setEntity(entity);
		
				Map<String, String> placeCtxMap = new HashMap<String, String>();
				placeCtxMap.put("PLACE", splitedLine.get(5));
				c.setLocation(placeCtxMap); 
				
				Map<String, String> actionCtxMap = new HashMap<String, String>();
				actionCtxMap.put("ACTION", splitedLine.get(3));
				c.setSituation(actionCtxMap);			
				
				hc.saveContext(c);
				System.out.println("c : " + c.toString());
				
			}
			scr.close();
		} catch (FileNotFoundException ex) {
			System.out.println("File not found" + ex.getMessage());
		}
		
	}
}
