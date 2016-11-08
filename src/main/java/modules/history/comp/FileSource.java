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

public class FileSource {
	final static Logger logger = Logger.getLogger(FileSource.class);

	public static void processFile(File fileToProcess) {
		
		HistoryComposer hc = new HistoryComposer();
		
		/*
		 * Se a importa��o n�o � incremental, deleta todos os registros de hist�ricos anteriormente importados
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
