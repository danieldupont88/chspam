package kernel;

import java.io.File;

import org.apache.log4j.Logger;

import config.ChspamConfig;
import domain.AlertManager;
import modules.history.comp.DatabaseSource;
import modules.history.comp.FileSourceUCI;
import modules.pattern.manager.HistoryManager;
import modules.pattern.manager.PrefixSpam;

public class Kernel {
	
	final static Logger logger = Logger.getLogger(Kernel.class);
	
	public static void main(String[] args) {
		
		
		reset();
		
		//ler configs
		ChspamConfig config = ChspamConfig.getConfig();
		
		//Runnable fullChspamProcess = new Runnable() {
		//    public void run() {
		        
		    	//importar históricos
				if (config.getSourceType().equals(ChspamConfig.SourceType.FILE)) {
					File file = new File(config.getFilePath());
					logger.info("IMPORTING CONTEXT HISTORIES - BEGIN");
					FileSourceUCI.processFile(file);
					logger.info("IMPORTING CONTEXT HISTORIES - END");
				} else {
					DatabaseSource.process();
				}
				
				/*desocoberta de padrões
				 *  - armazenamento de históricos
				 *  - geração de alertas 
				*/
				if (config.getPatternStrategy().equals(ChspamConfig.PatternStrategy.PREFIXSPAM)) {
					logger.info("RUNNING PATTERN MINNING - BEGIN");
					PrefixSpam.processPattenrs();
					logger.info("RUNNING PATTERN MINNING - END");
				}
				 
				
		    }
		//};
		
		
		
		
		//ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
		//executor.scheduleWithFixedDelay(fullChspamProcess, 0, config.getTimeInterval(),TimeUnit.MINUTES);
		
//	}
	
	public static void reset() {
		HistoryManager hm = new HistoryManager();
		hm.clearHistoryCollection();
		hm.clearLastExecutionCollection();		
		hm.clearThisExecutionCollection();	
		
		AlertManager am = new AlertManager();
		am.clearAlertCollection();
	}
	
	
}