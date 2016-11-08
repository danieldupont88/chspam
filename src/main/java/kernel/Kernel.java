package kernel;

import java.io.File;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import config.ChspamConfig;
import modules.history.comp.DatabaseSource;
import modules.history.comp.FileSource;
import modules.pattern.manager.PrefixSpam;

public class Kernel {
	
	final static Logger logger = Logger.getLogger(Kernel.class);
	
	public static void main(String[] args) {
		
		//ler configs
		ChspamConfig config = ChspamConfig.getConfig();
		
		Runnable fullChspamProcess = new Runnable() {
		    public void run() {
		        
		    	//importar históricos
				if (config.getSourceType().equals(ChspamConfig.SourceType.FILE)) {
					File file = new File(config.getFilePath());
					logger.info("IMPORTING CONTEXT HISTORIES - BEGIN");
					FileSource.processFile(file);
					logger.info("IMPORTING CONTEXT HISTORIES - END");
				} else {
					DatabaseSource.process();
				}
				
				//aplicar filtros
				
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
		};
		
		
		ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
		executor.scheduleWithFixedDelay(fullChspamProcess, 0, config.getTimeInterval(),TimeUnit.MINUTES);
		
	}
	
	
}