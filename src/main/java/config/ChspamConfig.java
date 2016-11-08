package config;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.log4j.Logger;

import jline.ClassNameCompletor;
import kernel.Kernel;


public class ChspamConfig {
	final static Logger logger = Logger.getLogger(ChspamConfig.class);
	private static ChspamConfig uniqueInstance = new ChspamConfig();
	
	private ChspamConfig() {
		try {
			readConfig();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static ChspamConfig getConfig() {
		return uniqueInstance;
	}
	
	public enum SourceType{
		FILE, DB
	}
	
	
	public enum PatternStrategy {
		PREFIXSPAM
	}
	private SourceType sourceType;
	private boolean deleteFileAfterImport;
	private boolean incrementalImport;
	private String filePath;
	
	
	private double patternMinSupport;
	private Long patternMaxLenght;
	
	private Long timeInterval;
	
	private PatternStrategy patternStrategy;
	
	
	public boolean isIncrementalImport() {
		return incrementalImport;
	}

	public void setIncrementalImport(boolean incrementalImport) {
		this.incrementalImport = incrementalImport;
	}

	public String getFilePath() {
		return filePath;
	}

	public void setFilePath(String filePath) {
		this.filePath = filePath;
	}

	public SourceType getSourceType() {
		return sourceType;
	}

	public void setSourceType(SourceType sourceType) {
		this.sourceType = sourceType;
	}

	public PatternStrategy getPatternStrategy() {
		return patternStrategy;
	}

	public void setPatternStrategy(PatternStrategy patternStrategy) {
		this.patternStrategy = patternStrategy;
	}

	public boolean isDeleteFileAfterImport() {
		return deleteFileAfterImport;
	}

	public void setDeleteFileAfterImport(boolean deleteFileAfterImport) {
		this.deleteFileAfterImport = deleteFileAfterImport;
	}

	public double getPatternMinSupport() {
		return patternMinSupport;
	}

	public void setPatternMinSupport(double patternMinSupport) {
		this.patternMinSupport = patternMinSupport;
	}

	public Long getPatternMaxLenght() {
		return patternMaxLenght;
	}

	public void setPatternMaxLenght(Long patternMaxLenght) {
		this.patternMaxLenght = patternMaxLenght;
	}

	public Long getTimeInterval() {
		return timeInterval;
	}

	public void setTimeInterval(Long timeInterval) {
		this.timeInterval = timeInterval;
	}
	
	public String toString() {
		return String.format("sourceType [%s], /n timeInterval[%s], /n patternMaxLenght[%s], /n patternMinSupport[%s], /n filePath[%s]/n", sourceType, timeInterval, patternMaxLenght, patternMinSupport, filePath);
	}
	
	public void readConfig() throws Exception {
		logger.info("START");
		Properties prop = new Properties();
    	InputStream input = null;

    	try {

    		String filename = "config.properties";
    		logger.info("READING FILE PROPERTIES");
    		input = ChspamConfig.class.getClassLoader().getResourceAsStream(filename);
    		if(input==null){
    	            System.out.println("Sorry, unable to find " + filename);
    		    return;
    		}

    		prop.load(input);
			
			if ( prop.get("source") == null ){
				throw new Exception("Source not defined on configuration");
			} else {
				SourceType st = SourceType.valueOf(prop.getProperty("source"));
				this.setSourceType(st);
			}
			this.setTimeInterval(Long.parseLong(prop.getProperty("timeinterval")));
			this.setDeleteFileAfterImport( Boolean.valueOf(prop.getProperty("deletefileafterimport")));
			
			this.setIncrementalImport( Boolean.valueOf(prop.getProperty("incrementalimport")));
			this.setFilePath(prop.getProperty("filepath"));
			
			this.setPatternStrategy(PatternStrategy.valueOf(prop.getProperty("patternstrategy")));
			this.setPatternMaxLenght( Long.parseLong(prop.getProperty("maxlength")));
			this.setPatternMinSupport( Double.parseDouble(prop.getProperty("minsupport")));
			
			logger.info(this.toString());
			
		} catch (IOException ex) {
			ex.printStackTrace();
		} finally {
			if (input != null) {
				try {
					input.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
    	logger.info("END");
		
	}
	
	
	
}

