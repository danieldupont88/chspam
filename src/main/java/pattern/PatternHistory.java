package pattern;

import java.io.Serializable;
import java.util.Date;

public class PatternHistory implements Serializable {
	private static final long serialVersionUID = 1L;
	private Date discoveredIn;
	private long frequency;
	
	public Date getDiscoveredIn() {
		return discoveredIn;
	}
	public void setDiscoveredIn(Date discoveredIn) {
		this.discoveredIn = discoveredIn;
	}
	public long getFrequency() {
		return frequency;
	}
	public void setFrequency(Long frequency) {
		this.frequency = frequency;
	}
	
	public PatternHistory (long frequency) {
		this.frequency = frequency;
		this.discoveredIn = new Date();
	}
	
	public String toString() {
		return String.format("frequency: %s, discoveredIn: %s", this.frequency, this.discoveredIn);
	}
	
	
}
