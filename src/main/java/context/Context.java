package context;

import java.util.Date;
import java.util.Map;

public class Context {

	private Entity entity;
	private Date time;
	private Map<String, String> location;
	private Map<String, String> situation;
	
	public Context() {
		this.time = new Date();
	};
	
	public Entity getEntity() {
		return entity;
	}
	public void setEntity(Entity entity) {
		this.entity = entity;
	}
	public Date getTime() {
		return time;
	}
	public void setTime(Date time) {
		this.time = time;
	}
	public Map<String, String> getLocation() {
		return location;
	}
	public void setLocation(Map<String, String> location) {
		this.location = location;
	}
	public Map<String, String> getSituation() {
		return situation;
	}
	public void setSituation(Map<String, String> situation) {
		this.situation = situation;
	}
	
	
	
}
