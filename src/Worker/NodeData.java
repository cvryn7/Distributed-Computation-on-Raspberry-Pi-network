import java.io.Serializable;
import java.util.ArrayList;


/**
 * Data Structure for Worker
 * 
 * @author Vaibhav, Karan, Dler
 *
 */
public class NodeData implements Serializable {

	private String name; // Name of worker
	private int id; // id of worker

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	private String ipAddress;  // Ip address
	private boolean isDown; // master will set if worker is done
	private boolean isBusy; 
	private String masterIP; // master's ip
	private boolean hasAssgnd; // when worker sends result to master

	/**
	 * 
	 * @author vaibhav, karan , dler
	 *
	 */
	class Result implements Serializable {
		ArrayList<Integer> res;  // Result to send to master
		int howManyJobs; // Number of jobs to send
		String ipAddr;  // Ip address of worker
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getIpAddress() {
		return ipAddress;
	}

	public void setIpAddress(String ipAddress) {
		this.ipAddress = ipAddress;
	}

	public boolean isDown() {
		return isDown;
	}

	public void setDown(boolean isDown) {
		this.isDown = isDown;
	}

	public boolean isBusy() {
		return isBusy;
	}

	public void setBusy(boolean isBusy) {
		this.isBusy = isBusy;
	}

	public String getMasterIP() {
		return masterIP;
	}

	public void setMasterIP(String masterIP) {
		this.masterIP = masterIP;
	}
	
	public boolean getHasAssgnd() {
		return hasAssgnd;
	}

	public void setHasAssgnd(boolean hasAssgnd) {
		this.hasAssgnd = hasAssgnd;
	}
	
	

}
