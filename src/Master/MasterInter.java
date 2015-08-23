import java.rmi.Remote;
import java.rmi.RemoteException;

import javax.xml.transform.Result;

/**
 * 
 * Master Interface
 * 
 * @author vaibhav, karan dler
 *
 */
public interface MasterInter extends Remote{

	
	/**
	 * Joins workers in system
	 * 
	 * @param worker
	 * @return
	 * @throws RemoteException
	 */
	public boolean join(NodeData worker) throws RemoteException;
	
	/**
	 * Get results from worker
	 * 
	 * @param result
	 * @throws RemoteException
	 */	
	public void getSortedResultFromWorker(NodeData.Result result) throws RemoteException;
	
}
