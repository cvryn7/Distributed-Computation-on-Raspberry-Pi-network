import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.ArrayList;

/**
 * 
 * Worker's Interface
 * 
 * @author vaibhav, karan ,dler
 *
 */
public interface WorkerInter extends Remote {

	/**
	 * Sorts the input
	 * 
	 * @param input
	 * @return
	 * @throws RemoteException
	 */
	public ArrayList<Integer> getSortedResult(ArrayList<Integer> input)
			throws RemoteException;

	/**
	 * To get sum of elements in input
	 * 
	 * @param input
	 * @return
	 * @throws RemoteException
	 */
	public int getSumofElements(ArrayList<Integer> input)
			throws RemoteException;
	
	
	/**
	 * Master adds jobs to queue
	 * 
	 * @param job
	 * @return
	 * @throws RemoteException
	 */
	public boolean addToSortedQueue(ArrayList<Integer> job) throws RemoteException;
	
	//public void stopTheWorker() throws RemoteException;

	//public void startWorker() throws RemoteException;
	
	/**
	 * To ping worker to check whether it is alive
	 * @return
	 * @throws RemoteException
	 */
	public boolean ping() throws RemoteException;
}
