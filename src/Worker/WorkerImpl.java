import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.UnknownHostException;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Scanner;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/**
 * 
 * Worker Implementation
 * 
 * @author vaibhav, karan, dler
 * 
 * @version 1.0
 *
 */
public class WorkerImpl extends UnicastRemoteObject implements WorkerInter {

	private static final long serialVersionUID = 1L;
	private NodeData workerData = new NodeData(); // Worker's Datastructure
	private boolean isJoined; // Set if worker has joined
	private BlockingQueue<ArrayList<Integer>> sortJobQueue = new ArrayBlockingQueue<ArrayList<Integer>>(
			100);
	private int sortJobLimit;
	private ArrayList<Integer> alreadySorted = new ArrayList<Integer>(); // To
																			// put
																			// already
																			// sorted
																			// lists
	private ArrayList<Integer> sortedList = new ArrayList<Integer>(); // Sorted
																		// list
	private ArrayList<Integer> resultSort = new ArrayList<Integer>(); // Result
																		// Sorted
																		// list
	private Queue<ArrayList<Integer>> averageJobQueue = new LinkedList<ArrayList<Integer>>();
	private int averageJobLimit;
	private int globalCount = 0;

	private int returnCount = 0; // When to return to counter
	private MasterInter remoteMasterObj; // Remote Master Object
	private boolean stopWorker = false;
	Registry masterRegistry; // Master's RMI registry
	Registry clientReg; // Client's RMI registry

	public WorkerImpl() throws RemoteException {

	}

	/**
	 * TO Sort the input array list
	 */
	@Override
	public ArrayList<Integer> getSortedResult(ArrayList<Integer> input)
			throws RemoteException {
		// System.out.println("Job Recieved from master");
		Collections.sort(input);
		return input;
	}

	/**
	 * Join master
	 */
	public void joinMaster() {

		try {

			masterRegistry = LocateRegistry.getRegistry(
					workerData.getMasterIP(), 60000);

			remoteMasterObj = (MasterInter) masterRegistry.lookup(workerData
					.getMasterIP());
			isJoined = remoteMasterObj.join(workerData);
			if (isJoined)
				System.out.println("*****Worker joined to master*****");
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	/**
	 * Start execution of Worker
	 * 
	 * @param ipOfMaster
	 */
	public void startExe(String ipOfMaster) {

		Scanner sc = null;
		if (ipOfMaster == null) {
			// sc = new Scanner(System.in);
			// System.out.println("Enter IP of Master you want to join : ");
			workerData.setMasterIP(ipOfMaster);
			sc.close();
		} else {
			workerData.setMasterIP(ipOfMaster);
		}
		try {
			workerData.setName(InetAddress.getLocalHost().getHostName());
			System.out.println("Name is " + workerData.getName());
			WorkerSortJob workerSortJob = new WorkerSortJob();
			Thread workerSortThread = new Thread(workerSortJob);
			workerSortThread.start();
		} catch (UnknownHostException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		workerData.setBusy(false);

		try {
			workerData.setIpAddress(InetAddress.getLocalHost().getHostAddress()
					.toString());

		} catch (UnknownHostException e) {
			e.printStackTrace();
		}

		try {

			LocateRegistry.createRegistry(60000);

			// WorkerInter stub = (WorkerInter)
			// UnicastRemoteObject.exportObject(this, 60000);

			Registry registry = LocateRegistry.getRegistry(60000);

			registry.rebind(workerData.getIpAddress(), this);

		} catch (RemoteException e) {
			e.printStackTrace();
		}
		joinMaster();

	}

	/**
	 * 
	 * @param args
	 */
	public static void main(String[] args) {

		try {
			WorkerImpl workerImpl = new WorkerImpl();
			workerImpl.startExe(args[0]);

		} catch (RemoteException e) {
			e.printStackTrace();
		}
	}

	/**
	 * 
	 */
	@Override
	public int getSumofElements(ArrayList<Integer> input)
			throws RemoteException {

		int sum = 0;

		for (int i : input) {
			sum += i;
		}

		return sum;
	}

	/**
	 * Add jobs from master to job queue
	 */
	@Override
	public synchronized boolean addToSortedQueue(ArrayList<Integer> job)
			throws RemoteException {

		boolean succ = false;

		succ = sortJobQueue.offer(job);

		return succ;

	}

	/**
	 * Listens to Job queue and sorts the jobs and sends the result back to
	 * master
	 * 
	 * @author vaibhav, karan , dler
	 *
	 */
	class WorkerSortJob implements Runnable {

		@Override
		public void run() {
			NodeData.Result resu = workerData.new Result();
			int currentAlreadySortedIndex = 0;
			while (true) {

				int i = 0;

				// if(stopWorker){
				// break;
				// }

				if (!sortJobQueue.isEmpty()) {

					try {
						sortedList = sortJobQueue.poll();

						currentAlreadySortedIndex = 0;
						sortedList = getSortedResult(sortedList);

						while (i < sortedList.size()
								&& currentAlreadySortedIndex < alreadySorted
										.size()) {

							if (alreadySorted.get(currentAlreadySortedIndex) > sortedList
									.get(i)) {
								resultSort.add(sortedList.get(i));
								i++;
							} else {
								resultSort.add(alreadySorted
										.get(currentAlreadySortedIndex));
								currentAlreadySortedIndex++;
							}

						}

						while (i < sortedList.size()) {
							resultSort.add(sortedList.get(i));
							i++;
						}

						while (currentAlreadySortedIndex < alreadySorted.size()) {
							resultSort.add(alreadySorted
									.get(currentAlreadySortedIndex));
							currentAlreadySortedIndex++;
						}

						returnCount++;

						alreadySorted.clear();

						alreadySorted.addAll(resultSort);
						// sortedList.clear();

						if (returnCount == 5 || sortJobQueue.isEmpty()) {
							resu.res = alreadySorted;
							resu.howManyJobs = returnCount;
							globalCount += returnCount;
							resu.ipAddr = workerData.getIpAddress();
							returnCount = 0;

							try {
								remoteMasterObj.getSortedResultFromWorker(resu);
								currentAlreadySortedIndex = 0;
								resultSort.clear();
								resu.res.clear();
								alreadySorted.clear();
								sortedList.clear();
							} catch (RemoteException e) {
								e.printStackTrace();
							}
						}

						resultSort.clear();

					} catch (RemoteException e) {
						e.printStackTrace();
					}

				} else {
					// System.out.println("Global Count " + globalCount);
				}

			}

			/*
			 * returnCount = 0; currentAlreadySortedIndex = 0;
			 * resultSort.clear(); resu.res.clear(); alreadySorted.clear();
			 * sortedList.clear(); globalCount = 0;
			 */

		}
	}

	/*
	 * @Override public void stopTheWorker() throws RemoteException { stopWorker
	 * = true; }
	 * 
	 * @Override public void startWorker() throws RemoteException { stopWorker =
	 * false; WorkerSortJob workerSortJob = new WorkerSortJob(); Thread
	 * workerSortThread = new Thread(workerSortJob); workerSortThread.start();
	 * 
	 * }
	 */
	
	/**
	 * Pings back to master
	 */
	@Override
	public boolean ping() throws RemoteException {
		return true;
	}

}
