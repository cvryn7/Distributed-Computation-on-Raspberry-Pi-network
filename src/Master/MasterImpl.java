import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.PriorityQueue;
import java.util.Scanner;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.swing.JOptionPane;

import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.UIKeyboardInteractive;
import com.jcraft.jsch.UserInfo;

/**
 * A class for Master node of the distributed computation
 * 
 * @author Vaibhav Page, Dler Ahmad, Karan Bhagat
 * 
 * @version May 3, 2015
 */
public class MasterImpl extends UnicastRemoteObject implements MasterInter {

	private static final long serialVersionUID = 1L;
	private HashMap<Integer, NodeData> workerMap = new HashMap<Integer, NodeData>();
	private int workerCount;
	private ArrayList<ArrayList<Integer>> jobQueue = new ArrayList<ArrayList<Integer>>();
	private ArrayList<Integer> jobQueueAverage = new ArrayList<Integer>();
	private HashMap<Integer, JobDataStructure> jobMap = new HashMap<Integer, JobDataStructure>();
	HashMap<ArrayList<Integer>, Integer> grMap = new HashMap<ArrayList<Integer>, Integer>();
	private int jobID;
	private ArrayList<Integer> input = new ArrayList<Integer>();
	private String fileName;
	private Set<Integer> uniqueInput = new TreeSet<Integer>();
	private int inputDivideFactor = 0;
	private ArrayList<NodeData> aliveWorkers = new ArrayList<NodeData>();
	private ArrayList<NodeData> deadWorkers = new ArrayList<NodeData>();
	private HashMap<Integer, ArrayList<Integer>> workerJobList = new HashMap<Integer, ArrayList<Integer>>();
	private HashMap<String, Integer> ipToIdMapping = new HashMap<String, Integer>();
	private ArrayList<String> listOfResultFiles = new ArrayList<String>();
	private TreeSet<Long> mergeSet = new TreeSet<Long>();
	private long completedJob;
	private long globalTrue;
	private String userId;
	private String password;
	private BufferedReader brGlobal;
	private boolean breakFetchingInput;
	int choice = 0;
	public String wotkerFileName;

	// constructor
	public MasterImpl() throws RemoteException {

	}

	/**
	 * 
	 * A class for a chunk of data
	 * 
	 */
	class Node implements Comparable<Node> {
		int data;
		int id;

		public Node(int data, int id) {
			this.data = data;
			this.id = id;
		}

		@Override
		public int compareTo(Node o1) {
			// TODO Auto-generated method stub
			if (this.data < o1.data) {
				return -1;
			} else if (this.data > o1.data) {
				return 1;
			} else
				return 0;
		}

	}

	/**
	 * merges series of file to one file
	 * 
	 * @param filenames
	 * @param outFile
	 */
	public void merge(ArrayList<String> filenames, String outFile) {
		try {
			ArrayList<File> files = new ArrayList<File>();
			for (String file : filenames) {
				files.add(new File(file));
			}

			PriorityQueue<Node> pQ = new PriorityQueue<Node>();

			ArrayList<BufferedReader> fileReader = new ArrayList<BufferedReader>();
			PrintWriter output = new PrintWriter(outFile);
			for (File file : files) {
				fileReader.add(new BufferedReader(new FileReader(file)));
			}
			String lineIn;
			Node node1;

			for (int i = 0; i < fileReader.size(); ++i) {
				lineIn = fileReader.get(i).readLine();
				if (lineIn != null) {
					node1 = new Node(Integer.parseInt(lineIn), i);
					pQ.add(node1);
					// System.out.println(node1.data +" added from  "+node1.id);
				}
			}
			while (pQ.size() > 0) {
				Node node = pQ.remove();
				// System.out.println(node.data +" removed from  "+node.id);
				output.println(node.data);
				int id = node.id;
				lineIn = fileReader.get(id).readLine();
				if (lineIn != null) {
					node1 = new Node(Integer.parseInt(lineIn), id);
					pQ.add(node1);
					// System.out.println(node1.data +" added from  "+node1.id);
				}

			}
			output.flush();

			System.out.println("Done!");

			output.close();

		} catch (Exception ex) {
			ex.printStackTrace();
		}

	}

	/**
	 * A data structure to store chunks as job
	 * 
	 */
	class JobDataStructure {
		ArrayList<Integer> theJob;
		boolean isAssigned;
		boolean isInProcess;
		long threadID;
	}

	/**
	 * 
	 * A class to save avarage job id
	 * 
	 */
	class AverageJobData {
		int jobIdx;
		int groupID;

		public AverageJobData(int jid, int gid) {
			this.jobIdx = jid;
			this.groupID = gid;
		}

	}

	/**
	 * joins the worker to master
	 */
	@Override
	public boolean join(NodeData worker) throws RemoteException {

		Entry<Integer, NodeData> ent = null;
		int key = 0;
		boolean alreadyExisiting = false;
		Iterator<Entry<Integer, NodeData>> it = workerMap.entrySet().iterator();

		while (it.hasNext()) {
			ent = it.next();
			if (ent.getValue().getIpAddress().equals(worker.getIpAddress())) {
				key = ent.getKey();
				worker.setId(key);
				workerMap.put(key, worker);
				workerJobList.put(key, new ArrayList<Integer>());
				ipToIdMapping.put(worker.getIpAddress(), key);
				System.out.println("Added to alive");
				aliveWorkers.add(worker);
				alreadyExisiting = true;
				System.out.println("Worker Joined with ID " + key
						+ " (already exists)");
			}

		}

		if (!alreadyExisiting) {
			this.workerMap.put(workerCount, worker);
			workerJobList.put(workerCount, new ArrayList<Integer>());
			worker.setId(workerCount);
			aliveWorkers.add(worker);
			ipToIdMapping.put(worker.getIpAddress(), workerCount);
			System.out.println("New Worker Joined with ID " + workerCount);
			workerCount++;
		}

		return true;
	}

	/**
	 * devides input to chunks
	 * 
	 * @param inp
	 */
	public void divideInput(ArrayList<Integer> inp) {

		int divideStartCounter = 0;
		int divideEndCounter = inp.size() / inputDivideFactor;
		jobID = 0;
		while (true) {
			JobDataStructure jbs = new JobDataStructure();
			if (divideStartCounter < inp.size()
					&& divideEndCounter < inp.size()) {

				jobID++;

				jbs.theJob = new ArrayList<Integer>(inp.subList(
						divideStartCounter, divideEndCounter));
				jbs.isAssigned = false;
				jobMap.put(jobID, jbs);

				divideStartCounter = divideEndCounter;
				divideEndCounter += inp.size() / inputDivideFactor;

			} else {
				jobID++;
				jbs.theJob = new ArrayList<Integer>(inp.subList(
						divideStartCounter, inp.size()));
				jbs.isAssigned = false;
				jobMap.put(jobID, jbs);

				break;
			}

		}

	}

	/**
	 * fetch input from file
	 */
	public void fetchInput() {

		int data = 0;
		int readCounter = 0;
		try {
			String line = "";
			while ((line = brGlobal.readLine()) != null) {
				data = Integer.parseInt(line);
				if (choice == 1) {
					input.add(data);
				} else {
					uniqueInput.add(data);
				}
				readCounter++;
				if (readCounter == 10000) {
					break;
				}
			}

			if (line == null) {
				breakFetchingInput = true;
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * allocate job to workers using thread pool
	 */
	class MasterWorkAllocator {

		ExecutorService exeService = Executors.newFixedThreadPool(20);

		private int jobId;
		private volatile int workerId = 0;

		public void closeExe() {
			exeService.shutdown();
		}

		public void doSortJobs() {
			workerId = 1;

			// System.out.println("Start Time in mili "+
			// System.currentTimeMillis());
			long op = 0;
			while (true) {
				// System.out.println("OPPP " + (op++));
				// System.out.println("Global True " + globalTrue);
				if (completedJob >= jobMap.size()) {
					System.out.println("Completed This Input");
					break;
				}

				final Iterator<Entry<Integer, JobDataStructure>> ent = jobMap
						.entrySet().iterator();

				while (ent.hasNext()) {

					if (completedJob >= jobMap.size()) {
						// System.out.println("Second Byeyeyeye");
						break;
					}

					if (!aliveWorkers.isEmpty()) {

						if (exeService.isShutdown()) {
							exeService = Executors.newFixedThreadPool(20);
							// System.out.println("\n******NEW THREAD POOL CREATED*******8\n");
						}

						try {

							exeService.submit(new Runnable() {

								@Override
								public void run() {
									int jbid = 0;
									synchronized (ent) {
										jbid = ent.next().getKey();

										if (!jobMap.get(jbid).isInProcess
												&& !aliveWorkers.isEmpty()) {
											// System.out.println("IN FIRST IF");
											jobMap.get(jbid).isInProcess = true;
											jobMap.get(jbid).threadID = Thread
													.currentThread().getId();
											// System.out.println("OK "+
											// Thread.currentThread().getId());
										} else {
											// System.out.println("FUP "
											// + Thread.currentThread()
											// .getId());
										}

									}

									if (jobMap.get(jbid).isAssigned == false
											&& (jobMap.get(jbid).threadID == Thread
													.currentThread().getId())
											&& jobMap.get(jbid).isInProcess == true
											&& !aliveWorkers.isEmpty()) {

										ArrayList<Integer> workerJobs = null;
										WorkerInter worker = null;
										boolean isAbleToAddToWorkerQ = false;
										NodeData workerData = null;
										try {

											workerData = aliveWorkers
													.get(workerId
															% aliveWorkers
																	.size());

											// System.out.println("IN LOOP");

											Registry workerReg = LocateRegistry
													.getRegistry(workerData
															.getIpAddress(),
															60000);

											worker = (WorkerInter) workerReg
													.lookup(workerData
															.getIpAddress());

											isAbleToAddToWorkerQ = worker
													.addToSortedQueue(jobMap
															.get(jbid).theJob);

											if (isAbleToAddToWorkerQ) {
												 //System.out.println("Job Sent to worker "+
												 //workerData.getId());
												jobMap.get(jbid).isAssigned = true;
												// globalTrue++;
												synchronized (workerJobList) {
													workerJobs = workerJobList
															.get(workerData
																	.getId());
													workerJobs.add(jbid);
													workerJobList.put(
															workerData.getId(),
															workerJobs);
												}

												// System.out.println("Queue NOT full");
											} else {
												jobMap.get(jbid).isInProcess = false;
											}

										} catch (Exception e) {
											workerData.setDown(true);
											jobMap.get(jbid).isInProcess = false;

											// e.printStackTrace();

											synchronized (aliveWorkers) {
												if (aliveWorkers
														.contains(workerData)) {

													aliveWorkers
															.remove(workerData);
													System.out
															.println("Removed From Alive");
												}
											}

											synchronized (deadWorkers) {
												if (!deadWorkers
														.contains(workerData)) {
													deadWorkers.add(workerData);
													System.out
															.println("Added to Dead");
												}
											}

										}
									}
								}

							});

							workerId++;

							if (workerId > 1000) {
								workerId = 1;
							}

						} catch (Exception e) {
							// e.printStackTrace();
						}

						// workerId++;
					} else {
						if (!exeService.isShutdown()) {
							exeService.shutdownNow();
							// System.out.println("\nNEW THREAD POOL SHUTTING DOWN\n");
						}
					}

				}

			}

		}

		/**
		 * calculate the avarage
		 */
		public void doAverageJobs() {
			workerId = 0;

			// System.out.println("Start Time in mili "+
			// System.currentTimeMillis());
			for (final Entry<Integer, JobDataStructure> ent : jobMap.entrySet()) {

				jobId = ent.getKey();
				workerId++;
				try {
					jobQueueAverage.add(exeService.submit(
							new Callable<Integer>() {

								@Override
								public Integer call() throws Exception {

									int outputRes = 0;
									WorkerInter worker = null;
									boolean isSucc = false;

									while (true) {

										try {

											if (aliveWorkers.isEmpty()) {
												System.out
														.println("No Live Workers");
												break;
											}

											workerId = workerId
													% aliveWorkers.size();

											NodeData workerData = aliveWorkers
													.get(workerId);

											worker = (WorkerInter) Naming.lookup("rmi://"
													+ workerData.getIpAddress()
													+ "/"
													+ workerData.getIpAddress());

											try {
												outputRes = worker.getSumofElements(ent
														.getValue().theJob);
												isSucc = true;
												 System.out.println("Job is done by worker "+
												 workerData.getName());
											} catch (Exception e) {
												isSucc = false;
												aliveWorkers.remove(workerData);
												deadWorkers.add(workerData);
												workerId++;
											}
											if (isSucc) {
												break;
											}

										} catch (Exception e) {
											// e.printStackTrace();
										}

									}

									return outputRes;
								}

							}).get());
				} catch (Exception e) {
					// e.printStackTrace();
				}

			}

		}

	}

	/**
	 * calculate avarage
	 * 
	 * @param filename
	 * @return
	 */
	private double calculateAvg(String filename) {
		long sum = 0;
		long count = 0;
		double avg = 0;
		String line;
		try {
			BufferedReader br = new BufferedReader(new FileReader(filename));
			while ((line = br.readLine()) != null) {
				sum += Integer.parseInt(line);
				count++;
			}
			avg = (double) sum / count;

		} catch (Exception ex) {
			ex.printStackTrace();
		}
		return avg;

	}

	class MyUserInfo implements UserInfo, UIKeyboardInteractive {

		String passwd = "raspberry";

		public String getPassword() {
			return passwd;
		}

		public boolean promptYesNo(String str) {

			return true;

		}

		public String getPassphrase() {
			return null;
		}

		public boolean promptPassphrase(String message) {
			return true;
		}

		public boolean promptPassword(String message) {

			return false;

		}

		public void showMessage(String message) {

			JOptionPane.showMessageDialog(null, message);

		}

		public String[] promptKeyboardInteractive(String destination,

		String name,

		String instruction,

		String[] prompt,

		boolean[] echo) {

			return null;

		}

	}

	public void startWorkers() {

		String workerToInvokeIp = "";
		JSch jsch = new JSch();
		try {
			BufferedReader workerBr = new BufferedReader(new FileReader(
					wotkerFileName));

			String line = "";

			while ((line = workerBr.readLine()) != null) {
				workerToInvokeIp = line;

				java.util.Properties config = new java.util.Properties();
				config.put("StrictHostKeyChecking", "no");
				Session session = jsch.getSession("pi", workerToInvokeIp);
				UserInfo ui = new MyUserInfo();
				session.setUserInfo(ui);
				session.setPassword("raspberry");
				session.setConfig(config);
				session.connect();
				System.out.println("Is conn " + session.isConnected());

				System.out.println("Connected");

				ChannelExec channel = (ChannelExec) session.openChannel("exec");
				BufferedReader in = new BufferedReader(new InputStreamReader(
						channel.getInputStream()));
				
				System.out.println();
				
				channel.setCommand("sh LauncherCl.sh "
						+ InetAddress.getLocalHost().getHostAddress() + ";");
				channel.connect();
				channel.disconnect();
				session.disconnect();
				in.close();

			}
			workerBr.close();
		} catch (IOException | JSchException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public void startExe() {

		startWorkers();

		Scanner sc = new Scanner(System.in);

		ArrayList<Integer> result = new ArrayList<Integer>();
		Set<Long> dummySet = new TreeSet<Long>();
		long outputFileCounter = 0;
		String outputFile = "";
		long seekIndex = 10000;
		boolean leave = false;
		PrintWriter wr = null;
		CheckStatus chStatus = new CheckStatus();
		Thread checkStatusThread = new Thread(chStatus);
		checkStatusThread.start();
		MasterWorkAllocator mw = null;

		PingWorkers ping = new PingWorkers();
		Thread pingToWorkersThread = new Thread(ping);
		pingToWorkersThread.start();

		BufferedReader br = null;
		try {

			// File passFile = new File("password.txt");
			// br = new BufferedReader(new FileReader(passFile));

			// userId = br.readLine();
			// password = br.readLine();

			// br.close();
		} catch (Exception e1) {
			e1.printStackTrace();
		}

		while (true) {

			System.out
					.println("*********Distributed Sorting & Average*********");

			System.out.println("Enter file name : ");
			fileName = sc.nextLine();
			System.out
					.println("Enter name of file where you want to store output");
			outputFile = sc.nextLine();
			try {
				brGlobal = new BufferedReader(new FileReader(fileName));
			} catch (FileNotFoundException e1) {
				e1.printStackTrace();
			}

			System.out.println("1. Sort");
			System.out.println("2. Average of Unique Values");
			System.out.println("3. Exit");
			System.out.println("Enter Your Choice : ");

			choice = Integer.parseInt(sc.nextLine());
			long start_time = 0;
			switch (choice) {
			case 1:
				start_time = System.currentTimeMillis();
				try {

					while (!breakFetchingInput) {

						mw = new MasterWorkAllocator();

						fetchInput();

						if (breakFetchingInput)
							break;

						File file = new File(outputFile + ""
								+ outputFileCounter + ".txt");

						listOfResultFiles.add(outputFile + ""
								+ outputFileCounter + ".txt");

						outputFileCounter++;

						System.out.println("Input Size is " + input.size());
						// System.out.println("In how many parts you want to divide input");
						// inputDivideFactor = Integer.parseInt(sc.nextLine());

						if (input.size() > 100) {
							inputDivideFactor = 100;
						} else {
							inputDivideFactor = input.size();
						}

						divideInput(input);

						completedJob = 0;

						System.out.println("Job Map Size " + jobMap.size());

						try {
							// Thread.sleep(5000);
						} catch (Exception e) {
						}

						mw.doSortJobs();

						// System.out.println("Number of elements in Job Size Q "+
						// jobQueue.size());

						try {
							// Thread.sleep(5000);
						} catch (Exception e) {
						}

						int dummy = 0;
						int i = 0;
						PriorityQueue<Node> pq = new PriorityQueue<Node>();

						for (ArrayList<Integer> o : jobQueue) {
							if (o.size() > 0 && o != null) {
								dummy = o.remove(0);
								Node node = new Node(dummy, i);
								i++;
								pq.add(node);
							}
						}

						while (pq.size() > 0) {
							Node node = pq.remove();
							result.add(node.data);
							int id = node.id;
							ArrayList<Integer> list = jobQueue.get(id);
							if (list.size() > 0 && list != null) {
								int data = list.remove(0);
								Node newNode = new Node(data, id);
								pq.add(newNode);
							}
						}

						try {
							wr = new PrintWriter(new FileWriter(file));

						} catch (IOException e) {
							e.printStackTrace();
						}

						for (int d : result) {
							wr.println(d);
						}

						wr.flush();

						jobMap.clear();

						result.clear();

						input.clear();

						jobQueue.clear();

						for (NodeData al : aliveWorkers) {
							workerJobList.get(al.getId()).clear();
						}

						System.out.println("DONE");

						mw.closeExe();

					}

					breakFetchingInput = false;

					merge(listOfResultFiles, outputFile);

					listOfResultFiles.clear();

				} catch (Exception e) {
					e.printStackTrace();
				}
				System.out.println("Time elasped to sort : "
						+ (System.currentTimeMillis() - start_time)
						+ " mili seconds");
				break;

			case 2:
				start_time = System.currentTimeMillis();
				try {

					while (!breakFetchingInput) {

						mw = new MasterWorkAllocator();

						fetchInput();

						if (breakFetchingInput) {

							jobMap.clear();

							result.clear();

							input.clear();

							jobQueue.clear();

							System.out.println("DONE");

							mw.closeExe();

							break;

						}

						input.addAll(uniqueInput);

						File file = new File(outputFile + ""
								+ outputFileCounter + ".txt");

						listOfResultFiles.add(outputFile + ""
								+ outputFileCounter + ".txt");

						outputFileCounter++;

						System.out.println("Input Size is " + input.size());
						// System.out.println("In how many parts you want to divide input");
						// inputDivideFactor = Integer.parseInt(sc.nextLine());

						if (input.size() > 100) {
							inputDivideFactor = input.size() / 100;
						} else {
							inputDivideFactor = input.size();
						}

						divideInput(input);

						completedJob = 0;

						System.out.println("Job Map Size " + jobMap.size());

						try {
							// Thread.sleep(5000);
						} catch (Exception e) {
						}

						mw.doSortJobs();

						// System.out.println("Number of elements in Job Size Q "+
						// jobQueue.size());

						try {
							// Thread.sleep(5000);
						} catch (Exception e) {
						}

						int dummy = 0;
						int i = 0;
						PriorityQueue<Node> pq = new PriorityQueue<Node>();

						for (ArrayList<Integer> o : jobQueue) {
							if (o.size() > 0 && o != null) {
								dummy = o.remove(0);
								Node node = new Node(dummy, i);
								i++;
								pq.add(node);
							}
						}

						while (pq.size() > 0) {
							Node node = pq.remove();
							result.add(node.data);
							int id = node.id;
							ArrayList<Integer> list = jobQueue.get(id);
							if (list.size() > 0 && list != null) {
								int data = list.remove(0);
								Node newNode = new Node(data, id);
								pq.add(newNode);
							}
						}

						try {
							wr = new PrintWriter(new FileWriter(file));

						} catch (IOException e) {
							e.printStackTrace();
						}

						for (int d : result) {
							wr.println(d);
						}

						wr.flush();

						jobMap.clear();

						result.clear();

						input.clear();

						jobQueue.clear();

						for (NodeData al : aliveWorkers) {
							workerJobList.get(al.getId()).clear();
						}

						System.out.println("DONE");

						mw.closeExe();

					}

					merge(listOfResultFiles, outputFile);

					BufferedReader br_avg = new BufferedReader(new FileReader(
							outputFile));

					String avg_line = "";

					long average = 0L;
					long lastElement = 0;

					int avg_i = 1;
					int total_avg_num = 0;

					while ((avg_line = br_avg.readLine()) != null) {

						if (lastElement != Long.parseLong(avg_line)) {
							mergeSet.add(Long.parseLong(avg_line));
						}

						if (avg_i == 1000) {
							for (long ml : mergeSet) {
								average += ml;
								total_avg_num++;
							}

							lastElement = mergeSet.last();

							mergeSet.clear();
							avg_i = 1;

						}

						avg_i++;

					}

					if (avg_line == null) {

						if (mergeSet.size() > 0) {
							for (long ml : mergeSet) {
								average += ml;
								total_avg_num++;
							}
						}

					}

					br_avg.close();
					mergeSet.clear();
					System.out.println("Average is "
							+ (double) (average / total_avg_num));

					average = 0;
					total_avg_num = 0;
					avg_i = 1;
					breakFetchingInput = false;

					/*
					 * long fileNullCount = listOfResultFiles.size(); long
					 * globalFileCounter = 0L; long seekCounter = 0L; long iSeek
					 * = 0L; String line = ""; RandomAccessFile rFile = null;
					 * RandomAccessFile averageResultFile = new
					 * RandomAccessFile( outputFile, "rw"); String
					 * currentFileName = "";
					 * 
					 * while (true) {
					 * 
					 * for (String file : listOfResultFiles) {
					 * 
					 * currentFileName = file;
					 * 
					 * rFile = new RandomAccessFile(file, "r");
					 * rFile.seek(seekCounter);
					 * 
					 * while ((line = rFile.readLine()) != null) {
					 * 
					 * if (!dummySet.contains(Integer.parseInt(line))) {
					 * mergeSet.add(Integer.parseInt(line)); } iSeek++; if
					 * (iSeek == 1000) { break; } }
					 * 
					 * for (int mi : mergeSet) { String res = mi + "\n";
					 * averageResultFile.write(res.getBytes()); }
					 * 
					 * globalFileCounter += iSeek;
					 * 
					 * averageResultFile.seek(globalFileCounter);
					 * 
					 * if (line == null) { fileNullCount--; }
					 * 
					 * dummySet.add(mergeSet.last());
					 * 
					 * if (fileNullCount == 0) { break; }
					 * 
					 * }
					 * 
					 * mergeSet.clear();
					 * 
					 * if (line == null) {
					 * listOfResultFiles.remove(currentFileName); }
					 * 
					 * if (fileNullCount == 0) { rFile.close();
					 * averageResultFile.close(); break; }
					 * 
					 * seekCounter += iSeek; iSeek = 0;
					 * 
					 * }
					 * 
					 * listOfResultFiles.clear(); mergeSet.clear();
					 * dummySet.clear();
					 */

					// calculating avarage
					/*
					 * double avarage = calculateAvg(outputFile); PrintWriter
					 * output = new PrintWriter("Avg" + outputFile);
					 * output.println(avarage); output.flush(); output.close();
					 */

				} catch (Exception e) {
					e.printStackTrace();
				}
				System.out.println("Time elasped to sort : "
						+ (System.currentTimeMillis() - start_time)
						+ " mili seconds");
				break;

			case 3:
				leave = true;
				sc.close();
				wr.close();
				mw.closeExe();
				System.exit(0);
				break;

			}
			mw.closeExe();
			if (leave)
				break;
		}

		// mw.closeExe();
		wr.close();
		sc.close();

	}

	/**
	 * main program
	 * 
	 * @param args
	 * @throws IOException
	 */

	public static void main(String[] args) throws IOException {

		try {
			MasterImpl master = new MasterImpl();

			if (args.length > 0) {
				master.wotkerFileName = args[0];
			} else {
				master.wotkerFileName = "WorkerIPs.txt";
			}

			try {
				LocateRegistry.createRegistry(60000);
			} catch (RemoteException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}

			// Naming.rebind(InetAddress.getLocalHost().getHostAddress()
			// .toString(), master);
			// MasterInter stub = (MasterInter)
			// UnicastRemoteObject.exportObject(
			// master, 60000);

			Registry registry = LocateRegistry.getRegistry(60000);

			registry.rebind(InetAddress.getLocalHost().getHostAddress()
					.toString(), master);

			master.startExe();
		} catch (RemoteException e) {
			e.printStackTrace();
		}

	}

	/**
	 * ping workers to see if they are alive
	 * 
	 */
	class PingWorkers implements Runnable {
		public void run() {
			JSch jsch = new JSch();
			int i = 0;
			while (true) {
				// Iterator<NodeData> iter = aliveWorkers.iterator();
				NodeData worker = null;
				while (!aliveWorkers.isEmpty()) {
					try {
						i++;
						if (i > 1000)
							i = 0;
						i = i % aliveWorkers.size();
						worker = aliveWorkers.get(i);
						/*
						 * java.util.Properties config = new
						 * java.util.Properties();
						 * config.put("StrictHostKeyChecking", "no"); Session
						 * session = jsch.getSession(userId, worker.getName() +
						 * ".cs.rit.edu"); session.setPassword(password);
						 * session.setConfig(config); session.connect();
						 * session.disconnect();
						 */

						Registry workerReg = LocateRegistry.getRegistry(
								worker.getIpAddress(), 60000);

						WorkerInter workerInter = (WorkerInter) workerReg
								.lookup(worker.getIpAddress());
						workerInter.ping();

						try {
							Thread.sleep(3000);
						} catch (Exception e) {

						}

					} catch (Exception e) {
						worker.setDown(true);
						System.out.println("Problem");
						synchronized (aliveWorkers) {
							if (aliveWorkers.contains(worker)) {
								aliveWorkers.remove(worker);
								/*
								 * synchronized (workerJobList) {
								 * ArrayList<Integer> failedJobs = workerJobList
								 * .get(worker.getId()); if
								 * (!failedJobs.isEmpty()) { for (int failedJob
								 * : failedJobs) {
								 * jobMap.get(failedJob).isAssigned = false;
								 * jobMap.get(failedJob).isInProcess = false; }
								 * workerJobList.remove(worker.getId()); } }
								 */
							}
						}
						synchronized (deadWorkers) {
							if (!deadWorkers.contains(worker)) {
								deadWorkers.add(worker);
							}
						}
					}
				}

			}
		}
	}

	/**
	 * try to revive dead workers
	 * 
	 */
	class CheckStatus implements Runnable {

		@Override
		public void run() {

			JSch jsch = new JSch();
			int i = 0;
			while (true) {

				// Iterator<NodeData> iter = deadWorkers.iterator();
				NodeData deadWorker = null;
				while (!deadWorkers.isEmpty()) {
					try {

						System.out.println("Inside DEAD ZONE");

						i++;

						if (i > 1000)
							i = 0;

						i = i % deadWorkers.size();
						deadWorker = deadWorkers.get(i);
						ArrayList<Integer> failedJobs = workerJobList
								.get(deadWorker.getId());
						if (!failedJobs.isEmpty() && !deadWorker.getHasAssgnd()) {
							for (int failedJob : failedJobs) {
								jobMap.get(failedJob).isAssigned = false;
								jobMap.get(failedJob).isInProcess = false;
							}
						}

						workerJobList.remove(deadWorker.getId());

						java.util.Properties config = new java.util.Properties();
						config.put("StrictHostKeyChecking", "no");
						Session session = jsch.getSession("pi",
								deadWorker.getIpAddress());
						// session.setPassword(password);
						UserInfo ui = new MyUserInfo();
						session.setUserInfo(ui);
						session.setPassword("raspberry");
						session.setConfig(config);
						session.connect();
						System.out.println("Is conn " + session.isConnected());						
						
						System.out.println("Connected");

						ChannelExec channel = (ChannelExec) session
								.openChannel("exec");
						BufferedReader in = new BufferedReader(
								new InputStreamReader(channel.getInputStream()));

						channel.setCommand("sh LauncherDeadCl.sh "
								+ InetAddress.getLocalHost().getHostAddress()
								+ ";");
						channel.connect();
						String msg = null;

						// Thread.sleep(5000);
						synchronized (deadWorkers) {
							if (deadWorkers.contains(deadWorker)) {
								System.out.println("Removed from dead");
								deadWorkers.remove(deadWorker);
							}
						}

						// synchronized (aliveWorkers) {
						// if (!aliveWorkers.contains(deadWorker)) {
						// aliveWorkers.add(deadWorker);
						// System.out.println("Added to alive");
						// }
						// }

						/*
						 * while ((msg = in.readLine()) != null) {
						 * System.out.println(msg); }
						 */

						channel.disconnect();
						session.disconnect();

					} catch (Exception e) {
						// e.printStackTrace();
						// System.out.println("Problem");
						try {
							// Thread.sleep(5000);
						} catch (Exception e1) {
						}
					}
				}

			}

		}

	}

	/**
	 * gets the worker result
	 */

	@Override
	public synchronized void getSortedResultFromWorker(NodeData.Result result)
			throws RemoteException {
		jobQueue.add(result.res);
		int workerid = ipToIdMapping.get(result.ipAddr);
		NodeData theWorker = workerMap.get(workerid);
		if (!theWorker.isDown()) {
			theWorker.setHasAssgnd(true);
			completedJob += result.howManyJobs;
			/*
			 * System.out.println("Completed Job " + completedJob);
			 * System.out.println("Worker  ID " + workerid);
			 * System.out.println("Number of Jobs done " + result.howManyJobs);
			 * System.out.println("Worker's JobList Size " +
			 * workerJobList.get(workerid).size());
			 */
			synchronized (workerJobList) {

				for (int i = 0; i < result.howManyJobs; ++i) {

					if (!workerJobList.get(workerid).isEmpty())
						workerJobList.get(workerid).remove(0);

					// System.out.println("Completed Job " + completedJob);
				}
			}

		}

		theWorker.setHasAssgnd(false);

	}

}
