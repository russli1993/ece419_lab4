import java.io.IOException;
import java.net.UnknownHostException;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
/**
 * Client class for interfacing with the user. Has the following jobs: 
 * 1. Issue password hashing tasks to the zooKeeper
 * 2. Check status of a current task
 * 3. Report results of a task
 * @author tanggui
 *
 */

public class ClientDriver {
	private ZkConnector zkc = null;
	private ZooKeeper zooKeeper = null;
	private String jobTrackerPath = "/jobtracker_primary";

	private MSocket mSocket = null;
	
	public ClientDriver(String connectString) throws InterruptedException, KeeperException {
		// init zoo keeper
        this.zkc = new ZkConnector();
        try {
            zkc.connect(connectString);
        } catch(Exception e) {
        	Logger.print("Zookeeper connect "+ e.getMessage());
        }
        this.zooKeeper = zkc.getZooKeeper();
	}
	
	/**
	 * attempts to read znode for primary job tracker. Set a watcher
	 * if node does not exist, wait for watcher event change
	 * if node exists, read data
	 * if existing node deleted, wait for new node creation. then read data
	 * 
	 * @throws InterruptedException
	 * @throws IOException 
	 * @throws UnknownHostException 
	 * @throws KeeperException
	 */
	private void setTrackerSocket() throws InterruptedException {
		boolean gotIP = false;
		Logger.print("Client Driver is trying to obtain a new Job Tracker's IP");
		while (gotIP == false) {
			try {
				Stat stat = new Stat();
				byte[] data = this.zooKeeper.getData(jobTrackerPath, null, stat);
				// TODO: check stat is ok
				MPacket packet = MPacket.deserialize(data);
				this.mSocket = new MSocket(packet.host, packet.port);
				Logger.print("Connected to " + packet.host + ":" + packet.port);
				MPacket newpack= mSocket.readMPacket();
				gotIP = true;
			}
			catch (KeeperException.NoNodeException e) {
				// does not exist yet. what do?
				Logger.print("Job Tracker does not exist. Retrying in 5 seconds...");
				Thread.sleep(5000);
				continue;
			}
			catch (IOException e) {
				Logger.print("Job Tracker IP is unknown. Retrying in 5 seconds...");
				Thread.sleep(5000);
				continue;
			}
			catch (KeeperException e) {
				Logger.print("Something's screwed up with the keeper");
			}
		}
	}

	private void processJob(String jobType, String jobId) throws InterruptedException {
		boolean done = false;
		if (this.mSocket == null) {
			setTrackerSocket();
		}
		while (!done) {
			try {
				if (jobType.equals("job")) {
					newJob(jobType, jobId);
					done = true;
				}
				else if (jobType.equals("status")) {
					jobStatus(jobType, jobId);
					done = true;
				}
				else {
					Logger.print("ERROR: undefined job type. Valid job types are: job or status. Exitting");
					System.exit(-1);
				}
			}
			catch (IOException e) { // must be a socket reset. get another job tracker
				setTrackerSocket();
			}
		}
	}

	private void newJob(String jobType, String jobId) throws IOException {
		MPacket packet = new MPacket();
		packet.jobType = jobType;
		packet.jobId = jobId;
		mSocket.writeMPacket(packet);
	}
	
	private void jobStatus(String jobType, String jobId) throws IOException {
		MPacket packet = new MPacket();
		packet.jobType = jobType;
		packet.jobId = jobId;
		mSocket.writeMPacket(packet);
		MPacket result = mSocket.readMPacket();
		if (result.jobResult == null || result.jobResult.equals("")) {
			System.out.println("job in progress");
		}
		else {
			System.out.println("job finished");
			System.out.println(result.jobResult); // TODO: correct format according to test script
		}
	}

	public static void main(String[] args) throws InterruptedException, KeeperException {
		if (args.length != 3) {
			Logger.print("Invalid syntax. Usage: ClientDriver host:port <jobType> <jobId>");
			return;
		}
		String connectString = args[0]; // host info of zooKeeper
		String jobType = args[1]; // job; status
		String jobId = args[2];

		ClientDriver client = new ClientDriver(connectString);
		client.processJob(jobType, jobId);
	}

}

