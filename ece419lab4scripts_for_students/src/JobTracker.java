import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.Watcher.Event.EventType;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.WatchEvent;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

public class JobTracker {
    
    String primaryPath = "/jobtracker_primary";
    ZkConnector zkc;
    ZooKeeper zk;
    Queue<String> waitingTasks = new ConcurrentLinkedQueue<String>();  //full path of task
    String host;
    int port;
    ServerSocket serverSocket;
    
    class ServerThread implements Runnable {
    	ServerSocket serverSocket;
    	MSocket mSock;
    	String host;
    	int port;
    	Queue waitingTasks;
    	ZkConnector zkc;
    	ZooKeeper zk;
    	public void run(){
    		 Socket socket;
			try {
				socket = serverSocket.accept();
				MSocket mSock = new MSocket(socket);
	    	    this.mSock = mSock;
			} catch (IOException e) {
				// TODO Auto-generated catch block
				System.out.println("dfdfadf");
				e.printStackTrace();
				return;
			}
			
			while(true){
				try {
					MPacket mp = this.mSock.readMPacket();
					MPacket rep = new MPacket();
					switch(mp.requestType){
						case JobTrackerAddr:
							System.out.println("job tracker address request");
							rep.requestType = Request.Response;
							rep.host = host;
							rep.port = port;
							this.mSock.writeMPacket(rep);
							break;
						case NewJob:
							rep.jobId = mp.jobId;
							System.out.println("new job arrived");
							for (int i = 0; i<8;i++){
								String path = "/jobs/"+ mp.jobId +"-"+ i;
								rep.partitionId = i;
								zkc.create(path, rep.serialize(), CreateMode.PERSISTENT);
								zkc.create("/results/" + mp.jobId + "/"+getLastPath(path), rep.serialize(), CreateMode.PERSISTENT);
								System.out.println("adding new job/partition");
							}
							
					}
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
    	       
    	}
    	
    	ServerThread(ServerSocket serverSocket, String host, int port, Queue tasks, ZkConnector zkc, ZooKeeper zk){
    		this.serverSocket = serverSocket;
    		this.host = host;
    		this.port = port;
    		this.zkc = zkc;
    		this.zk = zk;
    		this.waitingTasks = tasks;
    	}
    }

    public static void main(String[] args) throws IOException{
      
        if (args.length != 1) {
            System.out.println("Usage: java -classpath lib/zookeeper-3.3.2.jar:lib/log4j-1.2.15.jar:. JobTracker zkServer:clientPort");
            return;
        }

        JobTracker jt = new JobTracker(args[0]);

 
       
    }
    

    public JobTracker(String hosts) throws IOException {
        zkc = new ZkConnector();
        
        System.out.println("Zookeeper connecting "+hosts);
        try {
            zkc.connect(hosts);
        } catch(Exception e) {
            System.out.println("Zookeeper connect failed "+ e.getMessage());
        }
        //start a serverSocket and get the host and port
        serverSocket = new ServerSocket(0);
        host = InetAddress.getLocalHost().getHostAddress();
        port = serverSocket.getLocalPort();
        if (attemptToBecomePrimary()){
        	runAsMaster();
        }else{
        	runAsBackup();
        }
        
       
 
    }
    
    private void initAndCheckConsistency(){
    	//check consistency between assignment and alive workers
    	//List<String> workers = zkc.getChildren("/worker", null);
    	
    	
    	
    	//get the list of all jobs
    	zkc.create("/jobs",null, CreateMode.PERSISTENT);
    	List<String> tasks = zkc.getChildren("/jobs", null,false);
    	for (String task : tasks){
    		
    		this.waitingTasks.add(task);
    		
    	}
    	System.out.println("init - reading all waiting tasks" + tasks.toString());
    	
    	
    }
    Watcher allWorkerWatcher = new Watcher(){
    	public void process(WatchedEvent e){
    		switch(e.getType()){
	            case NodeChildrenChanged:
	            	System.out.println(e.getPath());
	            	assignAllWorker();
	            	break;
				default:
					break;
            	
            }
    	}
    };
    private String getLastPath(String path){
    	return path.substring(path.lastIndexOf("/") + 1);
    }
    
    private void handleTanglingAssignment(String assignment, String assignRecord){
    	//tangling assignment is worker that is in assignment but not in workers dir
    	
		byte[] data = zkc.getData(assignment, null);
		zkc.create("/jobs/"+getLastPath(assignment), data, CreateMode.PERSISTENT);
		this.waitingTasks.add("/jobs/"+getLastPath(assignment));
	
		zkc.delete(assignRecord);
		
		System.out.println("handelTanglingAssignment: "+assignRecord+" : "+"/jobs/"+getLastPath(assignment));
		
    	
    	
    	
    }
    private void assignAllWorker(){
    	//get all workers
    	List<String> workerAssigns = zkc.getChildren("/assignments", null);
    	List<String> allWorkers = zkc.getChildren("/workers", allWorkerWatcher);
    	HashSet<String> freeWorkers = new HashSet<String>();
    	for (String worker : allWorkers){
    		freeWorkers.add(worker);
    	}
    	System.out.println("assign all worker: " + allWorkers.toString());
    	for (String workerAssign:workerAssigns){
    		List<String> taskAssignments = zkc.getChildren("/assignments/"+workerAssign, null, false);
    		if (taskAssignments.size()>0){
    			if (!freeWorkers.remove(workerAssign)){
    				for (String taskAssignment:taskAssignments){
    					handleTanglingAssignment(taskAssignment,"/assignments/"+workerAssign);
    				}
    			}
    			System.out.println("assign all worker (worker with jobs): " + workerAssign);
    		}
    	}
    	for (String worker: freeWorkers){
    		String task = this.waitingTasks.remove();
    		byte[] data = zkc.getData(task, null);
    		zkc.create("/assignments/"+getLastPath(worker)+"/"+ getLastPath(task), data, CreateMode.PERSISTENT);
    		zkc.delete(task);
			System.out.println("assign all worker: "+"/assignments/"+getLastPath(worker)+"/"+ getLastPath(task));

    	}
    	
    	
    }
    
    private void runAsMaster(){
    	//start the server thread
        ServerThread st = new ServerThread(serverSocket, host, port, this.waitingTasks, this.zkc,this.zk);
        (new Thread(st)).run();
        
        //Initialize data structures and check/restore consistent zookeeper state
        initAndCheckConsistency();
        assignAllWorker();
        
        
    }
    
    private void runAsBackup(){
    	  
    	while (true){
    		try {
				Thread.sleep(2000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
    	}
    }
    
    private boolean attemptToBecomePrimary() {
        Stat stat = zkc.exists(primaryPath, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                handleEvent(event);
        
            }});
        if (stat == null) {              // znode doesn't exist; let's try creating it
            System.out.println("Creating " + primaryPath);
            MPacket mp = new MPacket();
            mp.host = host;
            mp.port = port;
            Code ret = zkc.create(
                        primaryPath,         // Path of znode
                        mp.serialize(),           // Data not needed.
                        CreateMode.EPHEMERAL   // Znode type, set to EPHEMERAL.
                        );
            
            if (ret == Code.OK){
            	System.out.println("the primary!");
            	return true;
            }else{
            	System.out.println(ret);
            	return false;
            }
            
        } 
        return false;
    }

    private void handleEvent(WatchedEvent event) {
        String path = event.getPath();
        EventType type = event.getType();
        if(path.equalsIgnoreCase(primaryPath)) {
            if (type == EventType.NodeDeleted) {
                System.out.println("primary is dead! Attempt to become primary!");       
                attemptToBecomePrimary(); // try to become the boss
            }
        }
    }
   

}
