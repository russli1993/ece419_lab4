import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.Watcher.Event.EventType;

import java.io.IOException;
import java.util.List;


public class Worker {
    
    String path = "";
    String id = "";
    ZkConnector zkc;
    ZooKeeper zk;
    protected static final List<ACL> acl = Ids.OPEN_ACL_UNSAFE;
    
    public static void main(String[] args) throws IOException {
      
        if (args.length != 1) {
            System.out.println("Usage: java -classpath lib/zookeeper-3.3.2.jar:lib/log4j-1.2.15.jar:. JobTracker zkServer:clientPort");
            return;
        }

        Worker jt = new Worker(args[0]);
        
    }

    public Worker(String hosts) {
        zkc = new ZkConnector();
        System.out.println("Zookeeper connecting "+hosts);
        try {
            zkc.connect(hosts);
            zk = zkc.getZooKeeper();
        } catch(Exception e) {
            System.out.println("Zookeeper connect failed "+ e.getMessage());
        }
        registerWithZookeeper();
        
        
    	while (true){
    		try {
				Thread.sleep(2000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
    	}
       
 
    }
    private String getLastPath(String path){
    	return path.substring(path.lastIndexOf("/") + 1);
    }
    private void registerWithZookeeper(){
    	 try {
    		
    		 path = zk.create("/workers/", null, acl, CreateMode.EPHEMERAL_SEQUENTIAL);
    		
    		 id = getLastPath(path);
    		 zk.create("/assignments/"+id,null, acl,CreateMode.PERSISTENT);
    		 getAssignment();
    	 } catch(KeeperException e) {
    		 e.printStackTrace();
    		 exit();
         } catch(Exception e) {
        	 e.printStackTrace();
             exit();
         } 
    }
    private void getAssignment(){
    	try {
			List<String> tasks = zk.getChildren("/assignments/" + id, taskWatcher); //there should be only one task
			
			
			for(String task: tasks){
				System.out.println("got tasks");
				processTask(task);
				
				
				
			}
			
		} catch (KeeperException | InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }
    
    private void processTask(String assignmentId){
    	//write the results first to make sure the task is not lost
    	String result = "";
    	assignmentId = id +"/"+assignmentId;
    	Stat st = new Stat();
    	System.out.println("processing");
    	String jobId = "";
    	try {
			byte[] data = zk.getData("/assignments/"+assignmentId, null, st);
			MPacket mp = MPacket.deserialize(data);
			jobId = mp.jobId;
			System.out.println("got data");
		} catch (KeeperException | InterruptedException e1) {
			//this should not happen
			e1.printStackTrace();
			System.exit(1);
		}
    	try {
			Thread.sleep(1000);
		} catch (InterruptedException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
    	try {
    		//create a result first
    		Stat result_stat = new Stat();
    		byte[] data = zk.getData("/results/"+jobId+"/"+getLastPath(assignmentId), null, result_stat);
    		MPacket mp = MPacket.deserialize(data);
    		mp.jobResult = "resuls!";
    		zk.setData("/results/"+jobId+"/"+getLastPath(assignmentId), mp.serialize(), result_stat.getVersion());
    		
    		//delete the assignment in assigments
    		zk.delete("/assignments/"+assignmentId, st.getVersion());
			System.out.println("written results");

		} catch (KeeperException | InterruptedException e) {
			//if there is duplicated it is fine
			e.printStackTrace();
		}
		
		//delete the assignment
    }

    Watcher taskWatcher = new Watcher() { 
        public void process(WatchedEvent e) {
            switch(e.getType()){
            case NodeCreated:
            case NodeChildrenChanged:
            	assert "/waitingtasks".equals( e.getPath() );
                getAssignment();
                break;
			default:
				break;
            	
            }
        }
    };
    private void exit(){
    	 System.out.println("cannot register with zookeeper - exiting");
		 System.exit(1);
    }
}
