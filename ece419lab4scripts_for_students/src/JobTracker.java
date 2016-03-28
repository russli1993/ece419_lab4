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

public class JobTracker {
    
    String primaryPath = "/jobtracker_primary";
    ZkConnector zkc;

    public static void main(String[] args) {
      
        if (args.length != 1) {
            System.out.println("Usage: java -classpath lib/zookeeper-3.3.2.jar:lib/log4j-1.2.15.jar:. JobTracker zkServer:clientPort");
            return;
        }

        JobTracker jt = new JobTracker(args[0]);
        
    }

    public JobTracker(String hosts) {
        zkc = new ZkConnector();
        try {
            zkc.connect(hosts);
        } catch(Exception e) {
            System.out.println("Zookeeper connect "+ e.getMessage());
        }
        attemptToBecomePrimary();
        
        
        	while (true){
        		try {
					Thread.sleep(2000);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
        	}
       
 
    }
    
    private void attemptToBecomePrimary() {
        Stat stat = zkc.exists(primaryPath, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                handleEvent(event);
        
            }});
        if (stat == null) {              // znode doesn't exist; let's try creating it
            System.out.println("Creating " + primaryPath);
            Code ret = zkc.create(
                        primaryPath,         // Path of znode
                        null,           // Data not needed.
                        CreateMode.EPHEMERAL   // Znode type, set to EPHEMERAL.
                        );
            if (ret == Code.OK){
            	System.out.println("the primary!");
            }else{
            	System.out.println(ret);
            }
            
        } 
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
