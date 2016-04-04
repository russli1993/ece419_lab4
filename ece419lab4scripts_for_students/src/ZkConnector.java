import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooKeeper.States;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

import java.util.concurrent.CountDownLatch;
import java.util.ArrayList;
import java.util.List;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;

public class ZkConnector implements Watcher {

    // ZooKeeper Object
    ZooKeeper zooKeeper;

    // To block any operation until ZooKeeper is connected. It's initialized
    // with count 1, that is, ZooKeeper connect state.
    CountDownLatch connectedSignal = new CountDownLatch(1);
    
    // ACL, set to Completely Open
    protected static final List<ACL> acl = Ids.OPEN_ACL_UNSAFE;

    /**
     * Connects to ZooKeeper servers specified by hosts.
     * 
     * 
     */
    public void connect(String hosts) throws IOException, InterruptedException {

        zooKeeper = new ZooKeeper(
                hosts, // ZooKeeper service hosts
                5000,  // Session timeout in milliseconds
                this); // watcher - see process method for callbacks
	    connectedSignal.await();
    }

    /**
     * Closes connection with ZooKeeper
     */
    public void close() throws InterruptedException {
	    zooKeeper.close();
    }

    /**
     * @return the zooKeeper
     */
    public ZooKeeper getZooKeeper() {
        // Verify ZooKeeper's validity
        if (null == zooKeeper || !zooKeeper.getState().equals(States.CONNECTED)) {
	        throw new IllegalStateException ("ZooKeeper is not connected.");
        }
        return zooKeeper;
    }

    protected Stat exists(String path, Watcher watch) {
        
        Stat stat =null;
        try {
            stat = zooKeeper.exists(path, watch);
        } catch(Exception e) {
        }
        
        return stat;
    }

    protected KeeperException.Code create(String path, byte[] data, CreateMode mode) {
        
        try {
            byte[] byteData = null;
            if(data != null) {
                byteData = data;
            }
            zooKeeper.create(path, byteData, acl, mode);
            
        } catch(KeeperException e) {
            return e.code();
        } catch(Exception e) {
            return KeeperException.Code.SYSTEMERROR;
        }
        
        return KeeperException.Code.OK;
    }
    
    protected List<String> getChildren(String path, Watcher wt){
    	try{
    		List<String> list = zooKeeper.getChildren(path, wt);
    		List<String> ret = new ArrayList<String>();
    		for (String i : list){
    			ret.add(path+"/"+i);
    		}
    		return ret;
    	}catch(KeeperException e) {
            return new ArrayList<String>();
        } catch(Exception e) {
            return new ArrayList<String>();
        }
    }
    protected byte[] getData(String path, Watcher wt){
    	try{
    		Stat st = new Stat();
    		return zooKeeper.getData(path, wt, st);
    	}catch(KeeperException e) {
            return new byte[0];
        } catch(Exception e) {
            return new byte[0];
        }
    }
    protected MPacket getDataDes(String path, Watcher wt) throws IOException, ClassNotFoundException{
    	ByteArrayInputStream in = new ByteArrayInputStream(getData(path,wt));
    	ObjectInputStream is = new ObjectInputStream(in);
    	return (MPacket)is.readObject();
    	
    }
    protected boolean delete(String path){
    	try{
    		Stat st = zooKeeper.exists(path, null);
    		zooKeeper.delete(path, st.getVersion());
    		return true;
    	}catch(KeeperException e) {
            return delete(path);
        } catch(Exception e) {
        	return delete(path);
        }
    }
    protected List<String> getChildren(String path, Watcher wt, boolean base){
    	try{
    		List<String> list = zooKeeper.getChildren(path, wt);
    		if (base){
	    		List<String> ret = new ArrayList<String>();
	    		for (String i : list){
	    			ret.add(path+"/"+i);
	    		}
	    		return ret;
    		}else{
    			return list;
    		}
    	}catch(KeeperException e) {
            return new ArrayList<String>();
        } catch(Exception e) {
            return new ArrayList<String>();
        }
    }
    public void process(WatchedEvent event) {
        // release lock if ZooKeeper is connected.
        if (event.getState() == KeeperState.SyncConnected) {
            connectedSignal.countDown();
        }
    }
}

