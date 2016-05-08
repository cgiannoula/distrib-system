import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Set;
import java.io.*;
import java.net.*;

/*
 * ChordNode class
 * This is the class for a node-server.
 * Each node implements insert/delete/query requests
 * and the first node handle also join/depart requests for nodes.
 */
public class ChordNode extends Thread implements Comparable<Object>{
	public static int port = 49152;
	public static int k = 3;	// factor for replicas (if there is no replicas this factor must be 1)
	public static int mode = 1;	// mode = (0,1,2) => (noReplicas(with k=1), Linearizability, EventualConsistency)
	Chord chord;		// Just for debug
	private int alive;	// when alive == 0 the thread dies
	private int id;
	private String nodeId;
	private int predId;
	private String predNodeId;
	private int succId;
	private String succNodeId;
	private ArrayList<Socket> client = new ArrayList<Socket>();
	private Hashtable<String, String> nodeMap = new Hashtable<String, String>();
	private Hashtable<String, String> replicaMap = new Hashtable<String, String>();
	private ServerSocket s;
	private Object lock = new Object();

	public ChordNode(Chord chrd, int serialNo) throws NoSuchAlgorithmException {
		this.chord = chrd;
		String serialNoString = Integer.toString(serialNo);
		this.nodeId = Hash.hash(serialNoString);
		this.id = serialNo;
		this.alive = 1;
	}

	@Override
	public void run() {
		try {
			s = new ServerSocket(ChordNode.port + id); 
			for (;;) {
				if(this.alive == 0)
					break;
				Socket incoming = s.accept();
				Thread t = new Handler(this, incoming);
				t.start();
				//t.join(); // if we want to serve a request at a time 
			}
			System.out.println("A thread-node died");
			return;
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/*
	 * Insert key
	 */
	public String insertKey(String key, String value, int s)
			throws NoSuchAlgorithmException, UnknownHostException, IOException, ClassNotFoundException {
		String chrdKey = new String(Hash.hash(key));

		if ((this.predNodeId.compareTo(this.getNodeId()) > 0)) { 
			if (chrdKey.compareTo(this.predNodeId) > 0
					|| chrdKey.compareTo(this.getNodeId()) <= 0) {
				synchronized(this.getLock()){
					if (nodeMap.containsKey(chrdKey)) {
						nodeMap.remove(chrdKey);
					}
					nodeMap.put(chrdKey, value);
				}
				if(ChordNode.mode == 1){
					String message = "insertRep, " + chrdKey + ", " + value + ", " + (ChordNode.k-1) + "\n";
					Request req = new Request(this.getSuccId(), message);
					req.sendRequest(s);
					return new String("NOT");
				}else if(ChordNode.mode == 2){ 
					/*
					 * Create a new thread to insert replicas
					 */
					String message = "insertRepLazy, " + chrdKey + ", " + value + ", " + (ChordNode.k-1) + "\n";
					Thread t = new HandlerLazy(this, message);
					t.start();
					return new String("OK");
				}
				
				return new String("OK");
				
			}else {
				String message = "insertS, " + key + ", " + value + "\n";
				Request req = new Request(this.getSuccId(), message);
				req.sendRequest(s);
				return new String("NOT");
			}
		} else { 
			if ((chrdKey.compareTo(this.predNodeId) > 0)
					&& (chrdKey.compareTo(this.getNodeId()) <= 0)) {
				synchronized(this.getLock()){ 
					if (nodeMap.containsKey(chrdKey)) {
						nodeMap.remove(chrdKey);
					}
					nodeMap.put(chrdKey, value);
				}
				if(ChordNode.mode == 1){
					String message = "insertRep, " + chrdKey + ", " + value + ", " + (ChordNode.k-1) + "\n";
					Request req = new Request(this.getSuccId(), message);
					req.sendRequest(s);
					return new String("NOT");
				}else if(ChordNode.mode == 2){ 
					/*
					 * Create a new thread to insert replicas
					 */
					String message = "insertRepLazy, " + chrdKey + ", " + value + ", " + (ChordNode.k-1) + "\n";
					Thread t = new HandlerLazy(this, message);
					t.start();
					return new String("OK");
				}
				
				return new String("OK");
			}else {
				String message = "insertS, " + key + ", " + value + "\n";
				Request req = new Request(this.getSuccId(), message);
				req.sendRequest(s);
				return new String("NOT");
			}
		}
	}
	
	/*
	 * Delete key from a node
	 */
	public String deleteKey(String key, int s) throws NoSuchAlgorithmException,
			UnknownHostException, IOException, ClassNotFoundException {
		String chrdKey = new String(Hash.hash(key));

		if ((this.predNodeId.compareTo(this.getNodeId()) > 0)) {  
			if (chrdKey.compareTo(this.predNodeId) > 0
					|| chrdKey.compareTo(this.getNodeId()) <= 0) {
				Boolean flag; 
				synchronized(this.getLock()){ 
					flag = nodeMap.containsKey(chrdKey);
				}
				if (flag) {	
					
					if(ChordNode.mode == 1){
						synchronized(this.getLock()){
							nodeMap.remove(chrdKey);
						}
						String message = "deleteRep, " + chrdKey + ", " + (ChordNode.k-1) + "\n";
						Request req = new Request(this.getSuccId(), message);
						req.sendRequest(s);
						return new String("NOT");
					}else if(ChordNode.mode == 2){
						/*
						 * Create a new thread to delete replicas
						 */
						String message = "deleteRepLazy, " + chrdKey + ", " + (ChordNode.k-1) + "\n";
						Thread t = new HandlerLazy(this, message);
						t.start();
					}
					String value1;
					synchronized(this.getLock()){
						value1 = nodeMap.remove(chrdKey);
					}
					return value1;
					
				} else {
					System.out.println("This key doesn't exist !");
					return new String("NOT");
				}				
			}else {
				String message = "deleteS, " + key + "\n";
				Request req = new Request(this.getSuccId(), message);
				req.sendRequest(s);
				return new String("NOT");
			}
		} else { 
			if ((chrdKey.compareTo(this.predNodeId) > 0)
					&& (chrdKey.compareTo(this.getNodeId()) <= 0)) {
				Boolean flag;
				synchronized(this.getLock()){
					flag = nodeMap.containsKey(chrdKey);
				}
				if(flag) {	
					
					if(ChordNode.mode == 1){
						synchronized(this.getLock()){
							nodeMap.remove(chrdKey);
						}
						String message = "deleteRep, " + chrdKey + ", " + (ChordNode.k-1) + "\n";
						Request req = new Request(this.getSuccId(), message);
						req.sendRequest(s);
						return new String("NOT");
					}else if(ChordNode.mode == 2){
						/*
						 * Create a new thread to delete replicas
						 */
						String message = "deleteRepLazy, " + chrdKey + ", " + (ChordNode.k-1) + "\n";
						Thread t = new HandlerLazy(this, message);
						t.start();
					}
					String value1;
					synchronized(this.getLock()){
						value1 = nodeMap.remove(chrdKey);
					}
					return value1;

				} else {
					System.out.println("This key doesn't exist !");
					return new String("NOT");
				}
			}else {
				String message = "deleteS, " + key + "\n";
				Request req = new Request(this.getSuccId(), message);
				req.sendRequest(s);
				return new String("NOT");
			}
		}
	}
	
	/*
	 * The character '*' returns all the keys in the chord
	 * and the string '**' returns the keys in the current node-server.
	 */
	public Hashtable<String, String> queryKey(String key, int s)
			throws NoSuchAlgorithmException, UnknownHostException, IOException, ClassNotFoundException {
		Hashtable<String, String> keyArray = new Hashtable<String, String>();
		String chrdKey = new String(Hash.hash(key));

		if (!key.equals("*") && !key.equals("**")) {
			if (this.predNodeId.compareTo(this.getNodeId()) > 0) {   
				if (chrdKey.compareTo(this.predNodeId) > 0
						|| chrdKey.compareTo(this.getNodeId()) <= 0) {
					Boolean flag;
					synchronized(this.getLock()){
						flag = nodeMap.containsKey(chrdKey);
					}
					if (flag) {
						if(ChordNode.mode == 0){
							synchronized(this.getLock()){
								keyArray.put(key, nodeMap.get(chrdKey));
							}
							return keyArray;
						}else if(ChordNode.mode == 1){
							String message = "queryRep, " + key + ", " + chrdKey + "\n"; 
							Request req = new Request(findInChain(ChordNode.k - 2, this.succId), message);
							req.sendRequest(s);
							return null;
						}
						return null;
					} else {
						System.out.println("This key doesn't exist !");
						keyArray.put("The key doesn't exist", "The key doesn't exist");
						return keyArray;
					}
				} else {
					String message = "queryS, " + key + "\n";
					Request req = new Request(this.getSuccId(), message);
					req.sendRequest(s);
					return null;
				}
			} else {
				if ((chrdKey.compareTo(this.predNodeId) > 0)
						&& (chrdKey.compareTo(this.getNodeId()) <= 0)) {
					Boolean flag;
					synchronized(this.getLock()){
						flag = nodeMap.containsKey(chrdKey);
					}
					if (flag) {
						
						if(ChordNode.mode == 0){
							synchronized(this.getLock()){
								keyArray.put(key, nodeMap.get(chrdKey));
							}
							return keyArray;
						}else if(ChordNode.mode == 1){
							String message = "queryRep, " + key + ", " + chrdKey + "\n"; 
							Request req = new Request(findInChain(ChordNode.k - 2, this.succId), message);
							req.sendRequest(s);
							return null;
						}
						return null;
					} else {
						System.out.println("This key doesn't exist !");
						keyArray.put("The key doesn't exist", "The key doesn't exist");
						return keyArray;
					}
				} else {
					String message = "queryS, " + key + "\n";
					Request req = new Request(this.getSuccId(), message);
					req.sendRequest(s);
					return null;
				}
			}
		} else if (!key.equals("*")) {
			return this.nodeMap;
		} else {
			return requestAllKeys();
		}
	}
	
	/*
	 * Request for query for all keys must be transfered to all nodes
	 */
	public Hashtable<String, String> requestAllKeys() throws UnknownHostException, ClassNotFoundException, IOException {
		Hashtable<String, String> response = new Hashtable<String, String>();
		if (!this.nodeMap.isEmpty()) { 
			response.putAll(this.getHashTable());
		}

		int succ = this.succId;
		String message1 = "getNodeMap" + "\n";
		String message2 = "find succId" + "\n";
		
		while (succ != this.id) {
			Request req1 = new Request(succ, message1);
			Hashtable <String, String> readObject1 = req1.queryRequest();
				
			if (!readObject1.isEmpty())
				response.putAll(readObject1);
			
			Request req2 = new Request(succ, message2);
			succ = req2.IdRequest(); 
		}
		return response;
	}
	
	/*
	 * Search if key exists in replicaMap OR nodeMap
	 * if not query succNode
	 */
	public Hashtable<String, String> queryKeyLazy(String key,int begId, int s, int cycle)
			throws NoSuchAlgorithmException, UnknownHostException, IOException, ClassNotFoundException {
		Hashtable<String, String> keyArray = new Hashtable<String, String>();
		String chrdKey = new String(Hash.hash(key));
		//System.out.print(chrdKey); System.out.println(begId);
		if(this.getSerialNo() == begId && cycle == 1){
			System.out.println("This key doesn't exist !");
			keyArray.put("The key doesn't exist", "The key doesn't exist");
			return keyArray;
		}

		if (!key.equals("*") && !key.equals("**")) {
			Boolean flag;
			synchronized(this.getLock()){
				flag = this.replicaMap.containsKey(chrdKey);
			}
			if(flag){
				synchronized(this.getLock()){
					keyArray.put(key, this.replicaMap.get(chrdKey));
				}
				return keyArray;
			}else{
				String message = "queryS, " + key + ", " + begId + "\n";
				Request req = new Request(this.getSuccId(), message);
				req.sendRequest(s);
				return null;
			}
		} else if (!key.equals("*")) {
			return this.nodeMap;
		} else {
			return requestAllKeys();
		}
	}
	
	
	/*
	 * Join request for a node
	 * It gives the serialNodeNo as input for the new node
	 */
	public String joinNode(int nodeId) throws NoSuchAlgorithmException, UnknownHostException, ClassNotFoundException, IOException, InterruptedException {
		String response = new String("OK");
		/*
		 * Find  position in the chord
		 * Set new neighbors
		 * Grab the keys from successor
		 * Join the chord
		 */
		Hashtable<String, String> chrdNodeMap = new Hashtable<String, String>();
		ChordNode chrdNode = this.chord.createNode(nodeId);	// Add to nodeList Just for debug

		// SOS check if the node with nodeId already exists in the chord !!!
		int succ = this.getSerialNo();
		int pred = this.predId;
		String succVal = this.getNodeId();
		String predVal = this.predNodeId;
		int bool = 0;
		String message1 = "find succId" + "\n";
		String message2 = "find succNodeId" + "\n";
		
		while (true){
			if(predVal.compareTo(succVal) > 0){ 
				if((chrdNode.getNodeId().compareTo(predVal) > 0) || 
						(chrdNode.getNodeId().compareTo(succVal) < 0))
					bool = 1;
			}else{
				if((chrdNode.getNodeId().compareTo(predVal) > 0) && 
						(chrdNode.getNodeId().compareTo(succVal) < 0))
					bool = 1;
			}
			
			if(bool == 1)
				break;
			
			pred = succ;
			predVal = succVal;
			if(succ == this.getSerialNo()){
				succ = this.getSuccId();
				succVal = this.getSuccNodeId();
			}else{
				int temp = succ;
				Request req1 = new Request(succ, message1);
				succ = req1.IdRequest();
				Request req2 = new Request(temp, message2);
				succVal = req2.NodeIdRequest();
			}

		}
		
		//Succ's HashTable
		String message = "getNodeMap" + "\n";
		Request req = new Request(succ, message);
		Hashtable <String, String> tbl = req.queryRequest();
		Enumeration<String> enumer = tbl.keys();
		
		//Succ's last in chain
		int succLast = findInChain(ChordNode.k - 1, succ);
		
		while (enumer.hasMoreElements()) {
			String key = (String) enumer.nextElement();
			if (key.compareTo(chrdNode.getNodeId()) <= 0 || key.compareTo(chrdNode.getPredNodeId()) > 0) {
				String value = tbl.get(key);
				String msg = "deleteWH, " + key + "\n";
				Request request = new Request(succ, msg);
				request.sendReceiveRequest(); 
				
				// put key in new node's hashTable
				chrdNodeMap.put(key, value);
				
				if(ChordNode.k != 1){
					//insert keys to succ's replicaMap
					msg = "insertRepWH, " + key + ", " + value + "\n";
					request = new Request(succ, msg);
					request.sendReceiveRequest();
					
					//delete keys to succ's lastSucc replicaMap
					msg = "deleteRepWH, " + key + "\n";
					request = new Request(succLast, msg);
					request.sendReceiveRequest();
				}
			}
		}
		chrdNode.setHashTable(chrdNodeMap);
		
		if(ChordNode.k != 1){
			int currK = 0;
			int predK = pred;
			
			while(currK != ChordNode.k - 1){
				
				//Get NodeMap from k-1 predecessors
				String msg = "getNodeMap, " + "\n"; 
				Request request = new Request(predK, msg);
				chrdNode.replicaMap.putAll(request.queryRequest());
				
				//(k-1) predecessors must delete their NodeMap from their lastSucc's replicasMap
				msg = "deleteTableFromLastInChain, " + "\n"; 
				request = new Request(predK, msg);
				request.sendReceiveRequest();
				
				msg = "find predId, " + "\n";
				request = new Request(predK, msg);
				predK = request.IdRequest();
				
				currK++;
			}
		}
		
		// Set new neighbors
		chrdNode.setPredId(pred);
		chrdNode.setPredNodeId(predVal);
		chrdNode.setSuccId(succ);
		chrdNode.setSuccNodeId(succVal);
		String msg1 = "Set Pred, " + chrdNode.getSerialNo() + ", " + chrdNode.getNodeId() + "\n";
		Request request1 = new Request(succ, msg1);
		request1.sendReceiveRequest();
		
		String msg2 = "Set Succ, " + chrdNode.getSerialNo() + ", " + chrdNode.getNodeId() + "\n";
		Request request2 = new Request(pred, msg2);
		request2.sendReceiveRequest();
		
		return response;
	}
	
	public void deleteTableFromLast()throws UnknownHostException, ClassNotFoundException, IOException{
		int last = findInChain(k-2, this.succId);
		
		Set<String> keys = this.nodeMap.keySet();
		Iterator<String> itr = keys.iterator();
		// Delete without hashing the key 
		while(itr.hasNext()){
			String str = itr.next();
			String msg = "deleteRepWH, " + str + "\n";
			Request request = new Request(last, msg);
			request.sendReceiveRequest();
		}
	}
	
	/*
	 * Depart request  
	 */
	public String departNode(int nodeId) throws NoSuchAlgorithmException, IOException, ClassNotFoundException, InterruptedException {		
		String response = new String("OK");
		/*
		 * Get successor and hashtable of deleted node
		 * Release node from chord and set the new neighbors
		 * Transfer hashtable to successor 
		 */		
		
		int succLast = findInChain(ChordNode.k - 1, this.succId);
		
		Set<String> keys = this.nodeMap.keySet();
		Iterator<String> itr = keys.iterator();
		// Insert without hashing the key 
		while(itr.hasNext()){
			String str = itr.next();
			String msg = "insertWH, " + str + ", " + this.nodeMap.get(str) + "\n";
			Request request = new Request(this.succId, msg);
			request.sendReceiveRequest();
			
			if(ChordNode.k != 1){
				// Delete replicas from successor
				msg = "deleteRepWH, " + str + "\n";
				request = new Request(this.succId, msg);
				request.sendReceiveRequest();
				
				// Insert keys in replicaMap to the last node in successor's chain
				msg = "insertRepWH, " + str + ", " + this.nodeMap.get(str) + "\n";
				request = new Request(succLast, msg);
				request.sendReceiveRequest();
			}
		}	
		
		this.chord.nodeList.remove(this);	// Just for debug
		String msg1 = "Set Pred, " + this.predId + ", " + this.predNodeId + "\n";
		Request request1 = new Request(this.succId, msg1);
		request1.sendReceiveRequest();
		
		String msg2 = "Set Succ, " + this.succId + ", " + this.succNodeId + "\n";
		Request request2 = new Request(this.predId, msg2);
		request2.sendReceiveRequest();
		
		if(ChordNode.k != 1){
			//Predecessors must transfer their table to new lastSucc in their chain
			int pr = this.predId;
			int currK = 0;
			String message = "find predId" + "\n";
			
			while(currK != ChordNode.k - 1){
				String msg3 = "transferTableToLastInChain" + "\n";
				Request request3 = new Request(pr, msg3);
				request3.sendReceiveRequest();
				
				Request req2 = new Request(pr, message);
				pr = req2.IdRequest();
				currK++;
			}
		}
		
		this.alive = 0; // the thread dies
		return response;
	}
	
	public void transferTableToLast() throws UnknownHostException, ClassNotFoundException, IOException{
		
		int last = findInChain(k-2, this.succId);
		
		Set<String> keys = this.nodeMap.keySet();
		Iterator<String> itr = keys.iterator();
		// Insert without hashing the key 
		while(itr.hasNext()){
			String str = itr.next();
			String msg = "insertRepWH, " + str + ", " + this.nodeMap.get(str) + "\n";
			Request request = new Request(last, msg);
			request.sendReceiveRequest();
		}
		
	}
	
	public int findInChain(int kth, int succR) throws UnknownHostException, ClassNotFoundException, IOException{
		
		if(kth < 1)
			return 0;
		
		int currK = 0;
		int succ = succR;
		String message = "find succId" + "\n";
		
		while (currK != kth) {
			Request req = new Request(succ, message);
			succ = req.IdRequest(); 
			currK++;
		}
		
		return succ;
	}
	
	public String putReplicas(String key, String value, int k, int s) throws UnknownHostException, ClassNotFoundException, IOException {
		
		synchronized(this.getLock()){
			if(this.replicaMap.containsKey(key)){
				this.replicaMap.remove(key);
			}
			this.replicaMap.put(key, value);		
		}
		if(k == 1){
			return new String("OK");
		}else{
			String message = "insertRep, " + key + ", " + value + ", " + (k-1) + "\n"; 
			Request req = new Request(this.getSuccId(), message);
			req.sendRequest(s);
			return new String("NOT");
		}
	}
	
	public String removeReplicas(String key, int k, int s) throws UnknownHostException, ClassNotFoundException, IOException {
		
		Boolean flag;
		synchronized(this.getLock()){
			flag = this.replicaMap.containsKey(key);
		}
		if(!flag){
			return new String("ERROR");
		}
		String value;
		synchronized(this.getLock()){
			value = this.replicaMap.remove(key);
		}
		if(k == 1){
			return value;
		}else{
			String message = "deleteRep, " + key + ", " + (k-1) + "\n"; 
			Request req = new Request(this.succId, message);
			req.sendRequest(s);
			return new String("NOT");
		}
	}
	
	public void setPredId(int pred) {
		this.predId = pred;
	}

	public int getPredId() {
		return this.predId;
	}

	public void setSuccId(int succ) {
		this.succId = succ;
	}

	public int getSuccId() {
		return this.succId;
	}
	public void setPredNodeId(String pred) {
		this.predNodeId = pred;
	}

	public String getPredNodeId() {
		return this.predNodeId;
	}

	public void setSuccNodeId(String succ) {
		this.succNodeId = succ;
	}

	public String getSuccNodeId() {
		return this.succNodeId;
	}

	public void setSerialNo(int serialNo) {
		this.id = serialNo;
	}

	public int getSerialNo() {
		return this.id;
	}

	public String getNodeId() {
		return this.nodeId;
	}
	
	public void setHashTable(Hashtable<String, String> hash) {
		this.nodeMap = hash;
	}

	public Hashtable<String, String> getHashTable() {
		return this.nodeMap;
	}
	
	public void setRepTable(Hashtable<String, String> rep) {
		this.replicaMap = rep;
	}

	public Hashtable<String, String> getRepTable() {
		return this.replicaMap;
	}
	
	public void setClient(Socket incoming) {
		this.client.add(incoming);
	}
	
	public Socket getClient(int i){
		Socket ret;
		synchronized(this.getLock()){
			ret = this.client.get(i);
		}
		return ret;
	}
	
	public ArrayList<Socket> getClientList(){
		return this.client;
	}
	
	public Object getLock(){
		return this.lock;
	}
	
	@Override
	public int compareTo(Object arg0) {
		// TODO Auto-generated method stub
		ChordNode s = (ChordNode) arg0;
		return this.nodeId.compareTo(s.getNodeId());
	}

}

