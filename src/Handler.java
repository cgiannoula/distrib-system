import java.io.*;
import java.net.*;
import java.util.Hashtable;

/*
 * Class for handling requests to nodes-servers
 */
public class Handler extends Thread {
	ChordNode chrdNode;
	private Socket incoming;

	Handler(ChordNode chrdNode, Socket incoming) {
		this.chrdNode = chrdNode;
		this.incoming = incoming;
	}

	@Override
	public void run() {
		try {
			ObjectInputStream in = new ObjectInputStream(this.incoming.getInputStream());
			String str = (String) in.readObject();
			String[] requestList = str.split(", ");
		
			if (requestList[0].trim().equals("insert")) {
				int client = (int) in.readObject();
				String response = this.chrdNode.insertKey(requestList[1].trim(), requestList[2].trim(), client);

				if(response.equals("OK")){
					writeClient(client, response);
				}
				
			}else if(requestList[0].trim().equals("insertS")) {
				
				int client = (int) in.readObject();
				String response = this.chrdNode.insertKey(requestList[1].trim(), requestList[2].trim(), client);

				if(response.equals("OK")){ 
					writeClient(client, response);
				}		
				this.incoming.close();
			}else if (requestList[0].trim().equals("insertWH")) {
				synchronized(this.chrdNode.getLock()){
					this.chrdNode.getHashTable().put(requestList[1].trim(),requestList[2].trim());
				}
				String response = new String("OK");
				writeSocket(this.incoming, response);

			}else if (requestList[0].trim().equals("insertRepWH")) {
				synchronized(this.chrdNode.getLock()){
					this.chrdNode.getRepTable().put(requestList[1].trim(), requestList[2].trim());
				}
				String response = new String("OK");
				writeSocket(this.incoming, response);

			}else if (requestList[0].equals("insertRep")) {
				int k = Integer.parseInt(requestList[3].trim());		
				int client = (int) in.readObject();
				String response = this.chrdNode.putReplicas(requestList[1].trim(), requestList[2].trim(), k, client);
				
				if(response.equals("OK")){
					writeClient(client, response);
				}
				this.incoming.close();
			}else if(requestList[0].trim().equals("delete")) {
				int client = (int) in.readObject();
				String response = this.chrdNode.deleteKey(requestList[1].trim(), client);

				if(!response.equals("NOT")){
					writeClient(client, response);
				}
				
			}else if(requestList[0].trim().equals("deleteS")) {
				int client = (int) in.readObject();
				String response = this.chrdNode.deleteKey(requestList[1].trim(), client);

				if(!response.equals("NOT")){
					writeClient(client, response);
				}	
				this.incoming.close();
			}else if (requestList[0].trim().equals("deleteWH")) {
				synchronized(this.chrdNode.getLock()){
					this.chrdNode.getHashTable().remove(requestList[1].trim());
				}
				String response = new String("OK");
				writeSocket(this.incoming, response);

			}else if (requestList[0].trim().equals("deleteRepWH")) {
				synchronized(this.chrdNode.getLock()){
					this.chrdNode.getRepTable().remove(requestList[1].trim());
				}
				String response = new String("OK");				
				writeSocket(this.incoming, response);

			}else if (requestList[0].equals("deleteRep")) {
				int k = Integer.parseInt(requestList[2].trim());
				int client = (int) in.readObject();
				String response = this.chrdNode.removeReplicas(requestList[1].trim(), k, client);

				if(!response.equals("NOT")){
					writeClient(client, response);
				}
				this.incoming.close();

			}else if (requestList[0].trim().equals("query")) {
				int client = (int) in.readObject();
				Hashtable<String, String> response;
				if(ChordNode.mode == 2){
					response = this.chrdNode.queryKeyLazy(requestList[1].trim(), this.chrdNode.getSerialNo(), client, 0);
				}else{
					response = this.chrdNode.queryKey(requestList[1].trim(), client);
				}
				//Hashtable<String, String> response = this.chrdNode.queryKey2(requestList[1].trim(), this.incoming, 0);
				
				if(response != null){
					writeClient(client, response);
				}
				
			}else if(requestList[0].trim().equals("queryS")) {
				int client = (int) in.readObject();
				Hashtable<String, String> response;
				if(ChordNode.mode == 2){
					response = this.chrdNode.queryKeyLazy(requestList[1].trim(), Integer.parseInt(requestList[2].trim()), client, 1);
				}else{
					response = this.chrdNode.queryKey(requestList[1].trim(), client);
				}
				//Hashtable<String, String> response = this.chrdNode.queryKey2(requestList[1].trim(), s, 1);

				if(response != null){
					writeClient(client, response);
				}	
				this.incoming.close();
			}else if (requestList[0].equals("queryRep")) {
				int client = (int) in.readObject();
				String value;
				synchronized(this.chrdNode.getLock()){
					value = this.chrdNode.getRepTable().get(requestList[2].trim());
				}
				Hashtable<String, String> response = new Hashtable<String, String>();
				response.put(requestList[1].trim(), value);
				writeClient(client, response);
				this.incoming.close();
			}else if (requestList[0].trim().equals("join node")) {
				/*
				 * Requests for join node are handled only from the first
				 * node-server in the chord. This is the node with
				 * serialNodeNo = 1
				 */
				int nodeId = Integer.parseInt(requestList[1].trim());
				String response = this.chrdNode.joinNode(nodeId);
				writeSocket(this.incoming, response);
				
			}else if (requestList[0].trim().equals("depart node")) {
				int nodeId = Integer.parseInt(requestList[1].trim());		
				String response = this.chrdNode.departNode(nodeId);
				writeSocket(this.incoming, response);

			}else if (requestList[0].trim().equals("find succId")) {
				int response = this.chrdNode.getSuccId();
				writeSocket(this.incoming, response);

			}else if (requestList[0].trim().equals("find succNodeId")) {
				String response = this.chrdNode.getSuccNodeId();
				writeSocket(this.incoming, response);

			}else if (requestList[0].trim().equals("find predId")) {
				int response = this.chrdNode.getPredId();
				writeSocket(this.incoming, response);

			}else if (requestList[0].trim().equals("Set Pred")) {
				this.chrdNode.setPredId(Integer.parseInt(requestList[1].trim()));
				this.chrdNode.setPredNodeId(requestList[2].trim());
				String response = new String("OK");
				writeSocket(this.incoming, response);

			}else if (requestList[0].trim().equals("Set Succ")) {
				this.chrdNode.setSuccId(Integer.parseInt(requestList[1].trim()));
				this.chrdNode.setSuccNodeId(requestList[2].trim());
				String response = new String("OK");
				writeSocket(this.incoming, response);

			}else if (requestList[0].trim().equals("getNodeMap")) {
				Hashtable<String, String> response;
				synchronized(this.chrdNode.getLock()){
					response = this.chrdNode.getHashTable();
				}
				writeSocket(this.incoming, response);

			}else if(requestList[0].trim().equals("transferTableToLastInChain")) {
				this.chrdNode.transferTableToLast();
				String response = new String("OK");
				writeSocket(this.incoming, response);

			}else if (requestList[0].trim().equals("deleteTableFromLastInChain")) {
				this.chrdNode.deleteTableFromLast();
				String response = new String("OK");
				writeSocket(this.incoming, response);

			}else{
				System.out.println("There is not such request");
				System.out.println(requestList[0]);
				this.incoming.close();
			}
			
			return;
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public void writeSocket(Socket s, Object response){
		try{
			ObjectOutputStream out = new ObjectOutputStream(s.getOutputStream());
			out.writeObject(response);
			out.flush();
			
			s.close();
		}catch(Exception e){
			e.printStackTrace();
		}
	}
	
	public void writeClient(int client, Object response){
		try{ 
			Socket s = new Socket("127.0.0.1", client);
			
			ObjectOutputStream out = new ObjectOutputStream(s.getOutputStream()); 
			out.writeObject(response);  
			out.flush();
			
			s.close(); 
		}catch(Exception e){
			e.printStackTrace();
		}
	}
}
