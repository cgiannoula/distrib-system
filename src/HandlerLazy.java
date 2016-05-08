import java.io.IOException;

public class HandlerLazy extends Thread {
	ChordNode chrdNode;
	private String request;

	public HandlerLazy(ChordNode chordNode, String message) {
		this.chrdNode = chordNode;
		this.request = message;
	}

	@Override
	public void run() {
		String[] requestList = this.request.split(", ");
		try {
			if (requestList[0].equals("insertRepLazy")) {
				
				String key = requestList[1].trim();
				String value = requestList[2].trim();
				int currK = Integer.parseInt(requestList[3].trim());
				int succ = this.chrdNode.getSuccId();
				
				while(currK != 0){
					String msg = "insertRepWH, " + key + ", " + value + "\n";
					Request request = new Request(succ, msg);
					request.sendReceiveRequest();
					
					msg = "find succId" + "\n";
					Request req1 = new Request(succ, msg);
					succ = req1.IdRequest();
					currK--;
				}
				
			}else if (requestList[0].equals("deleteRepLazy")) {
				
				String key = requestList[1].trim();
				int currK = Integer.parseInt(requestList[2].trim());
				int succ = this.chrdNode.getSuccId();
				
				while(currK != 0){
					String msg = "deleteRepWH, " + key + "\n";
					Request request = new Request(succ, msg);
					request.sendReceiveRequest();
					
					msg = "find succId" + "\n";
					Request req1 = new Request(succ, msg);
					succ = req1.IdRequest();
					currK--;
				}
			}
		} catch (ClassNotFoundException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return;
	}
}
