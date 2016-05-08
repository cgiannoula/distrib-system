import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Random;


public class MainInsert {

	public static void main(String[] args) {
		try {
			// Read from file
			BufferedReader br = new BufferedReader(new FileReader(args[0]));
			ArrayList<String> readList = new ArrayList<String>();
			String currLine;
			while((currLine = br.readLine()) != null)
				readList.add(currLine);
			
			// Open a file for writing
			PrintWriter writer = new PrintWriter(args[1]);
			
			// Add "insert" in inputs
			currLine = new String("insert, ");
			int size = readList.size();
			for(int i = 0; i < size; i++) {
				String curr = readList.remove(0); 
				curr = curr.concat("\n");
				currLine = currLine.concat(curr);
				readList.add(currLine); 
				currLine = new String("insert, ");
			}
			
			// Create a Chord with 10 nodes
			Chord chord = new Chord();
			for(int i = 1; i <= 10; i++)
					chord.createNode(i);
			chord.setNeighbours();
			
			writer.println("Responses to client:");
			// Do the insertions
			long startTime = System.currentTimeMillis();
			
			for(int i = 0; i < size; i++){
				int r = randInt(1, 10) + 49152;
				int k = randInt(1, 10) + 49180;
				
				//System.out.print(i + " ");
				writer.print(i + " ");
				Socket s = new Socket("127.0.0.1", r); ServerSocket p = new ServerSocket(49190);  
				// Send the message to the server

				ObjectOutputStream out = new ObjectOutputStream(s.getOutputStream()); 
				out.writeObject(readList.get(i));  
				out.flush();
				out.writeObject(49190);
				out.flush(); 
				s.close(); 
				
				Socket incoming = p.accept();
				String response ; 
				ObjectInputStream in = new ObjectInputStream(incoming.getInputStream());
				response = (String) in.readObject(); 
				//System.out.println(response);
				writer.println(response); 
				p.close();
				Thread.sleep(100);
			}
			
			long endTime = System.currentTimeMillis();
			long miliseconds = (endTime - startTime) - 500 * 100;
			
			System.out.println(miliseconds);
			writer.println();
			writer.println("----------------------------------------");
			writer.println("Time in miliseconds: " + miliseconds);
			writer.println();
			
			for (ChordNode node : chord.getNodeList()) {
				System.out.print(node.getSerialNo() + " ");
				System.out.print(node.getNodeId() + " ");
				System.out.print(node.getPredId() + " ");
				System.out.println(node.getSuccId() + " ");
				writer.print("Node: " + node.getSerialNo() + " ");
				writer.print(" key: " +node.getNodeId() + " ");
				writer.print(" pred: " +node.getPredId() + " ");
				writer.println(" succ: " + node.getSuccId() + " ");
			}
			writer.println();
			writer.println();
			for (ChordNode node : chord.getNodeList()) {
				System.out.print(node.getSerialNo() + " ");
				System.out.print(node.getHashTable() + " ");
				writer.println("Hash map of Node: " + node.getSerialNo() + " ");
				writer.println(node.getHashTable() + " ");
				writer.println();
			}
			
			writer.println();
			writer.println();
			writer.println("--------------------------------------------");
			writer.println();
			writer.println();
			for (ChordNode node : chord.getNodeList()) {
				System.out.print(node.getSerialNo() + " ");
				System.out.println(node.getRepTable() + " ");
				writer.println("Replica map of Node: " + node.getSerialNo() + " ");
				writer.println(node.getRepTable() + " ");
				writer.println();
			}
			
			writer.close();
			
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		} catch (Exception e){
			e.printStackTrace();
		}
	}

	/*
	 * Produce random integers in range of [min,max]
	 */
	public static int randInt(int min, int max) {
		Random rand = new Random();
		int randomNum = rand.nextInt((max - min) + 1) + min;

		return randomNum;
	}

}
