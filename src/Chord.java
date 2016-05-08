import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;

/*
 * Class for Chord
 * It has an arrayList for all nodes-servers
 * and set the neighbors for all nodes.
 */
public class Chord {
	ArrayList<ChordNode> nodeList;

	public Chord() {
		this.nodeList = new ArrayList<ChordNode>();
	}

	public ChordNode createNode(int serialNo) throws NoSuchAlgorithmException {
		ChordNode node = new ChordNode(this, serialNo);
		nodeList.add(node);
		node.start();
		return node;
	}

	public ChordNode getNode(int i) {
		return (ChordNode) nodeList.get(i);
	}

	public void setNeighbours() {
		Collections.sort(nodeList);
		// cyclic
		nodeList.get(0).setPredId(nodeList.get(nodeList.size() - 1).getSerialNo());
		nodeList.get(0).setPredNodeId(nodeList.get(nodeList.size() - 1).getNodeId());
		nodeList.get(nodeList.size() - 1).setSuccId(nodeList.get(0).getSerialNo());
		nodeList.get(nodeList.size() - 1).setSuccNodeId(nodeList.get(0).getNodeId());

		for (ChordNode curr : nodeList) {
			if (!curr.equals(nodeList.get(0))) {
				curr.setPredId(nodeList.get(nodeList.indexOf(curr) - 1).getSerialNo());
				curr.setPredNodeId(nodeList.get(nodeList.indexOf(curr) - 1).getNodeId());
			}
			if (!curr.equals(nodeList.get(nodeList.size() - 1))) {
				curr.setSuccId(nodeList.get(nodeList.indexOf(curr) + 1).getSerialNo());
				curr.setSuccNodeId(nodeList.get(nodeList.indexOf(curr) + 1).getNodeId());
			}
		}
	}

	public ArrayList<ChordNode> getNodeList() {
		return this.nodeList;
	}
}
