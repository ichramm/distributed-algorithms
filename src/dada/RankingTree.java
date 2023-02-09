package dada;

import java.io.IOException;
import java.io.ObjectStreamException;
import java.io.Serializable;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.function.Predicate;

import distributed.plugin.runtime.*;
import distributed.plugin.runtime.engine.*;

/**
 * The ranking algorithm
 * 
 * @author ichramm
 * 
 *         Restrictions:
 * 
 *         - Standard set of restrictions - Any entity can start the algorithm.
 *         - There's no need for a unique initiator (TODO: proof) - It is
 *         required that all entities have unique IDs
 * 
 *         How it works
 * 
 *         - Message format: List of {node_id, value} - The list contains all
 *         the values seen so far by the entity - Every entity must remember the
 *         entities it has received
 * 
 *         - The initiator sends a single-element list to all its neighbors [
 *         {x, v(x)} ]
 * 
 *         - When an entity receives a message it must - Look for unknown
 *         entities in the message - Increase rank by one for each new entity
 *         that has a smaller value - Forward message to all neighbors (except
 *         the sender) if there is any new entity in the list
 * 
 *         - Entities must know when they have pending "child ndoes"
 */
public class RankingTree extends Entity {

	public static final int STATE_IDLE = 0;
	public static final int STATE_ACTIVE = 1;
	public static final int STATE_DONE = 2;

	public static final String MSG_RANKING = "Ranking-Message";

	private int rank;
	private int value;
	private HashSet<String> seenEntities;
	private HashSet<String> pendingNeighbors;
	private HashSet<String> doneNeighbors;

	/**
	 * Basic message unit: A pair of {entity, value}
	 */
	public class RankingMessageNode implements Serializable {
		private static final long serialVersionUID = 1L;

		private String nodeId;
		private int nodeValue;

		public RankingMessageNode() {

		}

		public RankingMessageNode(String nodeId, int nodeValue) {
			this.nodeId = nodeId;
			this.nodeValue = nodeValue;
		}

		public String getNodeId() {
			return nodeId;
		}

		public int getNodeValue() {
			return nodeValue;
		}

		private void writeObject(java.io.ObjectOutputStream out) throws IOException {
			out.writeObject(nodeId);
			out.writeInt(nodeValue);
		}

		private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException {
			nodeId = (String) in.readObject();
			nodeValue = in.readInt();
		}

		@Override
		public String toString() {
			return "{" + (nodeId != null ? nodeId : "null") + ", " + nodeValue + "}";
		}
	}

	/**
	 * Message: a list of RankingMessageNode
	 */
	public class RankingMessage implements Serializable {
		private static final long serialVersionUID = 1L;

		private boolean isFinalUpdate = false;
		private boolean nodeIsDone = false;
		private ArrayList<RankingMessageNode> payload = new ArrayList<>();

		public RankingMessage() {

		}

		public RankingMessage(boolean potentiallyFinal, boolean nodeIsDone, ArrayList<RankingMessageNode> payload) {
			this.isFinalUpdate = potentiallyFinal;
			this.nodeIsDone = nodeIsDone;
			this.payload = payload;
		}

		public boolean getIsFinalUpdate() {
			return isFinalUpdate;
		}

		public boolean getNodeIsDone() {
			return nodeIsDone;
		}

		public ArrayList<RankingMessageNode> getPayload() {
			return payload;
		}

		private void writeObject(java.io.ObjectOutputStream out) throws IOException {
			out.writeBoolean(isFinalUpdate);
			out.writeBoolean(nodeIsDone);
			out.writeObject(payload);
		}

		private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException {
			this.isFinalUpdate = in.readBoolean();
			this.nodeIsDone = in.readBoolean();
			this.payload = (ArrayList<RankingMessageNode>) in.readObject();
		}

		@Override
		public String toString() {
			StringBuilder b = new StringBuilder();
			b.append("final:" + isFinalUpdate + " done:" + nodeIsDone + " [");
			b.append(String.join(", ", payload.stream().map(RankingMessageNode::toString).toArray(String[]::new)));
			b.append("]");
			return b.toString();
		}
	}

	public RankingTree() {
		super(STATE_IDLE);
	}

	/**
	 * Comentarios:
	 * 
	 * - Parece correcto pero pinta a que se estan mandando muchos mensajes - Es
	 * necesario correrlo en papel con 2, 3 y 4 nodos - En cada caso cambiar
	 * condiciones como ser - Varios initiators - Mensajes simutaneos (de A a B y de
	 * B a A) - Iniciar en hojas y en nodos intermedios - Esta prueba puede servir
	 * de base para probar la correctitud del protocolo
	 */
	@Override
	public void receive(String incomingPort, IMessage message) {
		this.initialize();

		RankingMessage m = (RankingMessage) message.getContent();

		this.printToConsole(this.getName() + "  got message " + m + " from " + incomingPort);

		if (this.getState() == STATE_DONE) {
			this.printToConsole("ERROR: Node " + this.getName() + " is DONE");
			return;
		}

		ArrayList<RankingMessageNode> payload = m.getPayload();

		// neighbor considered this might be its last message so expect nothing new from
		// it
		if (m.getIsFinalUpdate()) {
			pendingNeighbors.remove(incomingPort);
		} else {
			pendingNeighbors.add(incomingPort); // could be a NOP
		}

		if (m.getNodeIsDone()) {
			doneNeighbors.add(incomingPort);
		}

		// to check if there's new info
		int seenCount = seenEntities.size();
		
		payload.forEach(rankingMessageNode -> {
			if (seenEntities.add(rankingMessageNode.getNodeId())) {
				if (rankingMessageNode.getNodeValue() < this.value) {
					this.rank += 1;
				}
			}
		});

		// leaf nodes have only one neighbor, when this neighbor is done so is the leave
		if (this.isLeaf()) {
			// allow parent node to terminate
			if (!m.getNodeIsDone()) {
				RankingMessage reply = new RankingMessage(true, pendingNeighbors.isEmpty(),
						new ArrayList<RankingMessageNode>(
								Arrays.asList(new RankingMessageNode(this.getName(), this.value))));
				this.sendToAll(MSG_RANKING, reply);
			}

			if (pendingNeighbors.isEmpty()) { // i.e: message is final
				this.becomeDone();
			}
		} else {
			// message must be forwarded to other nodes
			String[] others = this.getOutPorts().stream()
					.filter(port -> !incomingPort.equals(port))
					.toArray(String[]::new);

			if (seenCount != seenEntities.size()) {
				// always include own data to simplify the programming
				// RankingMessageNode[] newData = Arrays.copyOf(receivedData,
				// receivedData.length + 1);
				// newData[receivedData.length] = new RankingMessageNode(this.getName(),
				// this.value);
				if (payload.stream().noneMatch(p -> p.getNodeId().equals(this.getName()))) {
					payload.add(new RankingMessageNode(this.getName(), this.value));
				}

				RankingMessage m2 = new RankingMessage(pendingNeighbors.isEmpty(), false, payload);

				// must wait for all of them to respond
				for (String port : others) {
					// got new info, all other nodes become pending now
					pendingNeighbors.add(port); // could be a NOP
				}

				this.sendTo(MSG_RANKING, others, m2);
			} else if (!pendingNeighbors.isEmpty()) {
				// still waiting for some people but the node in this specific port
				// is already done so I must forward termination the others
				
				// revisar aca, parece que hay mensajes de mas
				// pero si los mando no termina ok
				RankingMessage m2 = new RankingMessage(m.getIsFinalUpdate(), false, new ArrayList<>());
				this.sendTo(MSG_RANKING,
						    this.getPorts().stream()
						    	.filter(port -> !incomingPort.equals(port))
						    	.filter(port -> !doneNeighbors.contains(port))
						    	.toArray(String[]::new),
						    m2);
			} else {
				// reply to sender telling I am done
				RankingMessage m2 = new RankingMessage(true, true, new ArrayList<>()); // no data needed, this node is not a
																					// leaf
				//this.sendToAll(MSG_RANKING, m2);

				this.sendTo(MSG_RANKING, this.getPorts()
						                     .stream()
						                     .filter(s -> !doneNeighbors.contains(s))
						                     .toArray(String[]::new), m2);

				this.becomeDone();
			}
		}
	}

	@Override
	public void init() {
		this.initialize();

		// a leaf node knows for sure it will send no new data
		// a non-leaf node doesn't know how many nodes are on each side
		RankingMessage m = new RankingMessage(this.isLeaf(), false,
				new ArrayList<RankingMessageNode>(Arrays.asList(new RankingMessageNode(this.getName(), this.value))));

		this.sendToAll(MSG_RANKING, m);
	}

	@Override
	public void alarmRing() {
	}

	private void becomeDone() {
		this.printToConsole("DONE - Node: " + this.getName() + ", value: " + this.value + ", rank: " + this.rank);
		this.become(STATE_DONE);
	}

	private void initialize() {
		if (this.getState() == STATE_IDLE) {
			rank = 1;
			value = Integer.parseInt(this.getUserInput());
			seenEntities = new HashSet<>();
			pendingNeighbors = new HashSet<>();
			doneNeighbors = new HashSet<>();

			for (String port : this.getPorts()) {
				// Node will be done after this set becomes empty
				pendingNeighbors.add(port);
			}

			this.printToConsole("Entity: " + this.getName() + ", value: " + this.value + ", neighbors: "
					+ pendingNeighborsString());

			this.become(STATE_ACTIVE);
		}
	}

	private boolean isLeaf() {
		return this.getPorts().size() == 1;
	}

	String pendingNeighborsString() {
		return String.join(",", pendingNeighbors.stream().map(s -> s).toArray(String[]::new));
	}
}
