package dada;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import dada.RankingTree.RankingMessageNode;
import distributed.plugin.runtime.*;
import distributed.plugin.runtime.engine.*;

public class Ranking extends Entity {

	class RankingMessage implements Serializable {
		private static final long serialVersionUID = 1L;

		public static final int COLLECT = 1;
		public static final int DATA = 2;

		public int type = COLLECT;
		public List<Integer> data;

		public RankingMessage() {
		}

		public RankingMessage(int type) {
			this.type = type;
			if (type == DATA) {
				this.data = new ArrayList<>();
			}
		}

		public RankingMessage(int type, List<Integer> data) {
			this.type = type;
			this.data = data;
		}

		private void writeObject(java.io.ObjectOutputStream out) throws IOException {
			out.writeInt(type);
			if (type == DATA) {
				out.writeObject(data);
			}
		}

		@SuppressWarnings("unchecked")
		private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException {
			type = in.readInt();
			if (type == DATA) {
				data = (List<Integer>) in.readObject();
			}
		}

		@Override
		public String toString() {
			return "{type=" + (type == COLLECT ? "COLLECT" : "DATA") + ", data=["
					+ (data != null ? data.stream().map(Object::toString).collect(Collectors.joining(", "))
							: "")
					+ "]}";
		}
	}

	public static final int STATE_IDLE = 0;
	public static final int STATE_SHOUT_ACTIVE = 1;
	public static final int STATE_RANKING_IDLE = 2;
	public static final int STATE_RANKING_COLLECTING = 3;
	public static final int STATE_RANKING_WAITING = 4;
	public static final int STATE_DONE = 5;

	private static final String MSG_LABEL_SHOUT = "Shout";
	private static final String MSG_LABEL_RANKING = "Ranking";

	private static final String SHOUT_Q = "Q";
	private static final String SHOUT_YES = "Yes";

	private int counter = 0;
	private boolean isRoot = false;
	private String parent = null;
	private ArrayList<String> children = new ArrayList<>();

	private int rank = 0;
	private int value = 0;

	// children that haven't replied a COLLECT request
	private Set<String> pendingChildren;
	// currently known values (warn: may grow big)
	private List<Integer> knownData;
	// it is very likely that the root node initiates ranking before the tree is
	// built so there may be a collect messages that cannot be processed immediately
	IMessage pendingCollect;
	String pendingCollectPort;

	public Ranking() {
		super(STATE_IDLE);
	}

	@Override
	public void init() {
		initialize(true);
		sendToAll(MSG_LABEL_SHOUT, "Q");
		become(STATE_SHOUT_ACTIVE);
	}

	@Override
	public void receive(String incomingPort, IMessage message) {
		String msgLabel = message.getLabel();
//		this.printToConsole("Node: " + getName() + ", Message: " + msgLabel + " [" + message.getContent()
//				+ "], From: " + incomingPort);

		switch (this.getState()) {
		case STATE_IDLE:
			initialize(false);
			if (msgLabel.equals(MSG_LABEL_SHOUT) && SHOUT_Q.equals(message.getContent())) {
				counter = 1;
				parent = incomingPort;
				sendTo(MSG_LABEL_SHOUT, incomingPort, SHOUT_YES);
				if (counter == getPorts().size()) {
					// leaf node
					onShoutDone();
				} else {
					// build sub-tree
					String[] others = this.getOutPorts().stream().filter(port -> !incomingPort.equals(port))
							.toArray(String[]::new);
					sendTo(MSG_LABEL_SHOUT, others, SHOUT_Q);
					become(STATE_SHOUT_ACTIVE);
				}
			} else {
				onUnexpectedMessage(incomingPort, message);
			}
			break;
		case STATE_SHOUT_ACTIVE:
			if (msgLabel.equals(MSG_LABEL_SHOUT)) {
				if (SHOUT_Q.equals(message.getContent())) {
					counter += 1; // implicit no
					if (counter == this.getPorts().size()) {
						onShoutDone();
					}
				} else { // Yes
					counter += 1;
					children.add(incomingPort);
					if (counter == this.getPorts().size()) {
						onShoutDone();
					}
				}
			} else if (msgLabel.equals(MSG_LABEL_RANKING)) {
				RankingMessage msg = (RankingMessage) message.getContent();
				if (msg.type == RankingMessage.COLLECT) {
					pendingCollect = message;
					pendingCollectPort = incomingPort;
				} else {
					onUnexpectedMessage(incomingPort, message);
				}
			} else {
				onUnexpectedMessage(incomingPort, message);
			}
			break;
		case STATE_RANKING_IDLE:
			if (msgLabel.endsWith(MSG_LABEL_RANKING)) {
				RankingMessage msg = (RankingMessage) message.getContent();
				if (msg.type == RankingMessage.COLLECT) {
					if (this.children.isEmpty()) { // leaf node
						sendTo(MSG_LABEL_RANKING, incomingPort,
								new RankingMessage(RankingMessage.DATA, Arrays.asList(value)));
						become(STATE_RANKING_WAITING);
					} else {
						startCollecting(); // changes state to COLLECTING
					}
				} else {
					onUnexpectedMessage(incomingPort, message);
				}
			} else {
				onUnexpectedMessage(incomingPort, message);
			}
			break;
		case STATE_RANKING_COLLECTING:
			if (msgLabel.endsWith(MSG_LABEL_RANKING)) {
				RankingMessage msg = (RankingMessage) message.getContent();
				if (msg.type == RankingMessage.DATA) {
					knownData.addAll(msg.data);
					pendingChildren.remove(incomingPort);
					if (pendingChildren.isEmpty()) {
						knownData.add(value);
						// all children have replied, we are done here
						if (parent == null) {
							// root node
							onRankingDone(knownData);
						} else {
							// intermediate node, must forward info to parent node
							sendTo(MSG_LABEL_RANKING, parent,
									new RankingMessage(RankingMessage.DATA, knownData));
							knownData.clear();
							become(STATE_RANKING_WAITING);
						}
					}
				} else {
					onUnexpectedMessage(incomingPort, message);
				}
			} else {
				onUnexpectedMessage(incomingPort, message);
			}
			break;
		case STATE_RANKING_WAITING:
			if (msgLabel.endsWith(MSG_LABEL_RANKING)) {
				RankingMessage msg = (RankingMessage) message.getContent();
				if (msg.type == RankingMessage.DATA) {
					onRankingDone(msg.data);
				} else {
					onUnexpectedMessage(incomingPort, message);
				}
			} else {
				onUnexpectedMessage(incomingPort, message);
			}
			break;
		default:
			onUnexpectedMessage(incomingPort, message);
			break;
		}
	}

	/**
	 * Common initialization stuff
	 */
	private void initialize(boolean root) {
		isRoot = root;
		value = Integer.parseInt(this.getUserInput());
	}

	/**
	 * Called when a node has finished building its sub-tree
	 */
	private void onShoutDone() {
		this.printToConsole(
				"Node: " + getName() + ", Parent: " + parent + ", Children: " + String.join(", ", children));
		// root node starts ranking, others wait for the COLLECT request
		if (isRoot) {
			startCollecting();
		} else {
			become(STATE_RANKING_IDLE);
			// in case root initiated ranking too early
			if (pendingCollect != null) {
				this.receive(pendingCollectPort, pendingCollect);
				pendingCollect = null;
				pendingCollectPort = null;
			}
		}
	}

	/**
	 * Sends a COLLECT message to all children.
	 */
	private void startCollecting() {
		sendTo(MSG_LABEL_RANKING, children.toArray(String[]::new),
				new RankingMessage(RankingMessage.COLLECT));

		// won't reply to parent node until all children have replied
		pendingChildren = new HashSet<>(children);
		knownData = new ArrayList<>();

		become(STATE_RANKING_COLLECTING);
	}

	/**
	 * Called when node is able to compute its rank
	 */
	private void onRankingDone(List<Integer> data) {
		rank = 1 + (int) data.stream().filter(v -> v < this.value).count();

		if (!children.isEmpty()) {
			sendTo(MSG_LABEL_RANKING, children.toArray(String[]::new),
					new RankingMessage(RankingMessage.DATA, data));
		}

		this.printToConsole("Node: " + getName() + ", Value: " + value + ", Ranking: " + rank);

		become(STATE_DONE);
	}

	/**
	 * Called when receiving an unexpected message (should never happen)
	 */
	private void onUnexpectedMessage(String incomingPort, IMessage message) {
		this.printToConsole("ERROR: Node " + getName() + " got message " + message.getLabel() + " ["
				+ message.getContent() + "] from " + incomingPort + " while on state " + this.stateString());
	}

	private String stateString() {
		switch (getState()) {
		case STATE_IDLE:
			return "IDLE";
		case STATE_SHOUT_ACTIVE:
			return "SHOUT_ACTIVE";
		case STATE_RANKING_IDLE:
			return "RANKING_IDLE";
		case STATE_RANKING_COLLECTING:
			return "RANKING_COLLECTING";
		case STATE_RANKING_WAITING:
			return "RANKING_WAITING";
		case STATE_DONE:
			return "DONE";
		}
		return "UNKNOWN" + getState();
	}

	@Override
	public void alarmRing() {
	}
}
