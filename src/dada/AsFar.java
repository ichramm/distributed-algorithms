package dada;

import distributed.plugin.runtime.IMessage;
import distributed.plugin.runtime.engine.Entity;

/**
 * Leader election protocol with multiple initiators, unique ID, synchronous and
 * total reliable communication.
 *
 * The initiators (candidates) will send messages with its ID to all directions,
 * and a message with smallest ID will kill every candidates and return back to
 * the sender. A candidate with smaller ID will kill all arrival messages with
 * bigger ID as well.
 */
public class AsFar extends Entity {
// all possible states
	public static final int STATE_SLEEP = 0;
	public static final int STATE_ELECTION = 1;
	public static final int STATE_PASSIVE = 2;
	public static final int STATE_FOLLOWER = 3;
	public static final int STATE_LEADER = 4;
// all message label will be used in the protocol
	private static final String MSG_LABEL_ELECTION = "Election";
	private static final String MSG_LABEL_NOTIFY = "Notify";
// tracking that both of my messages have returned
	private boolean leftMin;
	private boolean rightMin;

	/**
	 * Default constructor
	 */
	public AsFar() {
		super(STATE_SLEEP);
	}

	public void init() {
		String myId = this.getName();
		this.become(STATE_ELECTION);
		this.sendToAll(MSG_LABEL_ELECTION, myId);
	}

	@Override
	public void alarmRing() {
// this protocol does not need this function
	}

	public void receive(String incomingPort, IMessage message) {
		String msg = (String) message.getContent();
		String msgLabel = message.getLabel();
		if (this.getState() == STATE_SLEEP) {
			this.become(STATE_PASSIVE);
			this.sendToOthers(MSG_LABEL_ELECTION, msg);

		} else if (this.getState() == STATE_PASSIVE) {
			if (msgLabel.equals(MSG_LABEL_NOTIFY)) {
				this.become(STATE_FOLLOWER);
				this.sendToOthers(MSG_LABEL_NOTIFY, msg);
			} else {
				this.sendToOthers(MSG_LABEL_ELECTION, msg);
			}

		} else if (this.getState() == STATE_ELECTION) {
			if (msgLabel.equals(MSG_LABEL_ELECTION)) {
				this.electing(incomingPort, msg);
			} else {
				// should not happen!!
			}

		} else if (this.getState() == STATE_LEADER) {
			if (msg.equals(MSG_LABEL_NOTIFY)) {
				System.out.println("Election completed: " + this.getName() + " is a leader");
			} else {
				// should not happen!!
			}
		}
	}

	/*
	 * Helping function that will compare a received ID with my ID
	 */
	private void electing(String incomingPort, String id) {

		String myId = this.getName();
		if (id.compareTo(myId) < 0) {
			this.become(STATE_PASSIVE);
			this.sendToOthers(MSG_LABEL_ELECTION, id);
		} else if (id.compareTo(myId) == 0) {
			// my election msg has returned
			if (this.leftMin == true)
				this.rightMin = true;
			else
				this.leftMin = true;
		}
		if (this.rightMin && this.leftMin) {
			this.become(STATE_LEADER);
		}
		// send notify to one direction only
		this.sendTo(MSG_LABEL_NOTIFY, incomingPort, "I " + myId + " is your leader");
	}
}
