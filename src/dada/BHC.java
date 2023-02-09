package dada;

import java.util.List;
import distributed.plugin.runtime.engine.BoardAgent;

/**
 * Whiteboard: A Black Hole search in bidirecational un­oriented Ring with 2
 * co­locate agents, known N (number of nodes), known K (number of agents),
 * total reliability and FIFO
 *
 */
public class BHC extends BoardAgent {
	public static final int STATE_NODE_UNKNOWN = 0;
	public static final int STATE_NODE_CLEAN = 1;
	public static final int STATE_NODE_TO_BH = 2;
	public static final int STATE_AGENT_WORKING = 3;
	public static final int STATE_AGENT_FOUND_BH = 4;
	public static final int STATE_AGENT_DONE = 5;
	private boolean explored;
	private boolean confirm;
	private boolean traverse;
	private boolean forward;
	private int round;
	private int numReq;
	private int numDone;

	public BHC() {
		super(STATE_NODE_UNKNOWN);
		this.round = 0;
		this.numReq = 0;
		this.numDone = 0;
		this.confirm = false;
		this.explored = false;
		this.traverse = false;
		this.forward = false;
	}

	public void init() {
		this.become(STATE_AGENT_WORKING);
		this.setNodeState(STATE_NODE_CLEAN);
		this.round = 1;
		int o = (this.getNetworkSize() - 1) / 2;
		int e = this.getNetworkSize() - o - 1;
		List<String> ports = this.getOutPorts();
		List<String> info = this.readFromBoard();
		String p = null;
		System.out.println("Board: " + info);
		if (info.isEmpty()) {
			// first agent
			this.numReq = o;
			p = ports.get(0);
			this.appendToBoard("active:" + p);
		} else {
			// second agent
			this.numReq = e;
			String s = info.get(0);
			String[] v = s.split(":");
			for (int i = 0; i < ports.size(); i++) {
				p = ports.get(i);
				if (!p.equals(v[1])) {
					break;
				}
			}
			this.appendToBoard("active:" + p);
		}
		// set for next exploring
		this.moveTo(p);
	}

	/*
	 * My first visit to this node, and it is the first visit from this port
	 */
	private void myFirstVisit(String port) {
		// First arrived safely from safe node
		this.appendToBoard("safe:" + port);
		// Then go back to say that this node also safe
		this.explored = true;
		this.confirm = true;
		this.moveTo(port);
	}

	/*
	 * Confirming port check to a previous node that this port is safe then get back
	 * to the port to finish of the work
	 */
	private void confirmVisit(List<String> info, String port) {
		String s = null;
		String[] v = null;
		for (int i = 0; i < info.size(); i++) {
			s = info.get(i);
			v = s.split(":");

			if (v.length == 2) {
				// get my port status
				if (v[1].equals(port)) {
					if (v[0].equals("active")) {
						this.removeRecord(s);
						this.appendToBoard("safe:" + port);
					}
				}
			} else {
				// someone left a note
				// reset my work plan
				int share = Integer.parseInt(v[2]);
				this.numReq = this.numReq - share;
				this.round = Integer.parseInt(v[1]);
				this.removeRecord(s);
			}
		}
		// back to the port
		this.confirm = false;
		this.numReq--;
		this.moveTo(port);
	}

	public void arrive(String port) {
		// traversing back
		if (this.traverse == true) {
			this.traversBack(port);
			return;
		}
		// move to my current end territory
		if (this.forward == true) {
			this.toMyEnd(port);
			return;
		}

		// keep exploring
		List<String> info = this.readFromBoard();
		if (info.isEmpty()) {
			// I am the first to this node
			this.myFirstVisit(port);
			this.setNodeState(STATE_NODE_CLEAN);
		} else {
			String s = null;
			String[] v = null;

			if (this.confirm == true) {
				this.confirmVisit(info, port);
				return;
			}
			System.out.println("Agent: " + this.getAgentId() + " numReq " + numReq);

			// my second visit after confirm safe port to previous node
			if (this.explored == true) {
				// check my round
				if (numReq == 0) {
					// complete round, go backward to find partner
					// current location
					System.out.println("Start traversing back from node: " + this.getNodeId());
					System.out.println("whiteboard:" + this.readFromBoard());
					this.traverse = true;
					this.numDone = 0;
					String p = this.getAnotherPort(port);
					this.traversBack(p);
					return;
				}

				if (info.size() == 1) {
					// still the first at this node
					s = info.get(0);
					v = s.split(":");
					if (v[1].equals(port)) {
						if (v[0].equals("active")) {
							// should not happen, i just marked safe on my first arrival
							System.err.println("should not happen 1");
						}

						// find another port to explore
						List<String> ports = this.getOutPorts();
						String p = null;
						for (int i = 0; i < ports.size(); i++) {
							p = ports.get(i);
							if (!p.equals(port)) {
								this.appendToBoard("active:" + p);
							}

							// move to new port
							this.explored = false;
							this.moveTo(p);
						}
					} else {
						// should not happen
						// i just visited on my first arrival
						System.err.println("should not happen 2");
					}
				} else {
					// other has visited this node from another port

					for (int i = 0; i < info.size(); i++) {
						s = info.get(i);
						v = s.split(":");
						if (v[0].equals("active")) {
							if (v[1].equals(port)) {
								// should not happen
								// a port that i came from is safe
								System.err.println("should not happen 3");
							} else {
								// someone is exploring it right now!
								// so wait.
								this.registerHostEvent(NotifyType.BOARD_UPDATE);
							}

						} else {
							if (v[1].equals(port)) {
								// my port, ignore it

							} else {
								// another port that was explored and marked
								// safe by other (No black hole)!!! or
								// repeat my node territory (backward exploring)!!
								// should not happen
								System.err.println("should not happen 4");
								break;

							}
						}
					}
				}
			} else {
				// my first visit but other has been here before
				this.myFirstVisit(port);
			}
		}
	}

	private void traversBack(String port) {
		String p = this.getAnotherPort(port);
		this.numDone++;
		System.out.println("Travers back to port " + p);
		if (isSafe(p)) {
			this.moveTo(p);
		} else {
			// we found under process node
			// done traversing
			this.traverse = false;
			// check whether is it a last safe node
			if (this.numDone == (this.getNetworkSize() - 1)) {
				// we found black hole
				this.become(STATE_AGENT_FOUND_BH);
				// the active port is a port to BH
				this.setNodeState(STATE_NODE_TO_BH);
				System.out.println("Agent: " + this.getAgentId() + " found BH from Node " + this.getNodeId());
				// move to my end node
				this.numDone = 0;
				this.forward = true;
				this.toMyEnd(p);
			} else {
				// compute new share
				int remain = this.getNetworkSize() - this.numDone;
				System.out.println("Found the struggling node " + this.getNodeId() + " with remain " + remain);
				int share = remain / 2;
				this.round++;
				this.numReq = share;
				// post to board that i will do extra share from my end
				this.appendToBoard("round:" + round + ":" + share);
				// move to my end node
				this.numDone = 0;
				this.forward = true;
				this.toMyEnd(p);
			}
		}
	}

	private void toMyEnd(String port) {
		String p = this.getAnotherPort(port);
		if (isSafe(p)) {
			this.moveTo(p);
		} else {
			// arrive at my end
			this.forward = false;
			System.out.println("Arrived my end Node: " + this.getNodeId() + "numReq " + this.numReq);
			if (this.getState() != STATE_AGENT_FOUND_BH) {
				// arrive at my end of previous round
				List<String> info = this.readFromBoard();
				String s = null;
				String[] v = null;

				System.out.println("Board " + info);
				if (info.size() > 1) {
					for (int i = 0; i < info.size(); i++) {
						s = info.get(i);
						v = s.split(":");
						if (!v[0].equals("safe")) {
							this.explored = false;
							this.confirm = false;
							// continue exploring
							System.out.println("Heading to " + v[0] + " port " + v[1]);
							this.moveTo(v[1]);
							break;
						}
					}
				} else {
					this.explored = false;
					this.appendToBoard("active:" + p);
					this.moveTo(p);
				}
			} else {
				this.become(STATE_AGENT_DONE);
				this.setNodeState(STATE_NODE_TO_BH);
			}
		}
	}

	private boolean isSafe(String port) {
		List<String> info = this.readFromBoard();
		boolean safe = false;
		String s = null;
		String[] v = null;
		for (int i = 0; i < info.size(); i++) {
			s = info.get(i);
			v = s.split(":");
			if (v.length == 2) {
				if (v[0].equals("safe") && v[1].equals(port)) {
					safe = true;
					break;
				}
			}
		}
		return safe;
	}

	/*
	 * Get opposite port from a given port in a ring
	 */
	private String getAnotherPort(String port) {
		List<String> ports = this.getOutPorts();
		String p = null;
		for (int i = 0; i < ports.size(); i++) {
			p = ports.get(i);
			if (!p.equals(port)) {
				return p;
			}
		}
		return p;
	}

	public void notified(NotifyType arg0) {
	}

	public void alarmRing() {
	}

}