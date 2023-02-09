package dada;
import java.util.List;

import distributed.plugin.runtime.*;
import distributed.plugin.runtime.engine.*;

/*
 *  Message Passing Model -> distributed.plugin.runtime.engine.Entity
 *  Agent with Whiteboard Model -> distributed.plugin.runtime.engine.BoardAgent
 *  Agent with Token Model -> distributed.plugin.runtime.engine.TokenAgent
 */

public class Flooding extends Entity   {
	
	public static final int STATE_IDLE = 0;
	public static final int STATE_DONE = 1;
	
	public static final String MESSAGE_TYPE = "broadcast";
	
	public Flooding() {
		super(STATE_IDLE);
	}

	@Override
	public void receive(String incomingPort, IMessage message) { // invoked when a node receives a message
		if (this.getState() == STATE_IDLE) {
			this.printToConsole(this.getName() + ": receiving from " + incomingPort);
			String[] targets = this.getOutPorts().stream()
					.filter(port -> !incomingPort.equals(port))
					.toArray(String[]::new);
			this.sendTo(targets, message);
			//this.sendToOthers(message);
			this.become(STATE_DONE); // must be the last action according to the book
		}
	}

	@Override
	public void init() { // invoked only on the initiator node
		//String id = this.getName();
		this.sendToAll(MESSAGE_TYPE, "Message being broadcasted");
		this.become(STATE_DONE); // must be the last action according to the book
	}

	@Override
	public void alarmRing() { // invoked when an internal alarm clock of a node is rang
		this.printToConsole(this.getName() + ": Ring");
	}

}
