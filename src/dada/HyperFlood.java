package dada;
import java.util.Iterator;
import java.util.List;

import distributed.plugin.runtime.*;
import distributed.plugin.runtime.engine.*;

/*
 *  Message Passing Model -> distributed.plugin.runtime.engine.Entity
 *  Agent with Whiteboard Model -> distributed.plugin.runtime.engine.BoardAgent
 *  Agent with Token Model -> distributed.plugin.runtime.engine.TokenAgent
 */

// dada1.HyperFlood
public class HyperFlood extends Entity   {
	
	public static final int STATE_IDLE = 0;
	public static final int STATE_DONE = 1;
	
	public static final String MESSAGE_TYPE = "broadcast";
	
	public HyperFlood() {
		super(STATE_IDLE);
	}

	@Override
	public void receive(String incomingPort, IMessage message) {
		if (this.getState() == STATE_IDLE) {
			String[] targets = this.getOutPorts().stream()
					.filter(port -> port.compareTo(incomingPort) < 0)
					.toArray(String[]::new);
			this.sendTo(targets, message);
			this.become(STATE_DONE);
		}
	}

	@Override
	public void init() { // invoked only on the initiator node
		this.sendToAll(MESSAGE_TYPE, "Message being broadcasted");
		this.become(STATE_DONE);
	}

	@Override
	public void alarmRing() {
	}
}
