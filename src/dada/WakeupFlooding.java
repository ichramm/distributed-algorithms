package dada;
import distributed.plugin.runtime.*;
import distributed.plugin.runtime.engine.*;

public class WakeupFlooding extends Entity {
	public static final int STATE_ASLEEP = 0;
	public static final int STATE_AWAKE = 1;
	public static final int STATE_DONE = 2;
	
	public static final String MSG_WAKE_UP = "Wake-Up";
	public static final String MSG_BROADCAST = "broadcast";
	
	public WakeupFlooding() {
		super(STATE_ASLEEP);
	}

	@Override
	public void receive(String incomingPort, IMessage message) {
		this.printToConsole(this.getState() + "-" + message.getLabel());
		switch (this.getState()) {
		case STATE_ASLEEP:
			if (MSG_WAKE_UP.equals(message.getLabel())) {
				processWakeup(incomingPort, message);
			} else if (MSG_BROADCAST.equals(message.getLabel())) {
				processBroadcast(incomingPort, message);
			}
			break;
		case STATE_AWAKE:
			if (MSG_BROADCAST.equals(message.getLabel())) {
				processBroadcast(incomingPort, message);
			}
			break;
		}
	}

	@Override
	public void init() {
		this.sendToAll(MSG_WAKE_UP, "Wake up!");
		this.become(STATE_AWAKE);
	}

	@Override
	public void alarmRing() {
	}
	
	private boolean haveInitialInformation() {
		// arbitrarily selected from the initiator's neighbours
		return "n10".equals(this.getName());
	}
	
	private void processWakeup(String incomingPort, IMessage message) {
		if (this.haveInitialInformation()) {
			this.sendToAll(MSG_BROADCAST, "Some Broadcast Info");
			this.become(STATE_DONE);
		} else {
			this.sendToOthers(message.getLabel(), message);
			this.become(STATE_AWAKE);
		}
	}
	
	private void processBroadcast(String incomingPort, IMessage message) {
		this.sendToOthers(message.getLabel(), message);
		this.become(STATE_DONE);
	}
}
