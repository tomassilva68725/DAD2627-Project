package didatrade.server;

import java.util.*;

import didatrade.core.*;
import didatrade.configs.*;
import didatrade.configs.ConfigurationScheduler;

import didatrade.DidaTradeMain;
import didatrade.DidaTradePaxos;
import didatrade.DidaTradePaxosServiceGrpc;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;


public class DidaTradeServerState {
    public static final int     DEFAULT_POPULATION = 10;
	public static final int     SLOW_MAX_DELAY_MS = 1000;
    int                         max_participants;
    TradeManager                trade_manager;
    ConfigurationScheduler      scheduler;
    int                         base_port;
    int                         my_id;
    RequestHistory              req_history;
    PaxosLog                    paxos_log;

    List<Integer>               all_participants;
    int                         n_participants;
    String[]                    targets;
    ManagedChannel[]            channels;
    DidaTradePaxosServiceGrpc.DidaTradePaxosServiceStub[] async_stubs;

    private int                 current_ballot;
    private int                 completed_ballot;
    private int                 debug_mode;
    private boolean             fastpaxos_on;
	private boolean             frozen_on;
	private boolean             slow_on;
 
    MainLoop                    main_loop;
    Thread                      main_loop_worker;
    
    public DidaTradeServerState(int port, int myself, char schedule) {
	this.trade_manager    = new TradeManager();
	this.scheduler        = new ConfigurationScheduler (schedule);
	this.base_port        = port;
	this.my_id            = myself;
	this.debug_mode       = 0;
	this.fastpaxos_on     = false;
	this.current_ballot   = 0;
	this.completed_ballot = -1;
	this.req_history      = new RequestHistory();
	this.paxos_log        = new PaxosLog();
	this.main_loop        = new MainLoop(this);
	this.frozen_on        = false;
	this.slow_on 		  = false;

	// populate manager
	this.trade_manager.populate(DEFAULT_POPULATION);
	
	// init comms
	this.all_participants = this.scheduler.allparticipants ();
	this.n_participants = all_participants.size();
	
	this.targets = new String[this.n_participants];
	for (int i = 0; i < this.n_participants; i++) {
	    int target_port = this.base_port + all_participants.get(i);
	    this.targets[i] = new String();
	    this.targets[i] = "localhost:" + target_port;
	    System.out.printf("targets[%d] = %s%n", i, targets[i]);
	}
	
	this.channels = new ManagedChannel[this.n_participants];
	for (int i = 0; i < this.n_participants; i++) 
	    this.channels[i] = ManagedChannelBuilder.forTarget(this.targets[i]).usePlaintext().build();
	
	this.async_stubs = new DidaTradePaxosServiceGrpc.DidaTradePaxosServiceStub[this.n_participants];
	for (int i = 0; i < this.n_participants; i++) 
	    this.async_stubs[i] = DidaTradePaxosServiceGrpc.newStub(this.channels[i]);	

	// start worker
	this.main_loop_worker = new Thread (main_loop);
	this.main_loop_worker.start();
    }

    public synchronized int getCurrentBallot () {
	return this.current_ballot;
    }
    
    public synchronized void setCurrentBallot (int ballot) {
	if (ballot > this.current_ballot)
	    this.current_ballot = ballot;
    }

    public synchronized int getCompletedBallot () {
	return this.completed_ballot;
    }

    public int findMaxDecidedBallot () {
	int ballot = -1;
	int length = this.paxos_log.length();

	for (int i=0; i< length; i++) {
	    PaxosInstance entry = this.paxos_log.getEntry(i);
	    if (entry == null)
		return ballot;
	    if (!entry.decided)
		return ballot;
	    else if (entry.accept_ballot > ballot)
		ballot = entry.accept_ballot;

	}
	return ballot;
    }
    
    public synchronized void updateCompletedBallot (int ballot) {
	// WARNING: THIS ONLY WORKS FOR CONFIGURATIONS WHERE THERE IS NO NEED FOR STATE-TRANSFER!!!!!
	// NEEDS TO BE UPDATE FOR THE PROJECT


	ballot = this.findMaxDecidedBallot ();
	if (ballot > this.completed_ballot)
	    this.completed_ballot = ballot;
	this.notifyAll();
    }

    
    public synchronized void setCompletedBallot (int ballot) {
	if (ballot > this.completed_ballot)
	    this.completed_ballot = ballot;
	this.notifyAll();
    }

    
    public synchronized int waitForCompletedBallot(int ballot) {
        while (this.completed_ballot < ballot) {
            try {
		wait ();
	    }
	    catch (InterruptedException e) {
	    }
	}
	return this.completed_ballot;
    }

    public synchronized boolean getFastPaxosMode () {
	return this.fastpaxos_on;
    }

    public synchronized void setFastPaxosMode (boolean mode) {
	this.fastpaxos_on = mode;
    }

    public synchronized int getDebugMode () {
	return this.debug_mode;
    }

    public synchronized void setDebugMode (int mode) {
	this.debug_mode = mode;
    }

	public synchronized boolean isFrozen() {
		return this.frozen_on;
	}

	public synchronized void setFrozen(boolean frozen_value) {
		this.frozen_on = frozen_value;
		if(!frozen_value){
			this.notifyAll();	
		}
		
	}

	public synchronized boolean isSlow() {
		return this.slow_on;
	}

	public synchronized void setSlow(boolean slow_value) {
		this.slow_on = slow_value;
	}

	public synchronized void waitIfFrozen() {
		while (this.frozen_on) {
			try {
				wait();
			} catch (InterruptedException e) {
			}
		}
	}

	public void debugGate(){
		this.waitIfFrozen();
		if(this.isSlow()){
			try {
				Thread.sleep((long) (500 + (Math.random() * SLOW_MAX_DELAY_MS)));
		    } catch (InterruptedException e) {
			}
		}
	}
}
