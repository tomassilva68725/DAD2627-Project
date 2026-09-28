package didatrade.server;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import didatrade.DidaTradeMain;
import didatrade.DidaTradePaxos;
import didatrade.DidaTradePaxosServiceGrpc;

import didatrade.util.GenericResponseCollector;
import didatrade.util.CollectorStreamObserver;
import didatrade.util.PhaseOneResponseProcessor;
//import didatrade.util.PhaseOneBogusProcessor;
import didatrade.util.PhaseTwoResponseProcessor;

import didatrade.configs.ConfigurationScheduler;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;


public class MainLoop implements Runnable  {
    DidaTradeServerState server_state;

    private boolean has_work;
    private AtomicInteger next_log_entry;
    private List<Integer> all_participants;
    private int n_participants;
    private String[] targets;
    private ManagedChannel[] channels;
    private DidaTradePaxosServiceGrpc.DidaTradePaxosServiceStub[] async_stubs;
    
    
    public MainLoop(DidaTradeServerState state) {
	this.server_state   = state;
	this.has_work       = false;
	this.next_log_entry = new AtomicInteger(-1);
    }

    public void run() {
	while (true) {
		int ballot = this.server_state.getCurrentBallot();

		if (server_state.scheduler.leader(ballot) != this.server_state.my_id) {
			waitForWork();
			continue;
		}

		RequestRecord request = this.server_state.req_history.takeFirstPending();
		if (request == null) {
    		waitForWork();
    		continue;
		}
		int instance = allocateNextInstance();
		InstanceWorker worker = new InstanceWorker(this.server_state, instance, request);
		new Thread(worker).start();
	}
    }

	private synchronized void waitForWork() {
		this.has_work = false;
		while (!this.has_work) {
			try {
				wait();
			} catch (InterruptedException e) {
			}
		}
	}

	private int allocateNextInstance() {
		int log_length = server_state.paxos_log.length();
		if (log_length - 1 > this.next_log_entry.get()) {
			this.next_log_entry.set(log_length - 1);
		}
		return this.next_log_entry.incrementAndGet();
	}
      
    public synchronized void wakeup() {
	this.has_work = true;
	notify();    
    }
}
