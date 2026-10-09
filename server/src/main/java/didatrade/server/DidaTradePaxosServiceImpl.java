
package didatrade.server;

import java.util.*;

import didatrade.DidaTradeMain;
import didatrade.DidaTradePaxos;
import didatrade.DidaTradePaxosServiceGrpc;

import didatrade.util.GenericResponseCollector;
import didatrade.util.CollectorStreamObserver;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import io.grpc.Context;

public class DidaTradePaxosServiceImpl extends DidaTradePaxosServiceGrpc.DidaTradePaxosServiceImplBase {
    DidaTradeServerState server_state;


	private final Object acceptor_lock;

    public DidaTradePaxosServiceImpl(DidaTradeServerState state) {
	this.server_state = state;
	this.acceptor_lock = state.acceptor_lock;
    }
 
    @Override
    public void phaseone(DidaTradePaxos.PhaseOneRequest request, StreamObserver<DidaTradePaxos.PhaseOneReply> responseObserver) {
	this.server_state.debugGate();

		// System.out.println("Receive phase1 request: \n" + request);

	int instance          = request.getInstance();
	int ballot            = request.getRequestballot();
	boolean accepted      = false;
	int  maxballot;

	List<DidaTradePaxos.AcceptedEntry> entries = null;
	synchronized (this.acceptor_lock) {
		if (ballot >= this.server_state.getCurrentBallot()) {
			accepted = true;
			this.server_state.setCurrentBallot(ballot);
			entries = this.server_state.paxos_log.acceptedFrom(instance);
		}
		maxballot = this.server_state.getCurrentBallot();
	}

	DidaTradePaxos.PhaseOneReply.Builder response_builder = DidaTradePaxos.PhaseOneReply.newBuilder();
	response_builder.setServerid(this.server_state.my_id);
	response_builder.setRequestballot(ballot);
	response_builder.setAccepted(accepted);
	response_builder.setMaxballot(maxballot);
	if (entries != null)
		response_builder.addAllEntries(entries);

	DidaTradePaxos.PhaseOneReply response = response_builder.build();

	// System.out.println("Sending phase1 response: " + response);

	responseObserver.onNext(response);
	responseObserver.onCompleted();
    }


    @Override
    public void phasetwo(DidaTradePaxos.PhaseTwoRequest request, StreamObserver<DidaTradePaxos.PhaseTwoReply> responseObserver) {
	this.server_state.debugGate();
	// System.out.println ("Receive phase two request: \n" + request);

	int instance          = request.getInstance();
	int ballot            = request.getRequestballot();
	int value             = request.getValue();

	if (request.getAny()) {
		int max = this.server_state.fast_acceptor.onAny(ballot, instance);
		responseObserver.onNext(DidaTradePaxos.PhaseTwoReply.newBuilder()
			.setInstance(instance).setServerid(this.server_state.my_id).setRequestballot(ballot)
			.setAccepted(max <= ballot).setMaxballot(max).build());
		responseObserver.onCompleted();
		return;
	}

	PaxosInstance entry   = this.server_state.paxos_log.testAndSetEntry(instance);
	boolean accepted      = false;
	int  maxballot        = ballot;

	synchronized (this.acceptor_lock) {
		if (ballot >= this.server_state.getCurrentBallot()) {
			accepted           = true;
			synchronized (entry) {
				entry.accepted_value = value;
				entry.write_ballot = ballot;
			}
			this.server_state.setCurrentBallot(ballot);
		}
		else
			maxballot = this.server_state.getCurrentBallot();
	}	

	DidaTradePaxos.PhaseTwoReply.Builder response_builder = DidaTradePaxos.PhaseTwoReply.newBuilder();
	response_builder.setAccepted(accepted);
	response_builder.setInstance(instance);
	response_builder.setServerid(this.server_state.my_id);
	response_builder.setRequestballot(ballot);
	response_builder.setMaxballot(maxballot);


	DidaTradePaxos.PhaseTwoReply response = response_builder.build();
	
	// System.out.println("Sending phase2 response: " + response);
	
	responseObserver.onNext(response);
	responseObserver.onCompleted();
	
	// Notify learners
	if (accepted == true) {
	    
	    Context ctx = Context.current().fork();
	    ctx.run(() -> {
		    List<Integer> learners = this.server_state.scheduler.learners(ballot);
		    int n_targets          = learners.size();
		    
		    DidaTradePaxos.LearnRequest.Builder learn_request_builder = DidaTradePaxos.LearnRequest.newBuilder();
		    learn_request_builder.setInstance(instance);
		    learn_request_builder.setValue(value);
		    learn_request_builder.setBallot(ballot);
			learn_request_builder.setServerid(this.server_state.my_id);
		    
		    DidaTradePaxos.LearnRequest learn_request = learn_request_builder.build();
		    
		    // System.out.println("Sending learn request: \n" + learn_request);
		    
		    System.out.println("Paxos acceptor: going to notify learners for entry " + instance + " with timestamp " + ballot + " request = " + learn_request);
		    ArrayList<DidaTradePaxos.LearnReply> learn_responses = new ArrayList<DidaTradePaxos.LearnReply>();
		    GenericResponseCollector<DidaTradePaxos.LearnReply> learn_collector = new GenericResponseCollector<DidaTradePaxos.LearnReply>(learn_responses, n_targets);;
		    for (int i = 0; i < n_targets; i++) {
			CollectorStreamObserver<DidaTradePaxos.LearnReply> learn_observer = new CollectorStreamObserver<DidaTradePaxos.LearnReply>(learn_collector);
			this.server_state.async_stubs[learners.get(i)].learn(learn_request, learn_observer);
		    }
		    // System.out.println("Learn request completed for instance = " + instance);
		});
	}
	
	
    }

    @Override
    public void learn(DidaTradePaxos.LearnRequest request, StreamObserver<DidaTradePaxos.LearnReply> responseObserver) {
	this.server_state.debugGate();
		// System.out.println("Receive learn request: \n" + request);

	int instance         = request.getInstance();
	int ballot           = request.getBallot();
	int value            = request.getValue();
	int acceptor 		 = request.getServerid();

	
	synchronized (this) {
	    PaxosInstance entry  = this.server_state.paxos_log.testAndSetEntry(instance);
	    
	    // System.out.println("Paxos learner: learnin entry " + instance + " with timestamp " + ballot);

	    this.server_state.setCurrentBallot(ballot);
	    
	    if (ballot > entry.accept_ballot) {
		// ballot mais recente para esta instância: recomeça a contagem
		entry.accept_ballot = ballot;
		entry.votes.clear();
	    }

	    if (ballot == entry.accept_ballot) {
		// votos por VALOR: num ballot rápido acceptors diferentes podem aceitar valores diferentes
		HashSet<Integer> voters = entry.votes.get(value);
		if (voters == null) {
		    voters = new HashSet<Integer>();
		    entry.votes.put(value, voters);
		}
		voters.add(acceptor);   // é um Set: o mesmo acceptor só conta uma vez
		System.out.println("Paxos learner for instance " + instance + " ballot " + ballot + " : votes " + entry.votes);

		int needed = this.server_state.decisionQuorum(ballot);
		if (voters.size() >= needed) {
		    synchronized (entry) {
			if (!entry.decided) {
			    entry.command_id = value;
			    entry.decided    = true;
			}
			entry.notifyAll();
		    }
		    this.server_state.updateCompletedBallot(ballot);
		}
		else if (this.server_state.isFast(ballot) && !entry.decided) {
		    // colisão: nenhum valor ainda consegue chegar ao quórum rápido neste ballot
		    int n_acceptors = this.server_state.scheduler.acceptors(ballot).size();
		    int voted = 0, best = 0;
		    for (HashSet<Integer> v : entry.votes.values()) {
			voted += v.size();
			best = Math.max(best, v.size());
		    }
		    if (best + (n_acceptors - voted) < needed && this.server_state.getCurrentBallot() == ballot) {
			System.out.println("Paxos learner: COLLISION on instance " + instance + " in fast ballot " + ballot + " -> moving to ballot " + (ballot + 1));
			this.server_state.setCurrentBallot(ballot + 1);
			this.server_state.main_loop.wakeup();
		    }
		}
	    }
	}
	
	DidaTradePaxos.LearnReply.Builder response_builder = DidaTradePaxos.LearnReply.newBuilder();
	response_builder.setInstance(instance);
	response_builder.setBallot(ballot);

	DidaTradePaxos.LearnReply response = response_builder.build();
	
	// System.out.println("Sending learn response");
		
	responseObserver.onNext(response);
	responseObserver.onCompleted();
    }

}
