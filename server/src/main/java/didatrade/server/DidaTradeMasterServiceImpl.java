package didatrade.server;

import didatrade.DidaTradeMaster;
import didatrade.DidaTradeMasterServiceGrpc;
import didatrade.util.DebugMode;

import io.grpc.stub.StreamObserver;

public class DidaTradeMasterServiceImpl extends DidaTradeMasterServiceGrpc.DidaTradeMasterServiceImplBase {
    DidaTradeServerState server_state;

    public DidaTradeMasterServiceImpl(DidaTradeServerState state) {
	this.server_state = state;
    }

    @Override
    public void newballot(DidaTradeMaster.NewBallotRequest request, StreamObserver<DidaTradeMaster.NewBallotReply> responseObserver) {
	System.out.println(request);

	int request_id       = request.getReqid();
	int new_ballot       = request.getNewballot();
	int completed_ballot = request.getCompletedballot();;

	// for debug purposes
	System.out.println("Current ballot = " + this.server_state.getCurrentBallot() + " new ballot = " + new_ballot + " completed ballot = " + completed_ballot);

	this.server_state.setCompletedBallot (completed_ballot);
	
	if (new_ballot > this.server_state.getCurrentBallot()) {
	    this.server_state.setCurrentBallot (new_ballot);

	    this.server_state.main_loop.wakeup();

	    completed_ballot = this.server_state.waitForCompletedBallot(new_ballot);
	}
	else {
	    completed_ballot = this.server_state.getCompletedBallot();
	}
	
	DidaTradeMaster.NewBallotReply.Builder response_builder = DidaTradeMaster.NewBallotReply.newBuilder();
	response_builder.setReqid(request_id);
	response_builder.setCompletedballot(completed_ballot);

	DidaTradeMaster.NewBallotReply response = response_builder.build();
	responseObserver.onNext(response);
       	responseObserver.onCompleted();
    }


    @Override
    public void activate(DidaTradeMaster.ActivateRequest request, StreamObserver<DidaTradeMaster.ActivateReply> responseObserver) {
	System.out.println(request);

	int request_id       = request.getReqid();
	int completed_ballot = request.getCompletedballot();;

	// for debug purposes
	System.out.println("Current ballot = " + this.server_state.getCurrentBallot() + " activated ballot = " + completed_ballot);

	// do stuff
	
	DidaTradeMaster.ActivateReply.Builder response_builder = DidaTradeMaster.ActivateReply.newBuilder();
	response_builder.setReqid(request_id);
	response_builder.setAck(true);

	DidaTradeMaster.ActivateReply response = response_builder.build();
	responseObserver.onNext(response);
       	responseObserver.onCompleted();
    }

    @Override
    public void setdebug(DidaTradeMaster.SetDebugRequest request, StreamObserver<DidaTradeMaster.SetDebugReply> responseObserver) {
	// for debug purposes
	System.out.println(request);

	boolean response_value = true;

	int request_id   = request.getReqid();
	
	DebugMode mode;
	try {
		mode = DebugMode.fromCode(request.getMode());
	} catch (IllegalArgumentException e) {
		System.out.println("Invalid debug mode code: " + request.getMode());
		response_value = false;
		DidaTradeMaster.SetDebugReply reply = DidaTradeMaster.SetDebugReply.newBuilder()
			.setReqid(request_id)
			.setAck(response_value)
			.build();
		responseObserver.onNext(reply);
		responseObserver.onCompleted();
		return;
	}

	// for debug purposes
	System.out.println("Setting debug mode to = " + mode);

	switch (mode) {
	    case CRASH:
			Runtime.getRuntime().halt(1);
		break;
	    case FREEZE:
			this.server_state.setFrozen(true);
		break;
	    case UNFREEZE:
			this.server_state.setFrozen(false);
		break;
	    case SLOW_ON:
			this.server_state.setSlow(true);
		break;
	    case SLOW_OFF:
			this.server_state.setSlow(false);
		break;
	    default:
			System.out.println("Invalid debug mode code: " + request.getMode());
			response_value = false;
		break;
	}

	DidaTradeMaster.SetDebugReply.Builder response_builder = DidaTradeMaster.SetDebugReply.newBuilder();
	response_builder.setReqid(request_id);
	response_builder.setAck(response_value);
	
	DidaTradeMaster.SetDebugReply response = response_builder.build();
	responseObserver.onNext(response);
	responseObserver.onCompleted();
    }
}
