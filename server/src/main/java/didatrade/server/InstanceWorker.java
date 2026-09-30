package didatrade.server;

import java.util.ArrayList;
import java.util.List;

import didatrade.DidaTradePaxos;
import didatrade.util.GenericResponseCollector;
import didatrade.util.CollectorStreamObserver;
import didatrade.util.PhaseOneResponseProcessor;
import didatrade.util.PhaseTwoResponseProcessor;

public class InstanceWorker implements Runnable {
    private DidaTradeServerState server_state;
    private int instance;
    private RequestRecord request;
    private int learned_max_instance;
    public static final int NO_OP = -1;

    public InstanceWorker(DidaTradeServerState state, int instance, RequestRecord request) {
        this.server_state = state;
        this.instance     = instance;
        this.request      = request;
        this.learned_max_instance = -1;
    }


    public void run() {
        boolean decided = false;

        while (!decided) {
            int ballot = this.server_state.getCurrentBallot();

            if(server_state.scheduler.leader(ballot) != server_state.my_id) {
                // Not the leader
                giveBackRequest();
                return;
            }

            List<Integer> acceptors = server_state.scheduler.acceptors(ballot);
            int quorum = server_state.scheduler.quorum(ballot);
            int n_acceptors = acceptors.size();

            int value = ownValue();
            boolean adopted = false;

            //Phase 1 (only once per ballot: multi-paxos)
            if (!server_state.canSkipPhaseOne(ballot, this.instance)) {

            
                System.out.println("[" + System.currentTimeMillis() + "] Instance " + this.instance + ": starting phase 1 with ballot " + ballot);

                DidaTradePaxos.PhaseOneRequest.Builder phase_one_request_builder = DidaTradePaxos.PhaseOneRequest.newBuilder();
                phase_one_request_builder.setInstance(this.instance);
                phase_one_request_builder.setRequestballot(ballot);
                DidaTradePaxos.PhaseOneRequest phase_one_request = phase_one_request_builder.build();

                int completed_ballot = server_state.getCompletedBallot();
                int low_ballot  = Math.max(completed_ballot, 0);
                int high_ballot = ballot;

                PhaseOneResponseProcessor phase_one_processor = new PhaseOneResponseProcessor(server_state.scheduler, low_ballot, high_ballot);
                ArrayList<DidaTradePaxos.PhaseOneReply> phase_one_responses = new ArrayList<DidaTradePaxos.PhaseOneReply>();
                GenericResponseCollector<DidaTradePaxos.PhaseOneReply> phase_one_collector =
                    new GenericResponseCollector<DidaTradePaxos.PhaseOneReply>(phase_one_responses, n_acceptors, phase_one_processor);

                for (int i = 0; i < n_acceptors; i++) {
                    CollectorStreamObserver<DidaTradePaxos.PhaseOneReply> phase_one_observer =
                        new CollectorStreamObserver<DidaTradePaxos.PhaseOneReply>(phase_one_collector);
                    server_state.async_stubs[acceptors.get(i)].phaseone(phase_one_request, phase_one_observer);
                }
                phase_one_collector.waitUntilDone();

                if (phase_one_processor.getAccepted() == false) {
                    server_state.setCurrentBallot(phase_one_processor.getMaxballot());
                    try { Thread.sleep(100); } catch (InterruptedException e) {}
                    continue;
                }

                this.learned_max_instance = phase_one_processor.getMaxInstance();

                if (phase_one_processor.getValballot() > -1) {
                    value = phase_one_processor.getValue();
                    adopted = (value != ownValue());
                }
            }
            else{
                System.out.println("[" + System.currentTimeMillis() + "] Instance " + this.instance + ": skipping phase 1, ballot " + ballot + " already prepared");
            }

            System.out.println("[" + System.currentTimeMillis() + "] Instance " + this.instance + ": phase 1 done, starting phase 2 with value " + value);

            //Phase 2
            DidaTradePaxos.PhaseTwoRequest.Builder phase_two_request_builder = DidaTradePaxos.PhaseTwoRequest.newBuilder();
            phase_two_request_builder.setInstance(this.instance);
            phase_two_request_builder.setRequestballot(ballot);
            phase_two_request_builder.setValue(value);
            DidaTradePaxos.PhaseTwoRequest phase_two_request = phase_two_request_builder.build();

            PhaseTwoResponseProcessor phase_two_processor = new PhaseTwoResponseProcessor(quorum);
            ArrayList<DidaTradePaxos.PhaseTwoReply> phase_two_responses = new ArrayList<DidaTradePaxos.PhaseTwoReply>();
            GenericResponseCollector<DidaTradePaxos.PhaseTwoReply> phase_two_collector =
                new GenericResponseCollector<DidaTradePaxos.PhaseTwoReply>(phase_two_responses, n_acceptors, phase_two_processor);

            for (int i = 0; i < n_acceptors; i++) {
                CollectorStreamObserver<DidaTradePaxos.PhaseTwoReply> phase_two_observer =
                    new CollectorStreamObserver<DidaTradePaxos.PhaseTwoReply>(phase_two_collector);
                server_state.async_stubs[acceptors.get(i)].phasetwo(phase_two_request, phase_two_observer);
            }
            phase_two_collector.waitUntilDone();

            if (phase_two_processor.getAccepted() == false) {
                server_state.setCurrentBallot(phase_two_processor.getMaxballot());
                try { Thread.sleep(100); } catch (InterruptedException e) {}
                continue;
            }

            System.out.println("[" + System.currentTimeMillis() + "] Instance " + this.instance + ": DECIDED with value " + value);
            markDecided(value);

            if (adopted) 
                giveBackRequest();

            decided = true;
        }
    }

    private void markDecided(int value) {
        PaxosInstance entry = server_state.paxos_log.testAndSetEntry(this.instance);
        synchronized (entry) {
            if (!entry.decided) {
                entry.command_id = value;
                entry.decided    = true;
            }
            entry.notifyAll();
        }
    }

    private void giveBackRequest() {
        if (this.request == null)
            return;
        server_state.req_history.requeue(this.request.getId());
        server_state.main_loop.wakeup();
    }

    public int getLearnedMaxInstance(){
        return this.learned_max_instance;
    }

    private int ownValue() {
        return (this.request != null) ? this.request.getId() : NO_OP;
    }
}
