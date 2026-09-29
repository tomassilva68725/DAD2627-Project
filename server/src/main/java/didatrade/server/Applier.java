package didatrade.server;

import java.util.concurrent.atomic.AtomicInteger;

public class Applier implements Runnable {
    private DidaTradeServerState server_state;
    private AtomicInteger next_apply;

    public Applier(DidaTradeServerState state) {
        this.server_state = state;
        this.next_apply   = new AtomicInteger(0);
    }

    public int getNextApply() {
        return this.next_apply.get();
    }

    public void run() {
        while (true){
            PaxosInstance entry = server_state.paxos_log.testAndSetEntry(this.next_apply.get());
        
            synchronized (entry) {
                while (!entry.decided) {
                    try {
                        entry.wait();
                    } catch (InterruptedException e) {}
                }
            }
        applyEntry(entry.command_id);
        this.next_apply.incrementAndGet();
        }
    }

    private void applyEntry(int command_id) {
        while(server_state.req_history.getIfProcessed(command_id) == null) {
            RequestRecord request_record = server_state.req_history.getIfPending(command_id);
            if (request_record != null) {
                executeCommand(request_record);
                server_state.req_history.moveToProcessed(command_id);
                return;
            }
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {}
        }
    }
     
    private void executeCommand(RequestRecord request_record) {
        DidaTradeCommand command = request_record.getRequest();
        boolean result = false;
        int balance = 0;

        switch (command.getAction()) {
            case DidaTradeAction.POPULATE:
                result = server_state.trade_manager.populate(command.getQuantity());
                break;
            case DidaTradeAction.ADDUSER:
                result = server_state.trade_manager.add_user(command.getUserId(), command.getQuantity(), command.getStock());
                break;
            case DidaTradeAction.SELL:
                result = server_state.trade_manager.sell(command.getUserId(), command.getQuantity());
                break;
            case DidaTradeAction.BUY:
                result = server_state.trade_manager.acquire(command.getUserId(), command.getQuantity());
                break;
            case DidaTradeAction.BALANCE:
                balance = server_state.trade_manager.balance(command.getUserId());
                command.setQuantity(balance);
                result = (balance != -1);
                break;
            case DidaTradeAction.DUMP:
                server_state.trade_manager.dump();
                result = true;
                break;
            default:
                result = false;
                System.err.println("*** Unknown command ****");
                break;
        }

        request_record.setResponse(result);
    }
}
