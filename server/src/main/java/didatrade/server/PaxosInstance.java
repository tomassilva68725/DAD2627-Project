package didatrade.server;

import java.util.HashMap;
import java.util.HashSet;

public class PaxosInstance {
    int instance_nb;
    int command_id;
    int accepted_value;
    int write_ballot;
    int accept_ballot;
     HashMap<Integer, HashSet<Integer>> votes;
    boolean decided;
    boolean value_is_locked;

    public PaxosInstance() {
	this.instance_nb     = 0;
        this.command_id      = 0;
        this.accepted_value  = 0;
        this.write_ballot    = -1;
	this.accept_ballot   = -1;
        this.votes = new HashMap<Integer, HashSet<Integer>>();
	this.decided         = false;
        this.value_is_locked = false;
    }


    public PaxosInstance(int id) {
	this.instance_nb     = id;
        this.command_id      = 0;
        this.accepted_value  = 0;
        this.write_ballot    = -1;
	this.accept_ballot   = -1;
        this.votes = new HashMap<Integer, HashSet<Integer>>();
	this.decided         = false;
        this.value_is_locked = false;
    }
}
