from collections import deque
import threading
import pickle
import os

class AirportBagRouter:
    def _init_(self, allowed_gates=None, max_queue_length=1000, state_file="router_state.pkl"):
        """
        Initialize the router.
        - allowed_gates: List of valid gate identifiers.
        - max_queue_length: Max bags per gate queue before buffering.
        - state_file: Path for persistent state storage.
        """
        self.allowed_gates = set(allowed_gates) if allowed_gates else set()
        self.max_queue_length = max_queue_length
        self.state_file = state_file
        self.gate_queues = {}          # { gate: {"priority": deque(), "regular": deque()} }
        self.manual_inspection = deque()
        self.buffer_queue = deque()
        self.scanned_ids = set()
        self.lock = threading.Lock()
        self.load_state()

    def load_state(self):
        """Load saved state from disk (if any)."""
        if self.state_file and os.path.exists(self.state_file):
            with open(self.state_file, "rb") as f:
                state = pickle.load(f)
                self.gate_queues = state.get("gate_queues", {})
                self.manual_inspection = state.get("manual_inspection", deque())
                self.buffer_queue = state.get("buffer_queue", deque())
                self.scanned_ids = state.get("scanned_ids", set())

    def save_state(self):
        """Persist current state to disk."""
        if not self.state_file:
            return
        state = {
            "gate_queues": self.gate_queues,
            "manual_inspection": self.manual_inspection,
            "buffer_queue": self.buffer_queue,
            "scanned_ids": self.scanned_ids
        }
        with open(self.state_file, "wb") as f:
            pickle.dump(state, f)

    def scan_bag(self, bag_id, gate, is_priority=False):
        """
        Scan a bag:
        - Handles duplicates, missing/unknown gates.
        - Routes to priority or regular queue, or to buffer/manual inspection.
        """
        with self.lock:
            # Duplicate detection
            if bag_id in self.scanned_ids:
                return f"[Duplicate] {bag_id} ignored."
            self.scanned_ids.add(bag_id)

            # Missing tag
            if not gate:
                self.manual_inspection.append(bag_id)
                self.save_state()
                return f"[Manual Inspection] Missing gate tag for {bag_id}."

            # Unknown gate
            if self.allowed_gates and gate not in self.allowed_gates:
                self.manual_inspection.append(bag_id)
                self.save_state()
                return f"[Manual Inspection] Unknown gate {gate} for {bag_id}."

            # Initialize gate queue structure
            self.gate_queues.setdefault(gate, {"priority": deque(), "regular": deque()})

            # Overload handling
            total = len(self.gate_queues[gate]["priority"]) + len(self.gate_queues[gate]["regular"])
            if total >= self.max_queue_length:
                self.buffer_queue.append((bag_id, gate, is_priority))
                self.save_state()
                return f"[Buffered] Gate {gate} overloaded, {bag_id} sent to buffer."

            # Enqueue in appropriate queue
            queue_type = "priority" if is_priority else "regular"
            self.gate_queues[gate][queue_type].append(bag_id)
            self.save_state()
            return f"[Enqueued] {bag_id} -> Gate {gate} ({queue_type})."

    def get_next_bag(self, gate):
        """Retrieve the next bag for a gate (priority first, then regular)."""
        with self.lock:
            if gate not in self.gate_queues:
                return None
            if self.gate_queues[gate]["priority"]:
                bag = self.gate_queues[gate]["priority"].popleft()
            elif self.gate_queues[gate]["regular"]:
                bag = self.gate_queues[gate]["regular"].popleft()
            else:
                bag = None
            self.save_state()
            return bag

    def count_bags(self, gate):
        """Return total number of bags for a gate."""
        with self.lock:
            if gate not in self.gate_queues:
                return 0
            return (len(self.gate_queues[gate]["priority"]) +
                    len(self.gate_queues[gate]["regular"]))

    def view_all_bags(self, gate):
        """List all pending bags (priority then regular) for a gate."""
        with self.lock:
            if gate not in self.gate_queues:
                return []
            return list(self.gate_queues[gate]["priority"]) + list(self.gate_queues[gate]["regular"])

    def view_manual_inspection(self):
        """View bags flagged for manual inspection."""
        with self.lock:
            return list(self.manual_inspection)

    def view_buffer(self):
        """View bags sent to the buffer queue due to overload."""
        with self.lock:
            return list(self.buffer_queue)


# Demo usage
if _name_ == "_main_":
    router = AirportBagRouter(allowed_gates=["G1","G2","G3"], max_queue_length=2)

    print(router.scan_bag("BAG1", "G1"))
    print(router.scan_bag("BAG2", "G1", is_priority=True))
    print(router.scan_bag("BAG3", "G1"))
    print(router.scan_bag("BAG1", "G1"))  # Duplicate
    print("Next for G1:", router.get_next_bag("G1"))
    print("Manual Inspection:", router.view_manual_inspection())
    print("Buffer Queue:", router.view_buffer())
