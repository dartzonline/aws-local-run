"""Fault injection manager for testing app resilience."""
import random
import time
import logging

logger = logging.getLogger("localrun.faults")

class FaultManager:
    def __init__(self):
        # List of configured faults
        # Format:
        # {
        #   "id": "123",
        #   "service": "s3",        # optional, filter by service (e.g. "s3")
        #   "action": "PutObject",  # optional, filter by action (e.g. "PutObject")
        #   "type": "error",        # "error" or "latency"
        #   "probability": 1.0,     # 0.0 to 1.0
        #   "latency_ms": 500,      # for "latency" type
        #   "error_code": 503,      # HTTP status code for "error" type
        #   "error_message": "Service Unavailable" # string for "error" type
        # }
        self.faults = []

    def get_all(self):
        """Return all configured faults."""
        return self.faults

    def add(self, fault: dict) -> str:
        """Add a new fault and return its ID.
        
        Supports preset types:
          dynamodb_throttle - returns ProvisionedThroughputExceededException
          lambda_cold_start - adds delay before Lambda invocations
          s3_slow_response  - adds delay before S3 responses
        """
        import uuid
        fid = uuid.uuid4().hex[:8]
        fault = fault.copy()
        fault["id"] = fid
        # Set defaults
        fault.setdefault("probability", 1.0)

        # Expand preset types into standard fault configs
        ftype = fault.get("type", "")
        if ftype == "dynamodb_throttle":
            fault["type"] = "error"
            fault["service"] = "dynamodb"
            fault.setdefault("error_code", 400)
            fault.setdefault("error_type", "ProvisionedThroughputExceededException")
            fault.setdefault("error_message", "Rate exceeded for table")
        elif ftype == "lambda_cold_start":
            fault["type"] = "latency"
            fault["service"] = "lambda"
            fault.setdefault("latency_ms", 3000)
        elif ftype == "s3_slow_response":
            fault["type"] = "latency"
            fault["service"] = "s3"
            fault.setdefault("latency_ms", 2000)
        elif ftype == "sqs_message_drop":
            # Drop SQS messages by intercepting SendMessage with an error response
            fault["type"] = "error"
            fault["service"] = "sqs"
            fault.setdefault("action", "SendMessage")
            fault.setdefault("error_code", 500)
            fault.setdefault("error_type", "ServiceUnavailable")
            fault.setdefault("error_message", "Message dropped by fault injection")

        self.faults.append(fault)
        logger.info("Added fault injection config: %s", fault)
        return fid

    def remove(self, fault_id: str) -> bool:
        """Remove a fault by ID."""
        initial_len = len(self.faults)
        self.faults = [f for f in self.faults if f["id"] != fault_id]
        if len(self.faults) < initial_len:
            logger.info("Removed fault injection config: %s", fault_id)
            return True
        return False

    def clear(self):
        """Remove all faults."""
        self.faults.clear()
        logger.info("Cleared all fault injection configs")

    def _matches(self, fault: dict, service: str, action: str) -> bool:
        """Check if a fault applies to the current request."""
        f_svc = fault.get("service")
        # Fault service could be "s3" or "dynamodb". We do exact match if specified.
        if f_svc and f_svc.lower() != service.lower():
            return False

        f_act = fault.get("action")
        # Fault action could be "PutObject". Exact match.
        if f_act and action and f_act != action:
            return False

        # Check probability — skip the fault if a random roll misses
        prob = fault.get("probability", 1.0)
        if prob < 1.0 and random.random() >= prob:
            return False

        return True

    def apply_faults(self, service, action):
        """
        Evaluate faults for the given service/action.
        Applies latency synchronously.
        If an error fault triggers, returns a dict with the error response info.
        """
        # We find all matching faults. We apply all matching latencies (additive or max? Let's do max for simplicity).
        # We return the first matching error.
        
        max_latency = 0
        error_to_return = None

        for f in self.faults:
            if not self._matches(f, service, action):
                continue

            ftype = f.get("type")
            if ftype == "latency":
                lat = f.get("latency_ms", 0)
                if lat > max_latency:
                    max_latency = lat
            elif ftype == "error" and not error_to_return:
                error_to_return = {
                    "status_code": f.get("error_code", 500),
                    "error_type": f.get("error_type", "InternalFailure"),
                    "message": f.get("error_message", "Injected fault error")
                }

        if max_latency > 0:
            logger.info("Injecting %d ms latency for %s %s", max_latency, service, action)
            time.sleep(max_latency / 1000.0)

        if error_to_return:
            logger.info("Injecting %s error for %s %s", error_to_return["status_code"], service, action)
            return error_to_return

        return None
