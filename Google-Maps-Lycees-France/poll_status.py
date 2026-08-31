import sys
import time
import phantombuster_api as pb

agent_id = sys.argv[1]
expect_container = sys.argv[2]

r = pb.fetch_output(agent_id)
status = r.get("status")
container_id = r.get("containerId")
label = r.get("progressLabel")
print(f"{time.strftime('%H:%M:%S')} status={status} containerId={container_id} label={label}", flush=True)

if str(container_id) == str(expect_container) and status != "running":
    print(f"RUN_FINISHED status={status} containerId={container_id}")
    sys.exit(0)
sys.exit(2)
