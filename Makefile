clear:
	rm -rf /tmp/my-raft-cluster/node{A,B,C}
	mkdir -p /tmp/my-raft-cluster/node{A,B,C}


runA:
	go run main.go --raft_bootstrap --raft_id=nodeA --address=localhost:50051 --raft_data_dir /tmp/my-raft-cluster

runB:
	go run main.go --raft_id=nodeB --address=localhost:50052 --raft_data_dir /tmp/my-raft-cluster

runC:
	go run main.go --raft_id=nodeC --address=localhost:50053 --raft_data_dir /tmp/my-raft-cluster
