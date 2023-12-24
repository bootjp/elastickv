clear:
	rm -rf /tmp/my-raft-cluster/node{A,B,C}
	mkdir -p /tmp/my-raft-cluster/node{A,B,C}


runA:
	go run main.go --raft_id=nodeA --address=localhost:50051 --redis_address=localhost:63791 --raft_data_dir /tmp/my-raft-cluster --raft_bootstrap

runB:
	go run main.go --raft_id=nodeB --address=localhost:50052 --redis_address=localhost:63792 --raft_data_dir /tmp/my-raft-cluster

runC:
	go run main.go --raft_id=nodeC --address=localhost:50053 --redis_address=localhost:63793 --raft_data_dir /tmp/my-raft-cluster


addNodes:
	raftadmin localhost:50051 add_voter nodeB localhost:50052 0
	raftadmin --leader multi:///localhost:50051,localhost:50052 add_voter nodeC localhost:50053 0

lint:
	golangci-lint --config=.golangci.yaml run --fix


gen:
	@$(MAKE)  -C proto gen
