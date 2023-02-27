.PHONY: build run-server tpc-local-test tpc ycsb local tmp msgtest exp mi heu skew

all: build

clean:
	@docker rm -f $(docker ps -aq)
	@docker rmi $(docker images -aq)

build:
	@go build -o ./bin/flac-server ./flac-server/main.go

local:
	@tc qdisc add dev lo root handle 1: prio bands 4
	@tc qdisc add dev lo parent 1:4 handle 40: netem delay 10ms
	@tc filter add dev lo protocol ip parent 1:0 prio 4 u32  match ip dport 6001 0xffff flowid 1:4
	@tc filter add dev lo protocol ip parent 1:0 prio 4 u32  match ip dport 6002 0xffff flowid 1:4
	@tc filter add dev lo protocol ip parent 1:0 prio 4 u32  match ip dport 6003 0xffff flowid 1:4
	@tc filter add dev lo protocol ip parent 1:0 prio 4 u32  match ip dport 6004 0xffff flowid 1:4
	@tc filter add dev lo protocol ip parent 1:0 prio 4 u32  match ip dport 6005 0xffff flowid 1:4
	@tc filter add dev lo protocol ip parent 1:0 prio 4 u32  match ip dport 5001 0xffff flowid 1:4

del:
	@tc filter del dev lo pref 4
	@tc qdisc  del dev lo root

test:
	@go test -race ./lock
	@echo "Latch race test pass!"
	@go test -race ./storage/mockkv
	@echo "The KV unit test pass!"
	# currently the port will remain blocked after a unit test finish, plz test them one by one.	@go test ./participant
#	@echo "The participant side transaction manager test pass!"
#	@go test -race ./coordinator
#	@echo "The coordinator side transaction manager test pass!"
#
#help:
#	@echo "tpc 		----	run all tpc tests"
#	@echo "ycsb 	----	run all ycsb tests"
#	@echo "build 	----	build the binary for the FLAC-server"
#	@echo "local	----	adapt Msg queue with filter on net card to introduce local message delay"
#	@echo "serveri 	----	run participant i, i = 0, 1, 2"

buildrpc:
	@cd downserver
	@python -m grpc_tools.protoc --python_out=. --grpc_python_out=. -I. rpc.proto
	@protoc --go_out=plugins=grpc:. rpc.proto

exp:
	@make build
	@python3 experiment/experiment.py

down:
	@python3 downserver/main.py 68

check:
	@make build
	@./bin/flac-server -node=ca -addr=127.0.0.1:5001 -local -bench=ycsb -p=flac -len=5 -c=512 -r=1 -skew=0.5
