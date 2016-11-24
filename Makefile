all:
	go build redisproxy/servers/config-server
	go build redisproxy/servers/proxy-server
clean:
	rm -rf config-server proxy-server
run:
	./config-server &
	./proxy-server &
kill:
	killall -9 config-server proxy-server 
