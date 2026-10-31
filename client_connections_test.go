package main

import (
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestParseClientConnections(t *testing.T) {
	response := " /192.0.2.10:40100[1](queued=0,recved=2,sent=1)\n" +
		" /192.0.2.10:40101[1](queued=0,recved=2,sent=1)\n" +
		" /[::ffff:192.0.2.10]:40102[1](queued=0)\n" +
		" /[2001:db8::1]:41000[0](queued=0)\n" +
		" /2001:db8::1:41001[a](queued=0)\n\n"
	response += " /[fe80::1%2]:41002[0](queued=0)\n" +
		" /fe80::1%eth0:41003[0](queued=0)\n"
	counts, err := parseClientConnections(response)
	if err != nil {
		t.Fatal(err)
	}
	if len(counts) != 3 || counts["192.0.2.10"] != 3 ||
		counts["2001:db8::1"] != 2 || counts["fe80::1"] != 2 {
		t.Fatalf("unexpected counts: %v", counts)
	}
	for _, invalid := range []string{
		"cons is not executed because it is not in the whitelist.",
		instanceNotServingMessage, "/bad:4000[1](queued=0)",
		"/127.0.0.1:0[1](queued=0)", "/127.0.0.1:65536[1](queued=0)",
		"/127.0.0.1:bad[1](queued=0)", "/127.0.0.1:4000[1](queued=",
	} {
		if _, err := parseClientConnections(response + invalid); err == nil {
			t.Errorf("accepted malformed response %q", invalid)
		}
	}
	counts, err = parseClientConnections("\n")
	if err != nil || len(counts) != 0 {
		t.Fatalf("empty connection list: %v %v", counts, err)
	}
}

func TestParseClientConnectionLimit(t *testing.T) {
	for _, limit := range []int{0, 800, 1200} {
		got, err := parseClientConnectionLimit(fmt.Sprintf("clientPort=2181\nmaxClientCnxns=%d\n", limit))
		if err != nil || got != limit {
			t.Fatalf("limit %d: got %d, %v", limit, got, err)
		}
	}
	for _, invalid := range []string{
		"", "maxClientCnxns=bad", "maxClientCnxns=-1", "maxClientCnxns=8",
		"maxClientCnxns=800\nmaxClientCnxns=900",
	} {
		if _, err := parseClientConnectionLimit(invalid); err == nil {
			t.Errorf("accepted invalid configuration %q", invalid)
		}
	}
}

func TestTruncatedClientConnectionConfigFailsCollection(t *testing.T) {
	server := newFakeZookeeper(t)
	server.mu.Lock()
	server.replies["conf"] = "maxClientCnxns=8"
	server.mu.Unlock()

	options := server.options()
	host := options.Hosts[0]
	metrics := (&clientConnectionCollector{}).collect(options, host)
	success := fmt.Sprintf("zk_client_connections_collection_success{zk_host=%q}", host)
	if metrics[success] != "0" {
		t.Fatalf("truncated config reported success: %v", metrics)
	}
	if _, ok := metrics[fmt.Sprintf("zk_max_client_connections_per_ip{zk_host=%q}", host)]; ok {
		t.Fatalf("truncated config reported a limit: %v", metrics)
	}
	if _, ok := metrics[fmt.Sprintf("zk_client_connections{zk_host=%q,client_ip=\"127.0.0.1\"}", host)]; ok {
		t.Fatalf("truncated config reported client counts: %v", metrics)
	}
}

type fakeZookeeper struct {
	listener net.Listener
	mu       sync.Mutex
	replies  map[string]string
	calls    map[string]int
}

func newFakeZookeeper(t *testing.T) *fakeZookeeper {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := &fakeZookeeper{
		listener: listener,
		calls:    make(map[string]int),
		replies: map[string]string{
			"mntr": "zk_num_alive_connections\t3\nzk_server_state\tfollower\n",
			"ruok": "imok", "conf": "maxClientCnxns=800\n",
			"cons": " /127.0.0.1:4000[1](queued=0)\n",
		},
	}
	t.Cleanup(func() { listener.Close() })
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go func() {
				defer conn.Close()
				conn.SetDeadline(time.Now().Add(5 * time.Second))
				command := make([]byte, 4)
				if _, err := io.ReadFull(conn, command); err != nil {
					return
				}
				server.mu.Lock()
				server.calls[string(command)]++
				response := server.replies[string(command)]
				server.mu.Unlock()
				io.WriteString(conn, response)
			}()
		}
	}()
	return server
}

func (s *fakeZookeeper) options() *Options {
	return &Options{Timeout: 1, Hosts: []string{s.listener.Addr().String()}}
}

func TestClientCollectionOptIn(t *testing.T) {
	server := newFakeZookeeper(t)
	options := server.options()
	host := options.Hosts[0]
	legacyKey := fmt.Sprintf("zk_num_alive_connections{zk_host=%q}", host)
	if got := getMetrics(options)[legacyKey]; got != "3" {
		t.Fatalf("legacy metric changed: %q", got)
	}
	server.mu.Lock()
	calls := server.calls["cons"] + server.calls["conf"]
	server.mu.Unlock()
	if calls != 0 {
		t.Fatal("disabled collector sent new commands")
	}
	options.ClientConnections = &clientConnectionCollector{}
	metrics := getMetrics(options)
	for key, want := range map[string]string{
		legacyKey: "3",
		fmt.Sprintf("zk_client_connections{zk_host=%q,client_ip=\"127.0.0.1\"}", host): "1",
		fmt.Sprintf("zk_max_client_connections_per_ip{zk_host=%q}", host):              "800",
		fmt.Sprintf("zk_client_connections_collection_success{zk_host=%q}", host):      "1",
	} {
		if metrics[key] != want {
			t.Errorf("%s = %q; want %q", key, metrics[key], want)
		}
	}
}

func TestClientCollectionReportsHostnameResolutionFailure(t *testing.T) {
	host := "zookeeper-host-does-not-exist.invalid:2181"
	collector := &clientConnectionCollector{}
	metrics := getMetrics(&Options{
		Timeout:           1,
		Hosts:             []string{host},
		ClientConnections: collector,
	})
	success := fmt.Sprintf("zk_client_connections_collection_success{zk_host=%q}", host)
	duration := fmt.Sprintf("zk_client_connections_collection_duration_seconds{zk_host=%q}", host)
	timestamp := fmt.Sprintf("zk_client_connections_collection_timestamp_seconds{zk_host=%q}", host)
	if metrics[success] != "0" || metrics[duration] == "" || metrics[timestamp] == "" {
		t.Fatalf("missing collection failure metrics after resolution failure: %v", metrics)
	}
	if _, ok := collector.cache[host]; !ok {
		t.Fatal("hostname resolution failure was not cached")
	}
}

func TestClientCollectionCacheAndFailures(t *testing.T) {
	server := newFakeZookeeper(t)
	options := server.options()
	host := options.Hosts[0]
	collector := &clientConnectionCollector{}
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			collector.collect(options, host)
		}()
	}
	wg.Wait()
	server.mu.Lock()
	if server.calls["cons"] != 1 || server.calls["conf"] != 1 {
		t.Errorf("overlapping or uncached collection: %v", server.calls)
	}
	server.replies["cons"] = "cons is not executed because it is not in the whitelist.\n"
	server.mu.Unlock()
	collector.cache[host] = clientConnectionSnapshot{}
	metrics := collector.collect(options, host)
	success := fmt.Sprintf("zk_client_connections_collection_success{zk_host=%q}", host)
	if metrics[success] != "0" || len(metrics) != 3 {
		t.Fatalf("failure retained stale counts/limit: %v", metrics)
	}
	collector.collect(options, host)
	server.mu.Lock()
	calls := server.calls["cons"]
	server.replies["cons"] = "\n"
	server.mu.Unlock()
	if calls != 2 {
		t.Fatalf("failed collection was not cached: %d calls", calls)
	}
	collector.cache[host] = clientConnectionSnapshot{}
	metrics = collector.collect(options, host)
	if metrics[success] != "1" || len(metrics) != 4 {
		t.Fatalf("disconnected clients remained: %v", metrics)
	}
}

func TestClientCommandResponseBounds(t *testing.T) {
	server := newFakeZookeeper(t)
	for _, response := range []string{"", strings.Repeat("x", clientConnectionsMaxBytes+1), instanceNotServingMessage} {
		server.mu.Lock()
		server.replies["cons"] = response
		server.mu.Unlock()
		if _, err := readClientConnectionCommand(server.options(), server.listener.Addr().String(), "cons"); err == nil {
			t.Errorf("accepted invalid response of length %d", len(response))
		}
	}
}

func TestClientCommandReadDeadline(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	done := make(chan struct{})
	go func() {
		defer close(done)
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		ioutil.ReadAll(conn)
	}()
	start := time.Now()
	_, err = readClientConnectionCommand(&Options{Timeout: 1}, listener.Addr().String(), "cons")
	if err == nil || !strings.Contains(err.Error(), "timeout") || time.Since(start) > 3*time.Second {
		t.Fatalf("deadline not enforced: elapsed=%v err=%v", time.Since(start), err)
	}
	<-done
}

func BenchmarkParseClientConnections(b *testing.B) {
	for _, size := range []int{3000, 20000} {
		b.Run(fmt.Sprintf("%d", size), func(b *testing.B) {
			var response strings.Builder
			for i := 0; i < size; i++ {
				fmt.Fprintf(&response, " /10.0.%d.%d:%d[1](queued=0,recved=2,sent=1)\n",
					(i/200)%200, i%200+1, 10000+i)
			}
			input := response.String()
			b.ReportAllocs()
			b.SetBytes(int64(len(input)))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := parseClientConnections(input); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
