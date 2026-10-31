package main

import (
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	clientConnectionsInterval = 60 * time.Second
	clientConnectionsMaxBytes = 16 << 20
)

var connectionLineRE = regexp.MustCompile(`^/(.+)\[[0-9a-fA-F]+\]\(queued=.*\)$`)

type clientConnectionSnapshot struct {
	collected time.Time
	metrics   map[string]string
}

type clientConnectionCollector struct {
	mu    sync.Mutex
	cache map[string]clientConnectionSnapshot
}

func (c *clientConnectionCollector) collect(options *Options, host string) map[string]string {
	c.mu.Lock()
	defer c.mu.Unlock()
	if snapshot, ok := c.cache[host]; ok && time.Since(snapshot.collected) < clientConnectionsInterval {
		return snapshot.metrics
	}
	start := time.Now()
	label := fmt.Sprintf("zk_host=%q", host)
	metrics := map[string]string{
		fmt.Sprintf("zk_client_connections_collection_success{%s}", label): "0",
	}
	counts, limit, err := collectClientConnections(options, host)
	if err != nil {
		log.Printf("warning: client connection collection failed for %q: %v", host, err)
	} else {
		metrics[fmt.Sprintf("zk_client_connections_collection_success{%s}", label)] = "1"
		metrics[fmt.Sprintf("zk_max_client_connections_per_ip{%s}", label)] = strconv.Itoa(limit)
		for ip, count := range counts {
			metrics[fmt.Sprintf("zk_client_connections{%s,client_ip=%q}", label, ip)] = strconv.Itoa(count)
		}
	}
	metrics[fmt.Sprintf("zk_client_connections_collection_duration_seconds{%s}", label)] =
		strconv.FormatFloat(time.Since(start).Seconds(), 'g', -1, 64)
	metrics[fmt.Sprintf("zk_client_connections_collection_timestamp_seconds{%s}", label)] =
		strconv.FormatInt(time.Now().Unix(), 10)
	if c.cache == nil {
		c.cache = make(map[string]clientConnectionSnapshot)
	}
	c.cache[host] = clientConnectionSnapshot{collected: time.Now(), metrics: metrics}
	return metrics
}

func collectClientConnections(options *Options, host string) (map[string]int, int, error) {
	conf, err := readClientConnectionCommand(options, host, "conf")
	if err != nil {
		return nil, 0, err
	}
	limit, err := parseClientConnectionLimit(conf)
	if err != nil {
		return nil, 0, err
	}
	cons, err := readClientConnectionCommand(options, host, "cons")
	if err != nil {
		return nil, 0, err
	}
	counts, err := parseClientConnections(cons)
	return counts, limit, err
}

func readClientConnectionCommand(options *Options, host, command string) (string, error) {
	timeout := time.Duration(options.Timeout) * time.Second
	if timeout <= 0 {
		return "", fmt.Errorf("%s: timeout must be positive", command)
	}
	conn, err := dial(host, timeout, options.ClientCert)
	if err != nil {
		return "", fmt.Errorf("%s dial: %w", command, err)
	}
	defer conn.Close()
	if err := conn.SetDeadline(time.Now().Add(timeout)); err != nil {
		return "", fmt.Errorf("%s deadline: %w", command, err)
	}
	if _, err := io.WriteString(conn, command); err != nil {
		return "", fmt.Errorf("%s write: %w", command, err)
	}
	data, err := ioutil.ReadAll(io.LimitReader(conn, clientConnectionsMaxBytes+1))
	if err != nil {
		return "", fmt.Errorf("%s read: %w", command, err)
	}
	if len(data) > clientConnectionsMaxBytes {
		return "", fmt.Errorf("%s response exceeds %d bytes", command, clientConnectionsMaxBytes)
	}
	if len(data) == 0 {
		return "", fmt.Errorf("%s returned an empty response", command)
	}
	if strings.Contains(string(data), cmdNotExecutedSffx) ||
		strings.Contains(string(data), instanceNotServingMessage) {
		return "", fmt.Errorf("%s is unavailable or not whitelisted", command)
	}
	return string(data), nil
}

func parseClientConnectionLimit(response string) (int, error) {
	if !strings.HasSuffix(response, "\n") {
		return 0, fmt.Errorf("conf response is not newline-terminated")
	}
	found := false
	limit := 0
	for _, line := range strings.Split(response, "\n") {
		parts := strings.SplitN(strings.TrimSpace(line), "=", 2)
		if len(parts) != 2 || parts[0] != "maxClientCnxns" {
			continue
		}
		if found {
			return 0, fmt.Errorf("conf contains duplicate maxClientCnxns")
		}
		var err error
		limit, err = strconv.Atoi(strings.TrimSpace(parts[1]))
		if err != nil || limit < 0 {
			return 0, fmt.Errorf("conf contains invalid maxClientCnxns")
		}
		found = true
	}
	if !found {
		return 0, fmt.Errorf("conf is missing maxClientCnxns")
	}
	return limit, nil
}

func parseClientConnections(response string) (map[string]int, error) {
	counts := make(map[string]int)
	for index, line := range strings.Split(response, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		match := connectionLineRE.FindStringSubmatch(line)
		if match == nil {
			return nil, fmt.Errorf("cons contains malformed connection at line %d", index+1)
		}
		// Java versions may render IPv6 socket addresses without brackets.
		address := match[1]
		separator := strings.LastIndexByte(address, ':')
		if separator < 0 {
			return nil, fmt.Errorf("cons is missing a client port at line %d", index+1)
		}
		port, err := strconv.Atoi(address[separator+1:])
		address = strings.Trim(address[:separator], "[]")
		if zone := strings.IndexByte(address, '%'); zone >= 0 {
			if zone == len(address)-1 || net.ParseIP(address[:zone]) == nil {
				return nil, fmt.Errorf("cons contains invalid client address at line %d", index+1)
			}
			address = address[:zone]
		}
		ip := net.ParseIP(address)
		if ip == nil || err != nil || port < 1 || port > 65535 {
			return nil, fmt.Errorf("cons contains invalid client address at line %d", index+1)
		}
		counts[ip.String()]++
	}
	return counts, nil
}
