# Network and cluster observability

This project includes a small LAN dashboard and a repeatable Wireshark workflow. The dashboard reports host and Spark state; Wireshark captures individual packets.

## Start the visual dashboard

From the controller, with `cluster.env` configured:

```bash
bin/start-cluster
```

Open `http://<MASTER_LAN_IP>:8090`. The page refreshes every five seconds and shows Spark worker registration, CPU, load, RAM, root-disk usage, Ethernet byte rates, and TCP sockets using Spark ports. For remote CPU, memory, disk, and socket data, the controller needs working SSH aliases from `WORKER_HOSTS`. A powered-off node remains visible with its Spark state and is omitted from live resource totals. The dashboard uses only Python's standard library and is bound to `MASTER_HOST` by default. `DASHBOARD_BIND` and `DASHBOARD_PORT` can override the bind address and port in the local environment.

The dashboard is intended for a trusted lab LAN and does not add authentication. It reports socket endpoints and counters, not packet contents. Stop it with `bin/stop-cluster`. To change its bind address or port, set `DASHBOARD_BIND` or `DASHBOARD_PORT` in the controller's local `cluster.env`.

After a sudden power-off, the kernel may retain a TCP socket for a while even after Spark has marked that worker `DEAD`. Use the Spark state column as the registration status and compare it with socket state and the worker UI.

## Install Wireshark on Debian or Ubuntu

Install it on the machine where you want to capture packets. The package manager may request an interactive sudo password:

```bash
sudo apt update
sudo apt install wireshark
```

Allow non-root capture only if this is appropriate for the lab machine:

```bash
sudo dpkg-reconfigure wireshark-common
sudo usermod -aG wireshark "$USER"
```

Choose **Yes** for non-superuser packet capture, then log out and back in so the group membership takes effect. Otherwise, start Wireshark with the system's authorized privilege mechanism. Do not run packet capture on an untrusted network.

## Capture a Spark run

1. On the controller, find the wired interface and confirm its address:

   ```bash
   ip -brief -4 address
   ```

2. Open Wireshark and select the interface that owns the controller's `MASTER_HOST` LAN address. Start the capture before `bin/submit epsilon`.
3. To limit the capture file while collecting, use this capture filter:

   ```text
   tcp port 7077 or tcp port 7078 or tcp port 37071 or tcp port 37072 or tcp port 37073
   ```

4. To inspect the saved or live capture, use this display filter:

   ```text
   tcp.port == 7077 || tcp.port == 7078 || tcp.port == 8080 || tcp.port == 8081 || tcp.port == 4040 || tcp.port == 37071 || tcp.port == 37072 || tcp.port == 37073
   ```

Useful display filters:

| Filter | What it highlights |
|---|---|
| `tcp.flags.syn == 1` | TCP connection attempts and setup |
| `tcp.analysis.retransmission` | TCP retransmissions worth investigating |
| `icmp || arp` | Network reachability checks and address resolution |
| `ip.addr == <NODE_IP>` | Traffic to or from one node; replace the placeholder with its address |
| `tcp.port == 8080 || tcp.port == 8081 || tcp.port == 4040` | Spark status pages and the active application UI |

Use **Statistics → Conversations** to compare endpoints and bytes, **Statistics → Endpoints** to see participating hosts, and **Statistics → I/O Graphs** to see traffic over time. The packet list and packet-details panes show individual frames. Spark's RPC and shuffle traffic may not be decoded as a named Wireshark application protocol; TCP endpoints, volume, timing, and retransmissions are still visible.

## What a switch capture can see

With a regular unmanaged switch, a capture on the controller sees packets sent to or from that controller, such as Spark control traffic and block transfers involving its local worker. It normally cannot see unicast traffic exchanged only between two other workers. For that traffic, capture on one of those workers or use a managed switch configured for port mirroring/SPAN. A switch by itself does not make all packets visible on every port.

Use the Spark Master page at `http://<MASTER_LAN_IP>:8080`, the application page at `http://<MASTER_LAN_IP>:4040` while a job runs, the worker pages at `http://<WORKER_LAN_IP>:8081`, and the dashboard together. The UIs show Spark registration and task activity; Wireshark shows network frames.

## Ports used by this project

| Port | Purpose |
|---:|---|
| 7077/TCP | Spark Standalone master RPC |
| 7078/TCP | Worker service port |
| 8080/TCP | Spark Master web UI |
| 8081/TCP | Spark Worker web UI |
| 4040/TCP | Spark application web UI during a run |
| 37071/TCP | Configured Spark driver RPC port |
| 37072/TCP | Configured Spark block-manager port |
| 37073/TCP | Possible local executor block-manager port when the controller's driver already uses 37072 |
| 8090/TCP | Local cluster observability dashboard |

Spark Standalone is unauthenticated in this project. Keep these services on a trusted LAN and do not expose them to the public Internet.
