# Epsilon Spark lab runbook

Use this checklist to prepare a controller and Linux workers for a lab session. Keep the actual hostnames, usernames, IP addresses, and SSH settings in local files; do not commit them.

## Network and host setup

- Connect the controller and workers to the same wired LAN, directly or through a switch.
- Use DHCP reservations for stable addresses when the router supports them. Otherwise, coordinate unused static addresses on the same subnet and avoid assigning a gateway on a worker interface unless the network requires one.
- Gigabit Ethernet is recommended for staging the multi-gigabyte dataset. Check each wired link with `cat /sys/class/net/<interface>/speed`; the result is in Mb/s.
- Install the worker prerequisites and configure key-based SSH as described in [`ssh-access.md`](ssh-access.md).

On the controller, create the ignored local configuration and edit it with the real LAN addresses and SSH aliases:

```bash
cp cluster.env.example cluster.env
```

For example, the shape of a three-worker configuration is:

```bash
MASTER_HOST=<CONTROLLER_LAN_IP>
WORKER_HOSTS=worker1,worker2,worker3
WORKER_IPS=worker1=<WORKER1_LAN_IP>,worker2=<WORKER2_LAN_IP>,worker3=<WORKER3_LAN_IP>
LOCAL_CORES=4
LOCAL_MEMORY=3072m
WORKER_CORES=4
WORKER_MEMORY=6144m
```

The aliases in `WORKER_HOSTS` must match entries in the controller's `~/.ssh/config`. `WORKER_IPS` must map each alias to that worker's wired address. Leave enough memory for the operating system and check available RAM before increasing the example allocations.

Verify SSH, Java 21, and each worker's current wired address:

```bash
for host in worker1 worker2 worker3; do
  ssh "$host" 'hostname; java -version 2>&1 | head -1; ip -brief -4 address'
done
```

## Smoke run

Build the Scala application, distribute a small deterministic sample, and confirm that tasks run on multiple hosts:

```bash
bin/build
bin/make-sample 512
bin/stage sample
bin/start-cluster
bin/observe
bin/submit sample | tee results/sample-run.log
```

The `HOST` summaries in the output should include the controller and at least one worker. Stop the cluster before changing its configuration or restarting the run:

```bash
bin/stop-cluster
```

## Epsilon run

Fetch or validate the 400,000-row Epsilon training set on the controller, stage the selected format on each worker, and run the distributed scan:

```bash
bin/fetch-epsilon
bin/stage epsilon
bin/start-cluster
bin/observe
bin/submit epsilon | tee results/epsilon-run.log
bin/stop-cluster
```

The default source is the LIBSVM BZip2 training file. If it is unavailable, the fetch script uses 20 SHA-256-verified Parquet shards pinned in `conf/epsilon-hf-train.tsv`; it can fall back to a checksum-verified TSV archive. The Scala job checks the 400,000 rows and 2,000 features and reports which hosts processed each partition. Keep at least 10 GiB free on each worker for the dataset and Spark runtime.

## Monitoring

- Master UI: `http://<MASTER_LAN_IP>:8080`
- Spark application UI while a job is active: `http://<MASTER_LAN_IP>:4040`
- Worker UI: `http://<WORKER_LAN_IP>:8081`
- Wireshark display filter: `tcp.port == 7077 || tcp.port == 7078 || tcp.port == 37071 || tcp.port == 37072`

A regular switch forwards unicast packets only to their destination ports. To capture traffic between two other nodes, use switch port mirroring or capture on one of those nodes.
