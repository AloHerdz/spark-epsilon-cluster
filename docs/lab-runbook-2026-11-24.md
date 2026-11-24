# Epsilon Spark lab runbook

This records the LAN values observed on 2026-11-23 for the planned lab session on 2026-11-24. Confirm them again before starting Spark; DHCP leases can change. Reserve the addresses in the router if possible.

## Network map

```text
Router / gateway 192.168.1.254
          |
        Switch
        /    \
 Razer master   ASUS worker
 192.168.1.111  192.168.1.100
 wired Ethernet wired Ethernet
```

The Razer also has Wi-Fi `192.168.1.97`; use the wired address `192.168.1.111` for Spark. The gateway is not a Spark node. Connect the router and both computers to the switch, and keep both nodes on the same LAN.

## ASUS: enable the first SSH connection

The Razer can ping `192.168.1.100` and the ASUS now accepts SSH from the Razer. Open a terminal on the ASUS and run the following to install the worker runtime and utilities:

```bash
sudo apt-get update
sudo apt-get install -y openssh-server openjdk-21-jre-headless rsync curl tar bzip2 python3
sudo systemctl enable --now ssh
sudo systemctl status ssh --no-pager
hostname -I
```

If UFW is active on the ASUS, allow SSH from the Razer only:

```bash
sudo ufw status
sudo ufw allow from 192.168.1.111 to any port 22 proto tcp
```

The Razer's existing `asus` alias is already key-authenticated. Verify the connection from the controller:

```bash
ssh asus 'hostname; whoami; java -version; ip -brief -4 address'
```

For other controllers and worker accounts, use the generic key and alias instructions in [`ssh-access.md`](ssh-access.md); each teammate should set their own IP addresses and Linux usernames in their local `~/.ssh/config`.

## Prepare the worker and run

On the Razer, from the repository:

```bash
bin/bootstrap-debian master
bin/install master
bin/make-sample 512
bin/stage sample
bin/start-cluster
bin/observe
bin/submit sample | tee results/sample-run.log
bin/stop-cluster
```

The sample run should report the Razer and ASUS as task hosts before proceeding to the full data. Then run:

```bash
bin/fetch-epsilon
bin/stage epsilon
bin/start-cluster
bin/observe
bin/submit epsilon | tee results/epsilon-run.log
bin/stop-cluster
```

`bin/fetch-epsilon` first tries the LIBSVM BZip2 file. If that endpoint is down, it uses CatBoost's checksum-verified TSV conversion of the same Epsilon split. The full fetch and extraction need about 16 GiB free on the Razer; keep at least 10 GiB free on the ASUS for its staged data and runtime. The first download may take hours on a slow connection, so start it before the lab session if possible.

## Observation

- Spark master UI: `http://192.168.1.111:8080`
- Spark application UI while a job is active: `http://192.168.1.111:4040`
- ASUS worker UI: `http://192.168.1.100:8081`
- Wireshark display filter: `tcp.port == 7077 || tcp.port == 7078 || tcp.port == 37071 || tcp.port == 37072`

A normal switch sends unicast Spark traffic only to the intended ports. To see packets exchanged directly between the nodes from another computer, use switch port mirroring or capture on one of the cluster nodes.
