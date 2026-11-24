# SSH access for cluster nodes

Each teammate configures SSH on their own controller. Hostnames, IP addresses, Linux usernames, and key files differ by machine, so replace the example values locally. Do not commit `~/.ssh/config`, private keys, public keys, or `authorized_keys` to this repository.

## 1. Create a key only if needed

On the controller, reuse an existing Ed25519 key. Create one only if it is missing:

```bash
mkdir -p ~/.ssh
chmod 700 ~/.ssh
if [ ! -f ~/.ssh/id_ed25519 ]; then
  ssh-keygen -t ed25519 -a 64 -C "$(id -un)@$(hostname)" -f ~/.ssh/id_ed25519
fi
chmod 600 ~/.ssh/id_ed25519
chmod 644 ~/.ssh/id_ed25519.pub
```

The private key stays on the controller. Only the `.pub` file is installed on workers. If the key has a passphrase, unlock it once per login session with:

```bash
eval "$(ssh-agent -s)"
ssh-add ~/.ssh/id_ed25519
```

## 2. Enable SSH on each Debian worker

At the worker's local terminal, install and start the SSH server. Add the Java runtime and `rsync` for Spark worker setup:

```bash
sudo apt-get update
sudo apt-get install -y openssh-server openjdk-21-jre-headless rsync
sudo systemctl enable --now ssh
sudo systemctl status ssh --no-pager
```

If UFW is active, allow port 22 from the controller's LAN address only:

```bash
sudo ufw status
sudo ufw allow from <CONTROLLER_LAN_IP> to any port 22 proto tcp
```

## 3. Add local host aliases

Create or edit `~/.ssh/config` on the controller. The text after `Host` is the short name used with `ssh`; replace each example address and username with that node's actual values.

```sshconfig
Host hp
    HostName <HP_LAN_IP>
    User <HP_LINUX_USER>
    IdentityFile ~/.ssh/id_ed25519
    IdentitiesOnly yes
    ServerAliveInterval 30

Host asus
    HostName <ASUS_LAN_IP>
    User <ASUS_LINUX_USER>
    IdentityFile ~/.ssh/id_ed25519
    IdentitiesOnly yes
    ServerAliveInterval 30
```

Use any clear aliases if the machines have other names. Keep this file private to the controller and set its permissions:

```bash
chmod 600 ~/.ssh/config
```

## 4. Install the public key and verify access

Run once for each worker. `ssh-copy-id` asks for that worker account's password and appends the public key to its `~/.ssh/authorized_keys`:

```bash
ssh-copy-id -i ~/.ssh/id_ed25519.pub hp
ssh-copy-id -i ~/.ssh/id_ed25519.pub asus
```

Afterward, these short commands should work without a password:

```bash
ssh hp 'hostname; id -un; java -version'
ssh asus 'hostname; id -un; java -version'
```

To check which address and account an alias selects:

```bash
ssh -G asus | grep -E '^(hostname|user|identityfile) '
```

The same SSH aliases are used by `WORKER_HOSTS` in `cluster.env`. For example, `WORKER_HOSTS=asus` tells the cluster scripts to connect using `ssh asus`.
