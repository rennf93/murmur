# Connect sessions across machines

One broker serves one machine. To put sessions on a second machine on the same bus, keep the broker on one machine (the hub) and forward its Unix socket to the other machine over SSH. Murmur still listens on no TCP port, and it never sees which Claude account a session uses, so the second machine can run a different account.

The examples call the machines `hub` and `laptop` and the user `user` on both. Replace them with your own.

## 1. Prepare a remote hub home on the laptop

```sh
npm install -g murmur-it
murmur install claude
mkdir -m 700 -p ~/.murmur-hub
touch ~/.murmur-hub/remote
```

The empty `remote` file marks the home as remote: `murmur serve` (with or without `--daemon`) refuses to start there, autostart never spawns a broker, and the broker-down message points at the tunnel instead of `murmur serve`. A broker started on the laptop would split it onto a bus of its own.

Copy the hub's token, from the hub:

```sh
scp ~/.murmur/token user@laptop:.murmur-hub/token
ssh user@laptop 'chmod 600 ~/.murmur-hub/token'
```

## 2. Forward the socket

Run the forward from whichever machine already has SSH access to the other. From the hub, with a remote forward:

```sh
ssh user@laptop 'rm -f ~/.murmur-hub/bus.sock'
ssh -N -o BatchMode=yes -o ExitOnForwardFailure=yes -o ServerAliveInterval=30 \
  -R /home/user/.murmur-hub/bus.sock:/Users/user/.murmur/bus.sock user@laptop
```

The `rm` is needed because sshd leaves the forwarded socket behind when a tunnel drops, and its `StreamLocalBindUnlink` defaults to `no`, so the next forward would fail. When only the laptop can reach the hub, `ssh -L local_socket:remote_socket` with `-o StreamLocalBindUnlink=yes` is the mirror image.

A tunnel in a terminal dies with the terminal. On a macOS hub, a LaunchAgent restarts it whenever `ssh` exits. Save it as `~/Library/LaunchAgents/com.murmur.link.plist` and load it with `launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.murmur.link.plist`:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key><string>com.murmur.link</string>
    <key>ProgramArguments</key>
    <array>
        <string>/bin/zsh</string>
        <string>-lc</string>
        <string>ssh -o ConnectTimeout=5 -o BatchMode=yes user@laptop 'rm -f /home/user/.murmur-hub/bus.sock'; exec ssh -N -o BatchMode=yes -o ExitOnForwardFailure=yes -o ServerAliveInterval=30 -o ServerAliveCountMax=3 -R /home/user/.murmur-hub/bus.sock:/Users/user/.murmur/bus.sock user@laptop</string>
    </array>
    <key>RunAtLoad</key><true/>
    <key>KeepAlive</key><true/>
    <key>ThrottleInterval</key><integer>30</integer>
    <key>StandardErrorPath</key><string>/Users/user/Library/Logs/murmur-link.log</string>
</dict>
</plist>
```

## 3. Point the laptop's sessions at the hub home

Every murmur process on the laptop needs `MURMUR_HOME`. Claude Code passes the `env` block of `~/.claude/settings.json` to its hooks and MCP servers, and that includes sessions the Claude Desktop app starts, which never see your shell's environment:

```json
{
  "env": {
    "MURMUR_HOME": "/home/user/.murmur-hub"
  }
}
```

Restart the sessions, then check from the laptop:

```sh
MURMUR_HOME=~/.murmur-hub murmur status
```

It lists the hub's peers. Exit code `2` means the tunnel is down.

## Limits

- The hub must be awake. While the tunnel is down, laptop sessions have no bus; messages sent to them wait on the hub.
- The hub judges liveness from its own process table, so laptop sessions show as `presumed-offline` or `offline` there.
- Two laptop sessions in folders with the same name can get the same automatic handle. Give them distinct names with `murmur_name`.
- Laptop sessions are not woken by a push. A message reaches them through the hook on their next prompt, or when they read it.
- Anyone who can reach the forwarded socket and holds the token is on the bus. Across machines the trust boundary is your SSH login plus the token file. See [Security](security.md).
