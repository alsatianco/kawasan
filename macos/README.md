# macOS Deployment Files

This directory contains files for deploying Kawasan broker as a system service on macOS using launchd.

## Files

### `com.kawasan.broker.plist`

This is a template launchd property list file that defines the Kawasan broker service. It is automatically generated and customized by the installation script.

**Installation location**: `/Library/LaunchDaemons/com.kawasan.broker.plist`

**Key features**:
- Automatic startup on boot (`RunAtLoad`)
- Automatic restart on crash (`KeepAlive`)
- Proper resource limits (file descriptors)
- Configurable user/group execution
- Graceful shutdown with 30-second timeout
- Throttle interval to prevent rapid restart loops
- Log output to dedicated files

## Installation

Use the automated installation script:

```bash
sudo ./scripts/install-macos-service.sh
```

See `docs/PRODUCTION_DEPLOYMENT.md` for complete installation instructions.

## Manual Installation

If you need to manually install the service:

1. Copy the plist template to the system location:
   ```bash
   sudo cp macos/com.kawasan.broker.plist /Library/LaunchDaemons/
   ```

2. Edit the plist file to customize paths and user settings:
   ```bash
   sudo nano /Library/LaunchDaemons/com.kawasan.broker.plist
   ```

3. Load the service:
   ```bash
   sudo launchctl load /Library/LaunchDaemons/com.kawasan.broker.plist
   ```

## Service Management

```bash
# Start service
sudo launchctl load /Library/LaunchDaemons/com.kawasan.broker.plist

# Stop service
sudo launchctl unload /Library/LaunchDaemons/com.kawasan.broker.plist

# Check status
sudo launchctl list | grep kawasan

# View logs
tail -f /usr/local/var/log/kawasan/broker.log
```

## Uninstallation

Use the automated uninstallation script:

```bash
sudo ./scripts/uninstall-macos-service.sh
```

Or manually:

```bash
sudo launchctl unload /Library/LaunchDaemons/com.kawasan.broker.plist
sudo rm /Library/LaunchDaemons/com.kawasan.broker.plist
```

## Configuration

The service uses the configuration file at:
- `/usr/local/etc/kawasan/broker.properties`

The template configuration is available at:
- `config/broker.macos.properties`

## Directory Structure

The service uses these directories:

- **Binaries**: `/usr/local/bin/`
  - `kawasan-broker`
  - `kawasan-topics`

- **Configuration**: `/usr/local/etc/kawasan/`
  - `broker.properties`

- **Data**: `/usr/local/var/kawasan/`
  - `data/` - Broker data files

- **Logs**: `/usr/local/var/log/kawasan/`
  - `broker.log` - Standard output
  - `broker-error.log` - Standard error

## Troubleshooting

### Service won't start

Check the logs:
```bash
tail -50 /usr/local/var/log/kawasan/broker.log
tail -50 /usr/local/var/log/kawasan/broker-error.log
```

Verify the plist is valid:
```bash
plutil -lint /Library/LaunchDaemons/com.kawasan.broker.plist
```

### Port already in use

Check what's using port 9092:
```bash
lsof -i :9092
```

### Permission issues

Ensure correct ownership:
```bash
sudo chown -R _kawasan:_kawasan /usr/local/var/kawasan
sudo chown -R _kawasan:_kawasan /usr/local/var/log/kawasan
```

## Notes

- The plist file in this directory is a **template**. The actual installation script generates a customized version based on your system and preferences.
- For production deployments, consider running the service as a dedicated user (use `--user` option with the install script).
- macOS has lower default file descriptor limits than Linux. The service plist increases these to 65536.
- The service uses `KeepAlive` with crash detection, meaning it will restart if it crashes but not if it exits cleanly.

## See Also

- [Production Deployment Guide](../docs/PRODUCTION_DEPLOYMENT.md)
- [Installation Scripts](../scripts/)
- [Configuration Files](../config/)
