#!/bin/bash
#
# Kawasan Broker - macOS Service Installation Script
#
# This script installs Kawasan broker as a launchd service on macOS
#
# Usage:
#   sudo ./install-macos-service.sh [OPTIONS]
#
# Options:
#   --broker-path PATH    Path to kawasan-broker binary (default: ./build/tools/kawasan-broker)
#   --config-path PATH    Path to broker config file (default: ./config/broker.macos.properties)
#   --user USERNAME       User to run service as (default: current user)
#   --help               Show this help message
#

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Default values
BROKER_PATH="./build/tools/kawasan-broker"
CONFIG_PATH="./config/broker.macos.properties"
SERVICE_USER=""
INSTALL_DIR="/usr/local/bin"
CONFIG_DIR="/usr/local/etc/kawasan"
DATA_DIR="/usr/local/var/kawasan"
LOG_DIR="/usr/local/var/log/kawasan"
PLIST_FILE="/Library/LaunchDaemons/com.kawasan.broker.plist"

# Function to print colored output
print_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

print_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Function to show help
show_help() {
    cat << EOF
Kawasan Broker - macOS Service Installation Script

Usage:
  sudo $0 [OPTIONS]

Options:
  --broker-path PATH    Path to kawasan-broker binary (default: ./build/tools/kawasan-broker)
  --config-path PATH    Path to broker config file (default: ./config/broker.macos.properties)
  --user USERNAME       User to run service as (default: current user, not root)
  --help               Show this help message

Examples:
  # Install with defaults
  sudo ./install-macos-service.sh

  # Install with custom paths
  sudo ./install-macos-service.sh --broker-path /path/to/kawasan-broker --config-path /path/to/config

  # Install to run as specific user
  sudo ./install-macos-service.sh --user kawasan

Requirements:
  - Must be run with sudo
  - Kawasan broker binary must exist
  - Configuration file must exist

EOF
    exit 0
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --broker-path)
            BROKER_PATH="$2"
            shift 2
            ;;
        --config-path)
            CONFIG_PATH="$2"
            shift 2
            ;;
        --user)
            SERVICE_USER="$2"
            shift 2
            ;;
        --help)
            show_help
            ;;
        *)
            print_error "Unknown option: $1"
            echo "Use --help for usage information"
            exit 1
            ;;
    esac
done

# Check if running as root
if [ "$EUID" -ne 0 ]; then
    print_error "This script must be run with sudo"
    exit 1
fi

# Get the actual user (not root when using sudo)
if [ -z "$SERVICE_USER" ]; then
    SERVICE_USER="${SUDO_USER:-$USER}"
    if [ "$SERVICE_USER" = "root" ]; then
        print_warn "No user specified and running as root. Service will run as root."
        print_warn "Consider using --user option to run as non-root user"
    fi
fi

print_info "Installing Kawasan Broker as macOS launchd service"
echo ""
echo "Configuration:"
echo "  Broker Binary: $BROKER_PATH"
echo "  Config File: $CONFIG_PATH"
echo "  Service User: $SERVICE_USER"
echo "  Install Directory: $INSTALL_DIR"
echo "  Config Directory: $CONFIG_DIR"
echo "  Data Directory: $DATA_DIR"
echo "  Log Directory: $LOG_DIR"
echo ""

# Verify broker binary exists
if [ ! -f "$BROKER_PATH" ]; then
    print_error "Broker binary not found: $BROKER_PATH"
    print_error "Please build the project first: make -C build"
    exit 1
fi

# Verify config file exists
if [ ! -f "$CONFIG_PATH" ]; then
    print_error "Configuration file not found: $CONFIG_PATH"
    exit 1
fi

# Step 1: Create directories
print_info "Creating directories..."
mkdir -p "$INSTALL_DIR"
mkdir -p "$CONFIG_DIR"
mkdir -p "$DATA_DIR"
mkdir -p "$LOG_DIR"

# Step 2: Copy broker binary
print_info "Installing broker binary to $INSTALL_DIR/kawasan-broker..."
cp "$BROKER_PATH" "$INSTALL_DIR/kawasan-broker"
chmod 755 "$INSTALL_DIR/kawasan-broker"

# Step 3: Copy configuration file
print_info "Installing configuration to $CONFIG_DIR/broker.properties..."
cp "$CONFIG_PATH" "$CONFIG_DIR/broker.properties"
chmod 644 "$CONFIG_DIR/broker.properties"

# Step 4: Set ownership
if [ "$SERVICE_USER" != "root" ]; then
    print_info "Setting ownership to $SERVICE_USER..."
    chown -R "$SERVICE_USER:staff" "$DATA_DIR"
    chown -R "$SERVICE_USER:staff" "$LOG_DIR"
    chown "$SERVICE_USER:staff" "$CONFIG_DIR/broker.properties"
fi

# Step 5: Update data directory in config if using default path
print_info "Updating data directory path in configuration..."
if grep -q '"/var/lib/kawasan/data"' "$CONFIG_DIR/broker.properties"; then
    sed -i '' 's|"/var/lib/kawasan/data"|"'$DATA_DIR'/data"|g' "$CONFIG_DIR/broker.properties"
fi
if grep -q '"/usr/local/var/kawasan/data"' "$CONFIG_DIR/broker.properties"; then
    sed -i '' 's|"/usr/local/var/kawasan/data"|"'$DATA_DIR'/data"|g' "$CONFIG_DIR/broker.properties"
fi

# Step 6: Create launchd plist
print_info "Creating launchd service file at $PLIST_FILE..."

# Determine if we should include UserName/GroupName keys
USER_KEYS=""
if [ "$SERVICE_USER" != "root" ]; then
    USER_KEYS="    <key>UserName</key>
    <string>$SERVICE_USER</string>
    <key>GroupName</key>
    <string>staff</string>"
fi

cat > "$PLIST_FILE" << EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>com.kawasan.broker</string>

    <key>ProgramArguments</key>
    <array>
        <string>$INSTALL_DIR/kawasan-broker</string>
        <string>--config</string>
        <string>$CONFIG_DIR/broker.properties</string>
        <string>--log-level</string>
        <string>info</string>
    </array>

    <key>RunAtLoad</key>
    <true/>

    <key>KeepAlive</key>
    <dict>
        <key>SuccessfulExit</key>
        <false/>
        <key>Crashed</key>
        <true/>
    </dict>

    <key>WorkingDirectory</key>
    <string>$DATA_DIR</string>

    <key>StandardOutPath</key>
    <string>$LOG_DIR/broker.log</string>

    <key>StandardErrorPath</key>
    <string>$LOG_DIR/broker-error.log</string>

$USER_KEYS

    <key>EnvironmentVariables</key>
    <dict>
        <key>PATH</key>
        <string>/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin</string>
    </dict>

    <key>ProcessType</key>
    <string>Background</string>

    <key>ThrottleInterval</key>
    <integer>10</integer>

    <key>ExitTimeOut</key>
    <integer>30</integer>

    <key>SoftResourceLimits</key>
    <dict>
        <key>NumberOfFiles</key>
        <integer>65536</integer>
    </dict>

    <key>HardResourceLimits</key>
    <dict>
        <key>NumberOfFiles</key>
        <integer>65536</integer>
    </dict>

    <key>AbandonProcessGroup</key>
    <true/>
</dict>
</plist>
EOF

chmod 644 "$PLIST_FILE"

# Step 7: Stop service if already running
if launchctl list | grep -q "com.kawasan.broker"; then
    print_info "Stopping existing service..."
    launchctl unload "$PLIST_FILE" 2>/dev/null || true
fi

# Step 8: Load the service
print_info "Loading launchd service..."
launchctl load "$PLIST_FILE"

# Step 9: Wait a moment for service to start
sleep 2

# Step 10: Check service status
print_info "Checking service status..."
if launchctl list | grep -q "com.kawasan.broker"; then
    print_info "${GREEN}Service installed and started successfully!${NC}"
    echo ""
    echo "Service Information:"
    echo "  Service Name: com.kawasan.broker"
    echo "  Config File: $CONFIG_DIR/broker.properties"
    echo "  Data Directory: $DATA_DIR"
    echo "  Log Files: $LOG_DIR/broker.log"
    echo "             $LOG_DIR/broker-error.log"
    echo ""
    echo "Useful Commands:"
    echo "  View logs:        tail -f $LOG_DIR/broker.log"
    echo "  Check status:     sudo launchctl list | grep kawasan"
    echo "  Stop service:     sudo launchctl unload $PLIST_FILE"
    echo "  Start service:    sudo launchctl load $PLIST_FILE"
    echo "  Restart service:  sudo launchctl unload $PLIST_FILE && sudo launchctl load $PLIST_FILE"
    echo ""
    
    # Show recent logs
    print_info "Recent log output:"
    if [ -f "$LOG_DIR/broker.log" ]; then
        tail -20 "$LOG_DIR/broker.log"
    else
        print_warn "Log file not yet created. Wait a few seconds and check $LOG_DIR/broker.log"
    fi
else
    print_error "Service failed to start. Check logs at:"
    echo "  $LOG_DIR/broker.log"
    echo "  $LOG_DIR/broker-error.log"
    exit 1
fi

print_info "Installation complete!"
