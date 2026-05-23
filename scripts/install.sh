#!/bin/bash
#
# Kawasan Installation Script
# Installs Kawasan broker to system directories
#

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Installation directories
INSTALL_DIR="/opt/kawasan"
BIN_DIR="/usr/local/bin"
CONFIG_DIR="/etc/kawasan"
DATA_DIR="/var/lib/kawasan"
LOG_DIR="/var/log/kawasan"
SYSTEMD_DIR="/etc/systemd/system"

# Source directory (where this script is located)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Check if running as root
if [ "$EUID" -ne 0 ]; then
    echo -e "${RED}Error: This script must be run as root${NC}"
    echo "Please run: sudo $0"
    exit 1
fi

echo -e "${GREEN}Kawasan Installation Script${NC}"
echo "================================"
echo

# Function to print status messages
print_status() {
    echo -e "${GREEN}[✓]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[!]${NC} $1"
}

print_error() {
    echo -e "${RED}[✗]${NC} $1"
}

# Check if kawasan-broker binary exists
if [ ! -f "$PROJECT_ROOT/build/tools/kawasan-broker" ]; then
    print_error "kawasan-broker binary not found in $PROJECT_ROOT/build/tools/"
    echo "Please build the project first:"
    echo "  cd $PROJECT_ROOT/build"
    echo "  cmake .. && make -j\$(nproc)"
    exit 1
fi

# Create kawasan user and group if they don't exist
if ! id -u kawasan > /dev/null 2>&1; then
    echo "Creating kawasan user and group..."
    groupadd -r kawasan
    useradd -r -g kawasan -d $DATA_DIR -s /sbin/nologin -c "Kawasan Broker" kawasan
    print_status "Created kawasan user and group"
else
    print_status "kawasan user already exists"
fi

# Create directories
echo "Creating directories..."
mkdir -p "$INSTALL_DIR"
mkdir -p "$BIN_DIR"
mkdir -p "$CONFIG_DIR"
mkdir -p "$DATA_DIR"
mkdir -p "$LOG_DIR"
print_status "Created installation directories"

# Copy binaries
echo "Installing binaries..."
cp "$PROJECT_ROOT/build/tools/kawasan-broker" "$BIN_DIR/"
chmod +x "$BIN_DIR/kawasan-broker"
print_status "Installed kawasan-broker to $BIN_DIR"

# Copy additional tools if they exist
for tool in kawasan-topics kawasan-metadata-check; do
    if [ -f "$PROJECT_ROOT/build/tools/$tool" ]; then
        cp "$PROJECT_ROOT/build/tools/$tool" "$BIN_DIR/"
        chmod +x "$BIN_DIR/$tool"
        print_status "Installed $tool to $BIN_DIR"
    fi
done

# Copy configuration files
echo "Installing configuration files..."
if [ ! -f "$CONFIG_DIR/broker.properties" ]; then
    if [ -f "$PROJECT_ROOT/config/server.properties.example" ]; then
        cp "$PROJECT_ROOT/config/server.properties.example" "$CONFIG_DIR/broker.properties"
        # Update paths in config
        sed -i.bak "s|log.dirs=.*|log.dirs=$DATA_DIR|" "$CONFIG_DIR/broker.properties"
        rm -f "$CONFIG_DIR/broker.properties.bak"
        print_status "Installed broker.properties to $CONFIG_DIR"
    else
        print_warning "No example config found, skipping configuration installation"
    fi
else
    print_warning "broker.properties already exists, skipping (use --force to overwrite)"
fi

# Set ownership
echo "Setting permissions..."
chown -R kawasan:kawasan "$DATA_DIR"
chown -R kawasan:kawasan "$LOG_DIR"
chown root:root "$BIN_DIR/kawasan-broker"
chown -R root:root "$CONFIG_DIR"
chmod 755 "$BIN_DIR/kawasan-broker"
chmod 644 "$CONFIG_DIR"/*.properties 2>/dev/null || true
print_status "Set correct permissions"

# Install systemd service
if [ -f "$PROJECT_ROOT/systemd/kawasan-broker.service" ]; then
    echo "Installing systemd service..."
    cp "$PROJECT_ROOT/systemd/kawasan-broker.service" "$SYSTEMD_DIR/"
    chmod 644 "$SYSTEMD_DIR/kawasan-broker.service"
    systemctl daemon-reload
    print_status "Installed systemd service"
    
    echo
    echo "To enable Kawasan to start on boot:"
    echo -e "  ${YELLOW}sudo systemctl enable kawasan-broker${NC}"
    echo
    echo "To start Kawasan now:"
    echo -e "  ${YELLOW}sudo systemctl start kawasan-broker${NC}"
    echo
    echo "To check status:"
    echo -e "  ${YELLOW}sudo systemctl status kawasan-broker${NC}"
else
    print_warning "systemd service file not found, skipping"
fi

# Print summary
echo
echo -e "${GREEN}Installation complete!${NC}"
echo "================================"
echo "Binaries installed to: $BIN_DIR"
echo "Configuration: $CONFIG_DIR"
echo "Data directory: $DATA_DIR"
echo "Log directory: $LOG_DIR"
echo
echo "Next steps:"
echo "1. Review and edit the configuration file:"
echo "   $CONFIG_DIR/broker.properties"
echo "2. Start the broker:"
echo "   sudo systemctl start kawasan-broker"
echo "3. Check logs:"
echo "   sudo journalctl -u kawasan-broker -f"
echo

exit 0
