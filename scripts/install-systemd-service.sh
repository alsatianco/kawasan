#!/bin/bash
# Installation script for Kawasan systemd service
# This script must be run as root or with sudo

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
KAWASAN_USER="kawasan"
KAWASAN_GROUP="kawasan"
INSTALL_DIR="/opt/kawasan"
CONFIG_DIR="/etc/kawasan"
DATA_DIR="/var/lib/kawasan"
LOG_DIR="/var/log/kawasan"
BIN_DIR="/usr/local/bin"

echo -e "${BLUE}======================================"
echo "  Kawasan Systemd Service Installer"
echo "======================================${NC}"
echo ""

# Check if running as root
if [ "$EUID" -ne 0 ]; then
    echo -e "${RED}Error: This script must be run as root or with sudo${NC}"
    echo "Usage: sudo $0 [build-dir]"
    exit 1
fi

# Get build directory from argument or use default
BUILD_DIR="${1:-./build}"

if [ ! -d "$BUILD_DIR" ]; then
    echo -e "${RED}Error: Build directory not found: $BUILD_DIR${NC}"
    echo "Please build the project first or specify the build directory:"
    echo "  sudo $0 /path/to/build"
    exit 1
fi

# Check if kawasan-broker binary exists
if [ ! -f "$BUILD_DIR/tools/kawasan-broker" ]; then
    echo -e "${RED}Error: kawasan-broker binary not found in $BUILD_DIR/tools/${NC}"
    echo "Please build the project first using: ./scripts/quick-start.sh"
    exit 1
fi

# Step 1: Create kawasan user and group
echo -e "${YELLOW}[1/8] Creating kawasan user and group...${NC}"

if id "$KAWASAN_USER" &>/dev/null; then
    echo -e "${GREEN}✓${NC} User $KAWASAN_USER already exists"
else
    useradd --system --home-dir "$INSTALL_DIR" --shell /bin/false --comment "Kawasan Broker Service" "$KAWASAN_USER"
    echo -e "${GREEN}✓${NC} Created user $KAWASAN_USER"
fi

# Step 2: Create directory structure
echo -e "${YELLOW}[2/8] Creating directory structure...${NC}"

mkdir -p "$INSTALL_DIR"
mkdir -p "$CONFIG_DIR"
mkdir -p "$DATA_DIR"
mkdir -p "$LOG_DIR"
mkdir -p "$BIN_DIR"

echo -e "${GREEN}✓${NC} Directories created"

# Step 3: Copy binaries
echo -e "${YELLOW}[3/8] Installing binaries...${NC}"

cp "$BUILD_DIR/tools/kawasan-broker" "$BIN_DIR/"
chmod +x "$BIN_DIR/kawasan-broker"

# Copy other tools if they exist
if [ -f "$BUILD_DIR/tools/kawasan-topics" ]; then
    cp "$BUILD_DIR/tools/kawasan-topics" "$BIN_DIR/"
    chmod +x "$BIN_DIR/kawasan-topics"
    echo -e "${GREEN}✓${NC} Installed kawasan-topics"
fi

echo -e "${GREEN}✓${NC} Installed kawasan-broker to $BIN_DIR/"

# Step 4: Copy configuration
echo -e "${YELLOW}[4/8] Installing configuration...${NC}"

if [ -f "config/broker.production.properties" ]; then
    cp "config/broker.production.properties" "$CONFIG_DIR/broker.properties"
    echo -e "${GREEN}✓${NC} Installed production configuration"
elif [ -f "config/server.properties.example" ]; then
    cp "config/server.properties.example" "$CONFIG_DIR/broker.properties"
    echo -e "${YELLOW}!${NC} Installed example configuration (please customize)"
else
    # Create minimal configuration
    cat > "$CONFIG_DIR/broker.properties" << 'EOF'
{
  "broker.id": 0,
  "host": "0.0.0.0",
  "port": 9092,
  "log.dirs": "/var/lib/kawasan/data"
}
EOF
    echo -e "${YELLOW}!${NC} Created minimal configuration (please customize)"
fi

# Step 5: Set permissions
echo -e "${YELLOW}[5/8] Setting permissions...${NC}"

chown -R "$KAWASAN_USER:$KAWASAN_GROUP" "$INSTALL_DIR"
chown -R "$KAWASAN_USER:$KAWASAN_GROUP" "$DATA_DIR"
chown -R "$KAWASAN_USER:$KAWASAN_GROUP" "$LOG_DIR"
chown root:root "$CONFIG_DIR/broker.properties"
chmod 644 "$CONFIG_DIR/broker.properties"

echo -e "${GREEN}✓${NC} Permissions set"

# Step 6: Install systemd service
echo -e "${YELLOW}[6/8] Installing systemd service...${NC}"

if [ -f "systemd/kawasan-broker.service" ]; then
    cp "systemd/kawasan-broker.service" /etc/systemd/system/
    echo -e "${GREEN}✓${NC} Installed systemd service file"
else
    echo -e "${RED}Error: systemd/kawasan-broker.service not found${NC}"
    exit 1
fi

# Step 7: Reload systemd and enable service
echo -e "${YELLOW}[7/8] Configuring systemd...${NC}"

systemctl daemon-reload
systemctl enable kawasan-broker.service

echo -e "${GREEN}✓${NC} Service enabled"

# Step 8: Configure firewall (optional)
echo -e "${YELLOW}[8/8] Firewall configuration...${NC}"

if command -v firewall-cmd &> /dev/null; then
    read -p "Configure firewall to allow port 9092? (y/n) " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        firewall-cmd --permanent --add-port=9092/tcp
        firewall-cmd --reload
        echo -e "${GREEN}✓${NC} Firewall configured"
    else
        echo -e "${YELLOW}!${NC} Skipped firewall configuration"
    fi
elif command -v ufw &> /dev/null; then
    read -p "Configure firewall to allow port 9092? (y/n) " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        ufw allow 9092/tcp
        echo -e "${GREEN}✓${NC} Firewall configured"
    else
        echo -e "${YELLOW}!${NC} Skipped firewall configuration"
    fi
else
    echo -e "${YELLOW}!${NC} No firewall detected, skipping"
fi

# Installation complete
echo ""
echo -e "${GREEN}======================================"
echo "  Installation Complete!"
echo "======================================${NC}"
echo ""
echo "Service installed and enabled."
echo ""
echo "Next steps:"
echo ""
echo "  1. Review and customize configuration:"
echo "     ${BLUE}sudo nano $CONFIG_DIR/broker.properties${NC}"
echo ""
echo "  2. Start the service:"
echo "     ${BLUE}sudo systemctl start kawasan-broker${NC}"
echo ""
echo "  3. Check service status:"
echo "     ${BLUE}sudo systemctl status kawasan-broker${NC}"
echo ""
echo "  4. View logs:"
echo "     ${BLUE}sudo journalctl -u kawasan-broker -f${NC}"
echo ""
echo "  5. Enable automatic start on boot (already done):"
echo "     ${BLUE}sudo systemctl enable kawasan-broker${NC}"
echo ""
echo "Useful commands:"
echo ""
echo "  Start:   ${BLUE}sudo systemctl start kawasan-broker${NC}"
echo "  Stop:    ${BLUE}sudo systemctl stop kawasan-broker${NC}"
echo "  Restart: ${BLUE}sudo systemctl restart kawasan-broker${NC}"
echo "  Status:  ${BLUE}sudo systemctl status kawasan-broker${NC}"
echo "  Logs:    ${BLUE}sudo journalctl -u kawasan-broker -f${NC}"
echo ""
echo "For more information, see docs/PRODUCTION_DEPLOYMENT.md"
echo ""
