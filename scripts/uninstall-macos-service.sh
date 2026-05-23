#!/bin/bash
#
# Kawasan Broker - macOS Service Uninstallation Script
#
# This script removes the Kawasan broker launchd service from macOS
#
# Usage:
#   sudo ./uninstall-macos-service.sh [OPTIONS]
#
# Options:
#   --keep-data       Keep data directory (don't delete broker data)
#   --keep-config     Keep configuration files
#   --keep-logs       Keep log files
#   --help            Show this help message
#

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Default values
KEEP_DATA=false
KEEP_CONFIG=false
KEEP_LOGS=false
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
Kawasan Broker - macOS Service Uninstallation Script

Usage:
  sudo $0 [OPTIONS]

Options:
  --keep-data       Keep data directory (don't delete broker data)
  --keep-config     Keep configuration files
  --keep-logs       Keep log files
  --help            Show this help message

Examples:
  # Complete uninstallation (removes everything)
  sudo ./uninstall-macos-service.sh

  # Remove service but keep data and config
  sudo ./uninstall-macos-service.sh --keep-data --keep-config

  # Remove service but keep data only
  sudo ./uninstall-macos-service.sh --keep-data

What gets removed:
  - Launchd service: $PLIST_FILE
  - Binary: $INSTALL_DIR/kawasan-broker
  - Config: $CONFIG_DIR (unless --keep-config)
  - Data: $DATA_DIR (unless --keep-data)
  - Logs: $LOG_DIR (unless --keep-logs)

EOF
    exit 0
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --keep-data)
            KEEP_DATA=true
            shift
            ;;
        --keep-config)
            KEEP_CONFIG=true
            shift
            ;;
        --keep-logs)
            KEEP_LOGS=true
            shift
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

print_info "Uninstalling Kawasan Broker launchd service"
echo ""

# Confirm with user
read -p "Are you sure you want to uninstall Kawasan broker service? (y/N) " -n 1 -r
echo ""
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    print_info "Uninstallation cancelled"
    exit 0
fi

# Step 1: Stop and unload the service
if [ -f "$PLIST_FILE" ]; then
    if launchctl list | grep -q "com.kawasan.broker"; then
        print_info "Stopping service..."
        launchctl unload "$PLIST_FILE" 2>/dev/null || true
        sleep 2
    fi
    
    print_info "Removing launchd service file..."
    rm -f "$PLIST_FILE"
else
    print_warn "Launchd service file not found: $PLIST_FILE"
fi

# Step 2: Remove binary
if [ -f "$INSTALL_DIR/kawasan-broker" ]; then
    print_info "Removing broker binary..."
    rm -f "$INSTALL_DIR/kawasan-broker"
else
    print_warn "Broker binary not found: $INSTALL_DIR/kawasan-broker"
fi

# Step 3: Remove configuration
if [ "$KEEP_CONFIG" = false ]; then
    if [ -d "$CONFIG_DIR" ]; then
        print_info "Removing configuration directory..."
        rm -rf "$CONFIG_DIR"
    else
        print_warn "Configuration directory not found: $CONFIG_DIR"
    fi
else
    print_info "Keeping configuration directory: $CONFIG_DIR"
fi

# Step 4: Remove data
if [ "$KEEP_DATA" = false ]; then
    if [ -d "$DATA_DIR" ]; then
        print_warn "Removing data directory: $DATA_DIR"
        print_warn "This will delete all broker data!"
        read -p "Are you sure? (y/N) " -n 1 -r
        echo ""
        if [[ $REPLY =~ ^[Yy]$ ]]; then
            rm -rf "$DATA_DIR"
            print_info "Data directory removed"
        else
            print_info "Keeping data directory: $DATA_DIR"
        fi
    else
        print_warn "Data directory not found: $DATA_DIR"
    fi
else
    print_info "Keeping data directory: $DATA_DIR"
fi

# Step 5: Remove logs
if [ "$KEEP_LOGS" = false ]; then
    if [ -d "$LOG_DIR" ]; then
        print_info "Removing log directory..."
        rm -rf "$LOG_DIR"
    else
        print_warn "Log directory not found: $LOG_DIR"
    fi
else
    print_info "Keeping log directory: $LOG_DIR"
fi

# Step 6: Verify service is stopped
if launchctl list | grep -q "com.kawasan.broker"; then
    print_error "Service is still running. Please stop it manually:"
    echo "  sudo launchctl unload $PLIST_FILE"
    exit 1
fi

echo ""
print_info "${GREEN}Kawasan broker service uninstalled successfully!${NC}"
echo ""

# Show what was kept
if [ "$KEEP_CONFIG" = true ] || [ "$KEEP_DATA" = true ] || [ "$KEEP_LOGS" = true ]; then
    echo "Kept directories:"
    [ "$KEEP_CONFIG" = true ] && echo "  Config: $CONFIG_DIR"
    [ "$KEEP_DATA" = true ] && echo "  Data: $DATA_DIR"
    [ "$KEEP_LOGS" = true ] && echo "  Logs: $LOG_DIR"
    echo ""
    echo "To completely remove Kawasan, delete these directories manually:"
    [ "$KEEP_CONFIG" = true ] && echo "  sudo rm -rf $CONFIG_DIR"
    [ "$KEEP_DATA" = true ] && echo "  sudo rm -rf $DATA_DIR"
    [ "$KEEP_LOGS" = true ] && echo "  sudo rm -rf $LOG_DIR"
    echo ""
fi

print_info "Uninstallation complete!"
