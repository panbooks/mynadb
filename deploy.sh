#!/usr/bin/env bash
set -euo pipefail

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
SOURCE_DIR="./"
DEPLOY_DIR="/opt/tsdb-stats"
SERVICE_NAME="tsdb-stats"
SYSTEMD_SERVICE="/etc/systemd/system/${SERVICE_NAME}.service"

# Print colored message
print_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check if running as root
if [[ $EUID -ne 0 ]]; then
   print_error "This script must be run as root (use sudo)"
   exit 1
fi

print_info "Starting deployment of TSDB Stats Service..."

# Step 1: Build release binary
#print_info "Building release binary..."
#cargo build --release
#if [[ $? -eq 0 ]]; then
#    print_success "Build completed successfully"
#else
#    print_error "Build failed"
#    exit 1
#fi

# Step 2: Check if service is running and stop it
if systemctl is-active --quiet ${SERVICE_NAME}; then
    print_info "Stopping running service..."
    systemctl stop ${SERVICE_NAME}
    print_success "Service stopped"
fi

# Step 3: Create deployment directory
print_info "Creating deployment directory: ${DEPLOY_DIR}"
mkdir -p ${DEPLOY_DIR}
#mkdir -p ${DEPLOY_DIR}/logs
#mkdir -p ${DEPLOY_DIR}/data

# Step 4: Copy binary
print_info "Copying binary..."
cp -f ${SOURCE_DIR}/tsdb-stats ${DEPLOY_DIR}/
cp -rf ${SOURCE_DIR}/tools ${DEPLOY_DIR}/
chmod +x ${DEPLOY_DIR}/tsdb-stats
print_success "Binary copied"

# Step 5: Copy configuration file
print_info "Copying configuration..."
if [[ -f "${DEPLOY_DIR}/config.toml" ]]; then
    # Backup existing config
    cp ${DEPLOY_DIR}/config.toml ${DEPLOY_DIR}/config.toml.backup.$(date +%Y%m%d_%H%M%S)
    print_warning "Existing config backed up"
fi
cp -f ${SOURCE_DIR}/config.toml ${DEPLOY_DIR}/
print_success "Configuration copied"

# Step 6: Update config paths (if needed)
print_info "Updating configuration paths..."
#sed -i 's|data_dir = ".*"|data_dir = "/opt/tsdb-stats/data"|g' ${DEPLOY_DIR}/config.toml
#sed -i 's|log_dir = ".*"|log_dir = "/opt/tsdb-stats/logs"|g' ${DEPLOY_DIR}/config.toml
print_success "Configuration updated"

# Step 7: Install systemd service
print_info "Installing systemd service..."
cp -f tsdb-stats.service ${SYSTEMD_SERVICE}
systemctl daemon-reload
print_success "Systemd service installed"

# Step 8: Enable service to start on boot
print_info "Enabling service to start on boot..."
systemctl enable ${SERVICE_NAME}
print_success "Service enabled"

# Step 9: Start the service
print_info "Starting service..."
systemctl start ${SERVICE_NAME}

# Wait a moment for service to start
sleep 2

# Step 10: Check service status
if systemctl is-active --quiet ${SERVICE_NAME}; then
    print_success "Service started successfully"
    echo ""
    print_info "Service Status:"
    systemctl status ${SERVICE_NAME} --no-pager -l
    echo ""
    print_info "Deployment completed successfully!"
    echo ""
    print_info "Useful commands:"
    echo "  - View logs:        journalctl -u ${SERVICE_NAME} -f"
    echo "  - Service status:   systemctl status ${SERVICE_NAME}"
    echo "  - Stop service:     systemctl stop ${SERVICE_NAME}"
    echo "  - Start service:    systemctl start ${SERVICE_NAME}"
    echo "  - Restart service:  systemctl restart ${SERVICE_NAME}"
    echo "  - Check version:    ${DEPLOY_DIR}/tsdb-stats version"
    echo "  - Test HTTP:        curl http://localhost:7000/health"
    echo "  - View version:     curl http://localhost:7000/version"
else
    print_error "Service failed to start"
    echo ""
    print_info "Check logs with: journalctl -u ${SERVICE_NAME} -n 50"
    exit 1
fi
