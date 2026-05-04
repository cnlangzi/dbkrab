#!/bin/bash
# dbkrab deployment script
# Usage: ./deploy.sh [config_file]
# Note: Run as normal user, sudo is used only where needed

set -e

BINARY_PATH="/opt/dbkrab/dbkrab"
INSTALL_DIR="/opt/dbkrab"
CONFIG_SRC="${1:-config.yml}"
CONFIG_DEST="$INSTALL_DIR/config.yml"

# Check if process is running
is_running() {
    pgrep -f "$BINARY_PATH" > /dev/null 2>&1
}

# Wait for process to stop, returns 0 if stopped
wait_for_stop() {
    local max_attempts=$1
    local msg=$2
    for ((i=1; i<=max_attempts; i++)); do
        if ! is_running; then
            [ -n "$msg" ] && echo "   $msg"
            return 0
        fi
        sleep 0.5
    done
    return 1
}

echo "🦀 Deploying dbkrab..."

# ============================================
# Step 1: Create directories (requires sudo)
# ============================================
echo "📁 Creating directories..."
sudo mkdir -p "$INSTALL_DIR"

# Change ownership to current user (requires sudo)
sudo chown -R $(whoami):$(whoami) "$INSTALL_DIR"

# ============================================
# Step 2: Build binary first (verify it works)
# ============================================
echo "🔨 Building..."
make build

# ============================================
# Step 3: Stop old process
# ============================================
echo "🛑 Stopping old process..."

# Stop systemd service first (this prevents restart before we can kill the process)
if command -v systemctl &> /dev/null && systemctl is-active --quiet dbkrab 2>/dev/null; then
    echo "   Stopping systemd service..."
    sudo systemctl stop dbkrab || true
    sleep 1
fi

# Send SIGTERM first for graceful shutdown, then SIGKILL if needed
if is_running; then
    echo "   Sending SIGTERM..."
    pkill -TERM -f "$BINARY_PATH" 2>/dev/null || true
    wait_for_stop 10 "Process stopped gracefully" || {
        echo "   Force killing..."
        pkill -9 -f "$BINARY_PATH" 2>/dev/null || true
    }
fi

# Verify process is stopped, fail if still running
if is_running; then
    echo "   ERROR: Process still running, cannot deploy"
    exit 1
fi
echo "   Process stopped"

# ============================================
# Step 4: Install binary (normal user)
# ============================================
echo "📦 Installing binary..."
# Use 'install' command which unlinks the old file first
# This avoids "Text file busy" error when overwriting a running binary
install -m 755 bin/dbkrab "$INSTALL_DIR/dbkrab"

# ============================================
# Step 5: Install config (normal user)
# ============================================
echo "⚙️  Installing config..."
if [ -f "$CONFIG_DEST" ]; then
    echo "   Config already exists at $CONFIG_DEST, keeping existing config"
else
    if [ -f "$CONFIG_SRC" ]; then
        cp "$CONFIG_SRC" "$CONFIG_DEST"
        echo "   Config: $CONFIG_DEST"
    else
        echo "⚠️  Config file not found: $CONFIG_SRC"
    fi
fi

# ============================================
# Step 6: Start via systemd
# ============================================
echo "🚀 Starting dbkrab..."

if command -v systemctl &> /dev/null; then
    echo "   Starting via systemd..."
    sudo systemctl start dbkrab
    sleep 3
    if systemctl is-active --quiet dbkrab 2>/dev/null; then
        echo "✅ Deployment successful!"
        echo "   Dashboard: http://localhost:9021"
    else
        echo "❌ systemd service failed to start"
        echo "Check log: tail -50 /opt/dbkrab/logs/dbkrab.log"
        exit 1
    fi
else
    echo "❌ systemd not available, cannot start"
    exit 1
fi