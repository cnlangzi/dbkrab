#!/bin/bash
# dbkrab deployment script
# Usage: ./deploy.sh [config_file]
# Note: Run as normal user, sudo is used only where needed

set -e

BINARY_NAME="dbkrab"
INSTALL_DIR="/opt/dbkrab"
CONFIG_SRC="${1:-config.yaml}"
CONFIG_DEST="$INSTALL_DIR/config.yaml"
PID_FILE="/var/run/dbkrab.pid"
LOG_FILE="/var/log/dbkrab/dbkrab.log"

echo "🦀 Deploying dbkrab..."

# ============================================
# Step 1: Create directories (requires sudo)
# ============================================
echo "📁 Creating directories..."
sudo mkdir -p "$INSTALL_DIR"
sudo mkdir -p /var/log/dbkrab
sudo mkdir -p /var/lib/dbkrab/data

# Change ownership to current user (requires sudo)
sudo chown -R $(whoami):$(whoami) "$INSTALL_DIR"
sudo chown -R $(whoami):$(whoami) /var/log/dbkrab
sudo chown -R $(whoami):$(whoami) /var/lib/dbkrab

# ============================================
# Step 2: Stop old process (normal user)
# ============================================
echo "🛑 Stopping old process..."
if [ -f "$PID_FILE" ]; then
    OLD_PID=$(cat "$PID_FILE")
    if kill -0 "$OLD_PID" 2>/dev/null; then
        echo "   Stopping PID $OLD_PID..."
        kill "$OLD_PID" || true
        sleep 2
        # Force kill if still running
        if kill -0 "$OLD_PID" 2>/dev/null; then
            kill -9 "$OLD_PID" 2>/dev/null || true
        fi
    fi
    # Remove PID file (might need sudo if created by root)
    sudo rm -f "$PID_FILE"
fi

# Also kill any running dbkrab processes (match by process name)
pkill -9 -x "$BINARY_NAME" 2>/dev/null || true
pkill -9 -f "$BINARY_NAME.*-config" 2>/dev/null || true

# Wait for process to fully exit and release the binary file
echo "   Waiting for process to exit..."
for i in {1..10}; do
    if ! pgrep -x "$BINARY_NAME" > /dev/null 2>&1; then
        break
    fi
    sleep 0.5
done

# Final check - force kill any remaining
if pgrep -x "$BINARY_NAME" > /dev/null 2>&1; then
    echo "   Force killing remaining processes..."
    pkill -9 -x "$BINARY_NAME" 2>/dev/null || true
    sleep 1
fi

# ============================================
# Step 3: Build binary (normal user - NO sudo)
# ============================================
echo "🔨 Building..."
make build

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
# Step 6: Start new process (normal user)
# ============================================
echo "🚀 Starting dbkrab..."
cd "$INSTALL_DIR"
nohup "$INSTALL_DIR/dbkrab" -config "$CONFIG_DEST" > "$LOG_FILE" 2>&1 &
NEW_PID=$!

# Save PID file (requires sudo for /var/run)
echo "$NEW_PID" | sudo tee "$PID_FILE" > /dev/null

sleep 3

# ============================================
# Step 7: Verify deployment
# ============================================
if kill -0 "$NEW_PID" 2>/dev/null; then
    echo "✅ Deployment successful!"
    echo "   PID: $NEW_PID"
    echo "   Log: $LOG_FILE"
    echo "   Dashboard: http://localhost:9021"
else
    echo "❌ Failed to start dbkrab"
    echo "Check log: $LOG_FILE"
    exit 1
fi
