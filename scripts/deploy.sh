#!/bin/bash
# dbkrab deployment script
# Usage: ./deploy.sh [config_file]

set -e

BINARY_NAME="dbkrab"
INSTALL_DIR="/opt/dbkrab"
CONFIG_SRC="${1:-config.yaml}"
CONFIG_DEST="$INSTALL_DIR/config.yaml"
PID_FILE="/var/run/dbkrab.pid"
LOG_FILE="/var/log/dbkrab/dbkrab.log"

echo "🦀 Deploying dbkrab..."

# Create directories
echo "📁 Creating directories..."
sudo mkdir -p "$INSTALL_DIR"
sudo mkdir -p /var/log/dbkrab
sudo mkdir -p /var/lib/dbkrab/data
sudo chown -R $(whoami):$(whoami) "$INSTALL_DIR"
sudo chown -R $(whoami):$(whoami) /var/log/dbkrab
sudo chown -R $(whoami):$(whoami) /var/lib/dbkrab

# Stop old process
echo "🛑 Stopping old process..."
if [ -f "$PID_FILE" ]; then
    OLD_PID=$(cat "$PID_FILE")
    if kill -0 "$OLD_PID" 2>/dev/null; then
        echo "   Stopping PID $OLD_PID..."
        kill "$OLD_PID" || true
        sleep 2
        kill -9 "$OLD_PID" 2>/dev/null || true
    fi
    rm -f "$PID_FILE"
fi

# Also kill any running dbkrab processes
pkill -f "dbkrab -config" 2>/dev/null || true
sleep 1

# Build
echo "🔨 Building..."
make build

# Install binary
echo "📦 Installing binary..."
cp bin/dbkrab "$INSTALL_DIR/dbkrab"
chmod +x "$INSTALL_DIR/dbkrab"

# Install config
echo "⚙️  Installing config..."
if [ -f "$CONFIG_SRC" ]; then
    cp "$CONFIG_SRC" "$CONFIG_DEST"
    echo "   Config: $CONFIG_DEST"
else
    echo "⚠️  Config file not found: $CONFIG_SRC"
fi

# Start new process
echo "🚀 Starting dbkrab..."
cd "$INSTALL_DIR"
nohup "$INSTALL_DIR/dbkrab" -config "$CONFIG_DEST" -api-port 9021 > "$LOG_FILE" 2>&1 &
NEW_PID=$!
echo "$NEW_PID" | sudo tee "$PID_FILE" > /dev/null

sleep 3

# Verify
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
