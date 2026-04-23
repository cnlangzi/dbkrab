#!/bin/bash
# dbkrab deployment script
# Usage: ./deploy.sh [config_file]
# Note: Run as normal user, sudo is used only where needed

set -e

BINARY_PATH="/opt/dbkrab/dbkrab"
INSTALL_DIR="/opt/dbkrab"
CONFIG_SRC="${1:-config.yaml}"
CONFIG_DEST="$INSTALL_DIR/config.yaml"

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

# Stop systemd service first (if installed and running)
# This prevents systemd from auto-restarting the process during deployment
if command -v systemctl &> /dev/null && systemctl is-active --quiet dbkrab 2>/dev/null; then
    echo "   Stopping systemd service..."
    sudo systemctl stop dbkrab || true
    sleep 2
fi

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
fi

# Kill any running dbkrab processes using full path
# Use -f to match the full command line with specific path
pkill -9 -f "$BINARY_PATH" 2>/dev/null || true

# Wait for process to fully exit and release the binary file
echo "   Waiting for process to exit..."
for i in {1..10}; do
    if ! pgrep -f "$BINARY_PATH" > /dev/null 2>&1; then
        break
    fi
    sleep 0.5
done

# Final check - force kill any remaining
if pgrep -f "$BINARY_PATH" > /dev/null 2>&1; then
    echo "   Force killing remaining processes..."
    pkill -9 -f "$BINARY_PATH" 2>/dev/null || true
    sleep 1
fi

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
        echo "Check log in config.yaml"
        exit 1
    fi
else
    echo "❌ systemd not available, cannot start"
    exit 1
fi