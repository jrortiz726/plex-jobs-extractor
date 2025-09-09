#!/bin/bash
# Install Plex Extractor as a systemd service on Linux

set -e

# Configuration
INSTALL_DIR="/opt/plex-extractor"
SERVICE_USER="extractor"
PYTHON_VERSION="3.11"

echo "Installing Plex CDF Extractor as systemd service..."

# Check if running as root
if [ "$EUID" -ne 0 ]; then 
   echo "Please run as root (use sudo)"
   exit 1
fi

# Create service user
if ! id "$SERVICE_USER" &>/dev/null; then
    echo "Creating service user: $SERVICE_USER"
    useradd -r -s /bin/false -m -d /home/$SERVICE_USER $SERVICE_USER
fi

# Install Python if needed
if ! command -v python$PYTHON_VERSION &> /dev/null; then
    echo "Installing Python $PYTHON_VERSION..."
    apt-get update
    apt-get install -y python$PYTHON_VERSION python$PYTHON_VERSION-venv python$PYTHON_VERSION-dev
fi

# Create installation directory
echo "Creating installation directory: $INSTALL_DIR"
mkdir -p $INSTALL_DIR
mkdir -p $INSTALL_DIR/logs
mkdir -p $INSTALL_DIR/state

# Copy application files
echo "Copying application files..."
cp *.py $INSTALL_DIR/
cp requirements.txt $INSTALL_DIR/
cp .env $INSTALL_DIR/

# Create virtual environment
echo "Creating Python virtual environment..."
cd $INSTALL_DIR
python$PYTHON_VERSION -m venv venv
source venv/bin/activate

# Install dependencies
echo "Installing Python dependencies..."
pip install --upgrade pip
pip install -r requirements.txt

# Set permissions
echo "Setting permissions..."
chown -R $SERVICE_USER:$SERVICE_USER $INSTALL_DIR
chmod 750 $INSTALL_DIR
chmod 640 $INSTALL_DIR/.env

# Install systemd service
echo "Installing systemd service..."
cp deploy/plex-extractor.service /etc/systemd/system/
systemctl daemon-reload

# Enable and start service
echo "Enabling service..."
systemctl enable plex-extractor.service

echo "Starting service..."
systemctl start plex-extractor.service

# Check status
sleep 2
systemctl status plex-extractor.service --no-pager

echo ""
echo "Installation complete!"
echo ""
echo "Useful commands:"
echo "  View status:  systemctl status plex-extractor"
echo "  View logs:    journalctl -u plex-extractor -f"
echo "  Restart:      systemctl restart plex-extractor"
echo "  Stop:         systemctl stop plex-extractor"
echo "  Disable:      systemctl disable plex-extractor"
echo ""
echo "Configuration: Edit $INSTALL_DIR/.env and restart service"