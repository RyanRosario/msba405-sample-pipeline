#!/bin/bash
# Airflow 3.x installation and startup script
#
# Configure via environment variables before running:
#
#   export AIRFLOW_ADMIN_PASSWORD="changeme"
#   export AIRFLOW_ADMIN_USER="admin"
#   export AIRFLOW_ADMIN_EMAIL="admin@example.com"
#   export AIRFLOW_VERSION="3.1.7"
#   bash install_airflow.sh

set -euo pipefail

# RUN THIS AT YOUR OWN RISK. IT IS BETTER TO FOLLOW README.md

# ── Configuration (override via environment variables) ────────────────────────
AIRFLOW_ADMIN_USER="${AIRFLOW_ADMIN_USER:-admin}"
AIRFLOW_ADMIN_PASSWORD="${AIRFLOW_ADMIN_PASSWORD:?AIRFLOW_ADMIN_PASSWORD is required}"
if [[ "$AIRFLOW_ADMIN_PASSWORD" == "changeme" ]]; then
    echo "ERROR: AIRFLOW_ADMIN_PASSWORD is still set to 'changeme'. Update pipeline.env before running." >&2
    exit 1
fi
AIRFLOW_ADMIN_EMAIL="${AIRFLOW_ADMIN_EMAIL:-admin@example.com}"
AIRFLOW_VERSION="${AIRFLOW_VERSION:-3.1.7}"
AIRFLOW_PORT="${AIRFLOW_PORT:-8080}"
VENV_DIR="${VENV_DIR:-$HOME/airflow-venv}"

# ── Derived ───────────────────────────────────────────────────────────────────
PYTHON_VERSION="$(python3 --version | cut -d ' ' -f 2 | cut -d '.' -f 1-2)"
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

# ── Install system dependencies ───────────────────────────────────────────────
echo ">>> Installing system dependencies..."
sudo apt-get update -q
sudo apt-get install -y python3-pip python3-venv

# ── Create virtual environment ────────────────────────────────────────────────
echo ">>> Creating virtual environment at ${VENV_DIR}..."
python3 -m venv "$VENV_DIR"
# shellcheck disable=SC1091
source "$VENV_DIR/bin/activate"

# ── Install Airflow ───────────────────────────────────────────────────────────
echo ">>> Installing Airflow ${AIRFLOW_VERSION} (Python ${PYTHON_VERSION})..."
pip install --quiet --upgrade pip
pip install -r requirements.txt --constraint "$CONSTRAINT_URL"

# ── Configure FAB as the auth manager ────────────────────────────────────────
# Required in Airflow 3 — FAB was removed from core and must be explicitly set
export AIRFLOW__CORE__AUTH_MANAGER="airflow.providers.fab.auth_manager.fab_auth_manager.FabAuthManager"

# ── Initialize databases ──────────────────────────────────────────────────────
echo ">>> Initializing Airflow database..."
airflow db migrate

echo ">>> Initializing FAB database..."
airflow fab-db migrate

# ── Create admin user ─────────────────────────────────────────────────────────
echo ">>> Creating admin user '${AIRFLOW_ADMIN_USER}'..."
airflow users create \
    --role Admin \
    --username "$AIRFLOW_ADMIN_USER" \
    --email "$AIRFLOW_ADMIN_EMAIL" \
    --firstname admin \
    --lastname admin \
    --password "$AIRFLOW_ADMIN_PASSWORD"

# ── Ensure DAGs directory exists ──────────────────────────────────────────────
mkdir -p "${AIRFLOW_HOME:-$HOME/airflow}/dags"

# ── Create systemd service files ──────────────────────────────────────────────
AIRFLOW_BIN="$(which airflow)"
CURRENT_USER="$(whoami)"
AIRFLOW_HOME_DIR="${AIRFLOW_HOME:-$HOME/airflow}"
PIPELINE_DIR="$(pwd)"

echo ">>> Creating systemd service files..."

sudo tee /etc/systemd/system/airflow-scheduler.service > /dev/null <<EOF
[Unit]
Description=Airflow Scheduler
After=network.target

[Service]
User=${CURRENT_USER}
Environment="AIRFLOW_HOME=${AIRFLOW_HOME_DIR}"
EnvironmentFile=${PIPELINE_DIR}/pipeline.env
Environment="AIRFLOW__CORE__AUTH_MANAGER=airflow.providers.fab.auth_manager.fab_auth_manager.FabAuthManager"
ExecStart=${AIRFLOW_BIN} scheduler
Restart=on-failure
RestartSec=5s
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
EOF

sudo tee /etc/systemd/system/airflow-dag-processor.service > /dev/null <<EOF
[Unit]
Description=Airflow DAG Processor
After=network.target

[Service]
User=${CURRENT_USER}
Environment="AIRFLOW_HOME=${AIRFLOW_HOME_DIR}"
EnvironmentFile=${PIPELINE_DIR}/pipeline.env
Environment="AIRFLOW__CORE__AUTH_MANAGER=airflow.providers.fab.auth_manager.fab_auth_manager.FabAuthManager"
ExecStart=${AIRFLOW_BIN} dag-processor
Restart=on-failure
RestartSec=5s
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
EOF

sudo tee /etc/systemd/system/airflow-api-server.service > /dev/null <<EOF
[Unit]
Description=Airflow API Server
After=network.target

[Service]
User=${CURRENT_USER}
Environment="AIRFLOW_HOME=${AIRFLOW_HOME_DIR}"
EnvironmentFile=${PIPELINE_DIR}/pipeline.env
Environment="AIRFLOW__CORE__AUTH_MANAGER=airflow.providers.fab.auth_manager.fab_auth_manager.FabAuthManager"
ExecStart=${AIRFLOW_BIN} api-server -p ${AIRFLOW_PORT}
Restart=on-failure
RestartSec=5s
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
EOF

# ── Enable and start services ─────────────────────────────────────────────────
echo ">>> Enabling and starting Airflow services..."
sudo systemctl daemon-reload
sudo systemctl enable --now airflow-scheduler airflow-dag-processor airflow-api-server

echo ""
echo "✓ Airflow is running at http://$(hostname):${AIRFLOW_PORT}"
echo ""
echo "  Manage services with:"
echo "    sudo systemctl status  airflow-scheduler airflow-dag-processor airflow-api-server"
echo "    sudo systemctl restart airflow-scheduler airflow-dag-processor airflow-api-server"
echo "    sudo systemctl stop    airflow-scheduler airflow-dag-processor airflow-api-server"
echo ""
echo "  View logs with:"
echo "    journalctl -u airflow-scheduler     -f"
echo "    journalctl -u airflow-dag-processor -f"
echo "    journalctl -u airflow-api-server    -f"
