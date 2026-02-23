# Airflow

## Installation

Install pip and create a virtual environment:

```bash
sudo apt-get install -y python3-pip python3-venv
python3 -m venv airflow
source airflow/bin/activate
```

Install Airflow and all dependencies using the project's requirements file with constraint files for a stable, reproducible installation:

```bash
AIRFLOW_VERSION=3.1.7
PYTHON_VERSION="$(python --version | cut -d ' ' -f 2 | cut -d '.' -f 1-2)"
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

pip install -r requirements.txt --constraint "$CONSTRAINT_URL"
# or, to install the project as a package:
pip install -e . --constraint "$CONSTRAINT_URL"
```

> `apache-airflow-providers-fab` is included in `requirements.txt` and `pyproject.toml`. It is required in Airflow 3 to enable `airflow users create`, which was removed from core.

Set required environment variables (or load via `set -a && source pipeline.env && set +a`):

```bash
export AIRFLOW_HOME=~/airflow
export AIRFLOW__CORE__AUTH_MANAGER="airflow.providers.fab.auth_manager.fab_auth_manager.FabAuthManager"
```

Initialize the Airflow and FAB databases:

```bash
airflow db migrate
airflow fab-db migrate
```

Create an administrator:

```bash
airflow users create --role Admin --username admin --email admin --firstname admin --lastname admin --password my-password
```

Ensure the DAGs directory exists:

```bash
mkdir -p $AIRFLOW_HOME/dags
```

## Starting Airflow (systemd)

Airflow 3 runs as three separate processes. Install them as systemd services so they survive reboots and restart automatically on failure.

Run the following from your project root (e.g. `~/msba405-sample-pipeline`). The script substitutes your actual username, home directory, and airflow binary path at write time — systemd does not expand `~` or `$HOME`:

```bash
AIRFLOW_BIN="$(which airflow)"
PIPELINE_DIR="$(pwd)"

sudo tee /etc/systemd/system/airflow-scheduler.service > /dev/null << EOF
[Unit]
Description=Airflow Scheduler
After=network.target

[Service]
User=${USER}
Environment="AIRFLOW_HOME=${HOME}/airflow"
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

sudo tee /etc/systemd/system/airflow-dag-processor.service > /dev/null << EOF
[Unit]
Description=Airflow DAG Processor
After=network.target

[Service]
User=${USER}
Environment="AIRFLOW_HOME=${HOME}/airflow"
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

sudo tee /etc/systemd/system/airflow-api-server.service > /dev/null << EOF
[Unit]
Description=Airflow API Server
After=network.target

[Service]
User=${USER}
Environment="AIRFLOW_HOME=${HOME}/airflow"
EnvironmentFile=${PIPELINE_DIR}/pipeline.env
Environment="AIRFLOW__CORE__AUTH_MANAGER=airflow.providers.fab.auth_manager.fab_auth_manager.FabAuthManager"
ExecStart=${AIRFLOW_BIN} api-server -p 8080
Restart=on-failure
RestartSec=5s
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
EOF

sudo systemctl daemon-reload
sudo systemctl enable --now airflow-scheduler airflow-dag-processor airflow-api-server
```

Open port 8080 in your GCP firewall rules, then access Airflow at:

```
http://hostname:8080
```

where `hostname` comes from the GCE console, or from the instructor during the demo.

Manage services:

```bash
sudo systemctl status  airflow-scheduler airflow-dag-processor airflow-api-server
sudo systemctl restart airflow-scheduler airflow-dag-processor airflow-api-server
sudo systemctl stop    airflow-scheduler airflow-dag-processor airflow-api-server
```

View logs:

```bash
journalctl -u airflow-scheduler     -f
journalctl -u airflow-dag-processor -f
journalctl -u airflow-api-server    -f
```

Source: https://docs.vultr.com/how-to-deploy-apache-airflow-on-ubuntu-20-04

---

## Execution

Once you have created a DAG, copy it to Airflow's DAGs folder:

```bash
echo $AIRFLOW_HOME  # default: ~/airflow
cp pipeline.py $AIRFLOW_HOME/dags/
```

Wait ~30 seconds for the DAG processor to pick it up, then verify:

```bash
airflow dags list
airflow dags list-import-errors
```

Trigger a DAG run:

```bash
airflow dags trigger fhvhv_spark_to_duckdb
```
