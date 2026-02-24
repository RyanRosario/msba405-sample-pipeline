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

---

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
ExecStart=${AIRFLOW_BIN} api-server -p 8080 --apps all
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

> **Note:** `--apps all` is required on the api-server so that the execution API is available for the scheduler to dispatch tasks.

Open port 8080 in your GCP firewall rules, then access Airflow at:

```
http://<external-ip>:8080
```

Find your external IP with `curl ifconfig.me`. Do not use the domain name directly — it may force HTTPS and cause a connection error.

Log in with the admin username and password set in `pipeline.env`.

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

Copy the DAG to Airflow's DAGs folder:

```bash
cp pipeline.py $AIRFLOW_HOME/dags/
```

Wait ~30 seconds for the DAG processor to pick it up, then verify:

```bash
airflow dags list
airflow dags list-import-errors
```

Airflow 3 pauses new DAGs by default. Unpause before triggering:

```bash
airflow dags unpause fhvhv_spark_to_duckdb
```

Trigger a DAG run:

```bash
airflow dags trigger fhvhv_spark_to_duckdb
```

Check the run status:

```bash
airflow dags list-runs -d fhvhv_spark_to_duckdb
```

---

## Troubleshooting

**DAG not showing up after `cp pipeline.py $AIRFLOW_HOME/dags/`**

Check for import errors:

```bash
airflow dags list-import-errors
```

Check the dag-processor is running:

```bash
sudo systemctl status airflow-dag-processor
journalctl -u airflow-dag-processor -n 50
```

**Tasks stuck in `queued` state**

First check if the DAG is paused:

```bash
airflow dags unpause fhvhv_spark_to_duckdb
```

Check that the api-server is serving the execution API:

```bash
curl http://localhost:8080/execution/health
```

If this returns `Not Found`, the api-server is not running with `--apps all`. Re-run the systemd setup commands above, which include `--apps all` in the `ExecStart` for `airflow-api-server`.

Check all three services are running:

```bash
sudo systemctl status airflow-scheduler airflow-dag-processor airflow-api-server
```

**Browser shows SSL error when accessing the UI**

Use the raw external IP instead of the domain name:

```bash
curl ifconfig.me  # get your external IP
```

Then go to `http://<external-ip>:8080` — the domain may have HSTS enabled which forces HTTPS.

**Forgot admin password**

```bash
airflow users reset-password --username admin --password newpassword
```
