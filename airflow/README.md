# Airflow

## Installation

Install pip and create a virtual environment:

```bash
sudo apt-get install -y python3-pip python3-venv
python3 -m venv airflow
source airflow/bin/activate
```

Install Airflow using constraint files for a stable, reproducible installation:

```bash
AIRFLOW_VERSION=3.1.7
PYTHON_VERSION="$(python --version | cut -d ' ' -f 2 | cut -d '.' -f 1-2)"
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
pip install "apache-airflow==${AIRFLOW_VERSION}" \
    apache-airflow-providers-apache-spark \
    apache-airflow-providers-fab \
    --constraint "$CONSTRAINT_URL"
```

> `apache-airflow-providers-fab` is required in Airflow 3 to enable the `airflow users create` CLI command, which was removed from core.

Or, if the project includes a `requirements.txt` or `setup.py`:

```bash
pip install -r requirements.txt
# or
pip install -e .
```

Set FAB as the auth manager (required in Airflow 3 to enable `airflow users` CLI commands):

```bash
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

Airflow 3 runs as three separate processes. Install them as systemd services so they
survive reboots and restart automatically on failure.

Create `/etc/systemd/system/airflow-scheduler.service`:

```ini
[Unit]
Description=Airflow Scheduler
After=network.target

[Service]
User=ubuntu
Environment="AIRFLOW_HOME=/home/ubuntu/airflow"
Environment="AIRFLOW__CORE__AUTH_MANAGER=airflow.providers.fab.auth_manager.fab_auth_manager.FabAuthManager"
ExecStart=/home/ubuntu/airflow-venv/bin/airflow scheduler
Restart=on-failure
RestartSec=5s
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
```

Create `/etc/systemd/system/airflow-dag-processor.service`:

```ini
[Unit]
Description=Airflow DAG Processor
After=network.target

[Service]
User=ubuntu
Environment="AIRFLOW_HOME=/home/ubuntu/airflow"
Environment="AIRFLOW__CORE__AUTH_MANAGER=airflow.providers.fab.auth_manager.fab_auth_manager.FabAuthManager"
ExecStart=/home/ubuntu/airflow-venv/bin/airflow dag-processor
Restart=on-failure
RestartSec=5s
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
```

Create `/etc/systemd/system/airflow-api-server.service`:

```ini
[Unit]
Description=Airflow API Server
After=network.target

[Service]
User=ubuntu
Environment="AIRFLOW_HOME=/home/ubuntu/airflow"
Environment="AIRFLOW__CORE__AUTH_MANAGER=airflow.providers.fab.auth_manager.fab_auth_manager.FabAuthManager"
ExecStart=/home/ubuntu/airflow-venv/bin/airflow api-server -p 8080
Restart=on-failure
RestartSec=5s
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
```

Enable and start all three services:

```bash
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

Once you have created a DAG, move it to Airflow's DAGs folder:

```bash
echo $AIRFLOW_HOME  # default: ~/airflow
mv pipeline.py $AIRFLOW_HOME/dags/
```

Trigger a DAG run:

```bash
airflow dags trigger name_of_dag
```

