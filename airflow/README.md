# Airflow

## Installation

To install airflow:

`sudo apt-get install -y python3-pip python3-venv`

Create a new virtual environment:

```
python3 -m venv airflow
source airflow/bin/activate 
```

Install Airflow:

```
pip install -r requirements.txt
- or-
pip install -e .
```

Initialize Airflow database:

```
airflow db init
```

Create an administrator:

```
airflow users create --role Admin --username admin --email admin --firstname admin --lastname admin --password my-password`
```
Run the Airflow job scheduler in the background. Airflow appends the output of running the scheduler to the scheduler.log file.:

`nohup airflow scheduler > scheduler.log 2>&1 &`

Open port 8080 in GCP security.

Then start Airflow's web server.

`nohup airflow webserver -p 8080 > webserver.log 2>&1 &`

Install nginx, a web and application server.

`sudo apt install nginx`

Configure Nginx as a reverse proxy to serve Airflow:

`sudo nano /etc/nginx/airflow.conf`

Add the following to the file:

```
 server {
     listen 80;
     server_name app-online.example.com;

     location / {
         proxy_pass http://localhost:8080;
         proxy_set_header Host $host;
         proxy_set_header X-Real-IP $remote_addr;
         proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
         proxy_set_header X-Forwarded-Proto $scheme;
         proxy_set_header X-Frame-Options SAMEORIGIN;
         proxy_buffers 16 4k;
         proxy_buffer_size 2k;
         proxy_busy_buffers_size 4k;
     }
 }
```

Replace `app-online.example.com` with your GCP/GCE hostname. There are others ways to do this within GCP.

Test the Nginx configuration and fix any errors:

```
sudo nginx -t
```

Restart Nginx to load changes.

```
sudo systemctl restart nginx
```

Access Airflow:

`https://hostname:8080`

where `hostname` comes from the GCE console, or from the instructor during the demo.

Source: https://docs.vultr.com/how-to-deploy-apache-airflow-on-ubuntu-20-04

## Execution

Once you have created a DAG, move it to Airflow's execution path:

```
echo $AIRFLOW_HOME  # ~/airflow/dags is the default
mv pipeline.py $AIRFLOW_HOME/
```

To start a run:

```
airflow dags trigger name_of_dag
```


