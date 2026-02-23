# Luigi Demo

To use the Luigi demo, you must create a virtual environment and install the prerequisites.
Run this command from this directory.

```
python3 -m venv ../../.demo
source ../../.demo/bin/activate
pip install --upgrade setuptools
pip install -r requirements.txt
```

Or instead of using the `requirements.txt` you can use the newer method of installing dependencies.

```
pip install -e .
```

The file existence task is used by each subtask:

```python pipeline.py```

We can start start the task visualizer (requires opening port 8082).

```
luigid --background --logdir tmp
```

Go to `http://hostname:8082` where hostname is your GCE instance, or for the demo, the hostname provided by the instructor.

`luigi` is **idempotent**. This means that once the pipeline succeeds successfully, running it again has no effect on the output (exactly once). To start over when testing:

```
rm -rf ../output ../duckdb/duckdb_loaded.flag
```
