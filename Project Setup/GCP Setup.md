# DSA5208 Project 2: Spark Notebooks on Google Cloud (Mac Setup Guide)

Last updated: Oct 2, 2026

Edit notebooks in VS Code on your Mac, inside your local GitHub repo, while every cell runs on a Google Cloud Spark cluster. Each session: create cluster, open tunnel, run notebook, push, delete cluster.

## How it works

- **Your notebook stays in your local git folder.** You open it in normal VS Code (no Remote-SSH), and save, commit and push from your Mac as usual.
- **An SSH tunnel** links `localhost:8123` on your Mac to the Jupyter server on the cluster.
- **VS Code uses the cluster's PySpark kernel** through that tunnel, so each cell's code runs on the cluster and only the results come back.
- **Nothing is cloned onto the cluster**, so deleting it loses no code. Data lives in a Cloud Storage bucket.

| | Where | Needs |
| --- | --- | --- |
| Edit, save, git | Your Mac (local repo) | VS Code + Jupyter extension, git |
| Run cells | The cluster | Tunnel open, kernel set to **GCP cluster → PySpark** |
| Data | Cloud Storage bucket | `gs://` paths in your code |

The example values below are one person's setup. Replace them with your own project, bucket and folder names.

| Thing | Example value used in this guide |
| --- | --- |
| Project | `dsa5208-rainfall-ashley` |
| Region / zone | `asia-southeast1` / `asia-southeast1-b` |
| Data bucket | `gs://dsa5208-rainfall-ashley-data` |
| Data on Mac | `rainfall-data/rainfall_2017.csv` ... `rainfall_2024.csv` |
| Data in bucket | `gs://dsa5208-rainfall-ashley-data/raw/` |
| Repo folder on Mac | `DSA5208-Rainfall-Analysis` |
| Cluster | `rainfall-test` (master VM: `rainfall-test-m`) |

## Contents

- **[Part 1: One-time setup](#part-1-one-time-setup)**
  - [1.1 Prerequisites](#11-prerequisites-install-once)
  - [1.2 Basic gcloud commands from the lecture](#12-basic-gcloud-commands-from-the-lecture-once-per-project)
  - [1.3 Put the data in the bucket](#13-put-the-data-in-the-bucket-once)
  - [1.4 GitHub repo on your Mac](#14-github-repo-on-your-mac-once)
- **[Part 2: Every session](#part-2-every-session)**
  - [2.1 Create the cluster](#21-create-the-cluster)
  - [2.2 Open the SSH tunnel](#22-open-the-ssh-tunnel)
  - [2.3 Open a second terminal](#23-open-a-second-terminal-for-everything-else)
  - [2.4 Connect your notebook to the cluster](#24-connect-your-notebook-to-the-cluster)
  - [2.5 Run sample code](#25-run-sample-code)
  - [2.6 Save, push and finish](#26-save-push-and-finish)
- **[Part 3: Reference](#part-3-reference)**
  - [3.1 Troubleshooting](#31-troubleshooting)
  - [3.2 Quick reference](#32-quick-reference-every-session)
  - [3.3 What lives where](#33-what-lives-where)
  - [3.4 Windows differences](#34-windows-differences)
  - [3.5 Sources](#35-sources)

## Part 1: One-time setup

### 1.1 Prerequisites (install once)

| Install | How | Check |
| --- | --- | --- |
| VS Code | [code.visualstudio.com](https://code.visualstudio.com), then Cmd+Shift+X → install **Python** and **Jupyter** | Jupyter shows Installed |
| gcloud CLI | Commands below, then quit and reopen VS Code | `gcloud --version` |
| git | Run the check; accept Command Line Tools if offered | `git --version` |
| PySpark (optional) | `brew install openjdk@17`, then `pip install pyspark`; only for free local runs | `pyspark` |

```
# gcloud CLI, Apple silicon (Intel: replace darwin-arm with darwin-x86_64)
cd ~
curl -O https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/google-cloud-cli-darwin-arm.tar.gz
tar -xf google-cloud-cli-darwin-arm.tar.gz
./google-cloud-sdk/install.sh
```

Turn off VS Code auto-activation (it interrupts commands): Settings → search *python terminal activate* → untick.

### 1.2 Basic gcloud commands from the lecture (once per project)

Once per project. Each block shows the lecture's format with `[PLACEHOLDERS]`, then an **Example** with real values. Run the example with your own values swapped in.

1. **Log in.** Opens a browser; sign in and click Allow.

   ```
   # Lecture format = example (no placeholder)
   gcloud auth login
   ```
2. **Create and choose the project.** Your project exists already, so skip the create line.

   ```
   # Lecture
   gcloud projects create [PROJECT NAME]
   gcloud config set project [PROJECT NAME]
   # Example
   gcloud config set project dsa5208-rainfall-ashley
   ```
3. **Attach billing.** The list shows your billing account ID; copy it into the link line.

   ```
   # Lecture
   gcloud billing accounts list
   gcloud billing projects link [PROJECT NAME] --billing-account=[ACCOUNT ID]
   # Example
   gcloud billing accounts list
   gcloud billing projects link dsa5208-rainfall-ashley --billing-account=<your ACCOUNT_ID>
   ```
4. **Switch on the services.** Dataproc is the Spark-cluster service; it turns on Compute Engine (VMs) too.

   ```
   # Lecture format = example (no placeholder)
   gcloud services enable dataproc.googleapis.com
   gcloud services enable cloudresourcemanager.googleapis.com
   ```
5. **Give the cluster machines permission.** Copy the email labelled Compute Engine default service account from the list.

   ```
   # Lecture
   gcloud iam service-accounts list
   gcloud projects add-iam-policy-binding [PROJECT NAME] \
     --member="serviceAccount:[EMAIL]" --role="roles/dataproc.worker"
   # Example
   gcloud iam service-accounts list
   gcloud projects add-iam-policy-binding dsa5208-rainfall-ashley \
     --member="serviceAccount:<NUMBER>-compute@developer.gserviceaccount.com" \
     --role="roles/dataproc.worker"
   ```
6. **Network fix** so cluster machines can reach Cloud Storage.

   ```
   # Lecture
   gcloud compute networks subnets update default \
     --region=[REGION] --enable-private-ip-google-access
   # Example
   gcloud compute networks subnets update default \
     --region=asia-southeast1 --enable-private-ip-google-access
   ```

Check with `gcloud config list`. Budget alert: [Console](https://console.cloud.google.com) → Billing → Budgets & alerts, at $20 and $40 (email only; doesn't stop spending).

### 1.3 Put the data in the bucket (once)

The bucket outlives every cluster. From your repo folder:

```
cd ~/DSA5208-Rainfall-Analysis
# once; skip if the bucket exists
gcloud storage buckets create gs://dsa5208-rainfall-ashley-data --location=asia-southeast1
# upload (~7.5 GiB, keep the Mac awake until 8/8)
gcloud storage cp rainfall-data/rainfall_*.csv gs://dsa5208-rainfall-ashley-data/raw/
# check: expect 8 files
gcloud storage ls -l gs://dsa5208-rainfall-ashley-data/raw/
```

**Wipe and reload:** `gcloud storage rm -r gs://dsa5208-rainfall-ashley-data/raw/`, then rerun the upload line. "Matched no objects" means it was already empty.

### 1.4 GitHub repo on your Mac (once)

Clone once, onto your Mac. This local folder is where you edit and run notebooks from now on; the cluster never needs a copy, and you don't need the Remote-SSH extension.

1. Cmd+Shift+P → **Git: Clone** → **Clone from GitHub** → pick the repo → **Open** (or `git clone <repo URL>` in a terminal).
2. Create `notebooks/eda.ipynb` (File → New File → Jupyter Notebook).
3. `.gitignore`: `rainfall-data/`, `venv/`, `.venv/`, `.DS_Store`, `.ipynb_checkpoints/`.
4. Source Control → **Commit** → **Sync Changes**.

## Part 2: Every session

### 2.1 Create the cluster

One VM with Spark and Jupyter: about 3 minutes, about $0.30/hour.

Lecture format:

```
gcloud dataproc clusters create [CLUSTER NAME] \
  --region [REGION] --image-version [IMAGE VERSION] \
  --master-machine-type [MASTER MACHINE TYPE] \
  --master-boot-disk-type [MASTER DISK TYPE] --master-boot-disk-size [DISK SIZE] \
  --num-workers [NUMBER OF WORKERS] \
  --worker-machine-type [WORKER MACHINE TYPE] \
  --worker-boot-disk-type [WORKER DISK TYPE] --worker-boot-disk-size [DISK SIZE] \
  --enable-component-gateway --optional-components JUPYTER
```

Example:

```
gcloud dataproc clusters create rainfall-test \
  --region=asia-southeast1 --zone=asia-southeast1-b \
  --single-node \
  --master-machine-type=n1-standard-4 \
  --master-boot-disk-type=pd-standard --master-boot-disk-size=50 \
  --image-version=3.0-debian13 \
  --public-ip-address --enable-component-gateway \
  --optional-components=JUPYTER \
  --max-idle=30m
```

| Flag | What it does |
| --- | --- |
| `clusters create rainfall-test` | Names the cluster. Its VM is called `rainfall-test-m`. |
| `--region` / `--zone` | Singapore, in a fixed zone so the tunnel command never changes. |
| `--single-node` | One VM acting as master and worker. Cheapest option for EDA. |
| `--master-machine-type=n1-standard-4` | 4 CPUs, about 15 GB RAM. |
| `--master-boot-disk-type` / `-size` | Cheapest disk, 50 GB. |
| `--image-version=3.0-debian13` | Linux and Spark versions, pinned. |
| `--public-ip-address` | Lets your Mac SSH in (needed for the tunnel). |
| `--enable-component-gateway` | Turns on web dashboards; Jupyter requires it. |
| `--optional-components=JUPYTER` | Starts the Jupyter server your notebook connects to. |
| `--max-idle=30m` | Deletes the cluster after 30 idle minutes, in case you forget. |

Done when it prints `Created [...]`. Warnings about Auto Zone, 1TB disks, firewall and the staging bucket are fine.

Full-data runs: replace `--single-node` with `--num-workers=2 --worker-machine-type=n1-standard-4 --worker-boot-disk-type=pd-standard --worker-boot-disk-size=50`, master `n1-standard-2` (about $0.70/hour).

### 2.2 Open the SSH tunnel

Links your Mac's port 8123 to Jupyter on the cluster. You stay on your Mac; VS Code uses the link.

```
Your Mac                                         Cluster master (rainfall-test-m)
 VS Code notebook                                 PySpark kernel (runs each cell)
        |                                                ^
        v                                                |
 localhost:8123  <====== SSH tunnel (encrypted) ======>  port 8123 (Jupyter)

      cell code goes over  ->              <-  results come back
```

Lecture format (logs you into a VM):

```
gcloud compute ssh [VM NAME]
```

Example (Terminal 1, any folder):

```
gcloud compute ssh rainfall-test-m --zone=asia-southeast1-b -- -L 8123:localhost:8123 -N
```

| Piece | Meaning |
| --- | --- |
| `gcloud compute ssh rainfall-test-m --zone=...` | Connect securely to the cluster's VM. |
| `--` | Pass the rest to SSH. |
| `-L 8123:localhost:8123` | Your Mac's port 8123 → through the pipe → port 8123 on the cluster, where Jupyter listens. |
| `-N` | Don't open a command prompt; just hold the pipe open. |

First run: Enter twice for the passphrase, `yes` to trust the host. A silent cursor means the tunnel is open; leave it.

### 2.3 Open a second terminal for everything else

Don't touch Terminal 1. For git and gcloud, click **+** in VS Code's terminal panel (opens in your repo), or use the Source Control panel.

### 2.4 Connect your notebook to the cluster

The tunnel must be running. Work in your normal VS Code window with your local repo open (File → Open Folder → your repo), not a Remote-SSH window.

1. Open `notebooks/eda.ipynb` from your local repo, click the kernel name (top right).
2. **Select Another Kernel…** → **Existing Jupyter Server…** (not Python Environments: those are your Mac).
3. URL `http://localhost:8123/gateway/default/jupyter/` → **Yes** to the HTTP warning → blank password → name it **GCP cluster**.
4. Pick **PySpark**. Every kernel in that list runs on the cluster.
5. Wait 20 to 60 seconds while Spark starts.

Next time: reopen the tunnel, then **Select Another Kernel…** → **GCP cluster** → **PySpark**. Stuck over 3 minutes: use **Python 3 (ipykernel)** from the same server.

### 2.5 Run sample code

One block per cell, Shift+Enter to run.

**Cell 1: where am I running?**

```python
import socket
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
print("Running on:   ", socket.gethostname())
print("Spark master: ", spark.sparkContext.master)
print("Spark version:", spark.version)
```

Expected: `rainfall-test-m` and `yarn`. Your Mac's name or `local[*]` means you picked a local kernel.

**Cell 2: can Spark compute?** (no data needed)

```python
import time
from pyspark.sql import functions as F

start = time.time()
nums = spark.range(0, 50_000_000).withColumn("group", F.col("id") % 10)
result = nums.groupBy("group").agg(F.count("*").alias("n"), F.avg("id").alias("avg_id"))
result.orderBy("group").show()
print(f"Took {time.time() - start:.1f} s")
```

**Cell 3: is the data in the bucket?**

```python
!gcloud storage ls gs://dsa5208-rainfall-ashley-data/raw/
```

**Cell 4: read the rainfall data**

```python
DATA = "gs://dsa5208-rainfall-ashley-data/raw/rainfall_*.csv"

rain = spark.read.option("header", True).csv(DATA)
rain.printSchema()
rain.show(5, truncate=False)
print("Rows:", rain.count())
```

Columns load as text; cast what you need (`.cast("double")`, `F.to_timestamp(...)`). Use `gs://` paths only; the cluster can't see your Mac.

**Cell 5: a chart**

```python
import matplotlib.pyplot as plt

pdf = result.orderBy("group").toPandas()
pdf.plot(x="group", y="avg_id", kind="bar", legend=False)
plt.title("Test chart from the cluster")
plt.show()
```

Aggregate in Spark, then `.toPandas()` only the small result. Never the full data.

### 2.6 Save, push and finish

Lecture format: `gcloud dataproc clusters delete [CLUSTER NAME] --region=[REGION]`. Example: step 3.

1. Save and push from your Mac (the notebook file, with its outputs, is in your local repo). Use Source Control → **Commit** → **Sync Changes**, or in Terminal 2:

   ```
   git add notebooks/eda.ipynb
   git commit -m "EDA progress"
   git push
   ```
2. Ctrl+C in Terminal 1.
3. Delete and confirm (empty list or DELETING = no more billing):

   ```
   gcloud dataproc clusters delete rainfall-test --region=asia-southeast1 --quiet
   gcloud dataproc clusters list --region=asia-southeast1
   ```

An interrupted delete usually still goes through; rerun only if the list shows RUNNING.

## Part 3: Reference

### 3.1 Troubleshooting

| You see | Fix |
| --- | --- |
| VS Code: `Connection failure ... 404 Not Found` | Use `http://localhost:8123/gateway/default/jupyter/`, not plain `localhost:8123`. |
| VS Code: connection refused, or a kernel that waits forever | The tunnel isn't running. Restart it in Terminal 1. |
| No **Existing Jupyter Server…** in the menu | Install or enable the Jupyter extension, Cmd+Shift+P → Developer: Reload Window, and make sure the file ends in `.ipynb`. |
| Kernel list shows only `/usr/...` or `venv` paths | You're in **Python Environments** (your Mac). Go back and choose **Existing Jupyter Server…**. |
| `PATH_NOT_FOUND: gs://...` | Wrong folder. Your data is in `raw/`: use `gs://dsa5208-rainfall-ashley-data/raw/rainfall_*.csv`. |
| Junk rows when reading a folder | A `.DS_Store` file got uploaded. Read with the `rainfall_*.csv` pattern, or `gcloud storage rm gs://dsa5208-rainfall-ashley-data/raw/.DS_Store`. |
| Upload stuck at `0B` | Normal for a few minutes on 7.5 GiB. Keep the terminal open and the Mac awake. |
| A command aborts with `source .../venv/bin/activate` typed in | VS Code auto-activation interrupted it. Turn it off (1.1) and rerun. |
| Tunnel: `Connection ... closed by remote host` | The cluster was deleted or `--max-idle` removed it. Recreate it (2.1). |
| `zone does not have enough resources` | Use `asia-southeast1-a` in both the create and tunnel commands. |
| `REMOTE HOST IDENTIFICATION HAS CHANGED` | New cluster, old key. Delete `~/.ssh/google_compute_known_hosts` and retry. |
| `NameError: spark` | You're on a local kernel. Switch to **GCP cluster → PySpark**. |

`gcloud config list` shows your account and project; `gcloud help <command>` lists options; see the [cheat sheet](https://docs.cloud.google.com/sdk/docs/cheatsheet).

### 3.2 Quick reference: every session

Once Part 1 is done, a session is only this.

```
# Terminal 2 (Mac): create the cluster, about 3 min
gcloud dataproc clusters create rainfall-test --region=asia-southeast1 --zone=asia-southeast1-b \
  --single-node --master-machine-type=n1-standard-4 \
  --master-boot-disk-type=pd-standard --master-boot-disk-size=50 \
  --image-version=3.0-debian13 --public-ip-address --enable-component-gateway \
  --optional-components=JUPYTER --max-idle=30m

# Terminal 1 (Mac): open the tunnel, leave it running
gcloud compute ssh rainfall-test-m --zone=asia-southeast1-b -- -L 8123:localhost:8123 -N

# VS Code: kernel -> Select Another Kernel -> GCP cluster -> PySpark
#   (first time: Existing Jupyter Server -> http://localhost:8123/gateway/default/jupyter/)
# Work, Cmd+S, Commit, Sync Changes

# Terminal 1: Ctrl+C
# Terminal 2: delete and confirm
gcloud dataproc clusters delete rainfall-test --region=asia-southeast1 --quiet
gcloud dataproc clusters list --region=asia-southeast1
```

### 3.3 What lives where

Deleting a cluster loses only memory and files on its own disk.

| What | Where it lives | Survives cluster delete? |
| --- | --- | --- |
| Notebooks and code | Your Mac + GitHub | Yes |
| Rainfall CSVs | `gs://dsa5208-rainfall-ashley-data/raw/` | Yes |
| Results you save with Spark | Write them to `gs://dsa5208-rainfall-ashley-data/results/` | Yes |
| Cell outputs (tables, charts) | Inside the `.ipynb` on your Mac | Yes |
| Variables in memory, cached DataFrames | The cluster | No: rerun cells next session |
| Files written to a plain path like `/tmp/x.csv` | The cluster's disk | No |

### 3.4 Windows differences

Everything else in this guide (bucket commands, notebook code, kernel, git, delete) is the same on Windows. These parts differ.

| Topic | Mac (this guide) | Windows |
| --- | --- | --- |
| gcloud install | `curl` + `tar` + `install.sh` | Run `GoogleCloudSDKInstaller.exe` from the [install page](https://docs.cloud.google.com/sdk/docs/install-sdk), then reopen VS Code |
| git install | Command Line Tools | [Git for Windows](https://git-scm.com); pick **VS Code** as the default editor (avoids the Vim screen) |
| Local PySpark | Optional | Skip it (needs Java plus Hadoop `winutils`); run on the cluster instead |
| Terminal | zsh | PowerShell (VS Code default) |
| Line continuation | `\` at line end | Backtick `` ` `` at line end, or put the command on one line |
| Variables | `PROJECT_ID=$(gcloud config get-value project)` | `$PROJECT_ID = gcloud config get-value project` |
| Shortcuts | Cmd | Ctrl |
| SSH program | Mac's `ssh` | PuTTY, bundled with gcloud |
| Junk files | `.DS_Store` | `Thumbs.db`, `desktop.ini` (add to `.gitignore`) |
| Host key changed | Delete `~/.ssh/google_compute_known_hosts` | Click **Yes** to accept the new key at the PuTTY prompt |

**Cluster create command in PowerShell** (one line):

```
gcloud dataproc clusters create rainfall-test --region=asia-southeast1 --zone=asia-southeast1-b --single-node --master-machine-type=n1-standard-4 --master-boot-disk-type=pd-standard --master-boot-disk-size=50 --image-version=3.0-debian13 --public-ip-address --enable-component-gateway --optional-components=JUPYTER --max-idle=30m
```

**SSH tunnel on Windows.** Pass the tunnel options with `--ssh-flag`:

```
gcloud compute ssh rainfall-test-m --zone=asia-southeast1-b --ssh-flag="-L 8123:localhost:8123" --ssh-flag="-N"
```

The first run creates a PuTTY key and may show a security prompt; click **Yes**. A PuTTY window may open instead of a silent terminal: that window is the tunnel, so leave it open and close it to end the tunnel. VS Code then connects to `http://localhost:8123/gateway/default/jupyter/` as in 2.4.

**If the tunnel won't work:** open Cloud Console → Dataproc → Clusters → `rainfall-test` → **Web Interfaces → JupyterLab** and work in the browser. No tunnel needed, but notebooks then save to the cluster's staging bucket rather than your local repo, so download them before deleting the cluster.

### 3.5 Sources

- [Install the Google Cloud CLI](https://docs.cloud.google.com/sdk/docs/install-sdk)
- [gcloud CLI cheat sheet](https://docs.cloud.google.com/sdk/docs/cheatsheet)
- [gcloud dataproc clusters create reference](https://docs.cloud.google.com/sdk/gcloud/reference/dataproc/clusters/create)
- [Dataproc optional Jupyter component](https://cloud.google.com/dataproc/docs/concepts/components/jupyter) (port 8123, localhost only)
- [Dataproc cluster web interfaces](https://docs.cloud.google.com/dataproc/docs/concepts/accessing/cluster-web-interfaces) (SSH tunnels)
- DSA5208 Lecture 6 slides: basic setup, create a cluster, run and delete
