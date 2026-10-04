# Dataproc + VS Code Setup — DSA5208 Rainfall Project

Setup guide for running Spark on Google Cloud Dataproc, connected to VS Code
via Remote-SSH.

**Example values used throughout** — replace with your own:

| Item | Value used here |
|---|---|
| Project ID | `dsa5208-rainfall-ashley` |
| Region | `asia-southeast1` |
| Zone | `asia-southeast1-a` |
| Cluster name | `rainfall-dev` |
| Mac username | `ashleyleeruoxin` |
| VM username | `ashleyleeruoxin` |

---

## Prerequisites

| # | What | How to get it | Why |
|---|---|---|---|
| 1 | **Google Cloud CLI** (`gcloud`) | `brew install --cask gcloud-cli` | Everything in this guide runs through it |
| 2 | **Remote - SSH** extension | VS Code Extensions panel → search "Remote SSH" (publisher: Microsoft) | Lets VS Code open a window running on the cluster |
| 3 | **Python** extension | VS Code Extensions panel (Microsoft) | Interpreter selection |
| 4 | **Jupyter** extension | VS Code Extensions panel (Microsoft) | Notebook support |
| 5 | A billing account | Redeem the course credit coupon (see Canvas) | Dataproc won't create clusters on an unbilled project |

> **Note on extensions 2–4:** they must also be installed **on the remote host**
> after you first connect. VS Code prompts you — accept. Local extensions do not
> carry over.
>
> You do **not** need the Google Cloud Code extension. It's aimed at
> Kubernetes/Cloud Run deployment, not Dataproc.

---
---

# ONE-TIME SETUP

Everything in this half is done **once**. After that, skip straight to
[EVERY WORK SESSION](#every-work-session).

---

## Step 1 — Basic project setup

These are the commands from the lecture slide:

![Basic setup slide](images/01-basic-setup-slide.png)

Filled in with real values:

### 1.1 Log in

```bash
gcloud auth login
```

Opens a browser. Pick the Google account that owns the project. Credentials
persist for weeks — not needed every session.

### 1.2 Create a project and set it as default

```bash
gcloud projects create dsa5208-rainfall-ashley
gcloud config set project dsa5208-rainfall-ashley
```

Project IDs are globally unique and cannot be changed after creation. Adding
your name avoids collisions.

The `config set` line means you don't have to pass `--project` on every later
command. Check with `gcloud config list`.

### 1.3 Link a billing account

> **You should already have a credit coupon from the course.** Redeem it first if
> you haven't — see the professor's instructions on Canvas for the coupon code and
> redemption steps. Once redeemed, it appears as a billing account named something
> like "Billing Account for Education".
>
> **Do not link a personal credit card.** Clusters would then bill to you rather
> than the course credits.

```bash
gcloud billing accounts list
```

Copy the `ACCOUNT_ID` from the output (format `01D969-E96302-78E5A1`), then:

```bash
gcloud billing projects link dsa5208-rainfall-ashley \
  --billing-account=YOUR_ACCOUNT_ID
```

Linking is not a charge. But Dataproc refuses to create clusters on an unbilled
project, so this must come before the next step.

If `billing accounts list` comes back empty, the coupon hasn't been redeemed yet —
go back to the Canvas instructions.

### 1.4 Enable APIs

```bash
gcloud services enable dataproc.googleapis.com
gcloud services enable cloudresourcemanager.googleapis.com
```

New projects have almost every API switched off. Each takes 30–60 seconds.

- `dataproc` — the Spark cluster service
- `cloudresourcemanager` — needed to modify project IAM in the next step

### 1.5 Set permissions

Your cluster's VMs don't log in as you — they run as a **service account**,
which needs the `dataproc.worker` role or the nodes can't do cluster work.

Find the email:

```bash
gcloud iam service-accounts list
```

You want the Compute Engine default, formatted
`<PROJECT_NUMBER>-compute@developer.gserviceaccount.com`. Then:

```bash
gcloud projects add-iam-policy-binding dsa5208-rainfall-ashley \
  --member="serviceAccount:4696566071-compute@developer.gserviceaccount.com" \
  --role="roles/dataproc.worker"
```

Returns a YAML dump of the project's IAM policy — that's normal output, not an
error.

### 1.6 Set the default region and zone

Not on the slide, but saves typing later:

```bash
gcloud config set dataproc/region asia-southeast1
gcloud config set compute/zone asia-southeast1-a
```

---

## Step 2 — Enable IAP and the firewall rule

Dataproc image 2.2+ gives VMs **internal IP addresses only** by default, and
gcloud has no flag to turn that off. So there's no public IP to SSH to.

IAP (Identity-Aware Proxy) tunnels SSH through Google's own network instead,
authenticating against your Google identity rather than a network route.

```bash
gcloud services enable iap.googleapis.com
```

```bash
gcloud compute firewall-rules create allow-iap-ssh \
  --network=default \
  --allow=tcp:22 \
  --source-ranges=35.235.240.0/20
```

`35.235.240.0/20` is IAP's fixed IP range — the same for everyone, don't change
it. This rule lets that range reach port 22 on your VMs.

**Both of these are one-time.** They persist across every cluster delete and
recreate.

---

## Step 2b — Create a storage bucket and upload the data (one-time)

Your data must **not** live on the cluster or in git:

- The cluster's disk is destroyed every time you delete it
- GitHub caps files at 100MB and repos get unhappy past a few GB

It goes in a **GCS bucket**, which is permanent and independent of any cluster.

Create the bucket once:

```bash
gcloud storage buckets create gs://dsa5208-rainfall-ashley-data --location=asia-southeast1
```

Download the annual rainfall CSVs (2017–2024) from
[data.gov.sg collection 2279](https://data.gov.sg/collections/2279/view), then
upload them from your Mac:

```bash
gcloud storage cp *.csv gs://dsa5208-rainfall-ashley-data/raw/
```

Suggested layout inside the bucket:

```
gs://dsa5208-rainfall-ashley-data/
├── raw/          the downloaded CSVs — never overwrite these
├── clean/        Parquet after cleaning
├── models/       trained MLlib models
└── results/      figures, metrics, exports
```

Keeping `raw/` separate matters — you'll rewrite `clean/` many times and don't
want to re-download.

### Reading it in Spark

Spark on Dataproc has the GCS connector built in, so `gs://` paths work exactly
like local paths. **There is no download step:**

```python
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("rainfall").getOrCreate()

df = spark.read.csv(
    "gs://dsa5208-rainfall-ashley-data/raw/*.csv",
    header=True,
    inferSchema=True
)

print(df.count())
df.show(5)
```

Writing results back:

```python
df_clean.write.parquet("gs://dsa5208-rainfall-ashley-data/clean/", mode="overwrite")
```

Why this beats copying onto the VM:

- The data never has to fit on the cluster's disk
- Reads go over Google's internal network, same region — fast
- Every new cluster reads it immediately, no re-copying
- Your groupmates read the same bucket, so you're all on identical data

> **Convert CSV to Parquet early.** It's columnar, compressed and carries a
> schema, so every later read is several times faster and filtering by year
> skips whole files. This will do more for your runtime than any cluster
> resizing:
> `df.write.partitionBy("year").parquet("gs://dsa5208-rainfall-ashley-data/clean/")`

**The mental model:** the bucket is your hard drive, the cluster is a CPU you
rent by the second. Nothing important ever lives on the cluster.

---

## Step 3 — Create the cluster

```bash
gcloud dataproc clusters create rainfall-dev \
  --region=asia-southeast1 \
  --zone=asia-southeast1-a \
  --single-node \
  --master-machine-type=n2-standard-4 \
  --master-boot-disk-size=100GB \
  --image-version=2.2-debian12 \
  --max-idle=4h \
  --enable-component-gateway \
  --optional-components=JUPYTER
```

Takes about 2 minutes. Warnings about Auto Zone, 1TB disks, and firewall
`0.0.0.0/0` are normal — ignore them.

| Flag | Why |
|---|---|
| `--single-node` | One VM doing both master and worker. ~$0.20/hr. Enough for a few GB. For final runs, drop this and add `--num-workers=2`. |
| `--master-machine-type=n2-standard-4` | 4 vCPU, 16GB RAM. "standard" = 4GB per vCPU, the balanced ratio. |
| `--master-boot-disk-size=100GB` | Default is 500GB, which you don't need. |
| `--image-version=2.2-debian12` | **Pin this.** Spark 3.5, Python 3.11. Unpinned, a Google update can break your code mid-project. Also gives an exact version string for the report. |
| `--max-idle=4h` | Self-deletes after 4h with no jobs — insurance against a forgotten overnight cluster. |
| `--enable-component-gateway` | Browser access to the Spark History UI. Useful for diagnosing slow jobs and for report figures. |
| `--optional-components=JUPYTER` | Installs Jupyter on the cluster. |

> `--max-idle` counts **jobs**, not SSH activity. A long notebook session with
> gaps between runs can still trip it.

Check it's ready:

```bash
gcloud dataproc clusters list --region=asia-southeast1
```

Wait for `RUNNING`, not `PROVISIONING`.

---

## Step 4 — Get your connection details

This prints the exact `ssh` command gcloud would use, **without connecting**:

```bash
gcloud compute ssh rainfall-dev-m --zone=asia-southeast1-a --tunnel-through-iap --dry-run
```

Note the `-m` suffix — that's the master node.

The output looks like:

```
/usr/bin/ssh -t -i /Users/ashleyleeruoxin/.ssh/google_compute_engine \
  -o CheckHostIP=no -o HashKnownHosts=no \
  -o HostKeyAlias=compute.5349341028129217650 -o IdentitiesOnly=yes \
  -o StrictHostKeyChecking=yes \
  -o UserKnownHostsFile=/Users/ashleyleeruoxin/.ssh/google_compute_known_hosts \
  -o "ProxyCommand ... compute start-iap-tunnel rainfall-dev-m %p --listen-on-stdin \
      --project=dsa5208-rainfall-ashley --zone=asia-southeast1-a" \
  -o ProxyUseFdpass=no ashleyleeruoxin@compute.5349341028129217650
```

Two things you need from it:

- **Your VM username** — at the very end, before the `@`
- **The ProxyCommand** — this is the tunnel

**Optional:** test that it actually connects (dry-run only prints):

```bash
gcloud compute ssh rainfall-dev-m --zone=asia-southeast1-a --tunnel-through-iap
```

If you land in a Debian shell, it works. Type `exit`.

### Remove the key passphrase (optional)

If it prompts for a passphrase, VS Code will too on every connect. To remove it —
enter the old passphrase, then press Enter twice:

```bash
ssh-keygen -p -f ~/.ssh/google_compute_engine
```

> **Tradeoff:** an unprotected key means anyone with your laptop can reach your
> VMs. Fine for a short-lived student cluster. The safer alternative caches the
> passphrase in macOS Keychain so you type it once ever:
> `ssh-add --apple-use-keychain ~/.ssh/google_compute_engine`

---

## Step 5 — Ask Claude for the Host config block

Paste your `--dry-run` output from Step 4 into Claude and ask:

> "Turn this into an SSH config Host block for VS Code Remote-SSH."

VS Code can't run `gcloud compute ssh` — it only reads `~/.ssh/config` and runs
plain `ssh`. The Host block translates the tunnel into a form VS Code understands.

The result looks like this:

```
Host rainfall-dev-m
  HostName rainfall-dev-m
  User ashleyleeruoxin
  IdentityFile /Users/ashleyleeruoxin/.ssh/google_compute_engine
  IdentitiesOnly yes
  CheckHostIP no
  HashKnownHosts no
  StrictHostKeyChecking no
  UserKnownHostsFile /Users/ashleyleeruoxin/.ssh/google_compute_known_hosts
  ProxyCommand /opt/homebrew/bin/gcloud compute start-iap-tunnel rainfall-dev-m %p --listen-on-stdin --project=dsa5208-rainfall-ashley --zone=asia-southeast1-a
```

Three details worth understanding:

- **`HostName` is the VM name, not an IP.** That's why this entry keeps working
  after you delete and recreate the cluster — same name, same entry.
- **Use the full path to gcloud** (`which gcloud` to find it). VS Code often
  doesn't inherit Homebrew's PATH, so a bare `gcloud` here can fail.
- **`StrictHostKeyChecking no`** — each recreated cluster gets a new host key.
  Without this you'd get a mismatch warning every session.

---

## Step 6 — Open the SSH config file in VS Code

Press **`Cmd+Shift+P`** (Mac) or **`Ctrl+Shift+P`** (Windows/Linux), then type
`ssh config`:

![Command palette](images/02-command-palette.png)

Select **Remote-SSH: Open SSH Configuration File...**, then pick
`/Users/ashleyleeruoxin/.ssh/config`.

Paste the Host block at the bottom. **Save with `Cmd+S`.**

> Indentation matters — every line under `Host` needs two leading spaces.

---

## Step 6b — Set up GitHub access (one-time)

A fresh VM has no GitHub credentials, and anything you configure there dies with
the cluster. **SSH agent forwarding** solves this: your Mac's GitHub key gets
used by the VM without ever being copied onto it.

On your Mac, check your GitHub key is loaded into the agent:

```bash
ssh-add -l
```

If it says "no identities", add it (check the filename with `ls ~/.ssh/`):

```bash
ssh-add --apple-use-keychain ~/.ssh/id_ed25519
```

Then add **one line** to the Host block you just pasted:

```
Host rainfall-dev-m
  HostName rainfall-dev-m
  User ashleyleeruoxin
  ForwardAgent yes
  IdentityFile /Users/ashleyleeruoxin/.ssh/google_compute_engine
  ...rest unchanged...
```

Save. Reconnect VS Code for it to take effect.

**Test it** — on the VM, once connected:

```bash
ssh -T git@github.com
```

- `Hi KohSiXing!` → working
- `Permission denied (publickey)` → the key isn't loaded, or `ForwardAgent` is
  missing

> Private repos are fine — SSH key auth works the same for public and private.

---

## Step 7 — Connect

Click the **remote button** in the bottom-left corner of the VS Code window:

![Remote button](images/03-remote-button.png)

Then choose **Connect to Host** → `rainfall-dev-m`.

(Or `Cmd+Shift+P` → **Remote-SSH: Connect to Host**.)

First connect to a new cluster takes ~60 seconds while VS Code installs its
server on the VM. The bottom-left turns green showing `SSH: rainfall-dev-m`.

Then:

1. **File → Open Folder** → `/home/ashleyleeruoxin`
2. Accept the prompt to install **Python + Jupyter** extensions on the remote
3. `Cmd+Shift+P` → **Python: Select Interpreter** → **Enter interpreter path** →
   `/opt/conda/default/bin/python3`

That conda environment is where PySpark and ipykernel already live.

### Verify Spark

New notebook, one cell:

```python
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("test").getOrCreate()
print(spark.version, spark.sparkContext.master)
```

Expect Spark 3.5.x and `yarn`. If it prints `local`, you're not using the
cluster.

**One-time setup is now complete.**

---
---

# EVERY WORK SESSION

Steps 1 through 6b never need doing again. A session is only the seven steps
below — about two minutes of setup.

## B1. Create the cluster (on your Mac)

```bash
gcloud dataproc clusters create rainfall-dev \
  --region=asia-southeast1 \
  --zone=asia-southeast1-a \
  --single-node \
  --master-machine-type=n2-standard-4 \
  --master-boot-disk-size=100GB \
  --image-version=2.2-debian12 \
  --max-idle=4h \
  --enable-component-gateway \
  --optional-components=JUPYTER
```

About 2 minutes.

## B2. Connect VS Code

`Cmd+Shift+P` → **Remote-SSH: Connect to Host** → `rainfall-dev-m`

Wait for the bottom-left to turn green. Then **File → Open Folder** →
`/home/ashleyleeruoxin`.

## B3. Clone the repo (on the VM)

The VM's disk is empty every time, so the repo has to come down fresh:

```bash
git clone git@github.com:KohSiXing/DSA5208-Rainfall-Analysis.git
```

```bash
cd DSA5208-Rainfall-Analysis
```

Set your git identity — this lives in `~/.gitconfig` on the VM, so it resets too:

```bash
git config --global user.email "sixingashley@gmail.com"
```

```bash
git config --global user.name "Ashley Lee"
```

> GitHub matches commits to your account by **email**, so use the address your
> GitHub account uses.

Any extra packages beyond what the image ships:

```bash
pip install -r requirements.txt
```

## B4. Select the interpreter

`Cmd+Shift+P` → **Python: Select Interpreter** → **Enter interpreter path** →
`/opt/conda/default/bin/python3`

That conda environment already has PySpark and ipykernel.

## B5. Work

Read data straight from the bucket — no download:

```python
df = spark.read.csv("gs://dsa5208-rainfall-ashley-data/raw/*.csv", header=True, inferSchema=True)
```

Write results straight back:

```python
df_clean.write.parquet("gs://dsa5208-rainfall-ashley-data/clean/", mode="overwrite")
```

## B6. Push before you delete

**Everything on the VM's disk is destroyed when the cluster is deleted.** There
is no "stop" for Dataproc — only delete.

```bash
git status
```

```bash
git add . && git commit -m "your message" && git push
```

Make this reflexive. Unpushed work is the one thing that genuinely can't be
recovered.

## B7. Delete the cluster (on your Mac)

Dataproc bills **per second while the cluster exists** — idle or busy, same cost.

```bash
gcloud dataproc clusters delete rainfall-dev --region=asia-southeast1
```

Confirm nothing is running:

```bash
gcloud dataproc clusters list --region=asia-southeast1
```

Empty output means nothing is billing.

---

## Automating the session loop

B1 and B3 can be scripted so a session starts with one command. Save as
`start_cluster.sh` on your Mac:

```bash
#!/bin/bash
set -e
gcloud dataproc clusters create rainfall-dev \
  --region=asia-southeast1 --zone=asia-southeast1-a --single-node \
  --master-machine-type=n2-standard-4 --master-boot-disk-size=100GB \
  --image-version=2.2-debian12 --max-idle=4h \
  --enable-component-gateway --optional-components=JUPYTER

# -A forwards your SSH agent so the VM can authenticate to GitHub
gcloud compute ssh rainfall-dev-m --zone=asia-southeast1-a --tunnel-through-iap -- -A \
  "git config --global user.email 'sixingashley@gmail.com' && \
   git config --global user.name 'Ashley Lee' && \
   git clone git@github.com:KohSiXing/DSA5208-Rainfall-Analysis.git 2>/dev/null || \
   (cd DSA5208-Rainfall-Analysis && git pull)"

echo "Ready. Cmd+Shift+P -> Remote-SSH: Connect to Host -> rainfall-dev-m"
```

Run with `bash start_cluster.sh`. Keep it out of the repo, or have groupmates
edit the variables.

---

## What survives a delete

| Survives | Lost |
|---|---|
| Steps 1 and 2 (project, APIs, IAM, firewall) | Files in `/home` on the VM |
| Your `~/.ssh/config` entry | `pip install`s |
| Your GCS bucket and everything in it | Notebooks saved on the node |
| Anything pushed to git | |

**Next session is just:** the seven steps under
[EVERY WORK SESSION](#every-work-session).

---

## Quick reference

```bash
# --- Start of session (Mac) ---
gcloud dataproc clusters create rainfall-dev --region=asia-southeast1 --zone=asia-southeast1-a --single-node --master-machine-type=n2-standard-4 --master-boot-disk-size=100GB --image-version=2.2-debian12 --max-idle=4h --enable-component-gateway --optional-components=JUPYTER

# Connect: Cmd+Shift+P -> Remote-SSH: Connect to Host -> rainfall-dev-m

# --- On the VM ---
git clone git@github.com:KohSiXing/DSA5208-Rainfall-Analysis.git
cd DSA5208-Rainfall-Analysis
git config --global user.email "sixingashley@gmail.com"
git config --global user.name "Ashley Lee"
# Interpreter: /opt/conda/default/bin/python3

# --- End of session ---
git add . && git commit -m "wip" && git push          # on the VM
gcloud dataproc clusters delete rainfall-dev --region=asia-southeast1   # on the Mac
gcloud dataproc clusters list --region=asia-southeast1                  # should be empty
```
