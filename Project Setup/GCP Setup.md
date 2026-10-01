# Dataproc + VS Code Setup: DSA5208 Rainfall Project

Setup guide for running Spark on Google Cloud Dataproc, connected to VS Code
via Remote-SSH.

## How to read this guide

Commands are shown twice wherever something needs substituting:

> **Template** — `UPPERCASE` words are yours to replace
> ```bash
> gcloud projects create PROJECT_ID
> ```
> **Example** — the same command with this guide's worked values
> ```bash
> gcloud projects create dsa5208-rainfall-ashley
> ```

Commands with nothing to substitute are shown once.

## Values you need to substitute

Fill in your own column before you start — you'll refer back to it constantly.

| Placeholder | Example used here | Yours | What it is / how to find it |
|---|---|---|---|
| `PROJECT_ID` | `dsa5208-rainfall-ashley` | | Your GCP project. Globally unique, can't be renamed. `gcloud config get-value project` |
| `REGION` | `asia-southeast1` | | Singapore. Keep unless you have a reason not to. |
| `ZONE` | `asia-southeast1-a` | | A datacentre inside the region. Any of `-a`/`-b`/`-c`. |
| `CLUSTER` | `rainfall-dev` | | Your choice of name. Appears in your SSH config, so pick one and keep it. |
| `CLUSTER-m` | `rainfall-dev-m` | | The master node's VM name — `CLUSTER` plus `-m`. This is what you SSH to. |
| `BUCKET` | `gs://dsa5208-rainfall-ashley-data` | | Your storage bucket. Globally unique, so put your name in it. `gcloud storage ls` |
| `LOCAL_USER` | `ashleyleeruoxin` | | Your laptop username. `whoami` |
| `VM_USER` | `ashleyleeruoxin` | | Your username **on the VM** — from your Google account, so it may differ from `LOCAL_USER`. Found in Step 5's `--dry-run` output. |
| `GITHUB_EMAIL` | `ashleyleeruoxin@gmail.com` | | The address on your **GitHub** account — not your Google Cloud one. |
| `GITHUB_NAME` | `Ashley Lee` | | Your name, for commit attribution. |
| `REPO` | `git@github.com:KohSiXing/DSA5208-Rainfall-Analysis.git` | | Your group's repo. Green "Code" button on GitHub → SSH tab. |
| `REPO_DIR` | `DSA5208-Rainfall-Analysis` | | The folder `git clone` creates — the repo name. |
| `SA_EMAIL` | `4696566071-compute@developer.gserviceaccount.com` | | Compute Engine default service account. `gcloud iam service-accounts list` |
| `BILLING_ID` | `01D969-E96302-78E5A1` | | Your course credit account. `gcloud billing accounts list` |

> **Names that must stay consistent:** `CLUSTER` appears in your SSH config
> (Step 6) — change it in one place and SSH will look for a VM that doesn't
> exist. Same for `BUCKET`, which is baked into your Spark read paths.

> **Names that are globally unique:** project IDs and bucket names are shared
> across all of Google Cloud, not just your account. `rainfall-data` is long
> taken. Putting your name in avoids collisions.

**Conventions**

- *Local machine* = your own laptop (Mac, Windows or Linux)
- *VM* = the cluster's master node, reached through VS Code Remote-SSH
- Every command block says which of the two it runs on
- Keyboard shortcuts written `Cmd+Shift+P` / `Ctrl+Shift+P` — use whichever
  matches your OS
- Paths written for Mac/Linux (`~/.ssh/config`). Windows equivalent is
  `C:\Users\YOU\.ssh\config`

---

## Prerequisites

| # | What | How to get it | Why |
|---|---|---|---|
| 1 | **Google Cloud CLI** (`gcloud`) | Mac: `brew install --cask gcloud-cli`<br>Windows/Linux: [installer](https://cloud.google.com/sdk/docs/install) | Everything in this guide runs through it |
| 2 | **Remote - SSH** extension | VS Code Extensions panel → search "Remote SSH" (publisher: Microsoft) | Lets VS Code open a window running on the cluster |
| 3 | **Python** extension | VS Code Extensions panel (Microsoft) | Interpreter selection |
| 4 | **Jupyter** extension | VS Code Extensions panel (Microsoft) | Notebook support |
| 5 | **Git** + a GitHub account with an SSH key | [github.com/settings/keys](https://github.com/settings/keys) | Code lives in git, not on the VM |
| 6 | A billing account | Redeem the course credit coupon (see Canvas) | Dataproc won't create clusters on an unbilled project |

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

## Step 1: Basic project setup

These are the commands from the lecture slide:

![Basic setup slide](images/01-basic-setup-slide.png)

**All of Step 1 runs on your local machine.**

### 1.1 Log in

```bash
gcloud auth login
```

Opens a browser. Pick the Google account that owns the project. Credentials
persist for weeks — not needed every session.

### 1.2 Create a project and set it as default

**Template**

```bash
gcloud projects create PROJECT_ID
gcloud config set project PROJECT_ID
```

**Example**

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

First find your billing ID:

```bash
gcloud billing accounts list
```

Copy the `ACCOUNT_ID` from the output — that's your `BILLING_ID`, in the format
`01D969-E96302-78E5A1`.

**Template**

```bash
gcloud billing projects link PROJECT_ID --billing-account=BILLING_ID
```

**Example**

```bash
gcloud billing projects link dsa5208-rainfall-ashley --billing-account=01D969-E96302-78E5A1
```

Linking is not a charge. But Dataproc refuses to create clusters on an unbilled
project, so this must come before the next step.

If `billing accounts list` comes back empty, the coupon hasn't been redeemed yet —
go back to the Canvas instructions.

### 1.4 Enable APIs

Nothing to substitute here:

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
`<PROJECT_NUMBER>-compute@developer.gserviceaccount.com`. That's your
`SA_EMAIL`.

**Template**

```bash
gcloud projects add-iam-policy-binding PROJECT_ID \
  --member="serviceAccount:SA_EMAIL" \
  --role="roles/dataproc.worker"
```

**Example**

```bash
gcloud projects add-iam-policy-binding dsa5208-rainfall-ashley \
  --member="serviceAccount:4696566071-compute@developer.gserviceaccount.com" \
  --role="roles/dataproc.worker"
```

Returns a YAML dump of the project's IAM policy — that's normal output, not an
error.

### 1.6 Set the default region and zone

**Template**

```bash
gcloud config set dataproc/region REGION
gcloud config set compute/zone ZONE
```

**Example**

```bash
gcloud config set dataproc/region asia-southeast1
gcloud config set compute/zone asia-southeast1-a
```

Not on the slide, but saves typing later.

---

## Step 2: Enable IAP and the firewall rule

Dataproc image 2.2+ gives VMs **internal IP addresses only** by default, and
gcloud has no flag to turn that off. So there's no public IP to SSH to.

IAP (Identity-Aware Proxy) tunnels SSH through Google's own network instead,
authenticating against your Google identity rather than a network route.

*On your local machine.* Nothing to substitute:

```bash
gcloud services enable iap.googleapis.com
```

```bash
gcloud compute firewall-rules create allow-iap-ssh \
  --network=default \
  --allow=tcp:22 \
  --source-ranges=35.235.240.0/20
```

`35.235.240.0/20` is IAP's fixed IP range — the same for everyone, **don't
change it**. This rule lets that range reach port 22 on your VMs.

### Cloud NAT — outbound internet for the VM

Because the VM has **no public IP**, it also has no route *out* to the internet.
Google APIs like Cloud Storage still work, but everything else doesn't:

- Installing VS Code extensions on the remote (marketplace unreachable)
- `git clone` from GitHub — **over SSH or HTTPS, it makes no difference**
- `pip install` from PyPI

**This is required, not optional** — a Dataproc 2.2 cluster created as described
in this guide has no outbound route until you do this.

> **NAT and authentication are separate problems**, and they produce different
> errors. Reading them correctly saves a lot of time:
>
> | Symptom | Problem | Fix |
> |---|---|---|
> | Clone **hangs** then times out | No route out — NAT | This section |
> | `Permission denied (publickey)` | Reached GitHub, can't authenticate | [Step 7](#step-7-set-up-github-access) |
>
> Switching from SSH to HTTPS does **not** avoid needing NAT — both have to
> reach github.com. HTTPS only changes how you authenticate once you get there.

**Template**

```bash
gcloud compute routers create ROUTER_NAME \
  --network=default \
  --region=REGION
```

```bash
gcloud compute routers nats create NAT_NAME \
  --router=ROUTER_NAME \
  --region=REGION \
  --auto-allocate-nat-external-ips \
  --nat-all-subnet-ip-ranges
```

**Example**

```bash
gcloud compute routers create rainfall-router \
  --network=default \
  --region=asia-southeast1
```

```bash
gcloud compute routers nats create rainfall-nat \
  --router=rainfall-router \
  --region=asia-southeast1 \
  --auto-allocate-nat-external-ips \
  --nat-all-subnet-ip-ranges
```

`ROUTER_NAME` and `NAT_NAME` are yours to pick — anything memorable. They only
have to match each other.

Both persist across cluster recreations. A NAT gateway costs a few cents an
hour while it exists, so delete it at the end of the project:

**Template**

```bash
gcloud compute routers delete ROUTER_NAME --region=REGION
```

**Example**

```bash
gcloud compute routers delete rainfall-router --region=asia-southeast1
```

**Check whether it worked** — on the VM, once connected:

```bash
curl -I -m 10 https://github.com
```

A response means outbound works. A timeout means the NAT isn't in place yet.

---

## Step 3: Create a storage bucket and upload the data

Your data must **not** live on the cluster or in git:

- The cluster's disk is destroyed every time you delete it
- GitHub caps files at 100MB and repos get unhappy past a few GB

It goes in a **GCS bucket**, which is permanent and independent of any cluster.

### 3.1 Create the bucket

*On your local machine.*

**Template**

```bash
gcloud storage buckets create BUCKET --location=REGION
```

**Example**

```bash
gcloud storage buckets create gs://dsa5208-rainfall-ashley-data --location=asia-southeast1
```

| Part | Meaning |
|---|---|
| `gcloud storage buckets create` | the command |
| `gs://...` | the bucket's name. `gs://` is the Google Storage prefix, like `https://` for a website |
| `--location=REGION` | where the files physically live |

Two things worth knowing:

- **Bucket names are globally unique** across all of Google Cloud, same as
  project IDs. That's why this one has a name in it.
- **Keep the bucket in the same region as your cluster.** Same-region reads go
  over Google's internal network — fast and free. Cross-region transfers are
  slower and billed.

Check it exists:

```bash
gcloud storage ls
```

### 3.2 Upload the data

Download the annual rainfall CSVs (2017–2024) from
[data.gov.sg collection 2279](https://data.gov.sg/collections/2279/view) first,
then `cd` to wherever they are.

**Template**

```bash
gcloud storage rsync . BUCKET/raw/ --recursive
```

**Example**

```bash
gcloud storage rsync . gs://dsa5208-rainfall-ashley-data/raw/ --recursive
```

| Part | Meaning |
|---|---|
| `rsync` | syncs a local folder to the bucket |
| `.` | the current folder — `cd` to the CSVs first |
| `--recursive` | include subfolders |

**Why `rsync` and not `cp`:** rsync skips files already uploaded. If your
connection drops partway through — which it will, on a multi-GB upload — you
just re-run the same command and it picks up where it stopped. `cp` would start
over.

> **Expect this to take a while.** Several GB on a home connection can run to an
> hour or more. Start it and leave it. Upload speed is almost always the
> bottleneck, not Google's end.

Check what landed:

**Template**

```bash
gcloud storage ls -l BUCKET/raw/
```

**Example**

```bash
gcloud storage ls -l gs://dsa5208-rainfall-ashley-data/raw/
```

> **Do this before creating a cluster.** Uploading needs no cluster, and you
> don't want one billing by the second while you wait on an upload.

Suggested layout inside the bucket:

```
BUCKET/
├── raw/          the downloaded CSVs — never overwrite these
├── clean/        Parquet after cleaning
├── models/       trained MLlib models
└── results/      figures, metrics, exports
```

Keeping `raw/` separate matters — you'll rewrite `clean/` many times and don't
want to re-download.

**The mental model:** the bucket is your hard drive, the cluster is a CPU you
rent by the second. Nothing important ever lives on the cluster.

---

## Step 4: Create the cluster

*On your local machine.*

**Template**

```bash
gcloud dataproc clusters create CLUSTER \
  --region=REGION \
  --zone=ZONE \
  --single-node \
  --master-machine-type=n2-standard-4 \
  --master-boot-disk-size=100GB \
  --image-version=2.2-debian12 \
  --max-idle=4h \
  --enable-component-gateway \
  --optional-components=JUPYTER
```

**Example**

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

**Template**

```bash
gcloud dataproc clusters list --region=REGION
```

**Example**

```bash
gcloud dataproc clusters list --region=asia-southeast1
```

Wait for `RUNNING`, not `PROVISIONING`.

---

## Step 5: Get your connection details

This prints the exact `ssh` command gcloud would use, **without connecting**.
*On your local machine.*

**Template**

```bash
gcloud compute ssh CLUSTER-m --zone=ZONE --tunnel-through-iap --dry-run
```

**Example**

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

- **`VM_USER`** — at the very end, before the `@`
- **The ProxyCommand** — this is the tunnel

**Then test that it actually connects** (dry-run only prints):

**Template**

```bash
gcloud compute ssh CLUSTER-m --zone=ZONE --tunnel-through-iap
```

**Example**

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
> passphrase so you type it once ever:
> Mac `ssh-add --apple-use-keychain ~/.ssh/google_compute_engine` ·
> Linux `ssh-add ~/.ssh/google_compute_engine` ·
> Windows: start the `ssh-agent` service, then `ssh-add`

---

## Step 6: Build the SSH config block

VS Code can't run `gcloud compute ssh` — it only reads your SSH config file and
runs plain `ssh`. This block translates the tunnel into a form VS Code
understands.

You can paste your `--dry-run` output from Step 5 into Claude and ask it to
convert, or fill in the template below.

**Template**

```
Host CLUSTER-m
  HostName CLUSTER-m
  User VM_USER
  ForwardAgent yes
  IdentityFile /Users/LOCAL_USER/.ssh/google_compute_engine
  IdentitiesOnly yes
  CheckHostIP no
  HashKnownHosts no
  StrictHostKeyChecking no
  UserKnownHostsFile /dev/null
  ProxyCommand GCLOUD_PATH compute start-iap-tunnel CLUSTER-m %p --listen-on-stdin --project=PROJECT_ID --zone=ZONE
```

**Example**

```
Host rainfall-dev-m
  HostName rainfall-dev-m
  User ashleyleeruoxin
  ForwardAgent yes
  IdentityFile /Users/ashleyleeruoxin/.ssh/google_compute_engine
  IdentitiesOnly yes
  CheckHostIP no
  HashKnownHosts no
  StrictHostKeyChecking no
  UserKnownHostsFile /dev/null
  ProxyCommand /opt/homebrew/bin/gcloud compute start-iap-tunnel rainfall-dev-m %p --listen-on-stdin --project=dsa5208-rainfall-ashley --zone=asia-southeast1-a
```

Find `GCLOUD_PATH` with:

```bash
which gcloud          # Mac / Linux
where gcloud          # Windows
```

Four details worth understanding:

- **`HostName` is the VM name, not an IP.** That's why this entry keeps working
  after you delete and recreate the cluster — same name, same entry.
- **`ForwardAgent yes`** lets the VM use your local GitHub key (Step 7).
- **Use the full path to gcloud.** VS Code often doesn't inherit your shell's
  PATH, so a bare `gcloud` here can fail.
- **`UserKnownHostsFile /dev/null` + `StrictHostKeyChecking no`** — together
  these stop SSH remembering host fingerprints for this host. **Both are
  required**, and the reason is in the box below.

> ### Why `/dev/null` matters
>
> SSH stores a fingerprint for every machine it connects to, keyed by hostname.
> Delete and recreate your cluster and you get a **different machine with the
> same name**, so the fingerprint no longer matches.
>
> SSH can't tell that apart from someone impersonating your server, so it
> defends itself — and one of the things it switches off is **port forwarding**:
>
> ```
> @@@ WARNING: REMOTE HOST IDENTIFICATION HAS CHANGED! @@@
> Port forwarding is disabled to avoid man-in-the-middle attacks.
> ```
>
> VS Code Remote-SSH needs port forwarding, so it fails with
> `Could not establish connection: Port forwarding is disabled`.
>
> `StrictHostKeyChecking no` alone is **not** enough — it suppresses the prompt
> for an *unknown* host, but a stored *conflicting* key still triggers the
> block. Pointing `UserKnownHostsFile` at `/dev/null` means nothing is ever
> stored, so there is never a mismatch.
>
> **If you already hit this**, clear the stale entry:
>
> **Template**
> ```bash
> ssh-keygen -R CLUSTER-m -f ~/.ssh/google_compute_known_hosts
> ```
> **Example**
> ```bash
> ssh-keygen -R rainfall-dev-m -f ~/.ssh/google_compute_known_hosts
> ```
>
> If that doesn't clear it, try the default known-hosts file too, or delete the
> GCE one outright — it only holds keys for VMs you've already destroyed:
>
> ```bash
> ssh-keygen -R CLUSTER-m
> rm ~/.ssh/google_compute_known_hosts
> ```
>
> **Security note:** this does disable host-key verification for this one host.
> That's an acceptable trade for a short-lived cluster you create yourself and
> reach over an authenticated IAP tunnel. Don't copy this pattern for a
> long-lived production server.

### Paste it into the SSH config file

Press **`Cmd+Shift+P`** (Mac) or **`Ctrl+Shift+P`** (Windows/Linux), then type
`ssh config`:

![Command palette](images/02-command-palette.png)

Select **Remote-SSH: Open SSH Configuration File...**, then pick your user
config — `/Users/LOCAL_USER/.ssh/config` on Mac,
`/home/LOCAL_USER/.ssh/config` on Linux,
`C:\Users\LOCAL_USER\.ssh\config` on Windows.

Paste the Host block at the bottom and **save**.

> Indentation matters — every line under `Host` needs two leading spaces.

---

## Step 7: Set up GitHub access

A fresh VM has no GitHub credentials, and anything you configure there dies with
the cluster. **SSH agent forwarding** solves this: your local GitHub key gets
used by the VM without ever being copied onto it.

> **This step is needed even if GitHub already works fine on your laptop.**
> Locally, `ssh` reads your key straight off disk at `~/.ssh/id_ed25519` — the
> agent is never involved. The agent only matters for *forwarding*: the VM has
> no key file (and shouldn't — don't copy private keys onto a disposable
> machine), so it asks your laptop to sign on its behalf, and that conversation
> goes through the agent. Modern macOS does **not** auto-load keys into the
> agent at login, so it is almost certainly empty.

*On your local machine*, check whether the agent has your GitHub key:

```bash
ssh-add -l
```

`The agent has no identities` is the usual answer. See which key files you have:

```bash
ls ~/.ssh/
```

`id_ed25519` (or `id_rsa`) is your GitHub key — **not** `google_compute_engine`,
which is the one gcloud made for VM access. Load it:

**Template**

```bash
ssh-add --apple-use-keychain ~/.ssh/YOUR_GITHUB_KEY
```

**Example**

```bash
ssh-add --apple-use-keychain ~/.ssh/id_ed25519
```

`--apple-use-keychain` is Mac-only and makes it persist across reboots. On
Linux drop that flag; on Windows, start the `ssh-agent` service first
(`Get-Service ssh-agent`).

Confirm it took:

```bash
ssh-add -l
```

Should now list a key.

> **Reconnect VS Code after this.** The agent is attached when the SSH session
> is established, so a session that's already open will not pick it up —
> `Cmd+Shift+P` → **Remote-SSH: Connect to Host**.

`ForwardAgent yes` is already in the Host block from Step 6 — if you left it out,
add it now and reconnect.

**Test it** — on the VM, once reconnected:

```bash
echo $SSH_AUTH_SOCK
```

A path like `/tmp/ssh-XXXX/agent.1234` means the agent arrived. Empty means it
didn't — you either skipped the reconnect or `ForwardAgent yes` is missing.

```bash
ssh -T git@github.com
```

- `Hi <your-github-username>!` → working
- `Permission denied (publickey)` → the key isn't in your local agent, or
  `ForwardAgent` is missing, or you didn't reconnect

> Private repos are fine — SSH key auth works the same for public and private.

### HTTPS instead of SSH

If agent forwarding won't cooperate, you can authenticate with a personal
access token instead.

**Template**

```bash
git clone https://github.com/GITHUB_USER/REPO_NAME.git
```

**Example**

```bash
git clone https://github.com/KohSiXing/DSA5208-Rainfall-Analysis.git
```

It prompts for username and password — the "password" is a token from GitHub →
Settings → Developer settings → Personal access tokens.

Two things to know before choosing this:

- **It does not avoid needing Cloud NAT.** HTTPS still has to reach github.com.
  If the clone *hangs*, that's the NAT, and switching protocol won't help.
- **The token isn't remembered**, so you'd paste it every session — and it
  leaves a secret sitting on a machine you're about to delete. Agent forwarding
  keeps the key on your laptop, which is why it's the default here.

Reasonable as a fallback to get moving; not what you want for the whole project.

---

## Step 8: Connect and verify

Click the **remote button** in the bottom-left corner of the VS Code window:

![Remote button](images/03-remote-button.png)

Then choose **Connect to Host** → your `CLUSTER-m` (example: `rainfall-dev-m`).

(Or `Cmd+Shift+P` / `Ctrl+Shift+P` → **Remote-SSH: Connect to Host**.)

First connect to a new cluster takes ~60 seconds while VS Code installs its
server on the VM. The bottom-left turns green showing `SSH: CLUSTER-m`.

Then:

1. **File → Open Folder** → `/home/VM_USER`
   (example: `/home/ashleyleeruoxin`)
2. Accept the prompt to install **Python + Jupyter** extensions on the remote
3. `Cmd+Shift+P` / `Ctrl+Shift+P` → **Python: Select Interpreter** →
   **Enter interpreter path** → `/opt/conda/default/bin/python3`

That conda environment is where PySpark and ipykernel already live. The path is
the same for everyone — nothing to substitute.

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

Steps 1 through 8 never need doing again. A session is only the seven steps
below — about two minutes of setup.

---

## S1. Create the cluster

*On your local machine.*

**Template**

```bash
gcloud dataproc clusters create CLUSTER \
  --region=REGION \
  --zone=ZONE \
  --single-node \
  --master-machine-type=n2-standard-4 \
  --master-boot-disk-size=100GB \
  --image-version=2.2-debian12 \
  --max-idle=4h \
  --enable-component-gateway \
  --optional-components=JUPYTER
```

**Example**

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

## S2. Connect VS Code

`Cmd+Shift+P` / `Ctrl+Shift+P` → **Remote-SSH: Connect to Host** → `CLUSTER-m`
(example: `rainfall-dev-m`)

Wait for the bottom-left to turn green. Then **File → Open Folder** →
`/home/VM_USER` (example: `/home/ashleyleeruoxin`).

## S3. Clone the repo

*On the VM*, in the VS Code terminal. The VM's disk is empty every time, so the
repo comes down fresh.

**Template**

```bash
git clone REPO
cd REPO_DIR
```

**Example**

```bash
git clone git@github.com:KohSiXing/DSA5208-Rainfall-Analysis.git
cd DSA5208-Rainfall-Analysis
```

Set your git identity — this lives in `~/.gitconfig` on the VM, so it resets too.

**Template**

```bash
git config --global user.email "GITHUB_EMAIL"
git config --global user.name "GITHUB_NAME"
```

**Example**

```bash
git config --global user.email "ashleyleeruoxin@gmail.com"
git config --global user.name "Ashley Lee"
```

> **Use your GitHub email here, not your Google Cloud one.** They are separate
> systems: `gcloud auth login` uses your Google Cloud account, `git config` uses
> your GitHub account. GitHub attributes commits by **email**, so if it doesn't
> match an address on your GitHub account, your commits won't show as yours.
>
> Check yours at github.com → Settings → Emails, or with
> `git config --global user.email`.

Any extra packages beyond what the image ships:

```bash
pip install -r requirements.txt
```

## S4. Select the interpreter

`Cmd+Shift+P` / `Ctrl+Shift+P` → **Python: Select Interpreter** →
**Enter interpreter path** → `/opt/conda/default/bin/python3`

Same path for everyone. That conda environment already has PySpark and
ipykernel.

## S5. Read the data with PySpark

New notebook. **Cell 1 — start the Spark session** (nothing to substitute):

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("rainfall").getOrCreate()
print(spark.version, spark.sparkContext.master)
```

Expect Spark `3.5.x` and `yarn`. If it prints `local`, you're not using the
cluster.

**Cell 2 — read straight from the bucket.** No download step: Spark on Dataproc
has the GCS connector built in, so `gs://` paths behave like local ones.

**Template**

```python
BUCKET = "BUCKET"          # <-- your bucket, including the gs:// prefix

df = spark.read.csv(f"{BUCKET}/raw/*.csv", header=True, inferSchema=True)
print(f"{df.count():,} rows")
df.printSchema()
df.show(5, truncate=False)
```

**Example**

```python
BUCKET = "gs://dsa5208-rainfall-ashley-data"

df = spark.read.csv(f"{BUCKET}/raw/*.csv", header=True, inferSchema=True)
print(f"{df.count():,} rows")
df.printSchema()
df.show(5, truncate=False)
```

Setting `BUCKET` once at the top means every later cell is copy-pasteable
unchanged.

> `df.count()` on several GB takes a minute or two the first time — that's Spark
> actually reading the files, not a hang.

### Check the schema is consistent across years

data.gov.sg has changed column names between years before. A glob read across
mismatched schemas produces **nulls rather than an error**, so check before
building anything on it:

```python
for year in range(2017, 2025):
    cols = spark.read.csv(f"{BUCKET}/raw/rainfall_{year}.csv", header=True).columns
    print(year, cols)
```

The 2016 file's columns are `Date`, `Timestamp`, `Update Timestamp`,
`Station Id`, `Station Name`, `Station Device Id`, `Location Longitude`,
`Location Latitude`, `Reading Update Timestamp`, `Reading Value`,
`Reading Type`, `Reading Unit`. Your years should match.

### Convert to Parquet

Do this once, early. It's the single biggest thing you can do for every later
run — more than any cluster resizing:

```python
from pyspark.sql.functions import year as year_fn, to_timestamp, col

df2 = (df
    .withColumn("ts", to_timestamp(col("Timestamp")))
    .withColumn("year", year_fn(col("ts"))))

df2.write.partitionBy("year").mode("overwrite").parquet(f"{BUCKET}/clean/")
```

From then on, read `clean/` instead of `raw/`:

```python
df = spark.read.parquet(f"{BUCKET}/clean/")
```

Parquet is columnar and compressed with the schema baked in, so reads are
several times faster. The year partitions mean Spark skips whole files when you
filter by year — worth a sentence in the report's methodology section.

### Writing results

Always to `gs://`, never to the VM's disk:

```python
df_clean.write.mode("overwrite").parquet(f"{BUCKET}/clean/")
model.write().overwrite().save(f"{BUCKET}/models/gbt_v1")
```

## S6. Push before you delete

**Everything on the VM's disk is destroyed when the cluster is deleted.** There
is no "stop" for Dataproc — only delete.

*On the VM.* Nothing to substitute:

```bash
git status
```

```bash
git add . && git commit -m "your message" && git push
```

Make this reflexive. Unpushed work is the one thing that genuinely can't be
recovered.

## S7. Delete the cluster

Dataproc bills **per second while the cluster exists** — idle or busy, same cost.

*On your local machine.*

**Template**

```bash
gcloud dataproc clusters delete CLUSTER --region=REGION
gcloud dataproc clusters list --region=REGION
```

**Example**

```bash
gcloud dataproc clusters delete rainfall-dev --region=asia-southeast1
gcloud dataproc clusters list --region=asia-southeast1
```

Empty output from the second command means nothing is billing.

---

## Automating the session loop

S1 and S3 can be scripted so a session starts with one command. Save as
`start_cluster.sh` on your local machine (Mac/Linux; on Windows use Git Bash or
WSL). Edit the variables at the top — everything below them uses those.

```bash
#!/bin/bash
set -e

# ---- EDIT THESE ----
CLUSTER=rainfall-dev
REGION=asia-southeast1
ZONE=asia-southeast1-a
REPO="git@github.com:KohSiXing/DSA5208-Rainfall-Analysis.git"
REPO_DIR=DSA5208-Rainfall-Analysis
GITHUB_EMAIL="ashleyleeruoxin@gmail.com"
GITHUB_NAME="Ashley Lee"
# --------------------

gcloud dataproc clusters create $CLUSTER \
  --region=$REGION --zone=$ZONE --single-node \
  --master-machine-type=n2-standard-4 --master-boot-disk-size=100GB \
  --image-version=2.2-debian12 --max-idle=4h \
  --enable-component-gateway --optional-components=JUPYTER

# -A forwards your SSH agent so the VM can authenticate to GitHub
gcloud compute ssh ${CLUSTER}-m --zone=$ZONE --tunnel-through-iap -- -A \
  "git config --global user.email '$GITHUB_EMAIL' && \
   git config --global user.name '$GITHUB_NAME' && \
   git clone $REPO 2>/dev/null || (cd $REPO_DIR && git pull)"

echo "Ready. Cmd+Shift+P -> Remote-SSH: Connect to Host -> ${CLUSTER}-m"
```

Run with `bash start_cluster.sh`. Keep it out of the repo, or have groupmates
edit the variables for themselves.

---

## What survives a delete

| Survives | Lost |
|---|---|
| Steps 1–3 (project, APIs, IAM, firewall, NAT, bucket) | Files in `/home` on the VM |
| Your SSH config entry | `pip install`s |
| Your GCS bucket and everything in it | Notebooks saved on the node |
| Anything pushed to git | |

---

## Troubleshooting

### "Could not establish connection: Port forwarding is disabled"

The host key changed because you recreated the cluster. See
[Why `/dev/null` matters](#why-devnull-matters) above.

**Template**

```bash
ssh-keygen -R CLUSTER-m -f ~/.ssh/google_compute_known_hosts
```

**Example**

```bash
ssh-keygen -R rainfall-dev-m -f ~/.ssh/google_compute_known_hosts
```

If that doesn't clear it, try the default known-hosts file too, or delete the
GCE one outright (it only holds keys for VMs that no longer exist):

```bash
ssh-keygen -R CLUSTER-m
rm ~/.ssh/google_compute_known_hosts
```

Then set `UserKnownHostsFile /dev/null` in your SSH config so it stops
recurring.

### Connection hangs on "Waiting for port forwarding to be ready"

Usually just slow: VS Code installs its server (~100MB) onto the VM over the
IAP tunnel, fresh for every new cluster. 60–90 seconds is normal.

To tell slow from stuck, open the log: `Cmd+Shift+U` → choose **Remote - SSH**
from the dropdown on the right (not "Tasks"). New lines appearing means it's
working. Nothing new for 3+ minutes means it's stuck.

### Diagnosing a connection failure from the terminal

This reproduces what VS Code does, with readable errors.

**Template**

```bash
ssh -f -N -D 12345 CLUSTER-m && sleep 2 && nc -z localhost 12345 && echo "FORWARDING WORKS" || echo "FORWARDING BLOCKED"
```

**Example**

```bash
ssh -f -N -D 12345 rainfall-dev-m && sleep 2 && nc -z localhost 12345 && echo "FORWARDING WORKS" || echo "FORWARDING BLOCKED"
```

Clean up after:

```bash
pkill -f "ssh -f -N -D 12345"
```

### `git clone`, `pip install`, or extension installs time out on the VM

The VM has no public IP, so it has no outbound internet route. Confirm with:

```bash
curl -I -m 10 https://github.com
```

If that times out, set up Cloud NAT — see
[Cloud NAT](#cloud-nat--outbound-internet-for-the-vm) in Step 2.

### "Permission denied (publickey)" when cloning from GitHub

Different problem from the above — this one means you *can* reach GitHub but
can't authenticate. Work through it in this order:

**1. Is the agent reaching the VM?** On the VM:

```bash
echo $SSH_AUTH_SOCK
```

Empty → the agent isn't forwarded. Check `ForwardAgent yes` is in your Host
block, then **reconnect VS Code** (an open session won't pick it up).

**2. Does your local agent have the key?** On your local machine:

```bash
ssh-add -l
```

`The agent has no identities` → load it, then reconnect VS Code:

```bash
ssh-add --apple-use-keychain ~/.ssh/id_ed25519
```

**3. Is it the right key?** `ssh -T git@github.com` on the VM should greet you
by GitHub username. If it doesn't, the key you loaded isn't one your GitHub
account knows about.

> Note that GitHub working fine on your laptop tells you nothing here — local
> git reads the key file directly and never touches the agent. See Step 7.

### The IAP tunnel is slow

gcloud's tunnel runs much faster with NumPy installed into the Python that
gcloud itself uses — this is what the "consider installing NumPy" warning is
about, and it affects every connection, including the ~100MB VS Code Server
install on each new cluster.

```bash
gcloud info --format="value(basic.python_location)"
```

Then, using that path:

**Template**

```bash
PYTHON_LOCATION -m pip install numpy
```

**Example**

```bash
/opt/homebrew/bin/python3.14 -m pip install numpy
```

### No Jupyter kernel in VS Code

The Python and Jupyter extensions need installing **on the remote** — they don't
carry over from your local VS Code. Accept the prompt, then pick the interpreter
explicitly: `Cmd+Shift+P` → **Python: Select Interpreter** → **Enter interpreter
path** → `/opt/conda/default/bin/python3`.

### If SSH won't cooperate at all

The cluster has JupyterLab running already, reachable through Google's
authenticated proxy with no SSH involved:

Console → **Dataproc** → **Clusters** → your cluster → **Web Interfaces** tab →
**JupyterLab**

Less pleasant than VS Code, but it always works and needs zero setup.

---

## Quick reference

**Template**

```bash
# --- Start of session (local machine) ---
gcloud dataproc clusters create CLUSTER --region=REGION --zone=ZONE --single-node --master-machine-type=n2-standard-4 --master-boot-disk-size=100GB --image-version=2.2-debian12 --max-idle=4h --enable-component-gateway --optional-components=JUPYTER

# Connect: Cmd+Shift+P / Ctrl+Shift+P -> Remote-SSH: Connect to Host -> CLUSTER-m

# --- On the VM ---
git clone REPO
cd REPO_DIR
git config --global user.email "GITHUB_EMAIL"    # GITHUB email, not gcloud
git config --global user.name "GITHUB_NAME"
# Interpreter: /opt/conda/default/bin/python3

# --- End of session ---
git add . && git commit -m "wip" && git push     # on the VM
gcloud dataproc clusters delete CLUSTER --region=REGION    # local machine
gcloud dataproc clusters list --region=REGION              # should be empty
```

**Example**

```bash
# --- Start of session (local machine) ---
gcloud dataproc clusters create rainfall-dev --region=asia-southeast1 --zone=asia-southeast1-a --single-node --master-machine-type=n2-standard-4 --master-boot-disk-size=100GB --image-version=2.2-debian12 --max-idle=4h --enable-component-gateway --optional-components=JUPYTER

# Connect: Cmd+Shift+P / Ctrl+Shift+P -> Remote-SSH: Connect to Host -> rainfall-dev-m

# --- On the VM ---
git clone git@github.com:KohSiXing/DSA5208-Rainfall-Analysis.git
cd DSA5208-Rainfall-Analysis
git config --global user.email "ashleyleeruoxin@gmail.com"
git config --global user.name "Ashley Lee"
# Interpreter: /opt/conda/default/bin/python3

# --- End of session ---
git add . && git commit -m "wip" && git push
gcloud dataproc clusters delete rainfall-dev --region=asia-southeast1
gcloud dataproc clusters list --region=asia-southeast1
```
