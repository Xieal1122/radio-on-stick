# Go-only fork and PyCharm workflow

This document describes how to publish the Go-only project in `go-radio/` as a
GitHub fork of `Pigman02/radio-on-stick` without shipping the Python app.

The local `go-radio/` directory is already a separate Git repository with no
Python application, no `app/` directory, no `private/` secrets, and no runtime
data. It contains the station, directory, relay, relay-agent and browser
assets.

## 1. Create the GitHub fork

1. Sign in to GitHub as the account that owns the fork.
2. Open:

   https://github.com/Pigman02/radio-on-stick

3. Click **Fork**.
4. Set **Owner** to your account and **Repository name** to
   `radio-on-stick`.
5. Click **Create fork**.

The fork retains the required GitHub fork relationship. The Go-only branch
will not contain the Python project.

## 2. Clone the fork in PyCharm

1. Open PyCharm.
2. **File > Remote Development > Get from VCS** (older PyCharm:
   **File > New > Project from Version Control**).
3. URL:

   ```text
   git@github.com:<YOUR_GITHUB_USERNAME>/radio-on-stick.git
   ```

   Use the HTTPS URL if SSH is not configured:

   ```text
   https://github.com/<YOUR_GITHUB_USERNAME>/radio-on-stick.git
   ```

4. Choose a directory such as:

   ```text
   ~/PycharmProjects/radio-on-stick-go
   ```

5. Click **Clone**.
6. Accept opening the project.

## 3. PyCharm Git setup

Open **Settings > Version Control > Git** (macOS **PyCharm > Settings**):

- **Git executable**: `/usr/bin/git`
- Click **Test**.
- Open the **Git** tool window (**View > Tool Windows > Git**).

Add the upstream remote. In **Terminal**:

```bash
git remote add upstream git@github.com:Pigman02/radio-on-stick.git
git remote -v
```

Expected:

```text
origin    git@github.com:<you>/radio-on-stick.git
upstream  git@github.com:Pigman02/radio-on-stick.git
```

## 4. Create the Go-only branch

Use an orphan branch so the committed tree contains only the Go project, not
the Python files from the fork's default branch.

### 4.1 Prepare a clean Go tree

In Finder, copy the local `go-radio/` directory to a temporary directory.
Do not copy its `.git` directory.

From Terminal:

```bash
cd ~/PycharmProjects/radio-on-stick-go
git checkout --orphan go
git rm -rf .
```

Keep the clone's `.git` directory.

### 4.2 Copy the Go project

From the original machine/checkout:

```bash
rsync -a --exclude='.git' --exclude='dist' --exclude='data' \
  --exclude='plugins/commercial/helper.py' \
  --exclude='plugins/commercial/app' \
  ~/PycharmProjects/radio-on-stick/go-radio/ \
  ~/PycharmProjects/radio-on-stick-go/
```

Then verify:

```bash
cd ~/PycharmProjects/radio-on-stick-go
ls
```

Required top-level entries include:

```text
cmd/ internal/ web/ plugins/ deploy/ docs/ go.mod go.sum Makefile README.md
```

There must be no `app/`, `tests/`, `data/`, Python modules or private helper.

### 4.3 Check ignored/private files

The checked-in `.gitignore` excludes:

```text
data/
dist/
plugins/commercial/helper.py
plugins/commercial/app/
```

Never force-add these.

```bash
git status --short
```

Only public Go/browser/deployment files should appear.

### 4.4 Commit

```bash
git add .
git commit -m "feat: Go-only station, relay and directory"
```

### 4.5 Push

```bash
git push -u origin go
```

## 5. Make the Go branch the default

In GitHub:

1. **Settings > General > Default branch**.
2. Change from `main`/`master` to `go`.
3. Save.
4. Optionally delete the old Python default branch after confirming the Go
   branch is healthy. The fork relationship remains.

The fork now has a default Go-only branch. Other branches may retain Python
history, but the published/default project does not contain the Python app.

## 6. PyCharm Go configuration

Install/enable the **Go** plugin if needed:

**Settings > Plugins > Marketplace > Go > Install > Restart**.

Open **Settings > Go > Go SDK**:

- Add SDK: `/usr/local/go` or your Go installation.
- Go 1.24+ is required by Pion.
- Apply.

Open **Settings > Go > Go Modules**:

- Enable Go modules integration.
- Environment: `GOFLAGS=-buildvcs=false` is optional for build reproducibility.

Install dependencies:

```bash
go mod tidy
```

## 7. PyCharm run configurations

Create a **Go Build** or **Go Test** configuration for each service:

```text
cmd/station
cmd/directory
cmd/relay
cmd/relay-agent
```

Recommended station run configuration environment:

```text
RADIO_GO_ADDR=127.0.0.1:8091
RADIO_GO_ADMIN_TOKEN=change-me
RADIO_LIBRARY_DIRS=/path/to/music
RADIO_MEDIA_MODE=p2p
RADIO_MESH_STUN=stun:stun.l.google.com:19302
```

For the station, ffmpeg with libopus is required:

```bash
ffmpeg -hide_banner -encoders | grep libopus
```

Tests:

```bash
go test ./...
go vet ./...
```

## 8. PyCharm push after the fork is ready

1. **Git > Commit**.
2. Select only intentional files.
3. Commit message.
4. **Git > Push**.
5. In the push dialog choose `origin/go`.

Never push `plugins/commercial/helper.py`, `plugins/commercial/app/`, `data/`,
`dist/`, `.env`, certificates or logs.

## 9. Updating from Pigman02

```bash
git fetch upstream
git log --oneline go..upstream/main
```

Select changes manually. Do not merge the Python application back into `go`.
If upstream protocol/files are useful, cherry-pick only those files/commits.

## 10. Current Go project features

- Go station with local library, queue, chat, admin and WebRTC RTP source
- Pion WebRTC server seed (no headless listener)
- Directory, relay, relay-agent
- Browser PWA assets
- Debian/Linux and macOS release packaging

Build locally:

```bash
make test
make build
make dist VERSION=0.4.0
```

Pion is used for the server-side WebRTC track; the browser still uses the
normal web UI.
