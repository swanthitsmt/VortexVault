# GitHub Private Repository Guide

Use this guide to store your project safely in a private GitHub repo.

## Before You Push
1. Confirm `.env` is not tracked.
2. Confirm raw/cleaned combo data is not tracked.
3. Confirm large generated files are ignored.

Check tracked files:
```bash
git status
```

## Create Private Repo on GitHub
1. Open GitHub.
2. Click **New repository**.
3. Enter repository name.
4. Choose **Private**.
5. Click **Create repository**.

Do not add README/gitignore from GitHub UI if your local project already has them.

## First Push (HTTPS)
In your local project folder:
```bash
git init
git branch -M main
git add .
git commit -m "Initial commit"
git remote add origin https://github.com/<your-username>/<repo-name>.git
git push -u origin main
```

If prompted for password, use a **GitHub Personal Access Token (PAT)**, not your GitHub login password.

## Create Personal Access Token (PAT)
1. GitHub -> Settings -> Developer settings -> Personal access tokens.
2. Create token with `repo` scope.
3. Copy token once and store securely.

## Alternative: SSH Push
1. Generate key:
```bash
ssh-keygen -t ed25519 -C "you@example.com"
```
2. Add public key to GitHub.
3. Set remote:
```bash
git remote set-url origin git@github.com:<your-username>/<repo-name>.git
```
4. Push:
```bash
git push -u origin main
```

## Daily Update Flow
```bash
git add .
git commit -m "Describe your change"
git push
```

## Recommended `.gitignore` Entries
Make sure these are ignored:
- `.env`
- `data/input/`
- `data/output/`
- `data/jobs/`
- `*.log`
- `*.sqlite3`

## If You Accidentally Committed Secrets
1. Rotate compromised passwords/tokens immediately.
2. Remove secret from files.
3. Rewrite git history (`git filter-repo` or BFG).
4. Force push cleaned history.

## Verify Repo Is Private
1. Open repository page.
2. Confirm badge says **Private**.
3. Check repository Settings -> Manage access.
