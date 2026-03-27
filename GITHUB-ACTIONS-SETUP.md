# GitHub Actions CI/CD Setup Guide for Accord

This guide walks you through configuring GitHub Actions to automatically build, test, and deploy Accord to your cloud server.

## Overview

The GitHub Actions workflow (`.github/workflows/deploy.yml`) performs:

1. **Test Phase**: Python syntax checks, backend dependencies, frontend build
2. **Build Phase**: Docker image builds, push to Docker Hub registry
3. **Deploy Phase**: SSH to cloud server, pull code, restart services via docker-compose
4. **Notify Phase**: Slack notifications on success/failure

## Prerequisites

- GitHub repository with Accord code
- Docker Hub account ([docker.io](https://docker.io)) for image registry
- Cloud server (EC2, DigitalOcean, Linode, etc.) with Docker + Docker Compose installed
- SSH key-based access to cloud server
- (Optional) Slack workspace for deployment notifications

## Step 1: Set Up Docker Hub Registry

```bash
# On your local machine, create Docker Hub account if you don't have one
# Visit https://hub.docker.com/signup

# Get Docker username:
echo $DOCKER_USERNAME  # e.g., "krishmk"

# Generate Docker Hub Personal Access Token (PAT):
# 1. Go to https://hub.docker.com/settings/personal-access-tokens
# 2. Click "Generate New Token"
# 3. Name: "Accord CI/CD"
# 4. Permissions: Read & Write
# 5. Copy the token (you won't see it again)
```

## Step 2: Configure GitHub Secrets

Go to your GitHub repository → **Settings** → **Secrets and variables** → **Actions**

Add the following secrets:

### Docker Registry Secrets
- **DOCKER_USERNAME**: your Docker Hub username
- **DOCKER_PASSWORD**: your Docker Hub Personal Access Token
- **REGISTRY**: `docker.io` (or your alternative registry)

### Cloud Server Secrets
- **DEPLOY_HOST**: IP address or hostname of your cloud server (e.g., `203.0.113.42`)
- **DEPLOY_USER**: SSH username (usually `root` or `ubuntu` on cloud VPS)
- **DEPLOY_SSH_KEY**: Your private SSH key for cloud server
  ```bash
  # On your local machine:
  cat ~/.ssh/id_rsa  # Copy the entire key including -----BEGIN/END-----
  # Paste into GitHub secret DEPLOY_SSH_KEY
  ```
- **DEPLOY_DIR**: Base directory on cloud server (e.g., `/home/deployer` or `/root`)

### Optional: Slack Notifications
- **SLACK_WEBHOOK**: Slack incoming webhook URL
  ```
  # From Slack workspace:
  # 1. Go to api.slack.com → Your apps → Create New App
  # 2. Enable Incoming Webhooks
  # 3. Add New Webhook to Workspace
  # 4. Copy the webhook URL
  ```

### Environment Variables (.env.production)

On your **cloud server**, create `/home/deployer/Accord/.env.production` (or your DEPLOY_DIR path):

```bash
# Database (Postgres recommended for production)
DATABASE_URL=postgresql://accord_user:strong_password@localhost/accord_db

# Deployment mode
ACCORD_DEPLOYMENT_MODE=cloud

# Public URLs (use your domain or IP)
BACKEND_PUBLIC_URL=https://api.accord.example.com
FRONTEND_PUBLIC_URL=https://accord.example.com

# CORS
CORS_ALLOW_ORIGINS=https://accord.example.com,https://www.accord.example.com
CORS_ALLOW_ORIGIN_REGEX=^https:\/\/.*\.accord\.example\.com$

# Secrets (generate random strings)
ACCORD_BIOMETRIC_SECRET=$(openssl rand -hex 32)
ACCORD_SSE_TOKEN_SECRET=$(openssl rand -hex 32)

# SMTP (for CA invites, test emails)
ACCORD_SMTP_HOST=smtp.gmail.com
ACCORD_SMTP_PORT=587
ACCORD_SMTP_USERNAME=your-email@gmail.com
ACCORD_SMTP_PASSWORD=your-app-password
ACCORD_SMTP_FROM=noreply@accord.example.com
ACCORD_SMTP_USE_TLS=true

# Ollama (if running locally on server)
OLLAMA_HOST=http://localhost:11434

# Redis (usually localhost in docker-compose)
ACCORD_REDIS_URL=redis://redis:6379/0
```

## Step 3: Prepare Cloud Server

Log into your cloud server via SSH and run:

```bash
# Update system
sudo apt-get update && sudo apt-get upgrade -y

# Install Docker (Ubuntu/Debian)
curl -fsSL https://get.docker.com -o get-docker.sh
sudo sh get-docker.sh

# Install Docker Compose
sudo apt-get install -y docker-compose-plugin

# Add user to docker group (so you don't need sudo)
sudo usermod -aG docker $USER
newgrp docker

# Create deployment directory
mkdir -p /home/deployer/Accord
cd /home/deployer/Accord

# Clone Accord repository
git clone https://github.com/YOUR_USERNAME/Accord.git .

# Create .env.production (see Environment Variables section above)
nano .env.production

# Verify docker-compose is accessible
docker compose version
```

## Step 4: Set Up SSH Key for GitHub Actions

**On cloud server**, add GitHub Actions SSH key to authorized_keys:

```bash
# On your local machine:
ssh-keygen -t ed25519 -f ~/.ssh/github_actions -C "github.actions@accord"
# Press Enter twice (no passphrase needed for CI/CD)

# Display public key:
cat ~/.ssh/github_actions.pub
```

**On cloud server:**

```bash
mkdir -p ~/.ssh
echo "PASTE_PUBLIC_KEY_HERE" >> ~/.ssh/authorized_keys
chmod 600 ~/.ssh/authorized_keys
chmod 700 ~/.ssh

# Test SSH access from local:
ssh -i ~/.ssh/github_actions deployer@your-cloud-server-ip
# Should connect without password
```

**In GitHub:**
- Copy the **private key** (entire contents of `~/.ssh/github_actions`):
  ```bash
  cat ~/.ssh/github_actions
  ```
- Paste into GitHub secret **DEPLOY_SSH_KEY**

## Step 5: Create Database on Cloud Server (PostgreSQL)

```bash
# Install PostgreSQL
sudo apt-get install -y postgresql postgresql-contrib

# Start and enable
sudo systemctl start postgresql
sudo systemctl enable postgresql

# Create database and user
sudo -u postgres psql <<EOF
CREATE USER accord_user WITH PASSWORD 'strong_password_here';
CREATE DATABASE accord_db OWNER accord_user;
GRANT ALL PRIVILEGES ON DATABASE accord_db TO accord_user;
\q
EOF

# Update DATABASE_URL in .env.production
DATABASE_URL=postgresql://accord_user:strong_password_here@localhost/accord_db
```

## Step 6: Set Up SSL/HTTPS (Let's Encrypt)

On your cloud server:

```bash
# Install certbot
sudo apt-get install -y certbot python3-certbot-nginx

# Request certificate (adjust domain)
sudo certbot certonly --standalone -d api.accord.example.com -d accord.example.com

# Certificates stored in /etc/letsencrypt/live/

# Update docker-compose.prod.yml to mount certificates:
docker compose -f docker-compose.prod.yml down
# ... then redeploy with -v /etc/letsencrypt:/etc/letsencrypt:ro
```

## Step 7: Update docker-compose.prod.yml for Cloud Deployment

When deploying to cloud, ensure:

1. Backend listens on `0.0.0.0` (already configured)
2. Frontend serves via Nginx with SSL (already in Dockerfile)
3. Environment variables passed from `.env.production`
4. Health checks are configured (already in place)

## Step 8: Test the Workflow

Push to `main` branch on GitHub:

```bash
git add .
git commit -m "Enable CI/CD: Add GitHub Actions workflow"
git push origin main
```

Monitor the deployment:

1. Go to GitHub repo → **Actions** tab
2. Watch the **Build & Deploy to Cloud** workflow execute
3. Check **Test** → **Build** → **Deploy** jobs
4. Verify Slack notification (if configured)
5. SSH into server and confirm:
   ```bash
   docker compose -f docker-compose.prod.yml ps
   curl http://localhost:8000/api/v1/health
   ```

## Troubleshooting

### Build fails on "Docker login"
- Check DOCKER_USERNAME and DOCKER_PASSWORD are correct
- Ensure Docker Hub PAT has "Read & Write" permissions

### Deploy fails on SSH connection
- Verify DEPLOY_HOST, DEPLOY_USER, DEPLOY_SSH_KEY are correct
- Test manually: `ssh -i KEY deployer@HOST "echo 'OK'"`
- Check cloud server firewall allows SSH (port 22)

### Backend doesn't become healthy after deploy
```bash
# On cloud server:
docker compose -f docker-compose.prod.yml logs backend --tail 100
docker compose -f docker-compose.prod.yml logs redis --tail 20
```

### Frontend shows "Cannot GET /"
- Check frontend container is running: `docker ps`
- Check Nginx config in `friday-insights/nginx.conf`
- Verify API proxy points to correct backend URL

### Rollback to previous deployment
```bash
# On cloud server:
docker compose -f docker-compose.prod.yml down
git checkout HEAD~1  # Previous commit
docker compose -f docker-compose.prod.yml up -d
```

## Monitoring & Alerts

### Check deployment logs
```bash
# GitHub Actions logs: Repository → Actions → Workflow runs

# Cloud server Docker logs:
docker compose -f docker-compose.prod.yml logs backend -f
docker compose -f docker-compose.prod.yml logs frontend -f
```

### Port mappings
- Backend API: `localhost:8000` → exposed on cloud
- Frontend: `localhost:3000` → mapped to port 80 (Nginx)
- Redis: `localhost:6379` → internal only

### Health check endpoints
```bash
curl https://api.accord.example.com/api/v1/health
curl https://api.accord.example.com/api/v1/insights/friday-health
```

---

**When workflow is live**, every push to `main` will automatically:
1. ✅ Test code syntax
2. 🐳 Build Docker images
3. 📦 Push to Docker Hub
4. 🚀 Deploy to cloud server
5. 📢 Notify on Slack

You can now develop locally, push to GitHub, and see live updates on your cloud server within 5-10 minutes.
