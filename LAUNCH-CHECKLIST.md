# 🚀 Accord V3 Launch Checklist

**Status**: Ready for deployment  
**Target**: Zero-touch cloud hosting with automated CI/CD  
**Timeline**: Complete in 1-2 hours  

---

## 📋 Pre-Launch Verification (5 min)

### Local Machine Checks
- [ ] Git repository initialized: `git log --oneline | head -5`
- [ ] Backend runs locally: `cd cloud-backend && python -m uvicorn main:app --reload`
  - Expected: `Application startup complete` on port 8000
  - Health check: `curl http://localhost:8000/api/v1/health | jq .`
- [ ] Frontend builds locally: `cd friday-insights && npm run build`
  - Expected: `dist/` folder with ~500KB JavaScript
- [ ] Docker Desktop running: `docker ps`
- [ ] All env files exist: `.env.production.example` or template ready

### GitHub Repository Status
- [ ] Accord repo pushed to GitHub (public or private)
- [ ] Main branch is clean: `git status`
- [ ] No uncommitted changes: `git status --short` (empty)

---

## 🔑 Phase 1: GitHub Actions Setup (10 min)

### 1.1 Create Docker Hub Account
- [ ] Visit https://hub.docker.com/
- [ ] Sign up or log in
- [ ] Create Personal Access Token:
  1. Settings → Security → Personal Access Tokens
  2. New Token → Name: "Accord CI/CD" → Read & Write
  3. Copy token (save somewhere safe)

### 1.2 Add GitHub Secrets
In your GitHub repo → Settings → Secrets and variables → Actions:

**Docker Hub Secrets:**
- [ ] `DOCKER_USERNAME` = your Docker Hub username (e.g., `krishmk`)
- [ ] `DOCKER_PASSWORD` = your Personal Access Token (NOT your password)

**Cloud Server Secrets:** (you'll fill these after cloud setup in Phase 2)
- [ ] `DEPLOY_HOST` = your server IP or hostname
- [ ] `DEPLOY_USER` = SSH username (usually `deployer` or `ubuntu`)
- [ ] `DEPLOY_SSH_KEY` = your private SSH key (full key, includes -----BEGIN-----/-----END-----)
- [ ] `DEPLOY_DIR` = base directory on server (e.g., `/home/deployer`)

**Optional - Slack Notifications:**
- [ ] `SLACK_WEBHOOK` = your Slack incoming webhook URL

### 1.3 Verify Workflow File
- [ ] `.github/workflows/deploy.yml` exists in repo
- [ ] Syntax: `cat .github/workflows/deploy.yml | head -30`

---

## ☁️ Phase 2: Cloud Server Setup (15 min)

### 2.1 Choose Cloud Provider & Create Instance
Pick one:

**DigitalOcean** (recommended for simplicity)
1. [ ] Create account at https://digitalocean.com
2. [ ] Create Droplet:
   - OS: Ubuntu 22.04 or 24.04 LTS
   - Size: $4-6/month (1GB RAM minimum)
   - Region: closest to your users
3. [ ] Under "Advanced options" → User data:
   - Copy entire contents of `cloud-init.sh` from Accord repo
   - Paste into "User data" text box
4. [ ] Create Droplet
5. [ ] Wait 2-3 minutes for cloud-init to complete
6. [ ] Note the server IP address (e.g., `203.0.113.42`)

**AWS EC2**
1. [ ] Sign in to AWS Console
2. [ ] Launch Instance:
   - Ubuntu 22.04 LTS
   - t3.micro (free tier eligible)
3. [ ] Security group: Allow SSH (22), HTTP (80), HTTPS (443)
4. [ ] User data: Paste `cloud-init.sh`
5. [ ] Note the Elastic IP (assign one for consistency)

**Linode**
1. [ ] Create account at https://linode.com
2. [ ] Create Linode:
   - Image: Ubuntu 22.04 LTS
   - Region: closest to users
   - Type: Nanode (1GB, $5/month)
3. [ ] Under "Add files" → Add from URL:
   - https://raw.githubusercontent.com/YOUR_USERNAME/Accord/main/cloud-init.sh
4. [ ] Create Linode
5. [ ] Note the IP address

### 2.2 SSH into Server & Configure Environment
Once cloud-init completes (2-3 min after creation):

```bash
# From your local machine
ssh -i ~/.ssh/your-key.pem ubuntu@YOUR_SERVER_IP

# On the server:
sudo su - deployer
cd ~/Accord

# Edit environment file
nano .env.production
```

Required edits in `.env.production`:
```bash
# Replace YOUR_IP_PLACEHOLDER with your actual server IP
BACKEND_PUBLIC_URL=http://203.0.113.42:8000
FRONTEND_PUBLIC_URL=http://203.0.113.42:3000

# Generate secrets (run locally, paste result):
# $ openssl rand -hex 32
ACCORD_BIOMETRIC_SECRET=<paste-random-hex-here>
ACCORD_SSE_TOKEN_SECRET=<paste-random-hex-here>

# Optional: Add SMTP for CA invites
ACCORD_SMTP_HOST=smtp.gmail.com
ACCORD_SMTP_USERNAME=your-email@gmail.com
ACCORD_SMTP_PASSWORD=your-app-password  # from myaccount.google.com
```

Save (Ctrl+X, Y, Enter)

### 2.3 Clone Accord Repository (on server)
```bash
cd /home/deployer/Accord

# Initialize as git repo for GitHub Actions to push to
git init
git config user.name "Accord Deployer"
git config user.email "deployer@accord.local"
git config user.password ""  # Will auth via SSH key from GitHub

# Create a placeholder structure (will be overwritten by first deployment)
mkdir -p cloud-backend friday-insights
touch cloud-backend/.gitkeep friday-insights/.gitkeep
git add .
git commit -m "Initial structure"
```

### 2.4 Create SSH Key for GitHub Actions (on server)
```bash
# As deployer user
ssh-keygen -t ed25519 -f ~/.ssh/github_actions -N ""
# Output: Your public key is in /home/deployer/.ssh/github_actions.pub

# Display public key
cat ~/.ssh/github_actions.pub

# Add to authorized_keys (allow GitHub Actions to deploy)
cat ~/.ssh/github_actions.pub >> ~/.ssh/authorized_keys
chmod 600 ~/.ssh/authorized_keys
chmod 700 ~/.ssh
```

### 2.5 Setup Firewall (on server)
```bash
# Cloud-init already configured UFW, verify:
sudo ufw status

# Should show:
# Status: active
# To                         Action      From
# --                         ------      ----
# 22/tcp                     ALLOW       Anywhere
# 80/tcp                     ALLOW       Anywhere
# 443/tcp                    ALLOW       Anywhere
```

---

## 🔐 Phase 3: GitHub Secrets (Complete) (10 min)

Now that cloud server is ready, retrieve and add missing secrets:

### 3.1 Get Server SSH Key
```bash
# On your local machine (NOT on the server)
# From server, you already have the public key
# Now we need the private key for GitHub Actions:

# Option A: If you created the server with a pre-existing key
cat ~/.ssh/your-server-key-name.pem  # Copy entire contents

# Option B: If server generated a new key
# SSH into server as deployer:
ssh -i ~/.ssh/your-key.pem deployer@YOUR_SERVER_IP
cat ~/.ssh/id_rsa  # Or id_ed25519 if server generated that

# Copy the ENTIRE key (from -----BEGIN PRIVATE KEY----- to -----END PRIVATE KEY-----)
```

### 3.2 Add Remaining Secrets to GitHub
Go to GitHub repo → Settings → Secrets and variables → Actions

Add:
- [ ] `DEPLOY_HOST` = Your server IP (e.g., `203.0.113.42`)
- [ ] `DEPLOY_USER` = `deployer`
- [ ] `DEPLOY_SSH_KEY` = (paste entire private SSH key)
- [ ] `DEPLOY_DIR` = `/home/deployer`

### 3.3 Verify All Secrets Present
```bash
# You should now have these secrets in GitHub:
DOCKER_USERNAME
DOCKER_PASSWORD
DEPLOY_HOST
DEPLOY_USER
DEPLOY_SSH_KEY
DEPLOY_DIR
# Plus optional:
# SLACK_WEBHOOK (if you want notifications)
```

---

## 🧪 Phase 4: Test Deployment (20 min)

### 4.1 Push Code to GitHub
```bash
# On your local machine, in Accord directory:
git add .
git commit -m "feat: enable GitHub Actions CI/CD and cloud-init"
git push origin main
```

### 4.2 Monitor GitHub Actions
1. [ ] Go to GitHub repo → Actions tab
2. [ ] Watch the "Build & Deploy to Cloud" workflow execute
3. [ ] Stages:
   - ✅ **Test** (5 min): Python syntax checks, npm build
   - ✅ **Build** (5 min): Docker image builds, push to Docker Hub
   - ✅ **Deploy** (5 min): SSH to server, pull images, start services

### 4.3 Check Deployment Success
Once workflow completes:

```bash
# SSH into server
ssh -i ~/.ssh/your-key.pem deployer@YOUR_SERVER_IP

# Check running containers
docker ps
# Should show: redis, accord-backend, accord-frontend

# Check logs
docker compose -f docker-compose.prod.yml logs backend --tail 20

# Health check
curl http://localhost:8000/api/v1/health | jq .
# Expected: { "status": "healthy", ... }

# Frontend health
curl http://localhost:3000/
# Expected: HTML response (200 OK)
```

### 4.4 Access Your App
- Frontend: http://YOUR_SERVER_IP:3000
- Backend API: http://YOUR_SERVER_IP:8000/docs
- Health: http://YOUR_SERVER_IP:8000/api/v1/health

---

## 🔒 Phase 5: SSL/HTTPS Setup (10 min, Optional but Recommended)

### 5.1 Point Domain to Server
If you have a domain:
1. [ ] In your domain registrar (GoDaddy, Namecheap, etc.)
2. [ ] Update A record: `your-domain.com → YOUR_SERVER_IP`
3. [ ] Update A record: `api.your-domain.com → YOUR_SERVER_IP`
4. [ ] Wait for DNS propagation (~5-15 min): `nslookup your-domain.com`

### 5.2 Request SSL Certificate
```bash
# On server as root
sudo certbot certonly --standalone -d your-domain.com -d api.your-domain.com

# Follow prompts, email required
# Certificates stored in: /etc/letsencrypt/live/your-domain.com/

# Test certificate
sudo certbot certificates
```

### 5.3 Update .env.production
```bash
# On server
nano /home/deployer/Accord/.env.production

# Change URLs to HTTPS:
BACKEND_PUBLIC_URL=https://api.your-domain.com
FRONTEND_PUBLIC_URL=https://your-domain.com

# Save and restart stack
docker compose -f docker-compose.prod.yml down
docker compose -f docker-compose.prod.yml up -d
```

---

## ✅ Phase 6: Verification Checklist (5 min)

- [ ] Backend API responds: `curl https://api.your-domain.com/api/v1/health`
- [ ] Frontend loads: Visit https://your-domain.com in browser
- [ ] Journal endpoint works: `curl https://api.your-domain.com/api/v1/journal -H "X-Role: admin"`
- [ ] No console errors in browser

### Run Health Check Script
```bash
# On server
accord-health
# Shows:
# - Running containers
# - Recent logs
# - Health status
# - Frontend response code
```

---

## 🔄 Phase 7: Continuous Deployment (Ongoing)

Now that CI/CD is live, every code push is automatic:

### Typical Workflow
```bash
# Make changes locally
echo "new feature" >> cloud-backend/main.py

# Commit and push
git add .
git commit -m "feat: add new feature"
git push origin main

# GitHub Actions automatically:
# 1. Tests code (syntax, build)
# 2. Builds Docker images
# 3. Pushes to Docker Hub
# 4. Deploys to your server
# 5. Validates health

# ✅ Your change is live in 5-10 minutes!
```

### Monitor Deployments
- GitHub: **Actions** tab → Recent workflow runs
- Server logs: `docker compose -f docker-compose.prod.yml logs -f`
- Email: Slack notification (if configured)

---

## 🚨 Troubleshooting

### GitHub Actions Fails on Build
**Symptom**: Workflow shows ❌ on "Build & Push Docker Images"

**Fix**:
1. Check Docker Hub credentials in GitHub Secrets
2. Verify `DOCKER_USERNAME` and `DOCKER_PASSWORD` are correct
3. In Docker Hub, check if PAT has "Read & Write" permissions

```bash
# Test Docker login locally:
docker login -u YOUR_USERNAME -p YOUR_PAT
docker tag my-image:latest YOUR_USERNAME/my-image:latest
docker push YOUR_USERNAME/my-image:latest
```

### Deploy Fails on SSH Connection
**Symptom**: Workflow shows ❌ on "Deploy via SSH"

**Fix**:
1. Verify `DEPLOY_HOST` is correct IP
2. Verify SSH key (`DEPLOY_SSH_KEY`) is pasted correctly
3. Verify `DEPLOY_USER` matches server account

```bash
# Test SSH manually:
ssh -i path-to-key -u deployer YOUR_SERVER_IP "echo hello"
# Should print "hello"
```

### Backend Not Healthy After Deploy
**Symptom**: Workflow passes but API doesn't respond

**Fix**:
```bash
# SSH to server
ssh deployer@YOUR_SERVER_IP

# Check container status
docker ps | grep backend
# Should show "Up X minutes"

# View logs
docker logs -f accord-backend --tail 50

# Check .env.production
cat .env.production | grep DATABASE_URL
```

### Frontend Shows "Cannot GET /"
**Symptom**: Browser shows blank or error

**Fix**:
```bash
# On server
docker logs accord-frontend --tail 20

# Check nginx config
cat friday-insights/nginx.conf | grep location

# Rebuild frontend
docker compose -f docker-compose.prod.yml down
docker compose -f docker-compose.prod.yml up -d
```

---

## 📚 Next Steps After Launch

Once live, implement the remaining 6 tasks:

1. ✅ **Task 1/4: Cloud Hosting + CI/CD** ← **COMPLETE**
2. **Task 2: UI/UX Overhaul** (Design system, dark mode, animations)
3. **Task 3: Bank Reconciliation** (Fuzzy matching algorithm)
4. **Task 5: Mobile OTA** (Expo EAS updates)
5. **Task 6: Offline-First** (SQLite sync)
6. **Task 7: CA Reports** (PDF/Excel export)

---

## 📞 Support Resources

- **GitHub Actions Docs**: https://docs.github.com/en/actions
- **Docker Docs**: https://docs.docker.com/
- **Let's Encrypt**: https://letsencrypt.org/docs/
- **Cloud Provider-Specific**:
  - DigitalOcean: https://docs.digitalocean.com/products/droplets/
  - AWS EC2: https://docs.aws.amazon.com/ec2/
  - Linode: https://www.linode.com/docs/guides/

---

**Congratulations! 🎉 Your Accord infrastructure is now production-ready with zero-touch deployments.**

Every time you push code to `main`, it automatically tests, builds, and deploys to your cloud server. No manual debugging, no SSH, no downtime.

Next: Polish the UI (Task 2) or implement advanced algorithms (Task 3).
