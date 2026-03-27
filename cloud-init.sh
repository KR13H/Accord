#!/bin/bash
# Accord Cloud Init Script
# Paste this into your cloud provider's "User data" or "cloud-init" section
# Tested on: Ubuntu 22.04 LTS, Debian 12, Ubuntu 24.04 LTS

set -euo pipefail

export DEBIAN_FRONTEND=noninteractive

# Logging
exec > >(tee -a /var/log/accord-init.log)
exec 2>&1
echo "[accord-init] Starting Accord cloud provisioning at $(date)"

# ==============================================================================
# STEP 1: System Update & Dependencies
# ==============================================================================
echo "[accord-init] Updating system packages..."
apt-get update
apt-get upgrade -y
apt-get install -y \
  curl \
  wget \
  git \
  build-essential \
  ca-certificates \
  gnupg \
  lsb-release \
  apt-transport-https \
  certbot \
  ufw

# ==============================================================================
# STEP 2: Docker Installation
# ==============================================================================
echo "[accord-init] Installing Docker..."

# Remove old Docker versions
apt-get remove -y docker docker.io docker-ce docker-ce-cli 2>/dev/null || true

# Install Docker using official script
curl -fsSL https://get.docker.com -o /tmp/get-docker.sh
bash /tmp/get-docker.sh

# Install Docker Compose
echo "[accord-init] Installing Docker Compose..."
apt-get install -y docker-compose-plugin

# Verify installation
docker --version
docker compose version

# Enable Docker daemon at boot
systemctl enable docker
systemctl start docker

# ==============================================================================
# STEP 3: User Setup for Deployments
# ==============================================================================
echo "[accord-init] Creating deploy user..."

# Create deployer user if it doesn't exist
if ! id -u deployer > /dev/null 2>&1; then
  useradd -m -s /bin/bash -G docker deployer
  echo "deployer:$(openssl rand -base64 32)" | chpasswd
  echo "[accord-init] Created deployer user added to docker group"
else
  usermod -aG docker deployer
  echo "[accord-init] deployer user already exists"
fi

# Create deployment directory
mkdir -p /home/deployer/Accord
chown -R deployer:deployer /home/deployer/Accord

# ==============================================================================
# STEP 4: Clone Accord Repository
# ==============================================================================
echo "[accord-init] Cloning Accord repository..."

cd /home/deployer/Accord

# Initialize git if not already a repo
if [ ! -d .git ]; then
  git init
  # Note: This cloud-init runs as root. Later, the GitHub Actions workflow
  # will push to this repo. For now, we set up the basic structure.
  git config user.name "Accord Deployer"
  git config user.email "deployer@accord.local"
fi

echo "[accord-init] Repository structure ready at /home/deployer/Accord"

# ==============================================================================
# STEP 5: Create .env.production Template
# ==============================================================================
echo "[accord-init] Creating .env.production template..."

# Get cloud instance metadata (if available)
INSTANCE_IP=$(curl -s http://169.254.169.254/latest/meta-data/public-ipv4 2>/dev/null || echo "YOUR_IP_HERE")

cat > /home/deployer/Accord/.env.production << 'EOF'
# ============================================================================
# Accord Production Configuration
# ============================================================================

# Database (PostgreSQL recommended; SQLite as fallback)
# For local PostgreSQL: postgresql://accord_user:password@localhost/accord_db
# For managed DB (RDS, DigitalOcean): postgresql://user:pass@host:5432/accord_db
DATABASE_URL=sqlite:////app/ledger.db

# Deployment mode (must be "cloud" for production)
ACCORD_DEPLOYMENT_MODE=cloud

# Public URLs (update with your domain or server IP)
# If using IP: http://203.0.113.42 (include port if not 80/443)
# If using domain: https://api.accord.example.com
BACKEND_PUBLIC_URL=http://INSTANCE_IP_PLACEHOLDER:8000
FRONTEND_PUBLIC_URL=http://INSTANCE_IP_PLACEHOLDER:3000

# CORS (comma-separated list of allowed origins)
CORS_ALLOW_ORIGINS=http://INSTANCE_IP_PLACEHOLDER:3000,http://localhost:3000
CORS_ALLOW_ORIGIN_REGEX=

# Security Secrets (generate with: openssl rand -hex 32)
ACCORD_BIOMETRIC_SECRET=GENERATE_WITH_OPENSSL_RAND_HEX_32
ACCORD_SSE_TOKEN_SECRET=GENERATE_WITH_OPENSSL_RAND_HEX_32
ACCORD_SSE_TOKEN_TTL_SECONDS=3600

# SMTP Configuration (for CA invites, email notifications)
# Gmail example: use your email + app-specific password from myaccount.google.com
ACCORD_SMTP_HOST=smtp.gmail.com
ACCORD_SMTP_PORT=587
ACCORD_SMTP_USERNAME=your-email@gmail.com
ACCORD_SMTP_PASSWORD=your-app-specific-password
ACCORD_SMTP_FROM=noreply@accord.local
ACCORD_SMTP_USE_TLS=true

# Ollama (AI/LLM endpoint)
# Local: http://localhost:11434
# Remote: http://host.docker.internal:11434 (Docker on Mac/Windows)
OLLAMA_HOST=http://host.docker.internal:11434

# Redis (internal, usually unchanged)
ACCORD_REDIS_URL=redis://redis:6379/0
EOF

chown deployer:deployer /home/deployer/Accord/.env.production
chmod 600 /home/deployer/Accord/.env.production

echo "[accord-init] Created .env.production template"
echo "[accord-init] ⚠️  IMPORTANT: Edit /home/deployer/Accord/.env.production and fill in:"
echo "  - DATABASE_URL (or use SQLite default)"
echo "  - BACKEND_PUBLIC_URL, FRONTEND_PUBLIC_URL (your domain or IP)"
echo "  - Security secrets (run: openssl rand -hex 32)"
echo "  - SMTP credentials (for email features)"

# ==============================================================================
# STEP 6: Firewall Configuration
# ==============================================================================
echo "[accord-init] Configuring firewall..."

# Enable UFW (Uncomplicated Firewall)
ufw --force enable

# Allow SSH (critical: don't lock yourself out!)
ufw allow 22/tcp

# Allow HTTP and HTTPS
ufw allow 80/tcp
ufw allow 443/tcp

# Allow backend on 8000 (optional, for direct API access)
# ufw allow 8000/tcp

# Show firewall status
ufw status

echo "[accord-init] Firewall configured"

# ==============================================================================
# STEP 7: SSL/TLS Certificate (Let's Encrypt)
# ==============================================================================
echo "[accord-init] Preparing SSL/TLS setup..."

# Note: You'll need a domain name to use Let's Encrypt
# After cloud-init completes, run manually:
#   certbot certonly --standalone -d your-domain.com
# 
# Or if you use Nginx reverse proxy:
#   certbot certonly --nginx -d your-domain.com

echo "[accord-init] To enable SSL/TLS:"
echo "  1. Point your domain to this server IP: $INSTANCE_IP"
echo "  2. SSH into server and run:"
echo "     certbot certonly --standalone -d your-domain.com"
echo "  3. Update BACKEND_PUBLIC_URL and FRONTEND_PUBLIC_URL to use https://"

# ==============================================================================
# STEP 8: Docker Compose Stack Ready
# ==============================================================================
echo "[accord-init] Preparing docker-compose..."

# Create docker-compose.prod.yml symlink for easy access
# (actual file comes from GitHub push)
ln -sf /home/deployer/Accord/docker-compose.prod.yml /home/deployer/Accord/docker-compose.yml 2>/dev/null || true

# Allow deployer to access docker
su - deployer -c "docker ps" > /dev/null 2>&1 && echo "[accord-init] Docker access verified for deployer user"

# ==============================================================================
# STEP 9: PostgreSQL Setup (Optional)
# ==============================================================================
# Uncomment if you want local PostgreSQL instead of SQLite

# echo "[accord-init] Installing PostgreSQL..."
# apt-get install -y postgresql postgresql-contrib
# systemctl enable postgresql
# systemctl start postgresql

# # Create Accord database and user
# sudo -u postgres psql <<SQL
# CREATE USER accord_user WITH PASSWORD 'strong_password_here';
# CREATE DATABASE accord_db OWNER accord_user;
# GRANT ALL PRIVILEGES ON DATABASE accord_db TO accord_user;
# SQL

# echo "[accord-init] PostgreSQL setup complete"
# echo "[accord-init] Update .env.production DATABASE_URL to:"
# echo "[accord-init]   postgresql://accord_user:strong_password_here@localhost/accord_db"

# ==============================================================================
# STEP 10: Systemd Service for Auto-Restart
# ==============================================================================
echo "[accord-init] Creating systemd service for Accord..."

cat > /etc/systemd/system/accord.service << 'EOF'
[Unit]
Description=Accord Cloud Stack
After=docker.service
Requires=docker.service

[Service]
Type=oneshot
User=deployer
WorkingDirectory=/home/deployer/Accord
ExecStart=/usr/bin/docker compose -f docker-compose.prod.yml up -d
ExecStop=/usr/bin/docker compose -f docker-compose.prod.yml down
RemainAfterExit=yes
Restart=always
RestartSec=10

StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable accord.service

echo "[accord-init] Created accord.service (will auto-start when code is deployed)"

# ==============================================================================
# STEP 11: Monitoring & Logging
# ==============================================================================
echo "[accord-init] Setting up logging..."

# Create log directory
mkdir -p /var/log/accord
chown deployer:deployer /var/log/accord

# Create log rotation config
cat > /etc/logrotate.d/accord << EOF
/var/log/accord/*.log {
    daily
    missingok
    rotate 14
    compress
    delaycompress
    notifempty
    create 0644 deployer deployer
    sharedscripts
}
EOF

echo "[accord-init] Logs will be stored in /var/log/accord"

# ==============================================================================
# STEP 12: Health Check & Readiness
# ==============================================================================
echo "[accord-init] Creating health check script..."

cat > /usr/local/bin/accord-health << 'EOF'
#!/bin/bash
echo "=== Accord Cloud Stack Health Check ==="
echo "Docker status:"
docker ps --format "table {{.Names}}\t{{.Status}}"

echo ""
echo "Stack logs (last 20 lines):"
cd /home/deployer/Accord && docker compose -f docker-compose.prod.yml logs --tail 20

echo ""
echo "Backend health:"
curl -s http://localhost:8000/api/v1/health | jq '.' || echo "Backend not responding"

echo ""
echo "Frontend status:"
curl -s -o /dev/null -w "%{http_code}" http://localhost:3000 && echo " (Frontend responding)" || echo " (Frontend not responding)"
EOF

chmod +x /usr/local/bin/accord-health

echo "[accord-init] Health check script installed: accord-health"

# ==============================================================================
# STEP 13: Final Message
# ==============================================================================

cat << FINAL_MESSAGE

################################################################################
#                        ACCORD CLOUD INIT COMPLETE
################################################################################

✅ Docker and Docker Compose installed
✅ Deployment user created (deployer)
✅ Firewall configured (SSH, HTTP, HTTPS)
✅ Repository structure ready at /home/deployer/Accord
✅ Systemd service configured (auto-restart on boot)

################################################################################
#                           NEXT STEPS
################################################################################

1. SSH into this server:
   ssh -i your-key.pem ubuntu@$INSTANCE_IP

2. Configure environment:
   sudo nano /home/deployer/Accord/.env.production
   
   Required:
   - DATABASE_URL (default SQLite is fine for testing)
   - BACKEND_PUBLIC_URL (your domain or IP)
   - FRONTEND_PUBLIC_URL (your domain or IP)
   - Generate secrets: openssl rand -hex 32
   - Add SMTP credentials (optional, for CA invites)

3. Setup GitHub Secrets (in your GitHub repo):
   DOCKER_USERNAME, DOCKER_PASSWORD (Docker Hub)
   DEPLOY_HOST = $INSTANCE_IP
   DEPLOY_USER = deployer
   DEPLOY_SSH_KEY = (contents of ~/.ssh/your-key.pem)
   DEPLOY_DIR = /home/deployer

4. Push Accord code to GitHub main branch
   GitHub Actions will automatically:
   - Test code
   - Build Docker images
   - Push to Docker Hub
   - Deploy to this server

5. Verify deployment:
   ssh -i your-key.pem ubuntu@$INSTANCE_IP
   accord-health

################################################################################
#                           USEFUL COMMANDS
################################################################################

# View stack logs
docker compose -f /home/deployer/Accord/docker-compose.prod.yml logs -f

# Stop/restart stack
docker compose -f /home/deployer/Accord/docker-compose.prod.yml down
docker compose -f /home/deployer/Accord/docker-compose.prod.yml up -d

# Check health
accord-health

# View env file
cat /home/deployer/Accord/.env.production

# SSH key setup (if you haven't done it for CI/CD):
# 1. Run: ssh-keygen -t ed25519 -f ~/.ssh/github_actions
# 2. Add to: ~/.ssh/authorized_keys
# 3. Paste private key into GitHub secret DEPLOY_SSH_KEY

################################################################################
#                         DOCUMENTATION
################################################################################

Full setup guide: GITHUB-ACTIONS-SETUP.md (in your repo)

Questions? Check:
- /var/log/accord-init.log (this script's output)
- docker logs accord-backend
- https://docs.docker.com/compose/

################################################################################

FINAL_MESSAGE

echo "[accord-init] Provisioning complete! Cloud-init logs saved to /var/log/accord-init.log"

# ==============================================================================
# Cleanup
# ==============================================================================
rm -f /tmp/get-docker.sh

echo "[accord-init] Cloud provisioning finished at $(date)"
