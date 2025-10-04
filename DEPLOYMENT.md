# Dataset State Historian - Deployment Guide

## Why Not Netlify?

**Netlify is not suitable for this Flask application** because:
- Netlify is designed for static sites and serverless functions
- Your app requires a persistent database (SQLite)
- Background monitoring processes need to run continuously
- WebSocket support for real-time updates
- Long-running Python processes

## Recommended Deployment Platforms

### 1. Railway (Recommended - Easiest)

Railway is the simplest option for Python Flask apps.

**Steps:**
1. Go to [railway.app](https://railway.app)
2. Sign up with GitHub
3. Click "New Project" → "Deploy from GitHub repo"
4. Select your repository
5. Railway will automatically detect it's a Python app
6. Deploy!

**Configuration files already created:**
- `railway.json` - Railway configuration
- `Procfile` - Process definition
- `runtime.txt` - Python version

**Environment Variables to set in Railway:**
- `FLASK_ENV=production`
- `PORT=8081` (Railway will set this automatically)

### 2. Render (Free Tier Available)

Render offers a generous free tier for web services.

**Steps:**
1. Go to [render.com](https://render.com)
2. Sign up with GitHub
3. Click "New" → "Web Service"
4. Connect your GitHub repository
5. Use these settings:
   - **Build Command:** `pip install -r requirements.txt`
   - **Start Command:** `python run.py`
   - **Environment:** Python 3

**Configuration files already created:**
- `render.yaml` - Render configuration
- `Procfile` - Process definition

### 3. Heroku (Paid, Most Reliable)

Heroku is the most established platform but requires payment for production use.

**Steps:**
1. Install Heroku CLI
2. Login: `heroku login`
3. Create app: `heroku create your-app-name`
4. Deploy: `git push heroku main`

**Configuration files already created:**
- `Procfile` - Process definition
- `runtime.txt` - Python version
- `app.json` - App configuration

**Add PostgreSQL (recommended for production):**
```bash
heroku addons:create heroku-postgresql:mini
```

### 4. DigitalOcean App Platform

**Steps:**
1. Go to [DigitalOcean App Platform](https://cloud.digitalocean.com/apps)
2. Create new app from GitHub
3. Select your repository
4. Configure:
   - **Source Directory:** `/`
   - **Build Command:** `pip install -r requirements.txt`
   - **Run Command:** `python run.py`

## Database Considerations

### Current Setup (SQLite)
- ✅ Works for development and small deployments
- ❌ Not suitable for production (file-based, no concurrent access)
- ❌ Data lost when container restarts

### Recommended Production Database
For production, consider upgrading to PostgreSQL:

1. **Update requirements.txt:**
```
psycopg2-binary==2.9.7
```

2. **Update database connection in your app:**
```python
# In src/database/connection.py
import os
DATABASE_URL = os.environ.get('DATABASE_URL', 'sqlite:///datasets.db')
```

3. **Add to deployment platform:**
- Railway: Add PostgreSQL service
- Render: Add PostgreSQL database
- Heroku: Add PostgreSQL addon

## Environment Variables

Set these in your deployment platform:

```env
FLASK_ENV=production
DATABASE_URL=postgresql://... (if using PostgreSQL)
WEB_PORT=8081
LOG_LEVEL=INFO
```

## Pre-Deployment Checklist

- [ ] Test app locally: `python run.py`
- [ ] Check requirements.txt is complete
- [ ] Verify all imports work
- [ ] Test database initialization
- [ ] Check static files are included

## Post-Deployment

1. **Check logs** for any errors
2. **Test all endpoints** work correctly
3. **Verify database** is accessible
4. **Monitor performance** and resource usage

## Troubleshooting

### Common Issues:

1. **Import errors:** Check Python path in run.py
2. **Database errors:** Ensure database file permissions
3. **Port binding:** Use environment PORT variable
4. **Static files:** Check static folder path in Flask app

### Debug Commands:

```bash
# Check if app starts locally
python run.py

# Test with production settings
FLASK_ENV=production python run.py

# Check requirements
pip install -r requirements.txt
```

## Cost Comparison

| Platform | Free Tier | Paid Plans | Best For |
|----------|-----------|------------|----------|
| Railway | $5/month | $20+/month | Easiest setup |
| Render | Free | $7+/month | Budget-friendly |
| Heroku | No free | $7+/month | Most reliable |
| DigitalOcean | No free | $5+/month | Full control |

## Recommendation

**Start with Railway** - it's the easiest to set up and has good free tier options. If you need more control or features, consider Render or Heroku.

---

**Need help?** Check the logs in your deployment platform's dashboard for specific error messages.
