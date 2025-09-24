#!/bin/bash

# Cursor PKL Extension - Quick Start Script
# This script sets up and runs the PKL Extension for testing

echo "Starting Cursor PKL Extension Setup..."

# Check if Node.js is installed
if ! command -v node &> /dev/null; then
    echo "ERROR: Node.js is not installed. Please install Node.js 18+ first."
    echo "   Visit: https://nodejs.org/"
    exit 1
fi

# Check Node.js version
NODE_VERSION=$(node -v | cut -d'v' -f2 | cut -d'.' -f1)
if [ "$NODE_VERSION" -lt 18 ]; then
    echo "ERROR: Node.js version $NODE_VERSION is too old. Please install Node.js 18+ first."
    exit 1
fi

echo "Node.js version $(node -v) detected"

# Check if we're on macOS
if [[ "$OSTYPE" != "darwin"* ]]; then
    echo "ERROR: This extension is designed for macOS only."
    exit 1
fi

echo "macOS detected"

# Install dependencies
echo "Installing dependencies..."
npm install

if [ $? -ne 0 ]; then
    echo "ERROR: Failed to install dependencies"
    exit 1
fi

echo "Dependencies installed"

# Create data directory
echo "Creating data directory..."
mkdir -p data
mkdir -p exports

echo "Data directory created"

# Check if Cursor is installed (optional)
if ! pgrep -f "Cursor" > /dev/null; then
    echo "WARNING: Cursor IDE is not running. The extension will work but conversation capture will be limited."
    echo "   For full functionality, please start Cursor IDE."
fi

# Start the web server
echo "Starting PKL Dashboard Server..."
echo ""
echo "Dashboard will be available at: http://localhost:3000"
echo "API endpoints will be available at: http://localhost:3000/api/*"
echo ""
echo "Press Ctrl+C to stop the server."
echo ""

npm start
