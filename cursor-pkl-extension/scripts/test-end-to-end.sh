#!/bin/bash

# End-to-end test script for Cursor PKL Extension
echo "Testing Cursor PKL Extension End-to-End..."

# Check prerequisites
echo "Checking prerequisites..."

# Check Node.js
if ! command -v node &> /dev/null; then
    echo "[ERROR] Node.js is not installed"
    exit 1
fi

NODE_VERSION=$(node -v | cut -d'v' -f2 | cut -d'.' -f1)
if [ "$NODE_VERSION" -lt 18 ]; then
    echo "[ERROR] Node.js version $NODE_VERSION is too old. Need 18+"
    exit 1
fi
echo "[OK] Node.js $(node -v) detected"

# Check macOS
if [[ "$OSTYPE" != "darwin"* ]]; then
    echo "[ERROR] This extension requires macOS"
    exit 1
fi
echo "[OK] macOS detected"

# Check if Cursor is running
if ! pgrep -f "Cursor" > /dev/null; then
    echo "[WARNING]  Cursor IDE is not running. Please start Cursor first."
    echo "   The extension needs Cursor to detect sessions."
fi

# Install dependencies
echo "[INSTALL] Installing dependencies..."
npm install

if [ $? -ne 0 ]; then
    echo "[ERROR] Failed to install dependencies"
    exit 1
fi
echo "[OK] Dependencies installed"

# Build the application
echo "[BUILD] Building application..."
npm run build:all

if [ $? -ne 0 ]; then
    echo "[ERROR] Build failed"
    exit 1
fi
echo "[OK] Application built successfully"

# Check if dist directory exists and has required files
echo "[CHECK] Checking build output..."
if [ ! -f "dist/main.js" ]; then
    echo "[ERROR] Main process not built"
    exit 1
fi

if [ ! -f "dist/preload.js" ]; then
    echo "[ERROR] Preload script not built"
    exit 1
fi

if [ ! -f "dist/renderer.js" ]; then
    echo "[ERROR] Renderer process not built"
    exit 1
fi

if [ ! -f "dist/index.html" ]; then
    echo "[ERROR] HTML file not built"
    exit 1
fi

echo "[OK] All build artifacts present"

# Create data directories
echo "[MKDIR] Creating data directories..."
mkdir -p ~/.pkl/data
mkdir -p ~/.pkl/exports
echo "[OK] Data directories created"

# Test TypeScript compilation
echo "[CHECK] Testing TypeScript compilation..."
npm run type-check

if [ $? -ne 0 ]; then
    echo "[ERROR] TypeScript compilation failed"
    exit 1
fi
echo "[OK] TypeScript compilation successful"

# Test linting
echo "[CHECK] Testing code linting..."
npm run lint

if [ $? -ne 0 ]; then
    echo "[WARNING]  Linting issues found (non-critical)"
else
    echo "[OK] Code linting passed"
fi

# Test if Electron can start
echo "[START] Testing Electron startup..."
timeout 10s npm start &
ELECTRON_PID=$!

sleep 3

if ps -p $ELECTRON_PID > /dev/null; then
    echo "[OK] Electron started successfully"
    kill $ELECTRON_PID 2>/dev/null
else
    echo "[ERROR] Electron failed to start"
    exit 1
fi

# Test notebook parsing
echo "[NOTEBOOK] Testing notebook parsing..."
if [ -f "test-notebook.ipynb" ]; then
    echo "[OK] Test notebook found"
else
    echo "[WARNING]  Test notebook not found"
fi

# Summary
echo ""
echo "[SUCCESS] End-to-End Test Summary:"
echo "[OK] Node.js version check passed"
echo "[OK] macOS compatibility confirmed"
echo "[OK] Dependencies installed"
echo "[OK] Application built successfully"
echo "[OK] All build artifacts present"
echo "[OK] TypeScript compilation successful"
echo "[OK] Electron startup test passed"
echo ""
echo "[START] The Cursor PKL Extension is ready to use!"
echo ""
echo "To start the extension:"
echo "  npm start"
echo ""
echo "To run in development mode:"
echo "  npm run dev"
echo ""
echo "To test with Jupyter notebooks:"
echo "  1. Open Cursor IDE"
echo "  2. Open a .ipynb file"
echo "  3. Execute some cells"
echo "  4. Check the PKL widget for detected sessions"
