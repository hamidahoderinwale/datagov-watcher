#!/bin/bash

# Production Startup Script for Comprehensive Dataset Discovery and Monitoring System
# This script starts both the web interface and the comprehensive monitoring system

set -e

# Configuration
PROJECT_DIR="/Users/hamidaho/dataset_monitor"
VENV_DIR="$PROJECT_DIR/venv"
WEB_PORT=8081
LOG_DIR="$PROJECT_DIR/logs"
PID_DIR="$PROJECT_DIR/pids"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Create necessary directories
mkdir -p "$LOG_DIR" "$PID_DIR"

echo -e "${BLUE}Starting Comprehensive Dataset Discovery and Monitoring System${NC}"
echo "================================================================"

# Function to check if a process is running
check_process() {
    local pid_file="$1"
    if [ -f "$pid_file" ]; then
        local pid=$(cat "$pid_file")
        if ps -p "$pid" > /dev/null 2>&1; then
            return 0
        else
            rm -f "$pid_file"
            return 1
        fi
    fi
    return 1
}

# Function to start a process
start_process() {
    local name="$1"
    local command="$2"
    local pid_file="$PID_DIR/${name}.pid"
    local log_file="$LOG_DIR/${name}.log"
    
    if check_process "$pid_file"; then
        echo -e "${YELLOW}WARNING: $name is already running (PID: $(cat $pid_file))${NC}"
        return 0
    fi
    
    echo -e "${BLUE}Starting $name...${NC}"
    cd "$PROJECT_DIR"
    source "$VENV_DIR/bin/activate"
    
    nohup $command > "$log_file" 2>&1 &
    local pid=$!
    echo $pid > "$pid_file"
    
    # Wait a moment and check if process started successfully
    sleep 2
    if ps -p "$pid" > /dev/null 2>&1; then
        echo -e "${GREEN}SUCCESS: $name started successfully (PID: $pid)${NC}"
        echo "   Log file: $log_file"
    else
        echo -e "${RED}ERROR: Failed to start $name${NC}"
        rm -f "$pid_file"
        return 1
    fi
}

# Function to stop a process
stop_process() {
    local name="$1"
    local pid_file="$PID_DIR/${name}.pid"
    
    if [ -f "$pid_file" ]; then
        local pid=$(cat "$pid_file")
        if ps -p "$pid" > /dev/null 2>&1; then
            echo -e "${YELLOW}Stopping $name (PID: $pid)...${NC}"
            kill "$pid"
            sleep 2
            if ps -p "$pid" > /dev/null 2>&1; then
                echo -e "${YELLOW}WARNING: Force killing $name...${NC}"
                kill -9 "$pid"
            fi
            rm -f "$pid_file"
            echo -e "${GREEN}SUCCESS: $name stopped${NC}"
        else
            echo -e "${YELLOW}WARNING: $name was not running${NC}"
            rm -f "$pid_file"
        fi
    else
        echo -e "${YELLOW}WARNING: $name was not running${NC}"
    fi
}

# Function to show status
show_status() {
    echo -e "${BLUE}System Status${NC}"
    echo "=================="
    
    local web_pid_file="$PID_DIR/web.pid"
    local monitor_pid_file="$PID_DIR/monitor.pid"
    
    if check_process "$web_pid_file"; then
        echo -e "${GREEN}SUCCESS: Web Interface: Running (PID: $(cat $web_pid_file))${NC}"
        echo "   URL: http://127.0.0.1:$WEB_PORT"
        echo "   Management: http://127.0.0.1:$WEB_PORT/management"
    else
        echo -e "${RED}ERROR: Web Interface: Not running${NC}"
    fi
    
    if check_process "$monitor_pid_file"; then
        echo -e "${GREEN}SUCCESS: Monitoring System: Running (PID: $(cat $monitor_pid_file))${NC}"
    else
        echo -e "${RED}ERROR: Monitoring System: Not running${NC}"
    fi
    
    # Check system health via API
    if check_process "$web_pid_file"; then
        echo ""
        echo -e "${BLUE}System Statistics${NC}"
        echo "===================="
        
        # Get monitoring status
        local monitor_status=$(curl -s "http://127.0.0.1:$WEB_PORT/api/monitoring/status" 2>/dev/null || echo "{}")
        if [ "$monitor_status" != "{}" ]; then
            local total_scheduled=$(echo "$monitor_status" | jq -r '.schedule_summary | to_entries | map(.value.total) | add // 0')
            local running=$(echo "$monitor_status" | jq -r '.running // false')
            echo "   Total Datasets Scheduled: $total_scheduled"
            echo "   Monitoring Active: $running"
        fi
        
        # Get discovery status
        local discovery_status=$(curl -s "http://127.0.0.1:$WEB_PORT/api/discovery/status" 2>/dev/null || echo "{}")
        if [ "$discovery_status" != "{}" ]; then
            local total_datasets=$(echo "$discovery_status" | jq -r '.total_datasets // 0')
            local recent_discoveries=$(echo "$discovery_status" | jq -r '.recent_discoveries // 0')
            echo "   Total Datasets Discovered: $total_datasets"
            echo "   Recent Discoveries (7d): $recent_discoveries"
        fi
    fi
}

# Function to show logs
show_logs() {
    local service="$1"
    local log_file="$LOG_DIR/${service}.log"
    
    if [ -f "$log_file" ]; then
        echo -e "${BLUE}$service Logs (last 50 lines)${NC}"
        echo "=================================="
        tail -n 50 "$log_file"
    else
        echo -e "${RED}ERROR: No log file found for $service${NC}"
    fi
}

# Main script logic
case "${1:-start}" in
    "start")
        echo -e "${BLUE}Starting all services...${NC}"
        
        # Start web interface
        start_process "web" "python src/main.py"
        
        # Wait for web interface to be ready
        echo -e "${BLUE}Waiting for web interface to be ready...${NC}"
        for i in {1..30}; do
            if curl -s "http://127.0.0.1:$WEB_PORT/api/stats" > /dev/null 2>&1; then
                echo -e "${GREEN}SUCCESS: Web interface is ready${NC}"
                break
            fi
            sleep 1
        done
        
        # Initialize monitoring schedule
        echo -e "${BLUE}Initializing monitoring schedule...${NC}"
        curl -s -X POST "http://127.0.0.1:$WEB_PORT/api/monitoring/init" > /dev/null
        
        # Start comprehensive monitoring
        start_process "monitor" "python run_comprehensive_system.py"
        
        echo ""
        show_status
        echo ""
        echo -e "${GREEN}System started successfully!${NC}"
        echo -e "${BLUE}Management Dashboard: http://127.0.0.1:$WEB_PORT/management${NC}"
        echo -e "${BLUE}Main Dashboard: http://127.0.0.1:$WEB_PORT${NC}"
        ;;
        
    "stop")
        echo -e "${YELLOW}Stopping all services...${NC}"
        stop_process "monitor"
        stop_process "web"
        echo -e "${GREEN}SUCCESS: All services stopped${NC}"
        ;;
        
    "restart")
        echo -e "${YELLOW}Restarting all services...${NC}"
        $0 stop
        sleep 2
        $0 start
        ;;
        
    "status")
        show_status
        ;;
        
    "logs")
        show_logs "${2:-web}"
        ;;
        
    "monitor")
        echo -e "${BLUE}Starting monitoring system only...${NC}"
        start_process "monitor" "python run_comprehensive_system.py"
        ;;
        
    "web")
        echo -e "${BLUE}Starting web interface only...${NC}"
        start_process "web" "python src/main.py"
        ;;
        
    "health")
        echo -e "${BLUE}System Health Check${NC}"
        echo "====================="
        
        # Check if services are running
        web_pid_file="$PID_DIR/web.pid"
        monitor_pid_file="$PID_DIR/monitor.pid"
        
        web_running=false
        monitor_running=false
        
        if check_process "$web_pid_file"; then
            web_running=true
        fi
        
        if check_process "$monitor_pid_file"; then
            monitor_running=true
        fi
        
        # Check API endpoints
        if [ "$web_running" = true ]; then
            echo -e "${BLUE}Testing API endpoints...${NC}"
            
            # Test basic API
            if curl -s "http://127.0.0.1:$WEB_PORT/api/stats" > /dev/null 2>&1; then
                echo -e "${GREEN}SUCCESS: Basic API: OK${NC}"
            else
                echo -e "${RED}ERROR: Basic API: FAILED${NC}"
            fi
            
            # Test monitoring API
            if curl -s "http://127.0.0.1:$WEB_PORT/api/monitoring/status" > /dev/null 2>&1; then
                echo -e "${GREEN}SUCCESS: Monitoring API: OK${NC}"
            else
                echo -e "${RED}ERROR: Monitoring API: FAILED${NC}"
            fi
            
            # Test discovery API
            if curl -s "http://127.0.0.1:$WEB_PORT/api/discovery/status" > /dev/null 2>&1; then
                echo -e "${GREEN}SUCCESS: Discovery API: OK${NC}"
            else
                echo -e "${RED}ERROR: Discovery API: FAILED${NC}"
            fi
        fi
        
        # Check database
        echo -e "${BLUE}Checking database...${NC}"
        if [ -f "$PROJECT_DIR/datasets.db" ]; then
            db_size=$(du -h "$PROJECT_DIR/datasets.db" | cut -f1)
            echo -e "${GREEN}SUCCESS: Database exists (Size: $db_size)${NC}"
            
            # Check monitoring schedule
            schedule_count=$(sqlite3 "$PROJECT_DIR/datasets.db" "SELECT COUNT(*) FROM monitoring_schedule;" 2>/dev/null || echo "0")
            echo "   Monitoring Schedule: $schedule_count datasets"
        else
            echo -e "${RED}ERROR: Database not found${NC}"
        fi
        
        # Overall health
        echo ""
        if [ "$web_running" = true ] && [ "$monitor_running" = true ]; then
            echo -e "${GREEN}System Health: EXCELLENT${NC}"
        elif [ "$web_running" = true ]; then
            echo -e "${YELLOW}WARNING: System Health: PARTIAL (Web only)${NC}"
        else
            echo -e "${RED}ERROR: System Health: POOR${NC}"
        fi
        ;;
        
    *)
        echo "Usage: $0 {start|stop|restart|status|logs|monitor|web|health}"
        echo ""
        echo "Commands:"
        echo "  start    - Start all services (web + monitoring)"
        echo "  stop     - Stop all services"
        echo "  restart  - Restart all services"
        echo "  status   - Show system status"
        echo "  logs     - Show logs for a service (web|monitor)"
        echo "  monitor  - Start monitoring system only"
        echo "  web      - Start web interface only"
        echo "  health   - Run system health check"
        echo ""
        echo "Examples:"
        echo "  $0 start"
        echo "  $0 status"
        echo "  $0 logs web"
        echo "  $0 health"
        exit 1
        ;;
esac
