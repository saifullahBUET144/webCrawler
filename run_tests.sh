set -e  # Exit on error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' 

print_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check if MongoDB is running
check_mongodb() {
    print_info "Checking MongoDB connection..."
    
    if [ -f .env ]; then
        export $(grep -v '^#' .env | xargs)
    fi
    
    # Check connection with MONGODB_URI from .env
    if python -c "
import os
import sys
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo.errors import ServerSelectionTimeoutError
import asyncio

async def check_connection():
    try:
        uri = os.getenv('MONGODB_URI', 'mongodb://localhost:27017/')
        client = AsyncIOMotorClient(uri, serverSelectionTimeoutMS=5000)
        await client.admin.command('ping')
        client.close()
        return True
    except Exception as e:
        print(f'Connection error: {e}', file=sys.stderr)
        return False

result = asyncio.run(check_connection())
sys.exit(0 if result else 1)
" 2>/dev/null; then
        print_success "MongoDB is connected (using URI from .env)"
        return 0
    else
        print_error "MongoDB is not accessible"
        print_warning "Check your MONGODB_URI in .env file"
        print_info "Current URI: ${MONGODB_URI:-Not set}"
        return 1
    fi
}

# Function to check dependencies
check_dependencies() {
    print_info "Checking dependencies..."
    if python -c "import pytest" 2>/dev/null; then
        print_success "Dependencies are installed"
        return 0
    else
        print_error "pytest not found"
        print_info "Installing dependencies..."
        pip install -r requirements.txt
        return $?
    fi
}

# Run all tests
run_all_tests() {
    print_info "Running all tests..."
    pytest -v
}

# Run tests with coverage
run_tests_with_coverage() {
    print_info "Running tests with coverage..."
    pytest \
        --cov=crawler \
        --cov=api \
        --cov=scheduler \
        --cov=utils \
        --cov-report=html \
        --cov-report=term-missing \
        -v
    
    print_success "Coverage report generated in htmlcov/index.html"
}

# Run specific test file
run_specific_test() {
    local test_file=$1
    print_info "Running tests in $test_file..."
    pytest tests/$test_file -v
}

# Run tests by marker
run_tests_by_marker() {
    local marker=$1
    print_info "Running tests marked as '$marker'..."
    pytest -m $marker -v
}

# Run quick tests (no integration)
run_quick_tests() {
    print_info "Running quick tests (excluding slow tests)..."
    pytest -v -m "not slow"
}

# Clean test artifacts
clean_test_artifacts() {
    print_info "Cleaning test artifacts..."
    rm -rf .pytest_cache
    rm -rf htmlcov
    rm -rf .coverage
    rm -f app.log
    print_success "Test artifacts cleaned"
}

# Main menu
show_menu() {
    echo ""
    echo "Web Crawler Test Suite"
    echo "1. Run all tests"
    echo "2. Run tests with coverage"
    echo "3. Run crawler tests only"
    echo "4. Run API tests only"
    echo "5. Run scheduler tests only"
    echo "6. Run quick tests (no slow tests)"
    echo "7. Clean test artifacts"
    echo "8. Check dependencies"
    echo "9. Exit"
    echo -n "Select an option [1-9]: "
}

# Parse command line arguments
if [ $# -eq 0 ]; then
    # Interactive mode
    check_mongodb || exit 1
    check_dependencies || exit 1
    
    while true; do
        show_menu
        read choice
        
        case $choice in
            1)
                run_all_tests
                ;;
            2)
                run_tests_with_coverage
                ;;
            3)
                run_specific_test "test_crawler.py"
                ;;
            4)
                run_specific_test "test_api.py"
                ;;
            5)
                run_specific_test "test_scheduler.py"
                ;;
            6)
                run_quick_tests
                ;;
            7)
                clean_test_artifacts
                ;;
            8)
                check_dependencies
                ;;
            9)
                print_info "Exiting..."
                exit 0
                ;;
            *)
                print_error "Invalid option. Please select 1-9."
                ;;
        esac
        
        echo ""
        echo -n "Press Enter to continue..."
        read
    done
else
    # Command line mode
    case "$1" in
        --all|-a)
            check_mongodb && check_dependencies && run_all_tests
            ;;
        --coverage|-c)
            check_mongodb && check_dependencies && run_tests_with_coverage
            ;;
        --crawler)
            check_mongodb && check_dependencies && run_specific_test "test_crawler.py"
            ;;
        --api)
            check_mongodb && check_dependencies && run_specific_test "test_api.py"
            ;;
        --scheduler)
            check_mongodb && check_dependencies && run_specific_test "test_scheduler.py"
            ;;
        --quick|-q)
            check_mongodb && check_dependencies && run_quick_tests
            ;;
        --clean)
            clean_test_artifacts
            ;;
        --help|-h)
            echo "Usage: ./run_tests.sh [OPTION]"
            echo ""
            echo "Options:"
            echo "  -a, --all         Run all tests"
            echo "  -c, --coverage    Run tests with coverage"
            echo "  --crawler         Run crawler tests only"
            echo "  --api             Run API tests only"
            echo "  --scheduler       Run scheduler tests only"
            echo "  -q, --quick       Run quick tests (no slow tests)"
            echo "  --clean           Clean test artifacts"
            echo "  -h, --help        Show this help message"
            echo ""
            echo "If no option is provided, interactive mode is used."
            ;;
        *)
            print_error "Unknown option: $1"
            echo "Use --help for usage information"
            exit 1
            ;;
    esac
fi