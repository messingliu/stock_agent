# Project Context

## Purpose
Stock Agent is a stock market analysis system that:
- Downloads and stores historical stock price data for US and Chinese (A-share) markets
- Applies technical analysis strategies to identify trading opportunities
- Provides a REST API for querying stocks by price ranges and applying analysis strategies
- Supports multiple trading strategies (Golden Line, Volume Break, Extreme Negative/Positive patterns)
- Processes large datasets efficiently using batch processing to prevent memory issues

## Tech Stack
- **Language**: Python 3.x
- **Web Framework**: Flask 2.3.3 with Flask-CORS for API endpoints
- **Database**: PostgreSQL with SQLAlchemy 2.0.27 as ORM
- **Data Processing**: pandas 2.2.0, numpy for stock data analysis
- **Data Sources**: 
  - yfinance 0.2.66 for US stock data
  - akshare 1.16.72 for Chinese stock data
  - tushare API for additional Chinese market data
- **Async/HTTP**: aiohttp 3.11.13, asyncio for concurrent downloads
- **Production Server**: Gunicorn 21.2.0 with WSGI
- **Security**: cryptography 41.0.3 for SSL certificate generation
- **Configuration**: YAML-based config with python-dotenv for environment variables

## Project Conventions

### Code Style
- **Language**: Python with type hints where appropriate (`List[str]`, `Dict[str, Any]`, etc.)
- **Naming**: 
  - Classes: PascalCase (e.g., `StockAnalyzer`, `TaskManager`)
  - Functions/methods: snake_case (e.g., `get_stock_data`, `apply_strategies`)
  - Variables: snake_case
  - Constants: UPPER_SNAKE_CASE
- **Docstrings**: Use triple-quoted strings for class and function documentation (中文/Chinese comments)
- **Imports**: Group imports: standard library, third-party, local imports
- **Error Handling**: Use try-except blocks with descriptive error messages
- **File Organization**: 
  - Main service logic in root directory
  - Strategies in `strategies/` subdirectory with base class pattern
  - Utilities in `utils/` subdirectory
  - Configuration in `config.py` and `config.yaml`

### Architecture Patterns
- **Strategy Pattern**: Trading strategies inherit from `Strategy` base class in `strategies/base.py`
- **Singleton Pattern**: Config class uses singleton pattern for global configuration access
- **Service Layer**: `StockService` and `StockAnalyzer` classes separate business logic from API endpoints
- **Batch Processing**: Analysis functions process stocks in configurable batches to manage memory
- **Task Management**: `TaskManager` handles asynchronous download tasks with status tracking
- **Database Schema**: 
  - Separate tables for US (`us_stock_prices`, `us_stocks_info`) and CN (`cn_stock_prices`, `cn_stocks_info`) markets
  - `task_status` table tracks download task progress
- **Configuration Management**: YAML-based config with singleton Config class, supports default values and dynamic processing
- **API Design**: RESTful endpoints under `/api/` prefix, JSON responses, CORS enabled

### Testing Strategy
- **Current State**: No formal test suite observed in codebase
- **Recommendation**: Add unit tests for strategies, integration tests for API endpoints
- **Test Structure**: Should mirror project structure (e.g., `tests/strategies/`, `tests/api/`)

### Git Workflow
- **Branching**: Main branch for production code
- **Commits**: Descriptive commit messages preferred
- **OpenSpec Integration**: Use OpenSpec for change proposals and specifications

## Domain Context

### Stock Markets
- **US Market**: US stock exchanges (NYSE, NASDAQ), symbols like AAPL, TSLA
- **CN Market**: Chinese A-share market, symbols typically 6-digit codes (e.g., 000001, 600000)
- **Market Data**: Historical OHLCV (Open, High, Low, Close, Volume) data with moving averages (MA5, MA10, MA20, MA60, MA200)

### Trading Strategies
- **Golden Line Strategies**: 
  - `GoldenLineDoubleGreenWin`: Identifies bullish patterns with consecutive green candles
  - `GoldenLineDoubleGreenWinWithConfirmation`: Adds confirmation signals
- **Volume Break Strategy**: `HighVolumeBreakStrategy`: Detects unusual volume spikes
- **Extreme Patterns**: `ExtremeNegativePositiveStrategy`: Identifies extreme price movements (requires 60 days of data)

### Data Processing
- **Batch Processing**: Stocks processed in batches (configurable via `analysis_batch.max_symbols_per_batch`, default: 500)
- **Data Requirements**: Strategies require minimum 3 days of data, some require up to 60 days
- **Analysis Flow**: 
  1. Load stock symbols from database
  2. Fetch historical price data for date range
  3. Apply each strategy to stock data
  4. Return matching stocks with metadata (symbol, name, date, price, volume, MA60)

### API Endpoints
- `/api/stocks/price`: Query stocks by price range
- `/api/strategies`: List available strategies
- `/api/stocks/strategies`: Apply strategies to find matching stocks
- `/api/tasks/download`: Start stock data download task
- `/api/tasks/status`: Check download task status

## Important Constraints

### Rate Limits
- **Yahoo Finance**: 1 request per second
- **akshare**: 5 requests per second
- **tushare**: 2 requests per second
- Rate limits enforced in download utilities to prevent API bans

### Memory Management
- **Batch Processing**: Analysis functions use batch processing to prevent OOM errors
- **Batch Size**: Configurable per market (US: 100, CN: 20 for downloads; 500 for analysis)
- **Data Cleanup**: Explicit memory management in batch processing loops

### Data Constraints
- **Date Range**: Default start date 2020-01-01, end date is current date
- **Minimum Data**: Strategies require minimum 3 days of historical data
- **Retry Logic**: Download failures retry up to 3 times with exponential backoff (base delay: 1 second)

### Security
- **SSL/TLS**: Production server uses SSL with self-signed certificates
- **CORS**: Enabled for all origins (`*`) - consider restricting in production
- **Database Credentials**: Stored in `config.yaml` - should use environment variables in production

### Performance
- **Concurrent Downloads**: Uses asyncio for parallel data fetching
- **Database Connections**: SQLAlchemy connection pooling
- **Production Server**: Gunicorn with WSGI for production deployment

## External Dependencies

### Data Source APIs
- **Yahoo Finance (yfinance)**: 
  - Primary source for US stock data
  - Rate limit: 1 req/sec
  - Provides OHLCV data and historical prices
- **akshare**: 
  - Primary source for Chinese A-share market data
  - Rate limit: 5 req/sec
  - Provides comprehensive Chinese market data
- **tushare API**: 
  - Secondary source for Chinese market data
  - Requires API token (configured in `config.yaml`)
  - Rate limit: 2 req/sec

### Infrastructure
- **PostgreSQL Database**: 
  - Stores stock prices, stock info, and task status
  - Connection via SQLAlchemy
  - Tables: `{market}_stock_prices`, `{market}_stocks_info`, `task_status`
- **Production Deployment**: 
  - Gunicorn WSGI server
  - SSL/TLS support with auto-generated certificates
  - Systemd service file (`stock-service.service`) for Linux deployment

### Configuration Files
- **config.yaml**: Main configuration file (database, API tokens, rate limits, batch sizes)
- **.env**: Environment variables (if used, via python-dotenv)
- **requirements.txt**: Python package dependencies with pinned versions
