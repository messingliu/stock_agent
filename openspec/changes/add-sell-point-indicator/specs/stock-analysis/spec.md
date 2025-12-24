## ADDED Requirements

### Requirement: Sell Point Indicators
The system SHALL provide sell point indicators to identify when stocks should be sold based on technical analysis patterns.

#### Scenario: Detect sell signal when price breaks below MA60
- **WHEN** a stock's current price closes below its MA60 moving average
- **AND** the stock was previously above MA60
- **THEN** the system SHALL identify this as a sell point indicator

#### Scenario: List available sell indicators via existing strategies API
- **WHEN** a client requests available strategies via `/api/strategies`
- **THEN** the system SHALL return sell point indicators along with buy strategies in the strategies list, with their names and descriptions

#### Scenario: Apply sell indicators via existing strategies API
- **WHEN** a client requests stocks matching strategies via `/api/stocks/strategies`
- **AND** specifies a market (us or cn) and a sell indicator name (e.g., "MA60BreakDown")
- **THEN** the system SHALL return stocks that match the sell point criteria with stock information (symbol, name, date, price, volume, MA60)

### Requirement: MA60 Break Down Sell Indicator
The system SHALL provide a sell point indicator that triggers when a stock's closing price breaks below the MA60 moving average after being above it.

#### Scenario: MA60 break down detection
- **WHEN** analyzing stock data with at least 60 days of history
- **AND** the previous day's close was above MA60
- **AND** the current day's close is below MA60
- **THEN** the indicator SHALL return true indicating a sell signal

#### Scenario: MA60 break down with volume confirmation
- **WHEN** the MA60 break down occurs
- **AND** the current day's volume is greater than the average volume of the last 20 days
- **THEN** the sell signal SHALL be considered stronger

