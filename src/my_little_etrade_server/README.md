# my_little_etrade_server

A gRPC server that proxies the E\*Trade API, allowing clients in any language to access account, portfolio, quote, order, and transaction data.

## Prerequisites

- A valid `config.toml` with E\*Trade API credentials
- The server starts without a valid OAuth session — clients can drive the authentication flow via gRPC (see [Authentication](#authentication) below)

## Running the server

```bash
# Start in the foreground with verbose logging
uv run my_little_etrade_server --loglevel INFO

# Start in the background
uv run my_little_etrade_server --background

# Stop a backgrounded server
kill $(cat my_little_etrade_server.pid)
```

### Options

| Flag | Default | Description |
|------|---------|-------------|
| `--loglevel` | `WARNING` | Console log verbosity (DEBUG, INFO, WARNING, ERROR) |
| `--logfile` | `my_little_etrade_server.log` | Log file path (always logs at DEBUG level) |
| `--background` | off | Run the server in the background |

### Configuration

The server port defaults to `38710` and can be changed in `config.toml`:

```toml
[etrade.proxy]
port = 38710
```

## Querying with grpcurl

The server supports [gRPC reflection](https://github.com/grpc/grpc/blob/master/doc/server-reflection.md), so no proto file is needed.

Install grpcurl: `brew install grpcurl`

### Discover available services and RPCs

```bash
grpcurl -plaintext localhost:38710 list
grpcurl -plaintext localhost:38710 list etrade.ProxyService
grpcurl -plaintext localhost:38710 describe etrade.ProxyService
```

### Authentication

The server exposes three RPCs for driving the OAuth flow remotely:

```bash
# 1. Check if the server has a valid OAuth session
grpcurl -plaintext localhost:38710 etrade.ProxyService/GetAuthenticationStatus

# 2. If not authenticated, get the authorization URL
#    Open the returned URL in a browser to authorize and obtain a verification code
grpcurl -plaintext localhost:38710 etrade.ProxyService/GetAuthorizationUrl

# 3. Complete authorization with the verification code from E*Trade
grpcurl -plaintext -d '{"verification_code": "<CODE>"}' \
  localhost:38710 etrade.ProxyService/CompleteAuthorization
```

### List accounts

```bash
grpcurl -plaintext localhost:38710 etrade.ProxyService/ListAccounts
```

### List positions

```bash
grpcurl -plaintext -d '{"account_id_key": "<KEY>"}' \
  localhost:38710 etrade.ProxyService/ListPositions
```

### List quotes

```bash
grpcurl -plaintext -d '{"symbols": ["TSLA"]}' \
  localhost:38710 etrade.ProxyService/ListQuotes

# With option details
grpcurl -plaintext -d '{"symbols": ["TSLA"], "detail_flag": "OPTIONS"}' \
  localhost:38710 etrade.ProxyService/ListQuotes
```

### List orders

```bash
grpcurl -plaintext \
  -d '{"account_id_key": "<KEY>", "start_date": "01012026", "end_date": "03162026"}' \
  localhost:38710 etrade.ProxyService/ListOrders
```

### Get order details

Use the `details_url` from a `ListOrders` response:

```bash
grpcurl -plaintext -d '{"details_url": "<URL>"}' \
  localhost:38710 etrade.ProxyService/GetOrderDetails
```

### List transactions

```bash
grpcurl -plaintext \
  -d '{"account_id_key": "<KEY>", "start_date": "01012026", "end_date": "03162026"}' \
  localhost:38710 etrade.ProxyService/ListTransactions
```

### Get transaction details

```bash
grpcurl -plaintext \
  -d '{"account_id_key": "<KEY>", "transaction_id": "12345"}' \
  localhost:38710 etrade.ProxyService/GetTransactionDetails
```

### Get option chains

```bash
# Basic: get all option chains for a symbol
grpcurl -plaintext -d '{"symbol": "TSLA"}' \
  localhost:38710 etrade.ProxyService/GetOptionChains

# With optional filters
grpcurl -plaintext \
  -d '{"symbol": "TSLA", "expiry_year": 2026, "expiry_month": 4, "no_of_strikes": 10, "include_weekly": true}' \
  localhost:38710 etrade.ProxyService/GetOptionChains
```

### Get option expire dates

```bash
# Get valid expiry dates for a symbol
grpcurl -plaintext -d '{"symbol": "TSLA"}' \
  localhost:38710 etrade.ProxyService/GetOptionExpireDates

# With expiry type filter
grpcurl -plaintext -d '{"symbol": "TSLA", "expiry_type": "ALL"}' \
  localhost:38710 etrade.ProxyService/GetOptionExpireDates
```

## Generating protobuf code

After modifying the proto file, regenerate the Python bindings:

```bash
uv run bash src/my_little_etrade_server/generate_proto.sh
```

Generated files go into `src/my_little_etrade_server/generated/` and are gitignored.
