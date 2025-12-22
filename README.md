# Stats Server

A simple Rust HTTP server that logs all incoming requests including URLs, headers, and request bodies.

## Features

- 🚀 Logs all incoming HTTP requests
- 📝 Captures request method, URL, query parameters
- 📋 Logs all request headers
- 📦 Logs request body (with JSON pretty-printing)
- 🔢 Request counter
- ✅ Health check endpoint

## Running the Server

```bash
cargo run
```

The server will start on `http://0.0.0.0:3000`

## Testing the Server

### Basic GET request:
```bash
curl http://localhost:3000/
```

### GET with query parameters:
```bash
curl http://localhost:3000/api/users?id=123&name=john
```

### POST with JSON body:
```bash
curl -X POST http://localhost:3000/api/users \
  -H "Content-Type: application/json" \
  -d '{"name": "John Doe", "email": "john@example.com", "age": 30}'
```

### PUT request:
```bash
curl -X PUT http://localhost:3000/api/users/123 \
  -H "Content-Type: application/json" \
  -d '{"name": "Jane Doe"}'
```

### DELETE request:
```bash
curl -X DELETE http://localhost:3000/api/users/123
```

### POST with form data:
```bash
curl -X POST http://localhost:3000/form \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "username=john&password=secret"
```

### Health check:
```bash
curl http://localhost:3000/health
```

## Example Output

When you send a request, you'll see detailed logs like:

```
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
📨 Request #1
🔹 Method: POST
🔹 URL: http://localhost:3000/api/users
🔹 Path: /api/users
🔹 Content-Type: application/json
🔹 Header: host: localhost:3000
🔹 Header: user-agent: curl/7.81.0
🔹 Header: accept: */*
🔹 Header: content-type: application/json
🔹 Header: content-length: 58
🎯 Catch-all handler triggered
📦 Body length: 58 bytes
📄 Body content:
{"name": "John Doe", "email": "john@example.com", "age": 30}
📋 Body as JSON:
{
  "name": "John Doe",
  "email": "john@example.com",
  "age": 30
}
✅ Response Status: 200 OK
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
```

## Dependencies

- **axum**: Web framework
- **tokio**: Async runtime
- **tracing**: Logging
- **serde_json**: JSON parsing for pretty-printing
