# Coffee Server

Go server for the Coffee ESP32 project. It currently provides WebSocket relay and status API routes, and can be extended with web and API services.

## Run

```sh
go run .
```

This runs with the production build configuration:

```text
127.0.0.1:18080
```

For hot reload during development:

```sh
air
```

Air builds with the `dev` tag and uses the development port:

```text
127.0.0.1:18081
```

To run the development configuration without Air:

```sh
go run -tags dev .
```

## Config

Runtime config is compiled into the binary from the `config` package.

Production address:

```text
config/addr_prod.go -> 127.0.0.1:18080
```

Development address:

```text
config/addr_dev.go -> 127.0.0.1:18081
```

The WebSocket token is defined in:

```text
config/secret.go
```

This file is ignored by Git. Use `config/secret.go.example` as the template when setting up a new machine.

Example:

```go
package config

const Token = "change-me"
```

## Build

Build the production binary and back up the previous binary:

```sh
./build.sh
```

The build script does not pass the `dev` tag, so it always uses the production port.

The binary is written to:

```text
bin/coffee_server
```

Existing binaries are backed up under:

```text
bin/backups/
```

## Routes

```text
GET /health
GET /api/status
GET /ws
```

WebSocket example:

```text
wss://your-domain/ws?token=change-me&device_id=coffee-001&room=default
```

## Nginx

```nginx
# Production service
location /ws {
    proxy_pass http://127.0.0.1:18080/ws;
    proxy_http_version 1.1;
    proxy_set_header Upgrade $http_upgrade;
    proxy_set_header Connection "upgrade";
    proxy_set_header Host $host;
    proxy_read_timeout 3600s;
}

location /api/ {
    proxy_pass http://127.0.0.1:18080/api/;
}

location /health {
    proxy_pass http://127.0.0.1:18080/health;
}

# Development service
location /dev/ws {
    proxy_pass http://127.0.0.1:18081/ws;
    proxy_http_version 1.1;
    proxy_set_header Upgrade $http_upgrade;
    proxy_set_header Connection "upgrade";
    proxy_set_header Host $host;
    proxy_read_timeout 3600s;
}
```
