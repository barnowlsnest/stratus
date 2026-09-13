Security audit: stratus + Docker + go-wal

Overall: no hardcoded secrets, no injection or path-traversal vectors, and go-wal's on-disk handling is solid (os.Root confinement, CRC-checked  
headers and records, 0600 files, atomic rewrite via temp+rename). The real exposure is at the network boundary and in the Docker deployment.

Findings

1. Unauthenticated, unencrypted gRPC with a destructive Delete RPC. cmd/app/stratus.go:76

- Severity: High (when reachable beyond localhost)
- The server is grpc.NewServer() with no credentials, no interceptors, and no authn/authz. Anyone who can reach the port can call Delete         
  (truncates and rewrites WAL segments), Add, and ReconcileCache.
- The default host is 127.0.0.1, but compose.yaml sets HOST=0.0.0.0 and publishes 8000:8000 on every host interface, so the compose deployment is
  exposed to the LAN by default.
- Fix: add TLS (grpc.Creds) and at minimum a shared-token or mTLS interceptor gating Delete/Add. In compose, bind to 127.0.0.1:8000:8000 unless  
  remote access is intended.

2. Compose likely pulls an untrusted image from Docker Hub. compose.yaml:3, 
