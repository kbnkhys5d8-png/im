# Local wkrpc fork

This directory starts from `github.com/WuKongIM/wkrpc` version
`v0.0.0-20250312122115-5e44de72d2c8`, pinned by the root module.

The local fork adds two narrow server hooks required by WuKongIM's local search
plugin boundary:

- authorize a connection before it enters the UID connection manager;
- remove a connection only when the closing connection is still the registered
  instance, so an old close event cannot evict a replacement.

Keep this fork source-only. Runtime log files are ignored and must not be
committed.
