// Package version holds the build version, stamped at release time via
// -ldflags "-X github.com/ncode/chronicle/internal/version.Version=...".
package version

// Version is the build version; "dev" for unstamped local builds.
var Version = "dev"
