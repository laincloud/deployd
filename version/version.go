package version

import (
	"fmt"

	"github.com/coreos/go-semver/semver"
)

var (
	Version    = "2.4.3+git"
	APIVersion = "unknown"

	// Git SHA Value will be set during build
	GitSHA = "Not provided (use ./build instead of go build)"
)

func init() {
	ver, err := semver.NewVersion(Version)
	if err == nil {
		APIVersion = fmt.Sprintf("%d.%d", ver.Major, ver.Minor)
	}
}
