package common

import (
	"errors"
	"fmt"
	"os"
	"strings"
)

// DefaultApp is the app of a request without X-App or a message without app: the mobile app, the only one before
// the apps were told apart
const DefaultApp = "production"

// ErrUnknownApp is an app that isn't in APPS, so a header or a message can't point a query at any other keyspace
var ErrUnknownApp = errors.New("unknown app")

// Apps are the apps a service shared by them serves. an app is its cassandra keyspace and the X-App the gateway sets,
// so its tables are "<app>.<table>" on one session without a keyspace of its own
type Apps map[string]bool

// LoadApps reads APPS, like "production,holidayfriend". without it, the keyspace of PROFILE alone, as before
func LoadApps() Apps {
	list := os.Getenv("APPS")
	if list == "" {
		list = os.Getenv("PROFILE")
	}
	apps := make(Apps)
	for app := range strings.SplitSeq(list, ",") {
		if app = strings.TrimSpace(app); app != "" {
			apps[app] = true
		}
	}
	if len(apps) == 0 {
		panic("APPS or PROFILE has to name the keyspaces to serve")
	}
	return apps
}

// AppOrDefault is the app, or DefaultApp when it's left out
func AppOrDefault(app string) string {
	if app == "" {
		return DefaultApp
	}
	return app
}

// Table is the table of the app qualified with its keyspace, like "holidayfriend.profile_by_id"
func (a Apps) Table(app, table string) (string, error) {
	if !a[app] {
		return "", fmt.Errorf("%w: %q", ErrUnknownApp, app)
	}
	return app + "." + table, nil
}
