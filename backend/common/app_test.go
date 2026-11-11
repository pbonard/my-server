package common

import (
	"errors"
	"testing"
)

func TestLoadApps(t *testing.T) {
	t.Run("from APPS", func(t *testing.T) {
		t.Setenv("APPS", "production, holidayfriend,")
		apps := LoadApps()
		if len(apps) != 2 || !apps["production"] || !apps["holidayfriend"] {
			t.Fatalf("apps = %v", apps)
		}
	})
	t.Run("PROFILE alone without APPS", func(t *testing.T) {
		t.Setenv("APPS", "")
		t.Setenv("PROFILE", "production")
		apps := LoadApps()
		if len(apps) != 1 || !apps["production"] {
			t.Fatalf("apps = %v", apps)
		}
	})
}

func TestTable(t *testing.T) {
	apps := Apps{"production": true, "holidayfriend": true}

	table, err := apps.Table("holidayfriend", "profile_by_id")
	if err != nil || table != "holidayfriend.profile_by_id" {
		t.Fatalf("table = %q, err = %v", table, err)
	}
	// a keyspace not served, or anything else a header could carry, never reaches a query
	for _, app := range []string{"other", "", "production.profile_by_id; DROP"} {
		if _, err := apps.Table(app, "profile_by_id"); !errors.Is(err, ErrUnknownApp) {
			t.Fatalf("app %q: err = %v", app, err)
		}
	}
}

func TestAppOrDefault(t *testing.T) {
	if AppOrDefault("") != DefaultApp || AppOrDefault("holidayfriend") != "holidayfriend" {
		t.Fatal("AppOrDefault")
	}
}
