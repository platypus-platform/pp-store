package pp

import (
	"errors"
	"github.com/platypus-platform/pp-kv-consul"
	. "github.com/platypus-platform/pp-logging"
	"path"
)

// The intended state for an application on a node.
type IntentApp struct {
	Name     string
	Versions map[string]string
	Basedir  string
}

// The intended state for a particular node.
type IntentNode struct {
	Apps map[string]IntentApp
}

// An app may have many versions ready to go, but only zero or one will be
// active.
func (app *IntentApp) ActiveVersion() string {
	for id, status := range app.Versions {
		if status == "active" {
			return id
		}
	}
	return ""
}

// Fetches data from the intent store and emits it on the channel.
// It skips any malformed data. The only error condition is if the store is not
// available in the first place.
func PollIntent(hostname string, callback func(IntentNode)) error {
	Info("Polling intent store")

	kv, _ := ppkv.NewClient()
	apps, err := kv.List(path.Join("nodes", hostname))
	if err != nil {
		return err
	}

	intent := IntentNode{
		Apps: map[string]IntentApp{},
	}

	for appName, data := range apps {
		Info("Checking spec for %s", appName)

		appData, worked := stringMap(data)
		if !worked {
			Fatal("Invalid node data for %s", appName)
			continue
		}

		cluster := appData["cluster"]
		if cluster == "" {
			Fatal("No cluster key in node data for %s", appName)
			continue
		}

		clusterKey := path.Join("clusters", appName, cluster, "versions")
		configKey := path.Join("clusters", appName, cluster, "deploy_config")

		versions, err := getMap(kv, clusterKey)
		if err != nil {
			Fatal("No or invalid data for %s: %s", clusterKey, err)
			continue
		}

		deployConfig, err := getMap(kv, configKey)
		if err != nil {
			Fatal("No or invalid data for %s: %s", configKey, err)
			continue
		}

		basedir := deployConfig["basedir"]
		if !path.IsAbs(basedir) {
			Fatal("Not allowing relative basedir in %s", configKey)
			continue
		}

		intent.Apps[appName] = IntentApp{
			Name:     appName,
			Basedir:  basedir,
			Versions: versions,
		}
	}

	callback(intent)

	return nil
}

func getMap(kv *ppkv.Client, query string) (map[string]string, error) {
	raw, err := kv.Get(query)

	if err != nil {
		return nil, err
	}

	mapped, worked := stringMap(raw)
	if !worked {
		return nil, errors.New("Not a string map")
	}

	return mapped, nil
}

func stringMap(raw interface{}) (map[string]string, bool) {
	mapped, worked := raw.(map[string]interface{})
	if !worked {
		return nil, false
	}
	ret := map[string]string{}
	for k, v := range mapped {
		str, worked := v.(string)
		if !worked {
			return nil, false
		}
		ret[k] = str
	}
	return ret, true
}
