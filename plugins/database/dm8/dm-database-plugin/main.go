// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: BUSL-1.1

package main

import (
	"log"
	"os"

	plugdm "github.com/hashicorp/vault/plugins/database/dm8"
	"github.com/hashicorp/vault/sdk/database/dbplugin/v5"
)

func main() {
	err := Run()
	if err != nil {
		log.Println(err)
		os.Exit(1)
	}
}

// Run instantiates a MySQL object, and runs the RPC server for the plugin
func Run() error {
	var f func() (interface{}, error)
	f = plugdm.New(plugdm.DefaultUserNameTemplate)

	dbplugin.ServeMultiplex(f)

	return nil
}
