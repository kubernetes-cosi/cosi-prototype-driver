package controller

import (
	"github.com/yard-turkey/cosi-prototype-driver/pkg/controller/pluginregistration"
)

func init() {
	// AddToManagerFuncs is a list of functions to create controllers and add them to a manager.
	AddToManagerFuncs = append(AddToManagerFuncs, pluginregistration.Add)
}
