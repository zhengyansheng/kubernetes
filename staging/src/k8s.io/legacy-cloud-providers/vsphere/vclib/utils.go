/*
Copyright 2016 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package vclib

import (
	"fmt"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"

	"github.com/vmware/govmomi/find"
	"github.com/vmware/govmomi/object"
	"github.com/vmware/govmomi/vim25/soap"
	"github.com/vmware/govmomi/vim25/types"
	"k8s.io/klog/v2"
)

// IsNotFound return true if err is NotFoundError or DefaultNotFoundError
func IsNotFound(err error) bool {
	_, ok := err.(*find.NotFoundError)
	if ok {
		return true
	}

	_, ok = err.(*find.DefaultNotFoundError)
	if ok {
		return true
	}

	return false
}

func getFinder(dc *Datacenter) *find.Finder {
	finder := find.NewFinder(dc.Client(), false)
	finder.SetDatacenter(dc.Datacenter)
	return finder
}

// formatVirtualDiskUUID removes any spaces and hyphens in UUID
// Example UUID input is 42375390-71f9-43a3-a770-56803bcd7baa and output after format is 4237539071f943a3a77056803bcd7baa
func formatVirtualDiskUUID(uuid string) string {
	uuidwithNoSpace := strings.Replace(uuid, " ", "", -1)
	uuidWithNoHypens := strings.Replace(uuidwithNoSpace, "-", "", -1)
	return strings.ToLower(uuidWithNoHypens)
}

// getSCSIControllersOfType filters specific type of Controller device from given list of Virtual Machine Devices
func getSCSIControllersOfType(vmDevices object.VirtualDeviceList, scsiType string) []*types.VirtualController {
	// get virtual scsi controllers of passed argument type
	var scsiControllers []*types.VirtualController
	for _, device := range vmDevices {
		devType := vmDevices.Type(device)
		if devType == scsiType {
			if c, ok := device.(types.BaseVirtualController); ok {
				scsiControllers = append(scsiControllers, c.GetVirtualController())
			}
		}
	}
	return scsiControllers
}

// getAvailableSCSIController gets available SCSI Controller from list of given controllers, which has less than 15 disk devices.
func getAvailableSCSIController(scsiControllers []*types.VirtualController) *types.VirtualController {
	// get SCSI controller which has space for adding more devices
	for _, controller := range scsiControllers {
		if len(controller.Device) < SCSIControllerDeviceLimit {
			return controller
		}
	}
	return nil
}

// getNextUnitNumber gets the next available SCSI controller unit number from given list of Controller Device List
func getNextUnitNumber(devices object.VirtualDeviceList, c types.BaseVirtualController) (int32, error) {
	var takenUnitNumbers [SCSIDeviceSlots]bool
	takenUnitNumbers[SCSIReservedSlot] = true
	key := c.GetVirtualController().Key

	for _, device := range devices {
		d := device.GetVirtualDevice()
		if d.ControllerKey == key {
			if d.UnitNumber != nil && *d.UnitNumber < SCSIDeviceSlots {
				takenUnitNumbers[*d.UnitNumber] = true
			}
		}
	}
	for unitNumber, takenUnitNumber := range takenUnitNumbers {
		if !takenUnitNumber {
			return int32(unitNumber), nil
		}
	}
	return -1, fmt.Errorf("SCSI Controller with key=%d does not have any available slots", key)
}

// getSCSIControllers filters and return list of Controller Devices from given list of Virtual Machine Devices.
func getSCSIControllers(vmDevices object.VirtualDeviceList) []*types.VirtualController {
	// get all virtual scsi controllers
	var scsiControllers []*types.VirtualController
	for _, device := range vmDevices {
		devType := vmDevices.Type(device)
		switch devType {
		case SCSIControllerType, strings.ToLower(LSILogicControllerType), strings.ToLower(BusLogicControllerType), PVSCSIControllerType, strings.ToLower(LSILogicSASControllerType):
			if c, ok := device.(types.BaseVirtualController); ok {
				scsiControllers = append(scsiControllers, c.GetVirtualController())
			}
		}
	}
	return scsiControllers
}

// RemoveStorageClusterORFolderNameFromVDiskPath removes the cluster or folder path from the vDiskPath
// for vDiskPath [DatastoreCluster/sharedVmfs-0] kubevols/e2e-vmdk-1234.vmdk, return value is [sharedVmfs-0] kubevols/e2e-vmdk-1234.vmdk
// for vDiskPath [sharedVmfs-0] kubevols/e2e-vmdk-1234.vmdk, return value remains same [sharedVmfs-0] kubevols/e2e-vmdk-1234.vmdk
func RemoveStorageClusterORFolderNameFromVDiskPath(vDiskPath string) string {
	datastore := regexp.MustCompile("\\[(.*?)\\]").FindStringSubmatch(vDiskPath)[1]
	if filepath.Base(datastore) != datastore {
		vDiskPath = strings.Replace(vDiskPath, datastore, filepath.Base(datastore), 1)
	}
	return vDiskPath
}

// GetPathFromVMDiskPath retrieves the path from VM Disk Path.
// Example: For vmDiskPath - [vsanDatastore] kubevols/volume.vmdk, the path is kubevols/volume.vmdk
func GetPathFromVMDiskPath(vmDiskPath string) string {
	datastorePathObj := new(object.DatastorePath)
	isSuccess := datastorePathObj.FromString(vmDiskPath)
	if !isSuccess {
		klog.Errorf("Failed to parse vmDiskPath: %s", vmDiskPath)
		return ""
	}
	return datastorePathObj.Path
}

// GetDatastorePathObjFromVMDiskPath gets the datastorePathObj from VM disk path.
func GetDatastorePathObjFromVMDiskPath(vmDiskPath string) (*object.DatastorePath, error) {
	datastorePathObj := new(object.DatastorePath)
	isSuccess := datastorePathObj.FromString(vmDiskPath)
	if !isSuccess {
		klog.Errorf("Failed to parse volPath: %s", vmDiskPath)
		return nil, fmt.Errorf("failed to parse volPath: %s", vmDiskPath)
	}
	return datastorePathObj, nil
}

// IsValidUUID checks if the string is a valid UUID.
func IsValidUUID(uuid string) bool {
	r := regexp.MustCompile("^[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[[a-fA-F0-9]{4}-[a-fA-F0-9]{12}$")
	return r.MatchString(uuid)
}

// IsManagedObjectNotFoundError returns true if error is of type ManagedObjectNotFound
func IsManagedObjectNotFoundError(err error) bool {
	isManagedObjectNotFoundError := false
	if soap.IsSoapFault(err) {
		_, isManagedObjectNotFoundError = soap.ToSoapFault(err).VimFault().(types.ManagedObjectNotFound)
	}
	return isManagedObjectNotFoundError
}

// IsInvalidCredentialsError returns true if error is of type InvalidLogin
func IsInvalidCredentialsError(err error) bool {
	isInvalidCredentialsError := false
	if soap.IsSoapFault(err) {
		_, isInvalidCredentialsError = soap.ToSoapFault(err).VimFault().(types.InvalidLogin)
	}
	return isInvalidCredentialsError
}

// VerifyVolumePathsForVMDevices verifies if the volume paths (volPaths) are attached to VM.
func VerifyVolumePathsForVMDevices(vmDevices object.VirtualDeviceList, volPaths []string, nodeName string, nodeVolumeMap map[string]map[string]bool) {
	volPathsMap := make(map[string]bool)
	for _, volPath := range volPaths {
		volPathsMap[volPath] = true
	}
	// Verify if the volume paths are present on the VM backing virtual disk devices
	for _, device := range vmDevices {
		if vmDevices.TypeName(device) == "VirtualDisk" {
			virtualDevice := device.GetVirtualDevice()
			if backing, ok := virtualDevice.Backing.(*types.VirtualDiskFlatVer2BackingInfo); ok {
				if volPathsMap[backing.FileName] {
					setNodeVolumeMap(nodeVolumeMap, backing.FileName, nodeName, true)
				}
			}
		}
	}

}

// isvCenterNotSupported takes vCenter version and vCenter API version as input and return true if vCenter is no longer
// supported by VMware for in-tree vSphere volume plugin
func isvCenterNotSupported(vCenterVersion string, vCenterAPIVersion string) (bool, error) {
	var vcversion, vcapiversion, minvcversion vcVersion
	var err error
	err = vcversion.parse(vCenterVersion)
	if err != nil {
		return false, fmt.Errorf("failed to parse vCenter version: %s. err: %+v", vCenterVersion, err)
	}
	err = vcapiversion.parse(vCenterAPIVersion)
	if err != nil {
		return false, fmt.Errorf("failed to parse vCenter API version: %s. err: %+v", vCenterAPIVersion, err)
	}
	err = minvcversion.parse(MinvCenterVersion)
	if err != nil {
		return false, fmt.Errorf("failed to parse minimum vCenter version: %s. err: %+v", MinvCenterVersion, err)
	}
	if vcversion.isLessThan(minvcversion) && vcapiversion.isLessThan(minvcversion) {
		return true, nil
	}
	return false, nil
}

// vcVersion represents a VC version
type vcVersion struct {
	Major    int64
	Minor    int64
	Revision int64
	Build    int64
}

// parse helps parse version string to VCVersion
// returns error when parse fail
func (v *vcVersion) parse(version string) error {
	for index, value := range strings.Split(version, ".") {
		var err error
		if index == 0 {
			v.Major, err = strconv.ParseInt(value, 10, 64)
		} else if index == 1 {
			v.Minor, err = strconv.ParseInt(value, 10, 64)
		} else if index == 2 {
			v.Revision, err = strconv.ParseInt(value, 10, 64)
		} else if index == 3 {
			v.Build, err = strconv.ParseInt(value, 10, 64)
		}
		if err != nil {
			return fmt.Errorf("failed to parse version: %q, err: %v", version, err)
		}
	}
	return nil
}

// isLessThan compares VCVersion v to o and returns
// true if v is less than o
func (v *vcVersion) isLessThan(o vcVersion) bool {
	if v.Major != o.Major {
		if v.Major > o.Major {
			return false
		}
		return true
	}
	if v.Minor != o.Minor {
		if v.Minor > o.Minor {
			return false
		}
		return true
	}
	if v.Revision != o.Revision {
		if v.Revision > o.Revision {
			return false
		}
		return true
	}
	if v.Build != o.Build {
		if v.Build > o.Build {
			return false
		}
		return true
	}
	return false
}
