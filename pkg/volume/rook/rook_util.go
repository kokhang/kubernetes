/*
Copyright 2017 The Kubernetes Authors.

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

//
// utility functions to setup rook volume
// mainly implement diskManager interface
//

package rook

import (
	"fmt"
	"math/rand"
	"os"
	"path"
	"strings"

	"github.com/golang/glog"
	"k8s.io/kubernetes/pkg/api/v1"
	"k8s.io/kubernetes/pkg/util/mount"
	"k8s.io/kubernetes/pkg/volume"
)

const (
	imageWatcherStr = "watcher="
)

// make a directory like /var/lib/kubelet/plugins/kubernetes.io/pod/rbd/pool-image-image
func makePDNameInternal(host volume.VolumeHost, pool string, image string) string {
	return path.Join(host.GetPluginDir(rookPluginName), "rook", pool+"-image-"+image)
}

type RookUtil struct{}

type diskMapper interface {
	MapDisk(disk rookMounter) (string, error)
	UnmapDisk(disk rookUnmounter, mntPath string) error
}

func createDiskMapper(attacherType string) (diskMapper, error) {
	glog.V(1).Infof("rook: creating diskMapper for attacherType %s", attacherType)
	switch strings.ToLower(attacherType) {
	case "krbd":
		return &RookKernel{}, nil
	}
	return nil, fmt.Errorf("unsupported attacherType %s", attacherType)
}

func (util *RookUtil) MakeGlobalPDName(rook rook) string {
	return makePDNameInternal(rook.plugin.host, rook.Pool, rook.Image)
}

func (util *RookUtil) AttachDisk(b rookMounter) error {

	// create mount point
	globalPDPath := b.manager.MakeGlobalPDName(*b.rook)
	notMnt, err := b.mounter.IsLikelyNotMountPoint(globalPDPath)
	// in the first time, the path shouldn't exist and IsLikelyNotMountPoint is expected to get NotExist
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("rook: %s failed to check mountpoint", globalPDPath)
	}
	if !notMnt {
		return nil
	}
	if err = os.MkdirAll(globalPDPath, 0750); err != nil {
		return fmt.Errorf("rook: failed to mkdir %s, error", globalPDPath)
	}

	// map a block device
	mapper, err := createDiskMapper(b.AttacherType)
	if err != nil {
		return fmt.Errorf("rook: cannot create diskMapper: %v", err)
	}
	devicePath, err := mapper.MapDisk(b)
	if err != nil {
		return fmt.Errorf("rook: cannot map block device")
	}

	// mount it
	if err = b.mounter.FormatAndMount(devicePath, globalPDPath, b.fsType, nil); err != nil {
		err = fmt.Errorf("rook: failed to mount rbd volume %s [%s] to %s, error %v", devicePath, b.fsType, globalPDPath, err)
	}
	return err
}

func (util *RookUtil) DetachDisk(c rookUnmounter, mntPath string) error {
	device, cnt, err := mount.GetDeviceNameFromMount(c.mounter, mntPath)
	if err != nil {
		return fmt.Errorf("rook detach disk: failed to get device from mnt: %s\nError: %v", mntPath, err)
	}
	if err = c.mounter.Unmount(mntPath); err != nil {
		return fmt.Errorf("rook detach disk: failed to umount: %s\nError: %v", mntPath, err)
	}
	// if device is no longer used, see if can unmap
	if cnt <= 1 {
		mapper, err := createDiskMapper(c.rookMounter.AttacherType)
		if err != nil {
			return fmt.Errorf("rook: cannot create diskMapper: %v", err)
		}
		if err := mapper.UnmapDisk(c, device); err != nil {
			return fmt.Errorf("rook detach disk: failed to unmap: %s\nError: %v", device, err)
		}

		glog.Infof("rook: successfully unmap device %s", device)
	}
	return nil
}

func (util *RookUtil) CreateImage(p *rookVolumeProvisioner) (r *v1.RookVolumeSource, size int, err error) {
	var output []byte
	capacity := p.options.PVC.Spec.Resources.Requests[v1.ResourceName(v1.ResourceStorage)]
	volSizeBytes := capacity.Value()
	// convert to MB that rbd defaults on
	sz := int(volume.RoundUpSize(volSizeBytes, 1024*1024))
	volSz := fmt.Sprintf("%d", sz)
	// rbd create
	l := len(p.rookMounter.Mon)
	// pick a mon randomly
	start := rand.Int() % l
	// iterate all monitors until create succeeds.
	for i := start; i < start+l; i++ {
		mon := p.Mon[i%l]
		glog.V(4).Infof("rook: create %s size %s using mon %s, pool %s id %s key %s", p.rookMounter.Image, volSz, mon, p.rookMounter.Pool, p.rookMounter.adminId, p.rookMounter.adminSecret)
		output, err = p.rookMounter.plugin.execCommand("rbd",
			[]string{"create", p.rookMounter.Image, "--size", volSz, "--pool", p.rookMounter.Pool, "--id", p.rookMounter.adminId, "-m", mon, "--key=" + p.rookMounter.adminSecret, "--image-format", "1"})
		if err == nil {
			break
		} else {
			glog.Warningf("failed to create rbd image, output %v", string(output))
		}
	}

	if err != nil {
		return nil, 0, fmt.Errorf("failed to create rbd image: %v, command output: %s", err, string(output))
	}

	return &v1.RookVolumeSource{
		Monitors: p.rookMounter.Mon,
		Image:    p.rookMounter.Image,
		Pool:     p.rookMounter.Pool,
	}, sz, nil
}

func (util *RookUtil) DeleteImage(p *rookVolumeDeleter) error {
	var output []byte
	found, err := util.rookStatus(p.rookMounter)
	if err != nil {
		return err
	}
	if found {
		glog.Info("rbd is still being used ", p.rookMounter.Image)
		return fmt.Errorf("rbd %s is still being used", p.rookMounter.Image)
	}
	// rbd rm
	l := len(p.rookMounter.Mon)
	// pick a mon randomly
	start := rand.Int() % l
	// iterate all monitors until rm succeeds.
	for i := start; i < start+l; i++ {
		mon := p.rookMounter.Mon[i%l]
		glog.V(4).Infof("rbd: rm %s using mon %s, pool %s id %s key %s", p.rookMounter.Image, mon, p.rookMounter.Pool, p.rookMounter.adminId, p.rookMounter.adminSecret)
		output, err = p.plugin.execCommand("rbd",
			[]string{"rm", p.rookMounter.Image, "--pool", p.rookMounter.Pool, "--id", p.rookMounter.adminId, "-m", mon, "--key=" + p.rookMounter.adminSecret})
		if err == nil {
			return nil
		} else {
			glog.Errorf("failed to delete rbd image: %v, command output: %s", err, string(output))
		}
	}
	return err
}

// run rbd status command to check if there is watcher on the image
func (util *RookUtil) rookStatus(b *rookMounter) (bool, error) {
	var err error
	var output string
	var cmd []byte

	l := len(b.Mon)
	start := rand.Int() % l
	// iterate all hosts until mount succeeds.
	for i := start; i < start+l; i++ {
		mon := b.Mon[i%l]
		// cmd "rbd status" list the rbd client watch with the following output:
		// Watchers:
		//   watcher=10.16.153.105:0/710245699 client.14163 cookie=1
		glog.V(4).Infof("rbd: status %s using mon %s, pool %s id %s key %s", b.Image, mon, b.Pool, b.adminId, b.adminSecret)
		cmd, err = b.plugin.execCommand("rbd",
			[]string{"status", b.Image, "--pool", b.Pool, "-m", mon, "--id", b.adminId, "--key=" + b.adminSecret})
		output = string(cmd)

		if err != nil {
			// ignore error code, just checkout output for watcher string
			glog.Warningf("failed to execute rbd status on mon %s", mon)
		}

		if strings.Contains(output, imageWatcherStr) {
			glog.V(4).Infof("rbd: watchers on %s: %s", b.Image, output)
			return true, nil
		} else {
			glog.Warningf("rbd: no watchers on %s", b.Image)
			return false, nil
		}
	}
	return false, nil
}
