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

package rook

import (
	"fmt"
	dstrings "strings"

	"github.com/golang/glog"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/uuid"
	"k8s.io/kubernetes/pkg/api/v1"
	"k8s.io/kubernetes/pkg/client/clientset_generated/clientset"
	"k8s.io/kubernetes/pkg/util/exec"
	"k8s.io/kubernetes/pkg/util/mount"
	"k8s.io/kubernetes/pkg/util/strings"
	"k8s.io/kubernetes/pkg/volume"
	volutil "k8s.io/kubernetes/pkg/volume/util"
)

// This is the primary entrypoint for volume plugins.
func ProbeVolumePlugins() []volume.VolumePlugin {
	return []volume.VolumePlugin{&rookPlugin{nil, exec.New()}}
}

type rookPlugin struct {
	host volume.VolumeHost
	exe  exec.Interface
}

var _ volume.VolumePlugin = &rookPlugin{}
var _ volume.PersistentVolumePlugin = &rookPlugin{}
var _ volume.DeletableVolumePlugin = &rookPlugin{}
var _ volume.ProvisionableVolumePlugin = &rookPlugin{}

const (
	rookPluginName  = "kubernetes.io/rook"
	secretKeyName   = "key" // key name used in secret
	monConfigMap    = "mon-config"
	monConfigMapKey = "endpoints"
)

var attacherTypes = [3]string{"krbd", "iscsi", "nbd"}

func (plugin *rookPlugin) Init(host volume.VolumeHost) error {
	plugin.host = host
	return nil
}

func (plugin *rookPlugin) GetPluginName() string {
	return rookPluginName
}

func (plugin *rookPlugin) GetVolumeName(spec *volume.Spec) (string, error) {
	volumeSource, _, err := getVolumeSource(spec)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf(
		"%v:%v",
		volumeSource.Monitors,
		volumeSource.Image), nil
}

func (plugin *rookPlugin) CanSupport(spec *volume.Spec) bool {
	if (spec.Volume != nil && spec.Volume.Rook == nil) || (spec.PersistentVolume != nil && spec.PersistentVolume.Spec.Rook == nil) {
		return false
	}

	return true
}

func (plugin *rookPlugin) RequiresRemount() bool {
	return false
}

func (plugin *rookPlugin) SupportsMountOption() bool {
	return true
}

func (plugin *rookPlugin) SupportsBulkVolumeVerification() bool {
	return false
}

func (plugin *rookPlugin) GetAccessModes() []v1.PersistentVolumeAccessMode {
	return []v1.PersistentVolumeAccessMode{
		v1.ReadWriteOnce,
		v1.ReadOnlyMany,
	}
}

func (plugin *rookPlugin) NewMounter(spec *volume.Spec, pod *v1.Pod, _ volume.VolumeOptions) (volume.Mounter, error) {
	var secret string
	var err error
	source, _ := plugin.getRookVolumeSource(spec)

	if source.SecretRef != nil {
		namespace := source.SecretRef.Namespace
		if namespace == "" {
			namespace = pod.Namespace
		}
		if secret, err = parseSecret(namespace, source.SecretRef.Name, plugin.host.GetKubeClient()); err != nil {
			glog.Errorf("Couldn't get secret from %v/%v", namespace, source.SecretRef.Name)
			return nil, err
		}
	}

	// Inject real implementations here, test through the internal function.
	return plugin.newMounterInternal(spec, pod.UID, &RookUtil{}, plugin.host.GetMounter(), secret)
}

func (plugin *rookPlugin) getRookVolumeSource(spec *volume.Spec) (*v1.RookVolumeSource, bool) {
	// rook volumes used directly in a pod have a ReadOnly flag set by the pod author.
	// rook volumes used as a PersistentVolume gets the ReadOnly flag indirectly through the persistent-claim volume used to mount the PV
	if spec.Volume != nil && spec.Volume.Rook != nil {
		return spec.Volume.Rook, spec.Volume.Rook.ReadOnly
	}
	return spec.PersistentVolume.Spec.Rook, spec.ReadOnly
}

func (plugin *rookPlugin) newMounterInternal(spec *volume.Spec, podUID types.UID, manager diskManager, mounter mount.Interface, secret string) (volume.Mounter, error) {
	source, readOnly := plugin.getRookVolumeSource(spec)
	return &rookMounter{
		rook: &rook{
			podUID:   podUID,
			volName:  spec.Name(),
			Image:    source.Image,
			Pool:     source.Pool,
			ReadOnly: readOnly,
			manager:  manager,
			mounter:  &mount.SafeFormatAndMount{Interface: mounter, Runner: exec.New()},
			plugin:   plugin,
		},
		Mon:          source.Monitors,
		Id:           source.User,
		Secret:       secret,
		fsType:       source.FSType,
		mountOptions: volume.MountOptionFromSpec(spec),
		AttacherType: source.AttacherType,
	}, nil
}

func (plugin *rookPlugin) NewUnmounter(volName string, podUID types.UID) (volume.Unmounter, error) {
	// Inject real implementations here, test through the internal function.
	return plugin.newUnmounterInternal(volName, podUID, &RookUtil{}, plugin.host.GetMounter())
}

func (plugin *rookPlugin) newUnmounterInternal(volName string, podUID types.UID, manager diskManager, mounter mount.Interface) (volume.Unmounter, error) {
	return &rookUnmounter{
		rookMounter: &rookMounter{
			rook: &rook{
				podUID:  podUID,
				volName: volName,
				manager: manager,
				mounter: &mount.SafeFormatAndMount{Interface: mounter, Runner: exec.New()},
				plugin:  plugin,
			},
			Mon: make([]string, 0),
		},
	}, nil
}

func (plugin *rookPlugin) ConstructVolumeSpec(volumeName, mountPath string) (*volume.Spec, error) {
	rookVolume := &v1.Volume{
		Name: volumeName,
		VolumeSource: v1.VolumeSource{
			Rook: &v1.RookVolumeSource{
				Monitors: []string{},
			},
		},
	}
	return volume.NewSpecFromVolume(rookVolume), nil
}

func (plugin *rookPlugin) NewDeleter(spec *volume.Spec) (volume.Deleter, error) {
	if spec.PersistentVolume != nil && spec.PersistentVolume.Spec.Rook == nil {
		return nil, fmt.Errorf("spec.PersistentVolumeSource.Spec.Rook is nil")
	}
	class, err := volutil.GetClassForVolume(plugin.host.GetKubeClient(), spec.PersistentVolume)
	if err != nil {
		return nil, err
	}
	adminSecretName := ""
	secretNamespace := "default"
	admin := ""

	for k, v := range class.Parameters {
		switch dstrings.ToLower(k) {
		case "adminid":
			admin = v
		case "adminsecretname":
			adminSecretName = v
		case "secretnamespace":
			secretNamespace = v
		}
	}

	secret, err := parseSecret(secretNamespace, adminSecretName, plugin.host.GetKubeClient())
	if err != nil {
		return nil, fmt.Errorf("failed to get admin secret from [%q/%q]: %v", secretNamespace, adminSecretName, err)
	}
	return plugin.newDeleterInternal(spec, admin, secret, &RookUtil{})
}

func (plugin *rookPlugin) newDeleterInternal(spec *volume.Spec, admin, secret string, manager diskManager) (volume.Deleter, error) {
	return &rookVolumeDeleter{
		rookMounter: &rookMounter{
			rook: &rook{
				volName: spec.Name(),
				Image:   spec.PersistentVolume.Spec.Rook.Image,
				Pool:    spec.PersistentVolume.Spec.Rook.Pool,
				manager: manager,
				plugin:  plugin,
			},
			Mon:         spec.PersistentVolume.Spec.Rook.Monitors,
			adminId:     admin,
			adminSecret: secret,
		}}, nil
}

func (plugin *rookPlugin) NewProvisioner(options volume.VolumeOptions) (volume.Provisioner, error) {
	return plugin.newProvisionerInternal(options, &RookUtil{})
}

func (plugin *rookPlugin) newProvisionerInternal(options volume.VolumeOptions, manager diskManager) (volume.Provisioner, error) {
	return &rookVolumeProvisioner{
		rookMounter: &rookMounter{
			rook: &rook{
				manager: manager,
				plugin:  plugin,
			},
		},
		options: options,
	}, nil
}

type rookVolumeProvisioner struct {
	*rookMounter
	options volume.VolumeOptions
}

func (r *rookVolumeProvisioner) Provision() (*v1.PersistentVolume, error) {
	glog.V(3).Info("Provisioning rook...")
	if r.options.PVC.Spec.Selector != nil {
		return nil, fmt.Errorf("claim Selector is not supported")
	}
	var err error
	adminSecretName := "rook-admin"
	rookNamespace := "rook"
	secretName := "rook-user"
	secret := ""
	attacherType := "krbd"
	r.adminId = "admin"
	r.Id = "rook-user"

	for k, v := range r.options.Parameters {
		switch dstrings.ToLower(k) {
		case "adminid":
			r.adminId = v
		case "adminsecretname":
			adminSecretName = v
		case "rooknamespace":
			rookNamespace = v
		case "userid":
			r.Id = v
		case "pool":
			r.Pool = v
		case "usersecretname":
			secretName = v
		case "attachertype":
			attacherType = v
		default:
			return nil, fmt.Errorf("invalid option %q for volume plugin %s", k, r.plugin.GetPluginName())
		}
	}
	// sanity check
	if adminSecretName == "" {
		return nil, fmt.Errorf("missing admin secret name")
	}
	if secret, err = parseSecret(rookNamespace, adminSecretName, r.plugin.host.GetKubeClient()); err != nil {
		return nil, fmt.Errorf("failed to get admin secret from [%q/%q]: %v", rookNamespace, adminSecretName, err)
	}
	r.adminSecret = secret
	if secretName == "" {
		return nil, fmt.Errorf("missing user secret name")
	}
	if r.adminId == "" {
		return nil, fmt.Errorf("missing user admin ID")
	}
	if r.Pool == "" {
		return nil, fmt.Errorf("missing Rook Pool")
	}
	if r.Id == "" {
		return nil, fmt.Errorf("missing user ID")
	}

	// Get monitor endpoints
	if r.Mon, err = getMonitorsFromConfigmap(rookNamespace, monConfigMap, r.plugin.host.GetKubeClient()); err != nil {
		return nil, fmt.Errorf("failed to get monitor configmap from [%q/%q]: %v", rookNamespace, monConfigMap, err)
	}

	// validate attacher type
	if !validateAttacherType(attacherType) {
		return nil, fmt.Errorf("invalid attacher type [%q]. Must be one of %v", attacherType, attacherTypes)
	}

	// create random image name
	image := fmt.Sprintf("kubernetes-dynamic-pvc-%s", uuid.NewUUID())
	r.rookMounter.Image = image
	rook, sizeMB, err := r.manager.CreateImage(r)
	if err != nil {
		glog.Errorf("rook: create volume failed, err: %v", err)
		return nil, err
	}
	glog.V(3).Infof("successfully created rook image %q", image)
	pv := new(v1.PersistentVolume)
	rook.SecretRef = new(v1.ObjectReference)
	rook.SecretRef.Namespace = rookNamespace
	rook.SecretRef.Name = secretName
	rook.User = r.Id
	rook.AttacherType = attacherType
	pv.Spec.PersistentVolumeSource.Rook = rook
	pv.Spec.PersistentVolumeReclaimPolicy = r.options.PersistentVolumeReclaimPolicy
	pv.Spec.AccessModes = r.options.PVC.Spec.AccessModes
	if len(pv.Spec.AccessModes) == 0 {
		pv.Spec.AccessModes = r.plugin.GetAccessModes()
	}
	pv.Spec.Capacity = v1.ResourceList{
		v1.ResourceName(v1.ResourceStorage): resource.MustParse(fmt.Sprintf("%dMi", sizeMB)),
	}
	glog.V(3).Info("Rook PV: %v", pv)
	return pv, nil
}

type rookVolumeDeleter struct {
	*rookMounter
}

func (r *rookVolumeDeleter) GetPath() string {
	name := rookPluginName
	return r.plugin.host.GetPodVolumeDir(r.podUID, strings.EscapeQualifiedNameForDisk(name), r.volName)
}

func (r *rookVolumeDeleter) Delete() error {
	return r.manager.DeleteImage(r)
}

type rook struct {
	volName  string
	podUID   types.UID
	Pool     string
	Image    string
	ReadOnly bool
	plugin   *rookPlugin
	mounter  *mount.SafeFormatAndMount
	// Utility interface that provides API calls to the provider to attach/detach disks.
	manager diskManager
	volume.MetricsNil
}

func (rook *rook) GetPath() string {
	name := rookPluginName
	// safe to use PodVolumeDir now: volume teardown occurs before pod is cleaned up
	return rook.plugin.host.GetPodVolumeDir(rook.podUID, strings.EscapeQualifiedNameForDisk(name), rook.volName)
}

type rookMounter struct {
	*rook
	// capitalized so they can be exported in persistRook()
	Mon          []string
	Id           string
	Secret       string
	fsType       string
	adminSecret  string
	adminId      string
	mountOptions []string
	AttacherType string
}

var _ volume.Mounter = &rookMounter{}

func (b *rook) GetAttributes() volume.Attributes {
	return volume.Attributes{
		ReadOnly:        b.ReadOnly,
		Managed:         !b.ReadOnly,
		SupportsSELinux: true,
	}
}

// Checks prior to mount operations to verify that the required components (binaries, etc.)
// to mount the volume are available on the underlying node.
// If not, it returns an error
func (b *rookMounter) CanMount() error {
	return nil
}

func (b *rookMounter) SetUp(fsGroup *int64) error {
	return b.SetUpAt(b.GetPath(), fsGroup)
}

func (b *rookMounter) SetUpAt(dir string, fsGroup *int64) error {
	// diskSetUp checks mountpoints and prevent repeated calls
	glog.V(4).Infof("rook: attempting to SetUp and mount %s", dir)
	err := diskSetUp(b.manager, *b, dir, b.mounter, fsGroup)
	if err != nil {
		glog.Errorf("rook: failed to setup mount %s %v", dir, err)
	}
	return err
}

type rookUnmounter struct {
	*rookMounter
}

var _ volume.Unmounter = &rookUnmounter{}

// Unmounts the bind mount, and detaches the disk only if the disk
// resource was the last reference to that disk on the kubelet.
func (c *rookUnmounter) TearDown() error {
	return c.TearDownAt(c.GetPath())
}

func (c *rookUnmounter) TearDownAt(dir string) error {
	if pathExists, pathErr := volutil.PathExists(dir); pathErr != nil {
		return fmt.Errorf("Error checking if path exists: %v", pathErr)
	} else if !pathExists {
		glog.Warningf("Warning: Unmount skipped because path does not exist: %v", dir)
		return nil
	}
	return diskTearDown(c.manager, *c, dir, c.mounter)
}

func (plugin *rookPlugin) execCommand(command string, args []string) ([]byte, error) {
	cmd := plugin.exe.Command(command, args...)
	return cmd.CombinedOutput()
}

func getVolumeSource(spec *volume.Spec) (*v1.RookVolumeSource, bool, error) {
	if spec.Volume != nil && spec.Volume.Rook != nil {
		return spec.Volume.Rook, spec.Volume.Rook.ReadOnly, nil
	} else if spec.PersistentVolume != nil &&
		spec.PersistentVolume.Spec.Rook != nil {
		return spec.PersistentVolume.Spec.Rook, spec.ReadOnly, nil
	}

	return nil, false, fmt.Errorf("Spec does not reference a Rook volume type")
}

func parseSecret(namespace, secretName string, kubeClient clientset.Interface) (string, error) {
	secret, err := volutil.GetSecretForPV(namespace, secretName, rookPluginName, kubeClient)
	if err != nil {
		glog.Errorf("failed to get secret from [%q/%q]", namespace, secretName)
		return "", fmt.Errorf("failed to get secret from [%q/%q]", namespace, secretName)
	}
	return parseSecretMap(secret)
}

// parseSecretMap locates the secret by key name.
func parseSecretMap(secretMap map[string]string) (string, error) {
	if len(secretMap) == 0 {
		return "", fmt.Errorf("empty secret map")
	}
	secret := ""
	for k, v := range secretMap {
		if k == secretKeyName {
			return v, nil
		}
		secret = v
	}
	// If not found, the last secret in the map wins as done before
	return secret, nil
}

func validateAttacherType(attacherType string) bool {
	for _, a := range attacherTypes {
		if a == attacherType {
			return true
		}
	}
	return false
}

// parseConfigmap locates configmap by name and namespace and returns the key value
func parseConfigmap(namespace, name, key string, kubeClient clientset.Interface) (string, error) {
	if kubeClient == nil {
		return "", fmt.Errorf("Cannot get kube client")
	}
	configmap, err := kubeClient.Core().ConfigMaps(namespace).Get(name, metav1.GetOptions{})
	if err != nil {
		return "", err
	}

	return configmap.Data[key], nil
}

func getMonitorsFromConfigmap(namespace, name string, kubeClient clientset.Interface) ([]string, error) {
	monitorEndpoints := ""
	var err error
	if monitorEndpoints, err = parseConfigmap(namespace, name, monConfigMapKey, kubeClient); err != nil {
		return nil, fmt.Errorf("failed to get monitor configmap from [%q/%q] key %q: %v", namespace, name, monConfigMapKey, err)
	}

	monSplitted := dstrings.Split(monitorEndpoints, ",")
	mons := []string{}
	for _, m := range monSplitted {
		v := dstrings.Split(m, "=")
		if len(v) != 2 {
			glog.Warningf("Warning: failed to parse monitor: %v", m)
			continue
		}
		mons = append(mons, v[1])
	}
	return mons, nil
}
