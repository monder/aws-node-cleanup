package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/ec2"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	types "k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
)

func volumeInUse(volumeName corev1.UniqueVolumeName, node *corev1.Node) bool {
	for _, name := range node.Status.VolumesInUse {
		if name == volumeName {
			return true
		}
	}
	return false
}

func deviceNameInUse(dev string, node *corev1.Node) bool {
	for _, volume := range node.Status.VolumesAttached {
		if volume.DevicePath == dev {
			return true
		}
	}
	return false
}

func volumeAttached(volumeName corev1.UniqueVolumeName, node *corev1.Node) bool {
	for _, volume := range node.Status.VolumesAttached {
		if volume.Name == volumeName {
			return true
		}
	}
	return false
}

func freeDeviceName(node *corev1.Node) (string, error) {
	for _, firstChar := range []rune{'b', 'c'} {
		for i := 'a'; i <= 'z'; i++ {
			dev := "/dev/xvd" + string([]rune{firstChar, i})
			if !deviceNameInUse(dev, node) {
				return dev, nil
			}
		}
	}
	return "", fmt.Errorf("all device names are in use")
}

func attachEBSVolume(volumeName corev1.UniqueVolumeName, node *corev1.Node) (string, error) {
	providerID := node.Spec.ProviderID
	parsedProviderID := strings.Split(providerID, "/")
	parsedVolumeID := strings.Split(string(volumeName), "/")

	region := node.Labels["failure-domain.beta.kubernetes.io/region"]
	instanceID := parsedProviderID[len(parsedProviderID)-1]
	volumeID := parsedVolumeID[len(parsedVolumeID)-1]

	svc := ec2.New(session.Must(session.NewSession(&aws.Config{
		Region: aws.String(region),
	})))

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	res, err := svc.DescribeVolumesWithContext(ctx, &ec2.DescribeVolumesInput{
		VolumeIds: []*string{
			aws.String(volumeID),
		},
	})
	if err != nil {
		return "", err
	}
	if len(res.Volumes) == 1 &&
		len(res.Volumes[0].Attachments) == 1 && res.Volumes[0].Attachments[0].InstanceId != nil &&
		res.Volumes[0].Attachments[0].Device != nil &&
		*res.Volumes[0].Attachments[0].InstanceId == instanceID {

		log.Printf("Volume %s is already attached\n", volumeName)
		return *res.Volumes[0].Attachments[0].Device, nil
	}
	deviceName, err := freeDeviceName(node)
	if err != nil {
		return "", err
	}
	_, err = svc.AttachVolumeWithContext(ctx, &ec2.AttachVolumeInput{
		InstanceId: aws.String(instanceID),
		VolumeId:   aws.String(volumeID),
		Device:     aws.String(deviceName),
	})
	return deviceName, err
}

func detachEBSVolume(volumeName corev1.UniqueVolumeName, node *corev1.Node) error {
	providerID := node.Spec.ProviderID
	parsedProviderID := strings.Split(providerID, "/")
	parsedVolumeID := strings.Split(string(volumeName), "/")

	region := node.Labels["failure-domain.beta.kubernetes.io/region"]
	instanceID := parsedProviderID[len(parsedProviderID)-1]
	volumeID := parsedVolumeID[len(parsedVolumeID)-1]

	svc := ec2.New(session.Must(session.NewSession(&aws.Config{
		Region: aws.String(region),
	})))

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	res, err := svc.DescribeVolumesWithContext(ctx, &ec2.DescribeVolumesInput{
		VolumeIds: []*string{
			aws.String(volumeID),
		},
	})
	if err != nil {
		return err
	}
	if len(res.Volumes) == 1 && res.Volumes[0].State != nil && *res.Volumes[0].State == "available" {
		log.Printf("Volume %s is already detached\n", volumeName)
		return nil
	}
	if len(res.Volumes) == 1 &&
		len(res.Volumes[0].Attachments) == 1 && res.Volumes[0].Attachments[0].InstanceId != nil &&
		res.Volumes[0].Attachments[0].Device != nil &&
		*res.Volumes[0].Attachments[0].InstanceId != instanceID {

		log.Printf("Volume %s is attached to other instance\n", volumeName)
		return nil
	}
	_, err = svc.DetachVolumeWithContext(ctx, &ec2.DetachVolumeInput{
		InstanceId: aws.String(instanceID),
		VolumeId:   aws.String(volumeID),
	})
	return err
}

func addVolumeToNode(clientset *kubernetes.Clientset, volumeName corev1.UniqueVolumeName, deviceName string, node *corev1.Node) error {
	patchData, err := json.Marshal([]map[string]interface{}{
		map[string]interface{}{
			"op":   "replace",
			"path": "/status/volumesAttached",
			"value": append(node.Status.VolumesAttached, corev1.AttachedVolume{
				DevicePath: deviceName,
				Name:       volumeName,
			}),
		},
	})
	if err != nil {
		return err
	}
	ns, err := clientset.CoreV1().Nodes().Patch(node.Name, types.JSONPatchType, patchData, "status")
	if err != nil {
		return err
	}
	log.Printf("Adding volume %s for node %s succeeded. VolumesAttached: %v\n", volumeName, node.Name, ns.Status.VolumesAttached)
	return nil
}

func removeVolumeFromNode(clientset *kubernetes.Clientset, volumeName corev1.UniqueVolumeName, node *corev1.Node) error {
	newAttachedVolumes := make([]corev1.AttachedVolume, 0)
	for _, v := range node.Status.VolumesAttached {
		if v.Name != volumeName {
			newAttachedVolumes = append(newAttachedVolumes, v)
		}
	}
	patchData, err := json.Marshal([]map[string]interface{}{
		map[string]interface{}{
			"op":    "replace",
			"path":  "/status/volumesAttached",
			"value": newAttachedVolumes,
		},
	})
	if err != nil {
		return err
	}
	ns, err := clientset.CoreV1().Nodes().Patch(node.Name, types.JSONPatchType, patchData, "status")
	if err != nil {
		return err
	}
	log.Printf("Removing volume %s from node %s succeeded. VolumesAttached: %v\n", volumeName, node.Name, ns.Status.VolumesAttached)
	return nil
}

func mountPendingEBSVolumes(clientset *kubernetes.Clientset) {
	nodes, err := clientset.CoreV1().Nodes().List(metav1.ListOptions{})
	if err != nil {
		panic(err.Error())
	}

	log.Printf("running\n")
	// First detach volumes we dont need
	for _, node := range nodes.Items {
		if node.Annotations["volumes.kubernetes.io/controller-managed-attach-detach"] != "true" {
			continue
		}
		// TODO fix stuck when no pods were scheduled
		for _, attachedVolume := range node.Status.VolumesAttached {
			if strings.HasPrefix(string(attachedVolume.Name), "kubernetes.io/aws-ebs/") && !volumeInUse(attachedVolume.Name, &node) {
				log.Printf("Need to detach volume %s from %s\n", attachedVolume.Name, node.Name)
				err := detachEBSVolume(attachedVolume.Name, &node)
				if err != nil {
					log.Printf("Unable to detach EBS: %s", err.Error())
					continue
				}
				err = removeVolumeFromNode(clientset, attachedVolume.Name, &node)
				if err != nil {
					log.Printf("Unable to sync EBS status: %s", err.Error())
					continue
				}
			}
		}
	}

	// Attach the ones we need
	for _, node := range nodes.Items {
		if node.Annotations["volumes.kubernetes.io/controller-managed-attach-detach"] != "true" {
			continue
		}
		for _, requiredVolume := range node.Status.VolumesInUse {
			if strings.HasPrefix(string(requiredVolume), "kubernetes.io/aws-ebs/") && !volumeAttached(requiredVolume, &node) {
				log.Printf("Need to attach volume %s to %s\n", requiredVolume, node.Name)
				device, err := attachEBSVolume(requiredVolume, &node)
				if err != nil {
					log.Printf("Unable to attach EBS: %s", err.Error())
					continue
				}
				err = addVolumeToNode(clientset, requiredVolume, device, &node)
				if err != nil {
					log.Printf("Unable to sync EBS status: %s", err.Error())
					continue
				}
			}
		}
	}
}
