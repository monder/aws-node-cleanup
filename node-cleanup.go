package main

import (
	"log"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/ec2"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	corev1 "k8s.io/api/core/v1"
)

func hasReadyCondition(conditions []corev1.NodeCondition) bool {
	for _, condition := range conditions {
		if condition.Type == corev1.NodeReady {
			if condition.LastHeartbeatTime.After(time.Now().Add(-30 * time.Second)) {
				return true
			}
		}
	}
	return false
}

func shouldRemoveNode(node corev1.Node) bool {
	providerID := node.Spec.ProviderID
	parsedProviderID := strings.Split(providerID, "/")

	region := node.Labels["failure-domain.beta.kubernetes.io/region"]
	instanceID := parsedProviderID[len(parsedProviderID)-1]

	svc := ec2.New(session.Must(session.NewSession(&aws.Config{
		Region: aws.String(region),
	})))

	result, err := svc.DescribeInstanceStatus(&ec2.DescribeInstanceStatusInput{
		InstanceIds: []*string{
			aws.String(instanceID),
		},
	})

	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			if aerr.Code() == "InvalidInstanceID.NotFound" {
				return true
			}
		}
		log.Println(err)
	} else if len(result.InstanceStatuses) == 0 {
		return true
	}
	return false
}

func cleanupNodesIfNeeded(clientset *kubernetes.Clientset) {
	nodes, err := clientset.CoreV1().Nodes().List(metav1.ListOptions{})
	if err != nil {
		panic(err.Error())
	}
	for _, node := range nodes.Items {
		if !hasReadyCondition(node.Status.Conditions) {
			if shouldRemoveNode(node) {
				log.Printf("Removing node %s\n", node.Name)
				clientset.CoreV1().Nodes().Delete(node.Name, &metav1.DeleteOptions{})
			} else {
				log.Printf("Node %s seems unresponsive, but alive\n", node.Name)
			}
		}
	}
}
