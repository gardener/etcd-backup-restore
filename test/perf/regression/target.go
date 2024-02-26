// SPDX-FileCopyrightText: 2024 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package regression

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"text/template"

	"github.com/ghodss/yaml"
	"github.com/sirupsen/logrus"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
)

type target struct {
	typedClient     kubernetes.Interface
	discoveryClient discovery.DiscoveryInterface
	untypedClient   dynamic.Interface
	namespacePrefix string
	logger          *logrus.Logger
	resourcePaths   []string
	etcdImage       string
	etcdbrImage     string
	namespace       string
}

func (t *target) setup() error {
	t.logger.Info("Setting up target...")

	if strings.TrimSpace(t.etcdImage) == "" || strings.TrimSpace(t.etcdbrImage) == "" {
		return fmt.Errorf("Cannot test with invalid images. etcdImage: %s, etcdbrImage: %s", t.etcdImage, t.etcdbrImage)
	}

	if err := t.createNamespace(); err != nil {
		return err
	}

	if err := t.createResources(); err != nil {
		return err
	}

	return nil
}

func (t *target) teardown() {
	t.logger.Info("Tearing down target...")

	t.logger.Infof("Deleting the namespace %s", t.namespace)
	err := t.typedClient.CoreV1().Namespaces().Delete(context.TODO(), t.namespace, metav1.DeleteOptions{})
	if err != nil {
		t.logger.Errorf("Error deleting namespace %s: %s", t.namespace, err)
		return
	}

	t.logger.Infof("Deleted the namespace %s", t.namespace)
}

func (t *target) createNamespace() error {
	t.logger.Infof("Creating namespace with prefix %s", t.namespacePrefix)
	ns, err := t.typedClient.CoreV1().Namespaces().Create(context.TODO(), &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: t.namespacePrefix,
		},
	}, metav1.CreateOptions{})

	if err != nil {
		return err
	}

	t.namespace = ns.Name
	t.logger.Infof("Created namespace %s", t.namespace)
	return nil
}

func (t *target) createResources() error {
	t.logger.Infof("Creating resources in the namespace %s", t.namespace)
	params := map[string]string{
		"namespace":   t.namespace,
		"etcdImage":   t.etcdImage,
		"etcdbrImage": t.etcdbrImage,
	}
	for _, resourcePath := range t.resourcePaths {
		t.logger.Infof("Creating resource from %s", resourcePath)

		var (
			u   *unstructured.Unstructured
			gvr *schema.GroupVersionResource
			ri  dynamic.ResourceInterface
			err error
		)
		{
			var renderedJSON []byte
			{
				templ, err := template.ParseFiles(resourcePath)
				if err != nil {
					return err
				}

				b := &bytes.Buffer{}
				err = templ.Execute(b, params)
				if err != nil {
					return err
				}

				renderedJSON, err = yaml.YAMLToJSON(b.Bytes())
				if err != nil {
					return err
				}
			}

			u = &unstructured.Unstructured{}
			err = u.UnmarshalJSON(renderedJSON)
			if err != nil {
				return err
			}
		}
		{
			gvr, err = t.getGroupVersionResource(u)
			if err != nil {
				return err
			}

			nri := t.untypedClient.Resource(*gvr)
			ri = nri.Namespace(u.GetNamespace())
		}
		u, err = ri.Create(context.TODO(), u, metav1.CreateOptions{})
		if err != nil {
			return err
		}
		t.logger.Infof("Created resource %s/%s in the namespace %s", gvr.Resource, u.GetName(), u.GetNamespace())
	}

	return nil
}

func (t *target) getGroupVersionResource(u *unstructured.Unstructured) (*schema.GroupVersionResource, error) {
	arl, err := t.discoveryClient.ServerResourcesForGroupVersion(u.GetAPIVersion())
	if err != nil {
		return nil, err
	}

	gvk := u.GroupVersionKind()
	for i := range arl.APIResources {
		ar := &arl.APIResources[i]
		if ar.Kind == gvk.Kind {
			return &schema.GroupVersionResource{
				Group:    gvk.Group,
				Version:  gvk.Version,
				Resource: ar.Name,
			}, nil
		}
	}

	return nil, fmt.Errorf("Not GroupVersionResource found for GroupVersionKind: %s", gvk)
}

func (t *target) isPodRunning(podSelector string) (bool, error) {
	t.logger.Infof("Starting watch on pod with selector %s in the namespace %s", podSelector, t.namespace)
	podList, err := t.typedClient.CoreV1().Pods(t.namespace).List(context.TODO(), metav1.ListOptions{
		LabelSelector: podSelector,
	})
	if err != nil {
		return false, err
	}

	for i := range podList.Items {
		p := &podList.Items[i]
		if p.Status.Phase != corev1.PodRunning {
			return false, nil
		}

		nNotReady := len(p.Status.ContainerStatuses)
		for _, cs := range p.Status.ContainerStatuses {
			if cs.State.Terminated != nil {
				return false, fmt.Errorf("Container %s the namespace %s terminated unexpectedly", cs.Name, t.namespace)
			} else if !cs.Ready {
				return false, fmt.Errorf("Container %s in the namespace %s is not ready", cs.Name, t.namespace)
			} else {
				nNotReady--
				t.logger.Infof("Container %sin the namespace %s is ready", cs.Name, t.namespace)
			}
		}
		if nNotReady <= 0 {
			t.logger.Infof("Pod in the namespace %s is running", p.Namespace)
			return true, nil
		}
	}

	return false, fmt.Errorf("No pods found for the selector %s", podSelector)
}

func (t *target) watchForJob(ctx context.Context, jobSelector string, readyCh chan<- interface{}) error {
	for {
		retry, err := t.doWatchForJob(ctx, jobSelector, readyCh)
		if !retry {
			return err
		}
	}
}

func (t *target) doWatchForJob(ctx context.Context, jobSelector string, readyCh chan<- interface{}) (bool, error) {
	t.logger.Infof("Starting watch on job with selector %s in the namespace %s", jobSelector, t.namespace)
	wr, err := t.typedClient.BatchV1().Jobs(t.namespace).Watch(context.TODO(), metav1.ListOptions{
		LabelSelector: jobSelector,
	})
	if err != nil {
		return false, err
	}

	defer wr.Stop()

	watchCh := wr.ResultChan()
	for {
		select {
		case event, ok := <-watchCh:
			if !ok {
				t.logger.Errorf("Watch terminated for pod in the namespace %s. Will retry...", t.namespace)
				return true, nil
			}

			switch event.Type {
			case watch.Deleted:
				return false, fmt.Errorf("Unexpected event type %s in the namespace %s", event.Type, t.namespace)
			case watch.Error:
				return false, fmt.Errorf("Unexpected event type %s in the namespace %s", event.Type, t.namespace)
			}

			switch j := event.Object.(type) {
			default:
				return false, fmt.Errorf("Unexpected event object of type %T in the namespace %s: %v", j, t.namespace, j)
			case *batchv1.Job:
				if j.Status.Succeeded < 1 {
					continue
				}

				for i := range j.Status.Conditions {
					cond := j.Status.Conditions[i]
					if cond.Type == batchv1.JobComplete && cond.Status == corev1.ConditionTrue {
						t.logger.Infof("Job in the namespace %s is completed", t.namespace)
						close(readyCh)
						return false, nil
					}
				}

				t.logger.Infof("Job in the namespace %s is not completed", t.namespace)
			}
		case <-ctx.Done():
			return false, ctx.Err()
		}
	}
}
