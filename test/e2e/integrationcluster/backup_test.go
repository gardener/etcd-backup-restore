// SPDX-FileCopyrightText: 2024 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package integrationcluster

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/gardener/etcd-backup-restore/pkg/miscellaneous"
	brtypes "github.com/gardener/etcd-backup-restore/pkg/types"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	. "github.com/onsi/gomega/gstruct"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

var _ = Describe("Backup", func() {
	var (
		err                error
		store              brtypes.SnapStore
		podName            = fmt.Sprintf("%s-etcd-0", releaseName)
		etcdEndpointName   = fmt.Sprintf("%s-etcd-client", releaseName)
		backupEndpointName = fmt.Sprintf("%s-backup-client", releaseName)
	)
	BeforeEach(func() {
		store, err = getSnapstore(storageProvider, storageContainer, storePrefix)
		Expect(err).ShouldNot(HaveOccurred())

		logger.Infof("waiting for %s pod to be running", podName)
		err = waitForPodToBeRunning(typedClient, podName, releaseNamespace)
		Expect(err).ShouldNot(HaveOccurred())
		logger.Infof("waiting for %s endpoint to be ready", etcdEndpointName)
		err = waitForEndpointPortsToBeReady(typedClient, etcdEndpointName, releaseNamespace, []int32{etcdClientPort})
		Expect(err).ShouldNot(HaveOccurred())
		logger.Infof("waiting for %s endpoint to be ready", backupEndpointName)
		err = waitForEndpointPortsToBeReady(typedClient, backupEndpointName, releaseNamespace, []int32{backupClientPort})
		Expect(err).ShouldNot(HaveOccurred())

		logger.Infof("pod %s and endpoints %s, %s ready", podName, etcdEndpointName, backupEndpointName)
	})

	Describe("Snapshotter", func() {
		It("should take full and delta snapshot", func() {
			snapList, err := store.List(false)
			Expect(err).ShouldNot(HaveOccurred())
			numFulls, numDeltas := getTotalFullAndDeltaSnapshotCounts(snapList)
			Expect(numFulls).Should(Equal(1))
			Expect(numDeltas).Should(Equal(0))

			populatorStopCh := make(chan struct{})
			populatorDoneCh := make(chan struct{})
			recorderStopCh := make(chan struct{})
			recorderResultCh := make(chan SnapListResult)
			go runEtcdPopulatorWithoutError(logger, populatorStopCh, populatorDoneCh, kubeconfigPath, releaseNamespace, podName, "etcd")
			go recordCumulativeSnapList(logger, recorderStopCh, recorderResultCh, store)
			time.Sleep(70 * time.Second)
			close(populatorStopCh)
			close(recorderStopCh)
			<-populatorDoneCh
			result := <-recorderResultCh
			Expect(result.Error).ShouldNot(HaveOccurred())
			cumulativeSnapList := result.Snapshots

			numFulls, numDeltas = getTotalFullAndDeltaSnapshotCounts(cumulativeSnapList)
			Expect(numFulls).Should(BeNumerically(">=", 2))
			Expect(numDeltas).Should(BeNumerically(">=", 5))
		})
	})

	Describe("Garbage Collector", func() {
		It("should garbage collect old snapshots", func() {
			populatorStopCh := make(chan struct{})
			populatorDoneCh := make(chan struct{})
			recorderStopCh := make(chan struct{})
			recorderResultCh := make(chan SnapListResult)
			go runEtcdPopulatorWithoutError(logger, populatorStopCh, populatorDoneCh, kubeconfigPath, releaseNamespace, podName, "etcd")
			go recordCumulativeSnapList(logger, recorderStopCh, recorderResultCh, store)
			time.Sleep(190 * time.Second)
			close(populatorStopCh)
			close(recorderStopCh)
			<-populatorDoneCh
			result := <-recorderResultCh
			Expect(result.Error).ShouldNot(HaveOccurred())
			cumulativeSnapList := result.Snapshots

			cumulativeNumFulls, cumulativeNumDeltas := getTotalFullAndDeltaSnapshotCounts(cumulativeSnapList)

			snapList, err := store.List(false)
			Expect(err).ShouldNot(HaveOccurred())
			numFulls, numDeltas := getTotalFullAndDeltaSnapshotCounts(snapList)

			Expect(cumulativeNumFulls).Should(BeNumerically(">", numFulls))
			Expect(cumulativeNumDeltas).Should(BeNumerically(">", numDeltas))
		})
	})

	Describe("Defragmentor", func() {
		It("should defragment the data", func() {
			cmd := "ETCDCTL_API=3 etcdctl put defrag-1 val-1"
			stdout, stderr, err := executeRemoteCommand(kubeconfigPath, releaseNamespace, podName, "etcd", cmd)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(stderr).Should(BeEmpty())
			Expect(stdout).Should(Equal("OK"))

			cmd = "ETCDCTL_API=3 etcdctl del defrag-1"
			stdout, stderr, err = executeRemoteCommand(kubeconfigPath, releaseNamespace, podName, "etcd", cmd)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(stderr).Should(BeEmpty())

			oldDbSize, oldRevision, err := getDbSizeAndRevision(kubeconfigPath, releaseNamespace, podName, "etcd")
			Expect(err).ShouldNot(HaveOccurred())

			logger.Infof("waiting for defragmentation to occur atleast once")
			time.Sleep(70 * time.Second)

			newDbSize, newRevision, err := getDbSizeAndRevision(kubeconfigPath, releaseNamespace, podName, "etcd")
			Expect(err).ShouldNot(HaveOccurred())

			Expect(newRevision).Should(BeNumerically("==", oldRevision))
			Expect(newDbSize).Should(BeNumerically("<", oldDbSize))
		})
	})

	Describe("HTTP Server", func() {
		It("should trigger on-demand full snapshot", func() {
			fullSnap, _, err := miscellaneous.GetLatestFullSnapshotAndDeltaSnapList(store)
			Expect(err).ShouldNot(HaveOccurred())
			oldFullSnapTimestamp := fullSnap.CreatedOn.Unix()

			cmd := "ETCDCTL_API=3 etcdctl put full-1 val-1"
			stdout, stderr, err := executeRemoteCommand(kubeconfigPath, releaseNamespace, podName, "etcd", cmd)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(stderr).Should(BeEmpty())
			Expect(stdout).Should(Equal("OK"))

			snap, err := triggerOnDemandSnapshot(kubeconfigPath, releaseNamespace, podName, "backup-restore", backupClientPort, "full")
			Expect(err).ShouldNot(HaveOccurred())
			Expect(snap).ShouldNot(BeNil())
			Expect(snap.Kind).Should(Equal(brtypes.SnapshotKindFull))

			fullSnap, _, err = miscellaneous.GetLatestFullSnapshotAndDeltaSnapList(store)
			Expect(err).ShouldNot(HaveOccurred())
			newFullSnapTimestamp := fullSnap.CreatedOn.Unix()

			Expect(newFullSnapTimestamp).Should(BeNumerically(">", oldFullSnapTimestamp))
		})

		It("should trigger on-demand delta snapshot", func() {
			_, deltaSnapList, err := miscellaneous.GetLatestFullSnapshotAndDeltaSnapList(store)
			Expect(err).ShouldNot(HaveOccurred())
			oldDeltaSnapTimestamp := int64(0)
			if len(deltaSnapList) > 0 {
				oldDeltaSnapTimestamp = deltaSnapList[len(deltaSnapList)-1].CreatedOn.Unix()
			}

			cmd := "ETCDCTL_API=3 etcdctl put delta-1 val-1"
			stdout, stderr, err := executeRemoteCommand(kubeconfigPath, releaseNamespace, podName, "etcd", cmd)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(stderr).Should(BeEmpty())
			Expect(stdout).Should(Equal("OK"))

			snap, err := triggerOnDemandSnapshot(kubeconfigPath, releaseNamespace, podName, "backup-restore", backupClientPort, "delta")
			Expect(err).ShouldNot(HaveOccurred())
			Expect(snap).ShouldNot(BeNil())
			Expect(snap.Kind).Should(Equal(brtypes.SnapshotKindDelta))

			_, deltaSnapList, err = miscellaneous.GetLatestFullSnapshotAndDeltaSnapList(store)
			Expect(err).ShouldNot(HaveOccurred())
			newDeltaSnapTimestamp := int64(0)
			if len(deltaSnapList) > 0 {
				newDeltaSnapTimestamp = deltaSnapList[len(deltaSnapList)-1].CreatedOn.Unix()
			}

			Expect(newDeltaSnapTimestamp).Should(BeNumerically(">", oldDeltaSnapTimestamp))
		})

		It("should return list of latest snapshots", func() {
			fullSnap, deltaSnapList, err := miscellaneous.GetLatestFullSnapshotAndDeltaSnapList(store)
			Expect(err).ShouldNot(HaveOccurred())

			latestSnapshots, err := getLatestSnapshots(kubeconfigPath, releaseNamespace, podName, "backup-restore", backupClientPort)
			Expect(err).ShouldNot(HaveOccurred())

			// since there is a chance of taking a scheduled full snapshot
			// between fetching the latest snapshots from the snapstore and
			// making the http call to get latest snapshots, we fetch the
			// latest snapshots from the snapstore again and expect the http
			// call results to match either one of the fetches
			newFullSnap, newDeltaSnapList, err := miscellaneous.GetLatestFullSnapshotAndDeltaSnapList(store)
			Expect(err).ShouldNot(HaveOccurred())

			if latestSnapshots.FullSnapshot == nil {
				Expect(latestSnapshots.FullSnapshot).Should(Or(Equal(fullSnap), Equal(newFullSnap)))
			} else {
				// prefix is not determined during http call So prefix can't be tested here.
				Expect(*latestSnapshots.FullSnapshot).To(MatchFields(IgnoreExtras, Fields{
					"Kind":              Or(Equal(fullSnap.Kind), Equal(newFullSnap.Kind)),
					"StartRevision":     Or(Equal(fullSnap.StartRevision), Equal(newFullSnap.StartRevision)),
					"LastRevision":      Or(Equal(fullSnap.LastRevision), Equal(newFullSnap.LastRevision)),
					"CreatedOn":         Or(Equal(fullSnap.CreatedOn), Equal(newFullSnap.CreatedOn)),
					"CompressionSuffix": Or(Equal(fullSnap.CompressionSuffix), Equal(newFullSnap.CompressionSuffix)),
				}))
			}

			if len(latestSnapshots.DeltaSnapshots) == 0 {
				Expect(len(latestSnapshots.DeltaSnapshots)).Should(Or(Equal(len(deltaSnapList)), Equal(len(newDeltaSnapList))))
			} else {
				for i, snap := range latestSnapshots.DeltaSnapshots {
					// prefix is not determined during http call, so prefix can't be tested here
					Expect(*snap).To(MatchFields(IgnoreExtras, Fields{
						"Kind":              Or(Equal(deltaSnapList[i].Kind), Equal(newDeltaSnapList[i].Kind)),
						"StartRevision":     Or(Equal(deltaSnapList[i].StartRevision), Equal(newDeltaSnapList[i].StartRevision)),
						"LastRevision":      Or(Equal(deltaSnapList[i].LastRevision), Equal(newDeltaSnapList[i].LastRevision)),
						"CreatedOn":         Or(Equal(deltaSnapList[i].CreatedOn), Equal(newDeltaSnapList[i].CreatedOn)),
						"CompressionSuffix": Or(Equal(deltaSnapList[i].CompressionSuffix), Equal(newDeltaSnapList[i].CompressionSuffix)),
					}))
				}
			}
		})
	})

	Describe("Initializer", func() {
		var i int
		JustBeforeEach(func() {
			i++
			cmd := fmt.Sprintf("ETCDCTL_API=3 etcdctl put init-%d val-%d", i, i)
			stdout, stderr, err := executeRemoteCommand(kubeconfigPath, releaseNamespace, podName, "etcd", cmd)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(stderr).Should(BeEmpty())
			Expect(stdout).Should(Equal("OK"))

			_, err = triggerOnDemandSnapshot(kubeconfigPath, releaseNamespace, podName, "backup-restore", backupClientPort, "delta")
			Expect(err).ShouldNot(HaveOccurred())

			i++
			cmd = fmt.Sprintf("ETCDCTL_API=3 etcdctl put init-%d val-%d", i, i)
			stdout, stderr, err = executeRemoteCommand(kubeconfigPath, releaseNamespace, podName, "etcd", cmd)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(stderr).Should(BeEmpty())
			Expect(stdout).Should(Equal("OK"))
		})
		It("should verify data integrity", func() {
			podClient := typedClient.CoreV1().Pods(releaseNamespace)
			err = podClient.Delete(context.TODO(), podName, metav1.DeleteOptions{})
			Expect(err).ShouldNot(HaveOccurred())
			time.Sleep(time.Duration(time.Second * 5))

			logger.Infof("waiting for %s pod to be running", podName)
			err = waitForPodToBeRunning(typedClient, podName, releaseNamespace)
			Expect(err).ShouldNot(HaveOccurred())
			logger.Infof("waiting for %s endpoint to be ready", etcdEndpointName)
			err = waitForEndpointPortsToBeReady(typedClient, etcdEndpointName, releaseNamespace, []int32{etcdClientPort})
			Expect(err).ShouldNot(HaveOccurred())
			logger.Infof("waiting for %s endpoint to be ready", backupEndpointName)
			err = waitForEndpointPortsToBeReady(typedClient, backupEndpointName, releaseNamespace, []int32{backupClientPort})
			Expect(err).ShouldNot(HaveOccurred())
			logger.Infof("pod %s and endpoints %s, %s ready", podName, etcdEndpointName, backupEndpointName)

			cmd := fmt.Sprintf("curl http://localhost:%d/initialization/status -s", backupClientPort)
			stdout, stderr, err := executeRemoteCommand(kubeconfigPath, releaseNamespace, podName, "backup-restore", cmd)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(stdout).Should(Equal("New"))

			cmd = "ETCDCTL_API=3 etcdctl get init-1"
			stdout, stderr, err = executeRemoteCommand(kubeconfigPath, releaseNamespace, podName, "etcd", cmd)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(stderr).Should(BeEmpty())
			lines := strings.Split(stdout, "\n")
			Expect(len(lines)).Should(Equal(2))
			Expect(lines[0]).Should(Equal("init-1"))
			Expect(lines[1]).Should(Equal("val-1"))

			cmd = "ETCDCTL_API=3 etcdctl get init-2"
			stdout, stderr, err = executeRemoteCommand(kubeconfigPath, releaseNamespace, podName, "etcd", cmd)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(stderr).Should(BeEmpty())
			lines = strings.Split(stdout, "\n")
			Expect(len(lines)).Should(Equal(2))
			Expect(lines[0]).Should(Equal("init-2"))
			Expect(lines[1]).Should(Equal("val-2"))
		})

		Context("when data is corrupt", func() {
			It("should restore data from latest snapshot", func() {
				testDataCorruptionRestoration := func() {
					cmd := "rm -rf /var/etcd/data/new.etcd/member"
					stdout, stderr, err := executeRemoteCommand(kubeconfigPath, releaseNamespace, podName, "backup-restore", cmd)
					Expect(err).ShouldNot(HaveOccurred())
					Expect(stderr).Should(BeEmpty())
					Expect(stdout).Should(BeEmpty())

					podClient := typedClient.CoreV1().Pods(releaseNamespace)
					err = podClient.Delete(context.TODO(), podName, metav1.DeleteOptions{})
					Expect(err).ShouldNot(HaveOccurred())
					time.Sleep(time.Duration(time.Second * 5))

					logger.Infof("waiting for %s pod to be running", podName)
					err = waitForPodToBeRunning(typedClient, podName, releaseNamespace)
					Expect(err).ShouldNot(HaveOccurred())
					logger.Infof("waiting for %s endpoint to be ready", etcdEndpointName)
					err = waitForEndpointPortsToBeReady(typedClient, etcdEndpointName, releaseNamespace, []int32{etcdClientPort})
					Expect(err).ShouldNot(HaveOccurred())
					logger.Infof("waiting for %s endpoint to be ready", backupEndpointName)
					err = waitForEndpointPortsToBeReady(typedClient, backupEndpointName, releaseNamespace, []int32{backupClientPort})
					Expect(err).ShouldNot(HaveOccurred())
					logger.Infof("pod %s and endpoints %s, %s ready", podName, etcdEndpointName, backupEndpointName)

					cmd = fmt.Sprintf("curl http://localhost:%d/initialization/status -s", backupClientPort)
					stdout, stderr, err = executeRemoteCommand(kubeconfigPath, releaseNamespace, podName, "backup-restore", cmd)
					Expect(err).ShouldNot(HaveOccurred())
					Expect(stdout).Should(Equal("New"))

					cmd = "ETCDCTL_API=3 etcdctl get init-3"
					stdout, stderr, err = executeRemoteCommand(kubeconfigPath, releaseNamespace, podName, "etcd", cmd)
					Expect(err).ShouldNot(HaveOccurred())
					Expect(stderr).Should(BeEmpty())
					lines := strings.Split(stdout, "\n")
					Expect(len(lines)).Should(Equal(2))
					Expect(lines[0]).Should(Equal("init-3"))
					Expect(lines[1]).Should(Equal("val-3"))

					cmd = "ETCDCTL_API=3 etcdctl get init-4"
					stdout, stderr, err = executeRemoteCommand(kubeconfigPath, releaseNamespace, podName, "etcd", cmd)
					Expect(err).ShouldNot(HaveOccurred())
					Expect(stderr).Should(BeEmpty())
					Expect(stdout).Should(BeEmpty())
				}
				for i := 0; i < 3; i++ { // 3 consecutive restorations
					testDataCorruptionRestoration()
				}
			})
		})
	})
})
