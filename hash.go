package consistenthashing

import (
	"fmt"
	"github.com/serialx/hashring"
	log "github.com/sirupsen/logrus"
	"sync"
	"time"
)

const (
	weightPerWorker          = 8
	nodesMaxIdleTimeDuration = 10 * time.Second
	queueMaxSize             = 1000
)

type consistentHashingRing struct {
	ring           *hashring.HashRing
	nodeToLastSeen map[string]time.Time
	mutex          sync.RWMutex
}

func (r *consistentHashingRing) addOrVerify(node string) {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	if _, ok := r.nodeToLastSeen[node]; !ok {
		log.WithField("node", node).Info("add to ring")
		r.ring = r.ring.AddWeightedNode(node, weightPerWorker)
	}
	r.nodeToLastSeen[node] = time.Now()

	r.cleanOldNodes()
}

func (r *consistentHashingRing) get(key string) (string, error) {
	r.mutex.RLock()
	defer r.mutex.RUnlock()

	node, ok := r.ring.GetNode(key)
	if !ok {
		return "", fmt.Errorf("failed to get node from key %s", key)
	}
	return node, nil
}

func (r *consistentHashingRing) cleanOldNodes() {
	activeTime := time.Now().Add(-nodesMaxIdleTimeDuration)
	for node, lastSeen := range r.nodeToLastSeen {
		if lastSeen.After(activeTime) {
			continue
		}

		log.WithField("node", node).Info("remove from ring")
		r.ring = r.ring.RemoveNode(node)
		delete(r.nodeToLastSeen, node)
	}
}

type consistentHashingQueues struct {
	ring        *consistentHashingRing
	nodeToQueue map[string]chan []byte
	mutex       sync.RWMutex
}

func createConsistentHashingQueues() *consistentHashingQueues {
	return &consistentHashingQueues{
		ring: &consistentHashingRing{
			ring:           hashring.New([]string{}),
			nodeToLastSeen: make(map[string]time.Time),
		},
		nodeToQueue: make(map[string]chan []byte),
	}
}

func (q *consistentHashingQueues) popNodeMessage(node string) []byte {
	q.ring.addOrVerify(node)
	queue := q.getOrCreateQueue(node)
	return <-queue
}

func (q *consistentHashingQueues) pushMessage(key string, message []byte) error {
	node, err := q.ring.get(key)
	if err != nil {
		return err
	}

	queue := q.getOrCreateQueue(node)
	queue <- message
	return nil
}

func (q *consistentHashingQueues) getOrCreateQueue(node string) chan []byte {
	q.mutex.Lock()
	defer q.mutex.Unlock()

	if _, ok := q.nodeToQueue[node]; !ok {
		q.nodeToQueue[node] = make(chan []byte, queueMaxSize)
	}
	return q.nodeToQueue[node]
}

func (q *consistentHashingQueues) getNodesWithPendingMessages() map[string]bool {
	q.mutex.RLock()
	defer q.mutex.RUnlock()

	res := make(map[string]bool, len(q.nodeToQueue))
	for node, queue := range q.nodeToQueue {
		if len(queue) > 0 {
			res[node] = true
		}
	}
	return res
}
