package test

import (
	"backend/common/mocks"
	"backend/common/proto"
	"backend/signalrelay/internal/client"
	"backend/signalrelay/internal/service"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

const serverIP = "10.0.0.1"

var (
	fromId  = uuid.New()
	online  = uuid.New()
	offline = uuid.New()
	signal  = json.RawMessage(`{"sdp":"offer"}`)
)

func newService(t *testing.T) (service.Service, *MockRepository, *MockRelayClient) {
	repo, relay := NewMockRepository(t), NewMockRelayClient(t)
	return service.NewService(repo, mocks.NewMockSyncProducer(t), relay), repo, relay
}

func propagate(s service.Service, toIds ...uuid.UUID) {
	var ids [][]byte
	for _, id := range toIds {
		ids = append(ids, id[:])
	}
	s.PropagateSignal(context.Background(), ids, fromId[:], signal)
}

func TestPropagateSignal_relaysToTheReceiversServer(t *testing.T) {
	s, repo, relay := newService(t)
	repo.EXPECT().GetServerIP(mock.Anything, "call-server:"+online.String()).Return(serverIP, nil)
	repo.EXPECT().GetServerIP(mock.Anything, "call-server:"+offline.String()).Return("", nil)
	relay.EXPECT().Do(mock.Anything, serverIP, mock.MatchedBy(func(r *proto.RelaySignalRequest) bool {
		return assert.ObjectsAreEqual([][]byte{online[:]}, r.ToIds) &&
			assert.ObjectsAreEqual(fromId[:], r.FromId) && string(r.Signal) == string(signal)
	})).Return(nil)

	propagate(s, online, offline)
}

func TestPropagateSignal_unavailableServerIsForgotten(t *testing.T) {
	s, repo, relay := newService(t)
	moved := uuid.New()
	repo.EXPECT().GetServerIP(mock.Anything, "call-server:"+online.String()).Return(serverIP, nil)
	repo.EXPECT().GetServerIP(mock.Anything, "call-server:"+moved.String()).Return(serverIP, nil).Once()
	relay.EXPECT().Do(mock.Anything, serverIP, mock.Anything).
		Return(fmt.Errorf("%w: down", client.ErrServerUnavailable))
	repo.EXPECT().RemoveServerIP(mock.Anything, "call-server:"+online.String()).Return(nil)
	// moved reconnected to another server meanwhile, its ip is kept
	repo.EXPECT().GetServerIP(mock.Anything, "call-server:"+moved.String()).Return("10.0.0.2", nil).Once()

	propagate(s, online, moved)
}

func TestPropagateSignal_otherErrorsKeepTheMembersServer(t *testing.T) {
	s, repo, relay := newService(t)
	repo.EXPECT().GetServerIP(mock.Anything, "call-server:"+online.String()).Return(serverIP, nil)
	relay.EXPECT().Do(mock.Anything, serverIP, mock.Anything).Return(errors.New("internal"))

	propagate(s, online)
}
