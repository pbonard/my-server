package service

import (
	"backend/chat/internal/dto"
	"backend/common"
	"context"
	"crypto/hmac"
	"crypto/sha1"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
)

// GenerateTurn is the TURN server and credentials valid for two hours: the username is when they expire, the
// credential its HMAC with the secret the turn app shares (TURN REST API), so the turn app checks them without a call
func (s *service) GenerateTurn() *dto.GetTurnResponse {
	res := &dto.GetTurnResponse{
		Uris: []string{
			fmt.Sprintf("turn:%s:3478?transport=udp", s.turnRealm),
			fmt.Sprintf("turn:%s:5349?transport=tcp", s.turnRealm),
		},
		Username: fmt.Sprintf("%d", time.Now().Add(2*time.Hour).Unix()),
	}
	mac := hmac.New(sha1.New, []byte(s.turnSecret))
	mac.Write([]byte(res.Username))
	res.Credential = base64.StdEncoding.EncodeToString(mac.Sum(nil))
	return res
}

func (s *service) SetCallServerIP(ctx context.Context, memberId uuid.UUID, ip string) error {
	return s.repository.SetCallServerIP(ctx, common.CallServerKey(memberId), ip)
}

func (s *service) RemoveCallServerIP(ctx context.Context, memberId uuid.UUID) error {
	return s.repository.RemoveCallServerIP(ctx, common.CallServerKey(memberId))
}

// GetCallParticipantsWithoutMe are the others already in the call
func (s *service) GetCallParticipantsWithoutMe(ctx context.Context, callId string, memberId uuid.UUID) ([]uuid.UUID, error) {
	raws, err := s.repository.FindCallParticipantIds(ctx, common.CallParticipantsKey(callId))
	if err != nil {
		return nil, err
	}
	pids := make([]uuid.UUID, 0, len(raws))
	for _, raw := range raws {
		pid, err1 := uuid.Parse(raw)
		if err1 != nil {
			slog.Error("fail to parse call participant id", "err", err1, "raw", raw)
			return nil, err1
		}
		if pid == memberId {
			continue
		}
		pids = append(pids, pid)
	}
	return pids, nil
}

func (s *service) AddCallParticipant(ctx context.Context, callId string, memberId uuid.UUID) error {
	return s.repository.AddCallParticipantId(ctx, common.CallParticipantsKey(callId), memberId)
}

func (s *service) RemoveCallParticipant(ctx context.Context, callId string, memberId uuid.UUID) error {
	return s.repository.RemoveCallParticipantId(ctx, common.CallParticipantsKey(callId), memberId)
}

// PublishCallSignal hands a signal for members on other chat servers to signalrelay, on this app's topic
func (s *service) PublishCallSignal(fromId uuid.UUID, toIds []uuid.UUID, signal json.RawMessage) error {
	p := common.Marshal(common.CallSignal{
		FromId: fromId,
		ToIds:  toIds,
		Signal: signal,
	})
	return s.producer.Commit(common.AppScoped(s.profile, "conversation-signal"), nil, p, nil)
}
