package service

import (
	"backend/common"
	"backend/common/proto"
	"backend/signalrelay/internal/client"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"
)

func (s *service) PropagateSignal(ctx context.Context, toIds [][]byte, fromId []byte, signal json.RawMessage) {
	var wg sync.WaitGroup
	relayToIdsByIPs := make(map[string][][]byte)
	var rm sync.Mutex
	for _, toId := range toIds {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ip, err := s.repository.GetServerIP(ctx, common.CallServerKey(uuid.UUID(toId)))
			if err != nil || ip == "" {
				return
			}
			rm.Lock()
			relayToIdsByIPs[ip] = append(relayToIdsByIPs[ip], toId)
			rm.Unlock()
		}()
	}
	wg.Wait()

	for ip, ids := range relayToIdsByIPs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			req := proto.RelaySignalRequest{
				ToIds:  ids,
				FromId: fromId,
				Signal: signal,
			}
			ctxg, cancel := context.WithTimeout(ctx, 3*time.Second)
			defer cancel()
			err := s.relayClient.Do(ctxg, ip, &req)
			if errors.Is(err, client.ErrServerUnavailable) {
				s.removeStaleIPs(ctx, ids, ip)
			}
			if err != nil {
				slog.Error("fail to relay signal", "err", err)
			}
		}()
	}
	wg.Wait()

	return
}

// removeStaleIPs forgets ip for the members that are still registered on it
func (s *service) removeStaleIPs(ctx context.Context, ids [][]byte, ip string) {
	var wg sync.WaitGroup
	for _, tid := range ids {
		wg.Add(1)
		go func() {
			defer wg.Done()
			currentIP, err := s.repository.GetServerIP(ctx, common.CallServerKey(uuid.UUID(tid)))
			if err != nil || currentIP != ip {
				return
			}
			_ = s.repository.RemoveServerIP(ctx, common.CallServerKey(uuid.UUID(tid)))
		}()
	}
	wg.Wait()
}
