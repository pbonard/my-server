package service

import (
	"backend/common"
	"backend/common/proto"
	"bytes"
	"context"
	"log"
	"log/slog"
	"slices"
	"sync"
	"time"

	"github.com/google/uuid"
)

func (s *service) RelayMessage(
	ctx context.Context,
	id uuid.UUID,
	toIds [][]byte,
	roomId, fromId uuid.UUID,
	contentType string,
	contents []string,
	noPush bool,
) error {
	var wg sync.WaitGroup
	relayToIdsByIP := make(map[string][][]byte)
	var rm sync.Mutex
	var pushToIds [][]byte
	var pm sync.Mutex
	log.Printf("toIds: %v", toIds)
	log.Printf("roomId: %v", roomId[:])
	log.Printf("fromId: %v", fromId[:])
	for _, tid := range toIds {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctxr, cancel := context.WithTimeout(ctx, 1*time.Second)
			defer cancel()
			ips, err := s.repository.GetServerIPs(ctxr, string(tid))
			if (err != nil || len(ips) == 0) && !bytes.Equal(tid, fromId[:]) && contentType != "quit" && contentType != "participate" && contentType != "create" {
				pm.Lock()
				pushToIds = append(pushToIds, tid)
				pm.Unlock()
				return
			}
			rm.Lock()
			for _, ip := range ips {
				log.Printf("ip: %v", ip)
				relayToIdsByIP[ip] = append(relayToIdsByIP[ip], tid)
			}
			rm.Unlock()
		}()
	}
	wg.Wait()
	log.Printf("pushToId: %v, relayToId: %v", pushToIds, relayToIdsByIP)

	// no one is pushed about it, so the offline ones get it when they connect and read the recent messages
	if pushToIds != nil && !noPush {
		p := common.Marshal(common.PreparedMessage{
			NotificationId: 0,
			Id:             id[:],
			ToIds:          pushToIds,
			RoomId:         roomId[:],
			FromId:         fromId[:],
			ContentType:    contentType,
			Contents:       contents,
		})
		err := s.producer.Commit("notification", nil, p, nil)
		if err != nil {
			return err
		}
		pushToIds = nil
	}

	for ip, tids := range relayToIdsByIP {
		wg.Add(1)
		go func() {
			defer wg.Done()
			log.Printf("relay start ip: %v, tids: %v", ip, tids)
			req := proto.RelayMessagingRequest{
				Id:          id[:],
				ToIds:       tids,
				RoomId:      roomId[:],
				FromId:      fromId[:],
				ContentType: contentType,
				Contents:    contents,
			}
			ctxt, cancel := context.WithTimeout(ctx, time.Second*5)
			defer cancel()
			undelivered, err := s.relayClient.Do(ctxt, ip, &req)
			if err != nil {
				slog.Error("fail to relay messaging", "err", err)
				if contentType != "quit" && contentType != "participate" && contentType != "create" {
					pm.Lock()
					pushToIds = append(pushToIds, tids...)
					pm.Unlock()
				}
				s.removeStaleIPs(tids, ip)
				return
			}
			if len(undelivered) == 0 {
				return
			}
			if contentType != "quit" && contentType != "participate" && contentType != "create" {
				slog.Info("add push id", "undelivered", undelivered)
				pm.Lock()
				pushToIds = append(pushToIds, undelivered...)
				pm.Unlock()
			}
			s.removeStaleIPs(undelivered, ip)
		}()
	}
	wg.Wait()

	var filteredIds [][]byte
	for _, ptid := range pushToIds {
		if !bytes.Equal(ptid, fromId[:]) {
			slog.Info("add push id", "tid", uuid.UUID(ptid))
			filteredIds = append(filteredIds, ptid)
		}
	}

	if filteredIds != nil && !noPush {
		p := common.Marshal(common.PreparedMessage{
			NotificationId: 1,
			Id:             id[:],
			ToIds:          filteredIds,
			RoomId:         roomId[:],
			FromId:         fromId[:],
			ContentType:    contentType,
			Contents:       contents,
		})
		return s.producer.Commit("notification", nil, p, nil)
	}
	return nil
}

// removeStaleIPs forgets ip for the members that are not connected there anymore
func (s *service) removeStaleIPs(tids [][]byte, ip string) {
	var wg sync.WaitGroup
	for _, tid := range tids {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s.checkAndRemoveStaleIP(tid, ip)
		}()
	}
	wg.Wait()
}

func (s *service) checkAndRemoveStaleIP(tid []byte, ip string) {
	ctxr, cancel1 := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel1()
	currentIPs, err1 := s.repository.GetServerIPs(ctxr, string(tid))
	if err1 != nil {
		return
	}
	if slices.Contains(currentIPs, ip) {
		err1 = s.repository.RemoveServerIP(ctxr, string(tid), ip)
		if err1 != nil {
			return
		}
	}
}
