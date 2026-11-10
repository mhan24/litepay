package store

import (
	"context"
	"fmt"
	"litepay/internal/model"
	"path/filepath"
	"testing"
	"time"
)

func TestHistoricalSchedulingBoundsAllQueuesAcrossRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "fair.db")
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	now := time.Now()
	insert := func(provider string, priority int) model.HistoricalScan {
		q := model.HistoricalScan{Provider: provider, Receiver: "receiver", Priority: priority, Automatic: priority != 1, From: now.Add(-time.Hour), To: now, Cursor: now.Add(-time.Hour), State: "pending", Operator: "test", Reason: "fairness", UpdatedAt: now}
		if err := s.DB().Create(&q).Error; err != nil {
			t.Fatal(err)
		}
		return q
	}
	manual := insert("binance", 1)
	routine := insert("binance", 0)
	manualSteps := 0
	routineSteps := 0
	sequence := []int{}
	for step := range 16 {
		insert("binance", 2) // Continuous replenishment, even after all final tasks complete.
		q, e := s.NextHistoricalScan(ctx, "binance")
		if e != nil {
			t.Fatal(e)
		}
		sequence = append(sequence, q.Priority)
		if q.ID == manual.ID {
			manualSteps++
			if step%8 != 3 {
				t.Fatal("manual slot missed", step)
			}
		}
		if q.ID == routine.ID {
			routineSteps++
			if step%8 != 7 {
				t.Fatal("routine slot missed", step)
			}
		}
		// A manual scan takes two pages; continued final arrivals cannot prevent completion.
		if q.ID == manual.ID && manualSteps == 2 || q.ID == routine.ID && routineSteps == 2 || q.Priority == 2 {
			s.DB().Model(&q).Update("state", "completed")
		} else {
			s.DB().Model(&q).Update("updated_at", time.Now())
		}
		// Do not allow restarting the service to reset final-task preference.
		if err = s.Close(); err != nil {
			t.Fatal(err)
		}
		s, e = Open(path)
		if e != nil {
			t.Fatal(e)
		}
	}
	defer s.Close()
	if manualSteps != 2 || routineSteps != 2 {
		t.Fatal(sequence)
	}
	other := insert("okx", 2)
	q, err := s.NextHistoricalScan(ctx, "okx")
	if err != nil || q.ID != other.ID {
		t.Fatal(q, err)
	}
	t.Log(fmt.Sprint("dispatch priorities: ", sequence, "; manual completes within 12 dispatches under continuous final work"))
}
