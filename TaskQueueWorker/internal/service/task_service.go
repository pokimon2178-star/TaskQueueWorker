package service

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"task-worker/internal/domain"
	"time"
)

// ErrNotFound и другие доменные ошибки
var (
	ErrNotFound = errors.New("task not found")
)

type TaskService interface {
	ExecuteTask(ctx context.Context, t domain.Task) error
}

type TaskServiceImpl struct {
}

func NewTaskService() *TaskServiceImpl {
	return &TaskServiceImpl{}
}

// ExecuteTask - имитация выполнения задачи
func (s *TaskServiceImpl) ExecuteTask(ctx context.Context, t domain.Task) error {
	log.Printf("Service: START executing task ID: %s, Type: %s", t.ID, t.Type)

	// Выбираем таймаут для имитации работы
	var workDuration time.Duration
	var shouldFail bool

	switch t.Type {
	case "EMAIL_SEND":
		// Быстрая задача (2-5 сек)
		workDuration = time.Duration(2+rand.Intn(4)) * time.Second
		shouldFail = rand.Intn(10) < 3 // 30% шанс на ошибку
	case "IMAGE_PROCESS":
		// Долгая задача (5-10 сек)
		workDuration = time.Duration(5+rand.Intn(6)) * time.Second
		shouldFail = rand.Intn(10) < 1 // 10% шанс на ошибку
	default:
		return fmt.Errorf("unknown task type: %s", t.Type)
	}

	// Имитация работы
	select {
	case <-time.After(workDuration):
		// Работа завершена
	case <-ctx.Done():
		// Задача отменена (например, из-за таймаута)
		log.Printf("Service: Task %s was cancelled via context.", t.ID)
		return ctx.Err()
	}

	if shouldFail && t.RetriesLeft > 0 {
		// Имитация временной ошибки, которая может быть исправлена повторной попыткой
		log.Printf("Service: Task %s FAILED (Temporary error). Retries left: %d", t.ID, t.RetriesLeft)
		return errors.New("simulated temporary network failure")
	}

	if shouldFail && t.RetriesLeft == 0 {
		// Имитация фатальной ошибки
		log.Printf("Service: Task %s FAILED (Fatal error, no retries left).", t.ID)
		return errors.New("simulated fatal configuration error")
	}

	log.Printf("Service: Task %s completed successfully in %s", t.ID, workDuration)
	return nil
}
