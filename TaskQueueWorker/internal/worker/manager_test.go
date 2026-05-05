package worker

import (
	"context"
	"task-worker/internal/domain"
	"testing"
	"time"
)

// MockService - заглушка сервиса для тестов
type MockService struct {
	Processed bool
}

func (m *MockService) ExecuteTask(ctx context.Context, t domain.Task) error {
	m.Processed = true
	return nil
}

// MockRepo - заглушка репозитория
type MockRepo struct{}

func (m *MockRepo) CreateTables(ctx context.Context) error              { return nil }
func (m *MockRepo) InsertTask(ctx context.Context, t domain.Task) error { return nil }
func (m *MockRepo) UpdateTaskStatus(ctx context.Context, id, status, err string, retries int) error {
	return nil
}
func (m *MockRepo) GetTask(ctx context.Context, id string) (domain.Task, error) {
	return domain.Task{}, nil
}
func (m *MockRepo) GetPendingTasks(ctx context.Context) ([]domain.Task, error) { return nil, nil }

func TestWorkerProcessing(t *testing.T) {
	repo := &MockRepo{}
	svc := &MockService{}
	// Создаем менеджер с 1 воркером
	manager := NewWorkerManager(repo, svc, 1, 10)

	manager.StartWorkers()

	// Отправляем тестовую задачу
	task := domain.Task{ID: "test-1", Type: "EMAIL_SEND"}
	manager.JobChannel <- task

	// Даем немного времени на обработку
	time.Sleep(100 * time.Millisecond)

	if !svc.Processed {
		t.Errorf("Задача не была обработана воркером")
	}

	manager.StopWorkers()
}
