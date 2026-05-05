package domain

import (
	"encoding/json"
	"time"
)

// Константы для статусов задач
const (
	StatusPending    = "PENDING"
	StatusProcessing = "PROCESSING"
	StatusDone       = "DONE"
	StatusFailed     = "FAILED"
	MaxRetries       = 3
)

// Task - основная структура для хранения задачи
type Task struct {
	ID          string    `json:"id"`
	Type        string    `json:"type"` // Тип операции: "EMAIL_SEND", "IMAGE_PROCESS"
	Payload     []byte    `json:"-"`    // Данные, необходимые для выполнения задачи (скрыты в JSON)
	Status      string    `json:"status"`
	RetriesLeft int       `json:"retries_left"`
	ErrorMsg    string    `json:"error_msg"`
	CreatedAt   time.Time `json:"created_at"`
}

// TaskRequest - структура, получаемая от клиента через HTTP
type TaskRequest struct {
	Type string          `json:"type"`
	Data json.RawMessage `json:"data"` // Используем RawMessage для гибкости данных
}

// APIResponse - стандартизированный ответ API
type APIResponse struct {
	Status string      `json:"status"`
	Error  string      `json:"error,omitempty"`
	Data   interface{} `json:"data,omitempty"`
}

// TaskStatusResponse - упрощенный ответ для проверки статуса
type TaskStatusResponse struct {
	ID     string `json:"id"`
	Status string `json:"status"`
	Error  string `json:"error,omitempty"`
}
