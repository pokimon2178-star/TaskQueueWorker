package handler

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"task-worker/internal/domain"
	"task-worker/internal/repository"
	"task-worker/internal/service"
	"task-worker/internal/worker"

	"github.com/google/uuid"
)

// Handler - структура для DI
type Handler struct {
	Repo    repository.TaskRepository
	Manager *worker.WorkerManager
}

func NewHandler(repo repository.TaskRepository, manager *worker.WorkerManager) *Handler {
	return &Handler{Repo: repo, Manager: manager}
}

// sendJSON - Хелпер для отправки JSON-ответа
func sendJSON(w http.ResponseWriter, status int, payload interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(payload); err != nil {
		log.Printf("Error encoding JSON response: %v", err)
	}
}

// PostTaskHandler - API для постановки задачи в очередь
func (h *Handler) PostTaskHandler(w http.ResponseWriter, r *http.Request) {
	var req domain.TaskRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		sendJSON(w, http.StatusBadRequest, domain.APIResponse{Status: "error", Error: "Ошибка парсинга JSON"})
		return
	}

	// 1. Создание объекта задачи
	newTask := domain.Task{
		ID:          uuid.New().String(),
		Type:        req.Type,
		Payload:     req.Data,
		Status:      domain.StatusPending,
		RetriesLeft: domain.MaxRetries,
	}

	// 2. Сохранение задачи в БД
	ctx := r.Context()
	if err := h.Repo.InsertTask(ctx, newTask); err != nil {
		log.Printf("Handler: ERROR inserting task %s: %v", newTask.ID, err)
		sendJSON(w, http.StatusInternalServerError, domain.APIResponse{Status: "error", Error: "Ошибка БД при создании задачи"})
		return
	}

	// 3. Отправка задачи в канал (Queue)
	select {
	case h.Manager.JobChannel <- newTask:
		// Задача успешно поставлена в очередь
		log.Printf("Handler: Task %s (%s) accepted and queued.", newTask.ID, newTask.Type)
		response := domain.TaskStatusResponse{
			ID:     newTask.ID,
			Status: domain.StatusPending,
		}
		// 202 Accepted - ответ мгновенный, работа будет выполнена позже
		sendJSON(w, http.StatusAccepted, domain.APIResponse{Status: "accepted", Data: response})
	default:
		// Очередь переполнена
		errMsg := "Worker queue is full, try again later."
		log.Printf("Handler: ERROR queue is full for task %s", newTask.ID)
		sendJSON(w, http.StatusServiceUnavailable, domain.APIResponse{Status: "error", Error: errMsg})
		// В реальной системе здесь можно удалить задачу из БД
	}
}

// GetTaskStatusHandler - API для проверки статуса задачи
func (h *Handler) GetTaskStatusHandler(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if id == "" {
		sendJSON(w, http.StatusBadRequest, domain.APIResponse{Status: "error", Error: "Не указан ID задачи"})
		return
	}

	ctx := r.Context()
	task, err := h.Repo.GetTask(ctx, id)
	if err != nil {
		if errors.Is(err, repository.ErrNotFound) || errors.Is(err, service.ErrNotFound) {
			sendJSON(w, http.StatusNotFound, domain.APIResponse{Status: "error", Error: fmt.Sprintf("Задача с ID %s не найдена", id)})
			return
		}
		log.Printf("Handler: ERROR getting task %s from repo: %v", id, err)
		sendJSON(w, http.StatusInternalServerError, domain.APIResponse{Status: "error", Error: "Ошибка БД при получении статуса"})
		return
	}

	response := domain.TaskStatusResponse{
		ID:     task.ID,
		Status: task.Status,
	}
	if task.Status == domain.StatusFailed {
		response.Error = task.ErrorMsg
	}

	sendJSON(w, http.StatusOK, domain.APIResponse{Status: "ok", Data: response})
}
