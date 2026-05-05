package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	_ "modernc.org/sqlite" // SQLite драйвер

	"task-worker/internal/handler"
	"task-worker/internal/repository"
	"task-worker/internal/service"
	"task-worker/internal/worker"
)

const (
	WorkerCount      = 4   // Количество воркеров
	JobChannelBuffer = 100 // Размер буфера канала задач
	ServerPort       = "8080"
	DBPath           = "./tasks.db"
)

func main() {
	// 1. Инициализация БД
	db, err := sql.Open("sqlite", DBPath)
	if err != nil {
		log.Fatalf("FATAL: Ошибка открытия БД: %v", err)
	}
	defer db.Close()
	log.Println("DB connection established.")

	// 2. Сборка Зависимостей (Dependency Injection)
	ctx := context.Background()

	// Repository:
	taskRepo := repository.NewSQLiteTaskRepository(db)
	if err := taskRepo.CreateTables(ctx); err != nil {
		log.Fatalf("FATAL: Ошибка создания таблиц: %v", err)
	}

	// Service:
	taskService := service.NewTaskService()

	// Worker Manager:
	manager := worker.NewWorkerManager(taskRepo, taskService, WorkerCount, JobChannelBuffer)
	manager.StartWorkers() // Запускаем горутины воркеров

	// Handler:
	h := handler.NewHandler(taskRepo, manager)

	// 3. Настройка Роутера
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/v1/tasks", h.PostTaskHandler)          // Постановка задачи
	mux.HandleFunc("GET /api/v1/tasks/{id}", h.GetTaskStatusHandler) // Проверка статуса

	server := &http.Server{
		Addr:    fmt.Sprintf(":%s", ServerPort),
		Handler: mux,
	}

	// 4. Запуск Сервера в отдельной горутине
	go func() {
		log.Printf("HTTP Server listening on port %s...", ServerPort)
		if err := server.ListenAndServe(); err != http.ErrServerClosed {
			log.Fatalf("FATAL: HTTP server ListenAndServe: %v", err)
		}
	}()

	// 5. Graceful Shutdown
	// Создаем канал для получения сигналов ОС
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM) // Наблюдаем за Ctrl+C и SIGTERM

	// Блокировка: ожидаем сигнал
	sig := <-sigCh

	log.Printf("Received signal: %s. Initiating graceful shutdown...", sig)

	// A. Останавливаем воркеры
	manager.StopWorkers()

	// B. Останавливаем HTTP-сервер
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second) // Даем 10 секунд на завершение HTTP
	defer cancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Printf("WARNING: HTTP Server forced to shutdown: %v", err)
	}

	log.Println("Application shutdown complete.")
}
