package repository

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"task-worker/internal/domain"
)

// ErrNotFound используется для обозначения отсутствия записи в БД.
// Это позволяет Handler-у не зависеть от конкретной реализации БД (sql.ErrNoRows).
var ErrNotFound = errors.New("task not found")

// TaskRepository описывает контракт взаимодействия с хранилищем задач.
type TaskRepository interface {
	CreateTables(ctx context.Context) error
	InsertTask(ctx context.Context, t domain.Task) error
	UpdateTaskStatus(ctx context.Context, id string, status string, errorMsg string, retriesLeft int) error
	GetTask(ctx context.Context, id string) (domain.Task, error)
	GetPendingTasks(ctx context.Context) ([]domain.Task, error)
}

// SQLiteTaskRepository - реализация репозитория для SQLite.
type SQLiteTaskRepository struct {
	db *sql.DB
}

// NewSQLiteTaskRepository создает новый экземпляр репозитория.
func NewSQLiteTaskRepository(db *sql.DB) *SQLiteTaskRepository {
	return &SQLiteTaskRepository{db: db}
}

// CreateTables создает таблицу задач, если она не существует.
func (r *SQLiteTaskRepository) CreateTables(ctx context.Context) error {
	log.Println("Repository: Creating 'tasks' table...")
	query := `
	CREATE TABLE IF NOT EXISTS tasks (
		id TEXT PRIMARY KEY,
		type TEXT NOT NULL,
		payload BLOB,
		status TEXT NOT NULL,
		retries_left INTEGER NOT NULL,
		error_msg TEXT,
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
	);`
	_, err := r.db.ExecContext(ctx, query)
	if err != nil {
		return fmt.Errorf("repository: failed to create tasks table: %w", err)
	}
	return nil
}

// InsertTask сохраняет новую задачу в базу данных.
func (r *SQLiteTaskRepository) InsertTask(ctx context.Context, t domain.Task) error {
	query := `INSERT INTO tasks (id, type, payload, status, retries_left) VALUES (?, ?, ?, ?, ?)`
	_, err := r.db.ExecContext(ctx, query, t.ID, t.Type, t.Payload, t.Status, t.RetriesLeft)
	if err != nil {
		return fmt.Errorf("repository: failed to insert task: %w", err)
	}
	return nil
}

// UpdateTaskStatus обновляет текущее состояние задачи.
func (r *SQLiteTaskRepository) UpdateTaskStatus(ctx context.Context, id string, status string, errorMsg string, retriesLeft int) error {
	query := `UPDATE tasks SET status = ?, error_msg = ?, retries_left = ? WHERE id = ?`
	res, err := r.db.ExecContext(ctx, query, status, errorMsg, retriesLeft, id)
	if err != nil {
		return fmt.Errorf("repository: failed to update task status: %w", err)
	}

	// Если ни одна строка не была обновлена, значит задачи с таким ID нет
	rowsAffected, _ := res.RowsAffected()
	if rowsAffected == 0 {
		return ErrNotFound
	}
	return nil
}

// GetTask возвращает задачу по её ID.
func (r *SQLiteTaskRepository) GetTask(ctx context.Context, id string) (domain.Task, error) {
	var t domain.Task
	query := `SELECT id, type, payload, status, retries_left, error_msg, created_at FROM tasks WHERE id = ?`
	row := r.db.QueryRowContext(ctx, query, id)

	err := row.Scan(&t.ID, &t.Type, &t.Payload, &t.Status, &t.RetriesLeft, &t.ErrorMsg, &t.CreatedAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			// Возвращаем нашу доменную ошибку вместо ошибки драйвера БД
			return domain.Task{}, ErrNotFound
		}
		return domain.Task{}, fmt.Errorf("repository: failed scanning task: %w", err)
	}
	return t, nil
}

// GetPendingTasks предназначен для восстановления работы после сбоя (заглушка для практики).
func (r *SQLiteTaskRepository) GetPendingTasks(ctx context.Context) ([]domain.Task, error) {
	return nil, nil
}
