package worker

import (
	"context"
	"log"
	"sync"
	"task-worker/internal/domain"
	"task-worker/internal/repository"
	"task-worker/internal/service"
)

// WorkerManager - структура для управления пулом воркеров
type WorkerManager struct {
	Repo        repository.TaskRepository
	Service     service.TaskService
	JobChannel  chan domain.Task // Канал, в который Producer добавляет задачи
	workerCount int
	wg          sync.WaitGroup // Группа ожидания для Graceful Shutdown
	ctx         context.Context
	cancel      context.CancelFunc
}

func NewWorkerManager(repo repository.TaskRepository, svc service.TaskService, workerCount int, jobChannelSize int) *WorkerManager {
	ctx, cancel := context.WithCancel(context.Background())
	return &WorkerManager{
		Repo:        repo,
		Service:     svc,
		JobChannel:  make(chan domain.Task, jobChannelSize),
		workerCount: workerCount,
		ctx:         ctx,
		cancel:      cancel,
	}
}

// StartWorkers - запускает пул воркеров в горутинах
func (m *WorkerManager) StartWorkers() {
	log.Printf("Manager: Starting %d worker goroutines...", m.workerCount)
	for i := 1; i <= m.workerCount; i++ {
		m.wg.Add(1) // Увеличиваем счетчик перед запуском
		go m.workerLoop(i)
	}
}

// workerLoop - основная функция воркера, работающая в горутине
func (m *WorkerManager) workerLoop(id int) {
	defer m.wg.Done() // Уменьшаем счетчик при выходе (в т.ч. при панике)
	log.Printf("Worker #%d started.", id)

	for {
		select {
		case task, ok := <-m.JobChannel:
			// 1. Получена новая задача из канала
			if !ok {
				log.Printf("Worker #%d: JobChannel closed. Exiting.", id)
				return // Канал закрыт, выходим из цикла
			}
			m.processTask(task, id)

		case <-m.ctx.Done():
			// 2. Получен сигнал отмены (от Graceful Shutdown)
			log.Printf("Worker #%d: Context cancelled. Exiting.", id)
			return // Выходим из цикла
		}
	}
}

// processTask - логика обработки одной задачи
func (m *WorkerManager) processTask(task domain.Task, workerID int) {
	log.Printf("Worker #%d: Processing task %s (Type: %s, Retries: %d)", workerID, task.ID, task.Type, task.RetriesLeft)
	ctx := context.Background() // Воркеры используют свой контекст для работы с БД

	// Обновляем статус в БД на PROCESSING
	err := m.Repo.UpdateTaskStatus(ctx, task.ID, domain.StatusProcessing, "", task.RetriesLeft)
	if err != nil {
		log.Printf("Worker #%d: ERROR updating status to PROCESSING for task %s: %v", workerID, task.ID, err)
		return
	}

	// Выполнение бизнес-логики
	taskErr := m.Service.ExecuteTask(m.ctx, task) // Используем менеджерский контекст для отмены

	if taskErr != nil {
		// Ошибка выполнения
		task.ErrorMsg = taskErr.Error()
		if task.RetriesLeft > 0 {
			// Повторная попытка: уменьшаем счетчик и возвращаем в очередь
			task.RetriesLeft--
			m.JobChannel <- task // ВАЖНО: возвращаем задачу в очередь для повторной попытки
			log.Printf("Worker #%d: Task %s failed. Returning to queue. Retries left: %d", workerID, task.ID, task.RetriesLeft)

			// Обновляем статус в БД на PENDING (снова)
			m.Repo.UpdateTaskStatus(ctx, task.ID, domain.StatusPending, task.ErrorMsg, task.RetriesLeft)
			return
		} else {
			// Фатальная ошибка
			log.Printf("Worker #%d: Task %s FAILED fatally. No retries left.", workerID, task.ID)
			m.Repo.UpdateTaskStatus(ctx, task.ID, domain.StatusFailed, task.ErrorMsg, 0)
			return
		}
	}

	// Успешное выполнение
	m.Repo.UpdateTaskStatus(ctx, task.ID, domain.StatusDone, "", 0)
	log.Printf("Worker #%d: Task %s COMPLETED.", workerID, task.ID)
}

// StopWorkers - инициирует корректное завершение работы
func (m *WorkerManager) StopWorkers() {
	log.Println("Manager: Stopping workers... Sending cancel signal.")

	// 1. Отправляем сигнал отмены всем воркерам через контекст
	m.cancel()

	// 2. Закрываем канал, чтобы новые задачи не добавлялись
	// и чтобы воркеры вышли из select-блока, где они читают канал
	close(m.JobChannel)

	// 3. Блокируем выполнение, пока все воркеры не завершат работу (wg.Done())
	log.Println("Manager: Waiting for active workers to finish current jobs...")
	m.wg.Wait()
	log.Println("Manager: All workers stopped successfully.")
}
