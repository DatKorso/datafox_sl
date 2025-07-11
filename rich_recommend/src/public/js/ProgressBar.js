// eslint-disable-next-line @typescript-eslint/no-unused-vars
class ProgressBar {
  constructor(containerId) {
    this.container = document.querySelector(`#${containerId}`);
    if (!this.container) {
      console.error(`Container with id ${containerId} not found`);
      return;
    }

    // Добавляем хранение EventSource и taskId
    this.eventSource = null;
    this.currentTaskId = null;
    this.statusCheckInterval = null;
    this.lastUpdateTime = null;
    this.noUpdateTimeout = null;
    this.MAX_NO_UPDATE_TIME = 30_000; // 30 секунд максимальное время без обновлений

    // Создаем структуру прогресс-бара
    this.container.innerHTML = `
            <div class="progress-wrapper">
                <div class="progress-info">
                    <span class="progress-percentage">0%</span>
                    <span class="progress-details"></span>
                </div>
                <div class="progress-bar">
                    <div class="progress"></div>
                </div>
            </div>
        `;

    this.progressElement = this.container.querySelector('.progress');
    this.progressPercentage = this.container.querySelector('.progress-percentage');
    this.progressDetails = this.container.querySelector('.progress-details');
  }

  async checkStatus() {
    if (!this.currentTaskId) return;

    try {
      const response = await fetch(`/progress/${this.currentTaskId}/status`);
      if (response.ok) {
        const data = await response.json();
        
        // Обновляем прогресс на основе полученных данных
        if (data.progress !== undefined) {
          this.updateProgress(data.progress, data.message, data.processed, data.total);
        }

        // Если задача завершена или произошла ошибка
        if (data.status === 'completed' || data.status === 'error') {
          this.stop();
          return true;
        }
      }
    } catch (error) {
      console.error('Error checking status:', error);
    }
    return false;
  }

  checkNoUpdates() {
    if (!this.lastUpdateTime) return;
    
    const timeSinceLastUpdate = Date.now() - this.lastUpdateTime;
    if (timeSinceLastUpdate > this.MAX_NO_UPDATE_TIME) {
      console.warn(`No updates received for ${timeSinceLastUpdate}ms`);
      this.handleTimeout();
    }
  }

  handleTimeout() {
    console.error('Progress timeout - no updates received');
    this.progressDetails.textContent = 'Превышено время ожидания ответа от сервера';
    this.checkStatus().then(isCompleted => {
      if (!isCompleted) {
        this.stop();
      }
    });
  }

  startProgress(taskId) {
    if (!this.container) return;
    
    // Закрываем предыдущее соединение, если оно существует
    this.stop();
    
    this.currentTaskId = taskId;
    this.container.style.display = 'block';
    this.updateProgress(0);
    
    // Устанавливаем время последнего обновления
    this.lastUpdateTime = Date.now();
    
    // Запускаем проверку таймаута
    this.noUpdateTimeout = setInterval(() => {
      this.checkNoUpdates();
    }, 5000);
    
    // Начинаем периодическую проверку статуса
    this.statusCheckInterval = setInterval(() => {
      this.checkStatus();
    }, 5000);
    
    // Подписываемся на события прогресса
    this.eventSource = new EventSource(`/progress/${taskId}`);
    
    this.eventSource.addEventListener('message', (event) => {
      try {
        const data = JSON.parse(event.data);
        if (data.progress !== undefined) {
          // Обновляем время последнего обновления
          this.lastUpdateTime = Date.now();
          
          this.updateProgress(data.progress, data.message, data.processed, data.total);
          
          if (data.progress >= 100) {
            setTimeout(() => {
              this.stop();
            }, 1000);
          }
        }
      } catch (error) {
        console.error('Error parsing progress data:', error);
        this.stop();
      }
    });
    
    this.eventSource.addEventListener('error', () => {
      console.error('EventSource failed');
      // При ошибке EventSource проверяем статус
      this.checkStatus().then(isCompleted => {
        if (!isCompleted) {
          // Если задача не завершена, ждем MAX_NO_UPDATE_TIME перед остановкой
          setTimeout(() => {
            this.checkStatus().then(isStillRunning => {
              if (!isStillRunning) {
                this.handleTimeout();
              }
            });
          }, this.MAX_NO_UPDATE_TIME);
        }
      });
    });
  }

  updateProgress(progress, message = '', processed = null, total = null) {
    if (!this.progressElement || !this.progressPercentage) return;
    
    const percentage = Math.round(progress);
    this.progressElement.style.width = `${percentage}%`;
    this.progressPercentage.textContent = `${percentage}%`;

    // Формируем детальную информацию
    let details = '';
    if (processed !== null && total !== null) {
      details = `Обработано ${processed} из ${total}`;
    }
    if (message) {
      details = details ? `${details} - ${message}` : message;
    }
    
    if (this.progressDetails) {
      this.progressDetails.textContent = details;
    }
  }

  // Добавляем метод для остановки и очистки
  stop() {
    // Очищаем все таймеры
    if (this.statusCheckInterval) {
      clearInterval(this.statusCheckInterval);
      this.statusCheckInterval = null;
    }
    
    if (this.noUpdateTimeout) {
      clearInterval(this.noUpdateTimeout);
      this.noUpdateTimeout = null;
    }

    if (this.eventSource) {
      this.eventSource.close();
      this.eventSource = null;
    }

    this.currentTaskId = null;
    this.lastUpdateTime = null;
    this.container.style.display = 'none';
    
    // Сбрасываем прогресс
    if (this.progressElement) {
      this.progressElement.style.width = '0%';
    }
    if (this.progressPercentage) {
      this.progressPercentage.textContent = '0%';
    }
    if (this.progressDetails) {
      this.progressDetails.textContent = '';
    }
  }
}
