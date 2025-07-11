import { Request, Response } from 'express';

import { ProgressManager } from './ProgressManager';

export const progressMiddleware = (req: Request, res: Response) => {
  const taskId = req.params.taskId;
  const progressManager = ProgressManager.getInstance();

  // Настройка SSE
  res.setHeader('Content-Type', 'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache');
  res.setHeader('Connection', 'keep-alive');
  res.setHeader('Access-Control-Allow-Origin', '*');

  // Отправка текущего прогресса сразу при подключении
  const currentProgress = progressManager.getProgress(taskId);
  if (currentProgress) {
    res.write(`data: ${JSON.stringify(currentProgress)}\n\n`);
  }

  // Подписка на обновления прогресса
  const unsubscribe = progressManager.subscribeToProgress(taskId, (data) => {
    res.write(`data: ${JSON.stringify(data)}\n\n`);
        
    // Если процесс завершен или произошла ошибка, закрываем соединение
    if (data.status === 'completed' || data.status === 'error') {
      res.end();
    }
  });

  // Очистка при закрытии соединения
  req.on('close', () => {
    unsubscribe();
  });
}; 