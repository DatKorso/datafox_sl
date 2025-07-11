import { EventEmitter } from 'node:events';

export interface ProgressData {
  progress: number;
  message?: string;
  status?: 'running' | 'completed' | 'error';
  error?: string;
}

export class ProgressManager {
    private static instance: ProgressManager;
    private progressMap: Map<string, ProgressData>;
    private eventEmitter: EventEmitter;

    private constructor() {
      this.progressMap = new Map();
      this.eventEmitter = new EventEmitter();
    }

    public static getInstance(): ProgressManager {
      if (!ProgressManager.instance) {
        ProgressManager.instance = new ProgressManager();
      }
      return ProgressManager.instance;
    }

    public startProgress(taskId: string): void {
      this.progressMap.set(taskId, {
        progress: 0,
        status: 'running',
      });
    }

    public updateProgress(taskId: string, data: Partial<ProgressData>): void {
      const currentData = this.progressMap.get(taskId) || {
        progress: 0,
        status: 'running',
      };

      const updatedData = {
        ...currentData,
        ...data,
      };

      this.progressMap.set(taskId, updatedData);
      this.eventEmitter.emit(`progress:${taskId}`, updatedData);
    }

    public completeProgress(taskId: string, message?: string): void {
      this.updateProgress(taskId, {
        progress: 100,
        status: 'completed',
        message,
      });
    }

    public errorProgress(taskId: string, error: string): void {
      this.updateProgress(taskId, {
        status: 'error',
        error,
      });
    }

    public getProgress(taskId: string): ProgressData | undefined {
      return this.progressMap.get(taskId);
    }

    public subscribeToProgress(taskId: string, callback: (data: ProgressData) => void): () => void {
      const eventName = `progress:${taskId}`;
      this.eventEmitter.on(eventName, callback);
      return () => this.eventEmitter.removeListener(eventName, callback);
    }

    public clearProgress(taskId: string): void {
      this.progressMap.delete(taskId);
    }
} 