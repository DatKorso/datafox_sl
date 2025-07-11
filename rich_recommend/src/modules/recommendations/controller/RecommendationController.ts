import { Request, Response } from 'express';

import { RecommendationService } from '../service/RecommendationService';

export class RecommendationController {
    private recommendationService: RecommendationService;

    constructor() {
      this.recommendationService = new RecommendationService();
    }

    async generateRecommendations(req: Request, res: Response) {
      res.setHeader('Content-Type', 'text/event-stream');
      res.setHeader('Cache-Control', 'no-cache');
      res.setHeader('Connection', 'keep-alive');

      try {
        await this.recommendationService.generateRecommendations((progress) => {
          res.write(`data: ${JSON.stringify({ progress })}\n\n`);
        });
            
        res.write(`data: ${JSON.stringify({ progress: 100, message: 'Рекомендации успешно сгенерированы' })}\n\n`);
        res.end();
      } catch (error) {
        let errorMessage = 'Неизвестная ошибка при генерации рекомендаций';
        if (error instanceof Error) {
          errorMessage = `Ошибка при генерации рекомендаций: ${error.message}`;
        }
        res.write(`data: ${JSON.stringify({ progress: 0, error: errorMessage })}\n\n`);
        res.end();
      }
    }

    async clearRecommendations(req: Request, res: Response) {
      try {
        await this.recommendationService.clearRecommendations();
        res.json({ message: 'Рекомендации успешно очищены' });
      } catch (error) {
        if (error instanceof Error) {
          res.status(500).json({ message: `Ошибка при очистке рекомендаций: ${error.message}` });
        } else {
          res.status(500).json({ message: 'Неизвестная ошибка при очистке рекомендаций' });
        }
      }
    }
} 