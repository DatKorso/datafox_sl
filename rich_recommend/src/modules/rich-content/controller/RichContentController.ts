import { Request, Response } from 'express';

import { RichContentService } from '../service/RichContentService';

export class RichContentController {
    public richContentService: RichContentService;

    constructor() {
      this.richContentService = new RichContentService();
    }

    async generateRichContent(req: Request, res: Response): Promise<void> {
      try {
        res.writeHead(200, {
          'Content-Type': 'text/event-stream',
          'Cache-Control': 'no-cache',
          'Connection': 'keep-alive',
        });

        await this.richContentService.generateRichContent((progress, message) => {
          res.write(`data: ${JSON.stringify({ progress, message })}\n\n`);
        });

        res.end();
      } catch (error) {
        console.error('Error in rich content generation:', error);
        res.status(500).json({ 
          message: 'Ошибка при генерации rich-контента',
          error: error instanceof Error ? error.message : 'Unknown error',
        });
      }
    }
} 