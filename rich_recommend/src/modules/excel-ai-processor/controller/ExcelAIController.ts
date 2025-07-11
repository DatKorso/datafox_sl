import { Request, Response } from 'express';

import { ExcelAIService } from '../service/ExcelAIService';

export class ExcelAIController {
    private excelAIService: ExcelAIService;

    constructor() {
      this.excelAIService = new ExcelAIService();
    }

    public async processExcel(req: Request, res: Response): Promise<void> {
      try {
        if (!req.file) {
          res.status(400).json({ message: 'No file uploaded' });
          return;
        }

        const processedBuffer = await this.excelAIService.processExcelFile(req.file.buffer);

        res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
        res.setHeader('Content-Disposition', 'attachment; filename=processed_file.xlsx');
        res.send(processedBuffer);
      } catch (error) {
        console.error('Error processing Excel file:', error);
        res.status(500).json({ 
          message: 'Error processing Excel file',
          error: error instanceof Error ? error.message : 'Unknown error',
        });
      }
    }
} 