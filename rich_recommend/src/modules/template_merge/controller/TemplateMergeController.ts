import { Request, Response } from 'express';

import { ProgressManager } from '../../progress/ProgressManager';
import { TemplateMergeService } from '../service/TemplateMergeService';

export class TemplateMergeController {
    private templateMergeService: TemplateMergeService;
    private progressManager: ProgressManager;

    constructor() {
      this.templateMergeService = new TemplateMergeService();
      this.progressManager = ProgressManager.getInstance();
    }

    public async mergeTemplates(req: Request, res: Response): Promise<void> {
      try {
        console.log('Starting template merge process');
        const files = req.files as { [fieldname: string]: Express.Multer.File[] };
            
        if (!files) {
          console.error('No files received');
          res.status(400).json({ message: 'Необходимо загрузить файлы' });
          return;
        }

        console.log('Files received:', {
          file1: files.file1?.length > 0,
          file2: files.file2?.length > 0,
        });
            
        if (!files.file1 || !files.file2) {
          const error = `Отсутствует ${files.file1 ? 'второй' : 'первый'} файл`;
          console.error(error);
          res.status(400).json({ message: error });
          return;
        }

        const file1 = files.file1[0];
        const file2 = files.file2[0];

        console.log('File details:', {
          file1: {
            originalname: file1.originalname,
            mimetype: file1.mimetype,
            size: file1.size,
          },
          file2: {
            originalname: file2.originalname,
            mimetype: file2.mimetype,
            size: file2.size,
          },
        });

        this.progressManager.startProgress('templateMerge');

        try {
          const resultBuffer = await this.templateMergeService.mergeExcelFiles(
            file1.buffer,
            file2.buffer,
            (progress: number, message?: string) => {
              this.progressManager.updateProgress('templateMerge', { progress, message });
            },
          );

          console.log('Files successfully merged');
          this.progressManager.completeProgress('templateMerge', 'Объединение файлов завершено');

          res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
          res.setHeader('Content-Disposition', 'attachment; filename=merged_template.xlsx');
          res.send(resultBuffer);
          console.log('Result sent to client');

        } catch (error) {
          console.error('Error in mergeExcelFiles:', error);
          const errorMessage = error instanceof Error ? error.message : 'Неизвестная ошибка';
          this.progressManager.errorProgress('templateMerge', errorMessage);
          res.status(500).json({ 
            message: 'Ошибка при объединении файлов', 
            error: errorMessage,
            details: error instanceof Error ? error.stack : undefined,
          });
        }

      } catch (error) {
        console.error('Error in controller:', error);
        const errorMessage = error instanceof Error ? error.message : 'Неизвестная ошибка';
        this.progressManager.errorProgress('templateMerge', errorMessage);
        res.status(500).json({ 
          message: 'Ошибка при обработке запроса', 
          error: errorMessage,
          details: error instanceof Error ? error.stack : undefined,
        });
      }
    }
} 