import https from 'node:https';

import axios from 'axios';
import * as XLSX from 'xlsx';

import { config } from '../config';


interface ExcelRow {
  [key: string]: string | number;
}

interface OpenAIResponse {
  choices: Array<{
    message: {
      content: string;
    };
  }>;
}

export class ExcelAIService {
  private async processQuestionWithPrompt(question: string, prompt?: string): Promise<string> {
    try {
      // Формируем сообщение с учетом промпта
      const content = prompt
        ? `${question}\n\nДополнительный контекст: ${prompt}`
        : question;

      const axiosConfig = {
        headers: config.openAI.headers,
        httpsAgent: new https.Agent({
          rejectUnauthorized: false, // Отключаем проверку сертификата для локальной разработки
        }),
      };

      console.log('Sending request to OpenAI with config:', {
        url: `${config.openAI.baseURL}/chat/completions`,
        headers: config.openAI.headers,
        model: config.openAI.model,
        content,
      });

      const response = await axios.post<OpenAIResponse>(
        `${config.openAI.baseURL}/chat/completions`,
        {
          model: config.openAI.model,
          messages: [
            { role: 'user', content },
          ],
        },
        axiosConfig,
      );

      if (!response.data.choices || response.data.choices.length === 0) {
        throw new Error('No response choices received from AI');
      }

      const answer = response.data.choices[0].message.content;
      console.log('Received answer from AI:', answer);
      return answer;

      // eslint-disable-next-line @typescript-eslint/no-explicit-any
    } catch (error: any) {
      console.error('Error processing question:', {
        error: error.message,
        status: error.response?.status,
        statusText: error.response?.statusText,
        data: error.response?.data,
      });
      throw error;
    }
  }

  public async processExcelFile(fileBuffer: Buffer): Promise<Buffer> {
    const workbook = XLSX.read(fileBuffer);
    const worksheet = workbook.Sheets[workbook.SheetNames[0]];
    const jsonData = XLSX.utils.sheet_to_json<ExcelRow>(worksheet);

    console.log('Processing Excel file with rows:', jsonData.length);

    for (const row of jsonData) {
      const question = row[config.excel.questionColumn];
      const prompt = row[config.excel.promptColumn];

      if (question && typeof question === 'string') {
        console.log(`Processing question: ${question}`);
        if (prompt) {
          console.log(`With prompt: ${prompt}`);
        }

        try {
          const answer = await this.processQuestionWithPrompt(
            question,
            typeof prompt === 'string' ? prompt : undefined,
          );
          row[config.excel.answerColumn] = answer;
          console.log(`Answer received: ${answer}`);
        } catch (error) {
          console.error(`Failed to process question: ${question}`, error);
          row[config.excel.answerColumn] = 'Error processing question';
        }

        // Добавляем небольшую задержку между запросами
        await new Promise(resolve => setTimeout(resolve, 1000));
      }
    }

    const newWorksheet = XLSX.utils.json_to_sheet(jsonData);
    const newWorkbook = XLSX.utils.book_new();
    XLSX.utils.book_append_sheet(newWorkbook, newWorksheet, 'Sheet1');

    return XLSX.write(newWorkbook, { type: 'buffer', bookType: 'xlsx' });
  }
}
