import * as XLSX from 'xlsx';

interface ProgressCallback {
  (progress: number, message?: string): void;
}

export class TemplateMergeService {
    private readonly SHEETS_TO_MERGE = ['Шаблон', 'Озон.Видео', 'Озон.Видеообложка'];
    private readonly HEADER_ROWS = 3;
    private readonly HEADER_ROW_INDEX = 1; // индекс строки с заголовками (2-я строка)

    public async mergeExcelFiles(
      file1Buffer: Buffer,
      file2Buffer: Buffer,
      progressCallback: ProgressCallback,
    ): Promise<Buffer> {
      try {
        progressCallback(0, 'Начало обработки файлов');
        console.log('Starting file processing');

        // Читаем оба файла
        let workbook1, workbook2;
        try {
          workbook1 = XLSX.read(file1Buffer);
          console.log('First file read successfully');
        } catch (error) {
          console.error('Error reading first file:', error);
          throw new Error('Ошибка при чтении первого файла: ' + (error instanceof Error ? error.message : 'Неизвестная ошибка'));
        }

        try {
          workbook2 = XLSX.read(file2Buffer);
          console.log('Second file read successfully');
        } catch (error) {
          console.error('Error reading second file:', error);
          throw new Error('Ошибка при чтении второго файла: ' + (error instanceof Error ? error.message : 'Неизвестная ошибка'));
        }

        progressCallback(10, 'Файлы успешно прочитаны');

        // Создаем новый workbook для результата
        const mergedWorkbook = XLSX.utils.book_new();
        let currentProgress = 10;
        const totalSheets = Object.keys(workbook1.Sheets).length;
        const progressPerSheet = 80 / totalSheets;

        // Копируем все листы из первого файла
        for (const sheetName in workbook1.Sheets) {
          if (this.SHEETS_TO_MERGE.includes(sheetName)) {
            // Для листов, которые нужно объединить
            console.log(`Processing sheet for merge: ${sheetName}`);
            progressCallback(currentProgress, `Обработка листа "${sheetName}"`);

            const sheet1 = workbook1.Sheets[sheetName];
            const sheet2 = workbook2.Sheets[sheetName];

            if (!sheet1 || !sheet2) {
              const error = `Лист "${sheetName}" отсутствует в ${sheet1 ? 'втором' : 'первом'} файле`;
              console.error(error);
              throw new Error(error);
            }

            try {
              // Получаем заголовки из первого файла
              const headers = this.getHeaders(sheet1);
              console.log('Headers extracted:', headers);

              // Копируем первые три строки из первого файла
              const headerRows = this.getHeaderRows(sheet1);
              console.log(`Header rows extracted from sheet "${sheetName}"`);

              // Получаем данные без заголовков из обоих файлов
              const data1 = XLSX.utils.sheet_to_json(sheet1, { range: this.HEADER_ROWS });
              const data2 = XLSX.utils.sheet_to_json(sheet2, { range: this.HEADER_ROWS });
              console.log(`Data rows count: file1 = ${data1.length}, file2 = ${data2.length}`);

              // Объединяем данные
              const mergedData = [...data1, ...data2];
              console.log(`Total merged rows: ${mergedData.length}`);

              // Создаем новый лист
              const mergedSheet = XLSX.utils.aoa_to_sheet(headerRows);

              // Добавляем объединенные данные
              XLSX.utils.sheet_add_json(mergedSheet, mergedData, {
                skipHeader: true,
                origin: this.HEADER_ROWS,
              });

              // Копируем форматирование и настройки из первого файла
              this.copySheetFormatting(sheet1, mergedSheet);

              // Добавляем лист в новый workbook
              XLSX.utils.book_append_sheet(mergedWorkbook, mergedSheet, sheetName);
              console.log(`Sheet "${sheetName}" successfully merged`);
            } catch (error) {
              console.error(`Error processing sheet "${sheetName}":`, error);
              throw new Error(`Ошибка при обработке листа "${sheetName}": ${error instanceof Error ? error.message : 'Неизвестная ошибка'}`);
            }
          } else {
            // Для остальных листов просто копируем из первого файла
            console.log(`Copying sheet without merge: ${sheetName}`);
            const sheet = workbook1.Sheets[sheetName];
            XLSX.utils.book_append_sheet(mergedWorkbook, sheet, sheetName);
          }

          currentProgress += progressPerSheet;
          progressCallback(currentProgress, `Лист "${sheetName}" обработан`);
        }

        // Создаем буфер с результатом
        console.log('Creating final buffer');
        progressCallback(90, 'Формирование итогового файла');

        try {
          const resultBuffer = XLSX.write(mergedWorkbook, { type: 'buffer', bookType: 'xlsx' });
          console.log('Result buffer created successfully');
          progressCallback(100, 'Объединение файлов завершено');
          return resultBuffer;
        } catch (error) {
          console.error('Error creating result buffer:', error);
          throw new Error('Ошибка при создании итогового файла: ' + (error instanceof Error ? error.message : 'Неизвестная ошибка'));
        }

      } catch (error) {
        console.error('Error in mergeExcelFiles:', error);
        throw error;
      }
    }

    private getHeaders(sheet: XLSX.WorkSheet): string[] {
      try {
        const headerRow = XLSX.utils.sheet_to_json(sheet, { header: 1, range: this.HEADER_ROW_INDEX })[0];
        console.log('Extracted headers:', headerRow);
        return headerRow as string[];
      } catch (error) {
        console.error('Error getting headers:', error);
        throw new Error('Ошибка при получении заголовков: ' + (error instanceof Error ? error.message : 'Неизвестная ошибка'));
      }
    }

    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    private getHeaderRows(sheet: XLSX.WorkSheet): any[][] {
      try {
        return XLSX.utils.sheet_to_json(sheet, { header: 1, range: `A1:${this.getLastColumn(sheet)}${this.HEADER_ROWS}` });
      } catch (error) {
        console.error('Error getting header rows:', error);
        throw new Error('Ошибка при получении строк заголовка: ' + (error instanceof Error ? error.message : 'Неизвестная ошибка'));
      }
    }

    private getLastColumn(sheet: XLSX.WorkSheet): string {
      const range = XLSX.utils.decode_range(sheet['!ref'] || 'A1');
      return XLSX.utils.encode_col(range.e.c);
    }

    private copySheetFormatting(sourceSheet: XLSX.WorkSheet, targetSheet: XLSX.WorkSheet): void {
      try {
        console.log('Copying sheet formatting');

        // Копируем настройки листа
        targetSheet['!ref'] = sourceSheet['!ref'];
        targetSheet['!margins'] = sourceSheet['!margins'];
        targetSheet['!cols'] = sourceSheet['!cols'];
        targetSheet['!rows'] = sourceSheet['!rows'];
        targetSheet['!merges'] = sourceSheet['!merges'];
        targetSheet['!autofilter'] = sourceSheet['!autofilter'];

        // Копируем форматирование ячеек для заголовков
        const range = XLSX.utils.decode_range(sourceSheet['!ref'] || 'A1');
        for (let R = 0; R < this.HEADER_ROWS; R++) {
          for (let C = range.s.c; C <= range.e.c; C++) {
            const sourceCell = XLSX.utils.encode_cell({ r: R, c: C });
            if (sourceSheet[sourceCell]) {
              targetSheet[sourceCell] = { ...sourceSheet[sourceCell] };
            }
          }
        }

        console.log('Sheet formatting copied successfully');
      } catch (error) {
        console.error('Error copying sheet formatting:', error);
        throw new Error('Ошибка при копировании форматирования: ' + (error instanceof Error ? error.message : 'Неизвестная ошибка'));
      }
    }
}
