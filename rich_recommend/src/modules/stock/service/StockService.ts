import * as XLSX from 'xlsx';

import { AppDataSource } from '../../../data-source';
import { StockItem } from '../../../entity/StockItem';
import { ProgressManager } from '../../progress/ProgressManager';
import { StockRow, ProcessStockResult } from '../dto/stock.dto';

export class StockService {
  private progressManager: ProgressManager;

  constructor() {
    this.progressManager = ProgressManager.getInstance();
  }

  async processStockFile(buffer: Buffer): Promise<ProcessStockResult> {
    try {
      this.progressManager.startProgress('stock');
      this.progressManager.updateProgress('stock', {
        progress: 0,
        message: 'Начало обработки файла остатков',
      });

      // Очистка существующих данных
      await AppDataSource.manager.clear(StockItem);
      this.progressManager.updateProgress('stock', {
        progress: 10,
        message: 'Таблица остатков очищена',
      });

      // Чтение файла Excel
      const workbook = XLSX.read(buffer);
      this.progressManager.updateProgress('stock', {
        progress: 20,
        message: 'Файл успешно прочитан',
      });

      const sheetName = 'Товар-склад';
      const sheet = workbook.Sheets[sheetName];

      if (!sheet) {
        throw new Error(`Лист "${sheetName}" не найден в файле. Убедитесь, что файл содержит лист с названием "Товар-склад".`);
      }

      // Получаем все данные из листа как массив массивов
      const rawDataArray = XLSX.utils.sheet_to_json<string[]>(sheet, { header: 1, raw: false });
      
      if (rawDataArray.length < 5) {
        throw new Error('Файл имеет неверный формат: недостаточно строк для обработки (минимум 5 строк)');
      }

      // Получаем заголовки из 3-й строки (индекс 2)
      const headers = rawDataArray[2];
      
      // Ищем индексы необходимых столбцов
      const columnMapping = {
        'SKU': headers.indexOf('SKU'),
        'Артикул': headers.indexOf('Артикул'),
        'Доступно к продаже': headers.indexOf('Доступно к продаже'),
      };

      // Проверяем наличие всех необходимых столбцов
      const missingColumns = Object.entries(columnMapping)
        .filter(([, index]) => index === -1)
        .map(([name]) => name);

      if (missingColumns.length > 0) {
        throw new Error(`Не найдены обязательные столбцы: ${missingColumns.join(', ')}`);
      }

      // Преобразуем данные в массив объектов, начиная с 5-й строки (пропускаем 4-ю строку)
      const rawData = rawDataArray.slice(4)
        .filter(row => row.some(cell => cell !== undefined && cell !== '')) // Фильтруем пустые строки
        .map(row => {
          const item: Partial<StockRow> = {};
          Object.entries(columnMapping).forEach(([key, index]) => {
            if (index !== -1) {
              item[key as keyof StockRow] = row[index];
            }
          });
          return item as StockRow;
        });

      if (rawData.length === 0) {
        throw new Error('Файл не содержит данных для обработки');
      }

      this.progressManager.updateProgress('stock', {
        progress: 30,
        message: 'Данные успешно извлечены из файла',
      });

      let loadedCount = 0;
      let errorCount = 0;

      // Группируем данные по артикулам для подсчета общего количества
      const stockByArticle = new Map<string, number>();
      
      // Первый проход - группировка и подсчет общего количества
      for (const item of rawData) {
        // Проверяем, что значения не undefined и не пустые строки
        if (!item.SKU?.toString().trim() || !item['Артикул']?.toString().trim()) {
          console.error('Пропущена запись: отсутствует SKU или Артикул', {
            sku: item.SKU,
            article: item['Артикул'],
          });
          errorCount++;
          continue;
        }

        const available = Number(item['Доступно к продаже']) || 0;
        const currentTotal = stockByArticle.get(item['Артикул']) || 0;
        stockByArticle.set(item['Артикул'], currentTotal + available);
      }

      // Второй проход - сохранение записей с правильным общим количеством
      for (let i = 0; i < rawData.length; i++) {
        try {
          const item = rawData[i];
          
          // Используем ту же проверку, что и в первом проходе
          if (!item.SKU?.toString().trim() || !item['Артикул']?.toString().trim()) {
            continue; // Уже посчитали как ошибку в первом проходе
          }

          const stockItem = new StockItem();
          
          stockItem.sku = item.SKU;
          stockItem.article = item['Артикул'];
          stockItem.article_code = item['Артикул'];
          stockItem.available_for_sale = Number(item['Доступно к продаже']) || 0;
          stockItem.stock = stockByArticle.get(item['Артикул']) || 0; // Общее количество для артикула

          // Устанавливаем пустые значения для обязательных полей
          stockItem.product_flag = '';
          stockItem.warehouse_name = '';
          stockItem.cluster = '';
          stockItem.product_name = '';
          stockItem.in_transit = 0;
          stockItem.reserved = 0;
          stockItem.transfer_rate_per_unit = 0;
          stockItem.total_transfer_rate = 0;
          stockItem.color = '';
          stockItem.size = '';

          await AppDataSource.manager.save(stockItem);
          loadedCount++;

          this.progressManager.updateProgress('stock', {
            progress: 30 + Math.floor((i / rawData.length) * 70),
            message: `Обработано ${loadedCount} записей`,
          });
        } catch (error) {
          errorCount++;
          console.error('Error processing stock item:', error);
        }
      }

      this.progressManager.completeProgress(
        'stock',
        `Загрузка остатков завершена. Обработано: ${loadedCount}, Ошибок: ${errorCount}`,
      );

      return {
        message: 'Остатки успешно загружены',
        details: { loaded: loadedCount, errors: errorCount },
      };
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : 'Неизвестная ошибка';
      this.progressManager.errorProgress('stock', errorMessage);
      throw error;
    }
  }
} 