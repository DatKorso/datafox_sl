import path from 'node:path';

import express, { Request, Response } from 'express';
import multer from 'multer';
import * as XLSX from 'xlsx';

import { AppDataSource } from './data-source';
import { Product } from './entity/Product';
import { ProductSize } from './entity/ProductSize';
import { ProductWork } from './entity/ProductWork';
import { StockItem } from './entity/StockItem';
import { Template } from './entity/Template';
import { ExcelAIController } from './modules/excel-ai-processor';
import { ProgressManager } from './modules/progress/ProgressManager';
import { progressMiddleware } from './modules/progress/progressMiddleware';
import { RecommendationService } from './modules/recommendations';
import { RichContentService } from './modules/rich-content/service/RichContentService';
import { StockService } from './modules/stock/service/StockService';
import { TemplateMergeController } from './modules/template_merge';

// Initialize express app
const app = express();
const progressManager = ProgressManager.getInstance();

// Middleware
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

// Initialize database and start server
AppDataSource.initialize()
  .then(() => {
    console.log('База данных успешно подключена');

    const PORT = 40_286;
    app.listen(PORT, () => {
      console.log(`Сервер запущен на порту ${PORT}`);
    });
  })
  .catch((error: Error) => {
    console.error('Ошибка при подключении к базе данных:', error);
    process.exit(1);
  });

// Configure multer for file upload
const upload = multer({
  storage: multer.memoryStorage(),
  fileFilter: (_req, file, cb: multer.FileFilterCallback) => {
    if (!file) {
      return cb(new Error('Файл не выбран'));
    }

    if (file.mimetype === 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet') {
      return cb(null, true);
    } else {
      return cb(new Error('Неверный формат файла. Пожалуйста, загрузите файл Excel (.xlsx)'));
    }
  },
});

// Middleware для обработки ошибок multer
// eslint-disable-next-line @typescript-eslint/no-explicit-any,@typescript-eslint/ban-types
const handleMulterError = (error: any, req: Request, res: Response, next: Function) => {
  if (error instanceof multer.MulterError) {
    progressManager.errorProgress(req.path.split('/')[2], error.message);
    return res.status(400).json({
      message: 'Ошибка при загрузке файла',
      error: error.message,
    });
  }

  if (error) {
    progressManager.errorProgress(req.path.split('/')[2], error.message);
    return res.status(400).json({
      message: 'Ошибка при загрузке файла',
      error: error.message,
    });
  }

  next();
};

// Interfaces
interface TemplateExcelRow {
  'Артикул*': string;
  'Штрихкод (Серийный номер / EAN)'?: string;
  'Ссылка на главное фото*': string;
  'Бренд в одежде и обуви*': string;
  'Цвет товара*': string;
  'Тип*': string;
  'Пол*': string;
  'Сезон'?: string;
  'Rich-контент JSON'?: string;
  'Материал'?: string;
  'Вид застёжки'?: string;
}

interface AssortmentRow {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  [key: string]: any; // Разрешаем любые ключи, так как Excel может использовать разные имена столбцов
  'Внешний код'?: string;
  'Цвет(основной)'?: string;
  'Новая колодка'?: string;
  'Колодка MEGA'?: string;
  'Колодка BEST'?: string;
  'Бренд'?: string;
  'Предмет'?: string;
  'Распределение по размерам'?: string;
  ''?: string;
  '__EMPTY'?: string;
  '__EMPTY_1'?: string;
  '__EMPTY_2'?: string;
  '__EMPTY_3'?: string;
  '__EMPTY_4'?: string;
  '__EMPTY_5'?: string;
  '#N/A'?: string;
}

// Template Upload Route
app.post('/upload/template', upload.single('file'), handleMulterError, async (req: Request, res: Response) => {
  try {
    if (!req.file) {
      throw new Error('Файл не загружен');
    }

    progressManager.startProgress('template');
    progressManager.updateProgress('template', {
      progress: 0,
      message: 'Начало обработки файла',
    });

    await AppDataSource.manager.clear(Template);
    progressManager.updateProgress('template', {
      progress: 10,
      message: 'Таблица очищена',
    });

    let workbook;
    try {
      workbook = XLSX.read(req.file.buffer);
      progressManager.updateProgress('template', {
        progress: 20,
        message: 'Файл успешно прочитан',
      });
    } catch {
      throw new Error('Ошибка при чтении файла Excel. Возможно, файл поврежден или имеет неверный формат.');
    }

    const sheetName = 'Шаблон';
    const sheet = workbook.Sheets[sheetName];

    if (!sheet) {
      throw new Error(`Лист "${sheetName}" не найден в файле. Убедитесь, что файл содержит лист с названием "Шаблон".`);
    }

    let rawData;
    try {
      rawData = XLSX.utils.sheet_to_json<TemplateExcelRow>(sheet, { range: 1 });
      if (rawData.length === 0) {
        throw new Error('Файл не содержит данных для обработки');
      }
      progressManager.updateProgress('template', {
        progress: 30,
        message: 'Данные успешно извлечены из файла',
      });
    } catch {
      throw new Error('Ошибка при чтении данных из файла. Проверьте структуру файла и формат данных.');
    }

    let loadedCount = 0;
    let errorCount = 0;

    for (let i = 0; i < rawData.length; i++) {
      try {
        const item = rawData[i];
        const barcodes = item['Штрихкод (Серийный номер / EAN)']
          ? String(item['Штрихкод (Серийный номер / EAN)']).split(';').map(code => code.trim()).filter(Boolean)
          : [];

        const fastenerTypes = item['Вид застёжки']
          ? String(item['Вид застёжки']).split(';').map(type => type.trim()).filter(Boolean)
          : [];

        const template = new Template();
        template.article = item['Артикул*'];
        template.barcode = JSON.stringify(barcodes);
        template.main_photo_url = item['Ссылка на главное фото*'];
        template.brand = item['Бренд в одежде и обуви*'];
        template.color = item['Цвет товара*'];
        template.type = item['Тип*'];
        template.gender = item['Пол*'];
        template.season = item['Сезон'] || null;
        template.rich_content = item['Rich-контент JSON'] || null;
        template.material = item['Материал'] || null;
        template.fastener_type = JSON.stringify(fastenerTypes);

        await AppDataSource.manager.save(template);
        loadedCount++;

        progressManager.updateProgress('template', {
          progress: 30 + Math.floor((i / rawData.length) * 70),
          message: `Обработано ${loadedCount} записей`,
        });
      } catch (error) {
        errorCount++;
        console.error('Error processing template item:', error);
      }
    }

    progressManager.completeProgress('template',
      `Загрузка завершена. Обработано: ${loadedCount}, Ошибок: ${errorCount}`,
    );

    res.json({
      message: 'Шаблоны успешно загружены',
      details: { loaded: loadedCount, errors: errorCount },
    });
  } catch (error) {
    const errorMessage = error instanceof Error ? error.message : 'Неизвестная ошибка';
    progressManager.errorProgress('template', errorMessage);
    res.status(500).json({ message: 'Ошибка при обработке файла шаблонов' });
  }
});

// ProductWork Routes
app.post('/calculate-product-work', async (req: Request, res: Response) => {
  try {
    progressManager.startProgress('productWork');
    progressManager.updateProgress('productWork', {
      progress: 0,
      message: 'Начало расчета',
    });

    const templates = await AppDataSource.manager.find(Template);
    progressManager.updateProgress('productWork', {
      progress: 10,
      message: 'Получены данные шаблонов',
    });

    await AppDataSource.manager.clear(ProductWork);
    progressManager.updateProgress('productWork', {
      progress: 20,
      message: 'Таблица очищена',
    });

    let processedCount = 0;
    let errorCount = 0;

    for (let i = 0; i < templates.length; i++) {
      try {
        const template = templates[i];
        const barcodes = JSON.parse(template.barcode || '[]');
        const firstBarcode = barcodes[0];

        if (!firstBarcode) continue;

        const productSize = await AppDataSource.manager.findOne(ProductSize, {
          where: { barcode: firstBarcode },
        });

        const stockItem = await AppDataSource.manager.findOne(StockItem, {
          where: { article: template.article },
        });

        const productWork = new ProductWork();
        Object.assign(productWork, {
          article: template.article,
          barcode: firstBarcode,
          main_photo_url: template.main_photo_url,
          brand: template.brand,
          color: template.color,
          type: template.type,
          gender: template.gender,
          season: template.season,
          rich_content: template.rich_content,
          material: template.material,
          fastener_type: template.fastener_type,
          stock: stockItem?.stock || 0,
        });

        if (productSize) {
          productWork.external_code = productSize.external_code;
          productWork.size = productSize.size;
          productWork.ozon_id = productSize.ozon_id;

          if (productSize.external_code) {
            const products = await AppDataSource.manager.find(Product);
            const product = products.find(p => {
              try {
                return p.external_code.includes(productSize.external_code!);
              } catch {
                return false;
              }
            });

            if (product) {
              Object.assign(productWork, {
                main_color: product.main_color,
                new_last: product.new_last,
                mega_last: product.mega_last,
                best_last: product.best_last,
                item_type: product.item_type,
              });
            }
          }
        }

        await AppDataSource.manager.save(productWork);
        processedCount++;

        progressManager.updateProgress('productWork', {
          progress: 20 + Math.floor((i / templates.length) * 80),
          message: `Обработано ${processedCount} записей`,
        });
      } catch (error) {
        errorCount++;
        console.error('Error processing template:', error);
      }
    }

    progressManager.completeProgress('productWork',
      `Расчет завершен. Обработано: ${processedCount}, Ошибок: ${errorCount}`,
    );

    res.json({
      message: `Расчет завершен. Обработано: ${processedCount}, Ошибок: ${errorCount}`,
      details: { processed: processedCount, errors: errorCount },
    });
  } catch (error) {
    const errorMessage = error instanceof Error ? error.message : 'Неизвестная ошибка';
    progressManager.errorProgress('productWork', errorMessage);
    res.status(500).json({ message: 'Ошибка при расчете таблицы' });
  }
});

app.post('/clear-product-work', async (req: Request, res: Response) => {
  try {
    await AppDataSource.manager.clear(ProductWork);
    res.json({ message: 'Таблица успешно очищена' });
  } catch (error) {
    console.error('Error clearing product work:', error);
    res.status(500).json({ message: 'Ошибка при очистке таблицы' });
  }
});

// Assortment Upload Route
app.post('/upload/assortment', upload.single('file'), handleMulterError, async (req: Request, res: Response) => {
  try {
    if (!req.file) {
      throw new Error('Файл не загружен');
    }

    progressManager.startProgress('assortment');
    progressManager.updateProgress('assortment', {
      progress: 0,
      message: 'Начало обработки файла',
    });

    // Очищаем таблицы
    await AppDataSource.manager.clear(Product);
    await AppDataSource.manager.clear(ProductSize);
    progressManager.updateProgress('assortment', {
      progress: 10,
      message: 'Таблицы очищены',
    });

    let workbook;
    try {
      workbook = XLSX.read(req.file.buffer);
      progressManager.updateProgress('assortment', {
        progress: 20,
        message: 'Файл успешно прочитан',
      });
    } catch {
      throw new Error('Ошибка при чтении файла Excel. Возможно, файл поврежден или имеет неверный формат.');
    }

    // Обработка листа "Ассортимент"
    const assortmentSheet = workbook.Sheets['Ассортимент'];
    if (!assortmentSheet) {
      throw new Error('Лист "Ассортимент" не найден в файле');
    }

    let assortmentData;
    try {
      assortmentData = XLSX.utils.sheet_to_json<AssortmentRow>(assortmentSheet, {
        range: 1,
        raw: true,
        defval: null,
      });
      if (assortmentData.length === 0) {
        throw new Error('Лист "Ассортимент" не содержит данных');
      }
      progressManager.updateProgress('assortment', {
        progress: 30,
        message: 'Данные ассортимента извлечены из файла',
      });
    } catch {
      throw new Error('Ошибка при чтении данных ассортимента');
    }

    // Обработка листа "ШК"
    const barcodeSheet = workbook.Sheets['ШК'];
    if (!barcodeSheet) {
      throw new Error('Лист "ШК" не найден в файле');
    }

    let barcodeData;
    try {
      // Добавляем отладочную информацию
      console.log('Заголовки листа "ШК":', XLSX.utils.sheet_to_json(barcodeSheet, { header: 1 })[0]);

      barcodeData = XLSX.utils.sheet_to_json<{
        'Внешний код': string;
        'Размер': string | number;
        'Штрихкод': string;
        'Ozon ID': string;
      }>(barcodeSheet, {
        range: 0, // Начинаем с первой строки, включая заголовки
        raw: true,
        defval: null,
        header: ['Внешний код', 'Размер', 'Штрихкод', 'Ozon ID'], // Явно указываем заголовки
      });

      // Удаляем первую строку, так как это заголовки
      barcodeData = barcodeData.slice(1);

      if (barcodeData.length === 0) {
        throw new Error('Лист "ШК" не содержит данных');
      }

      // Отладочная информация для первой строки данных
      console.log('Первая строка данных:', barcodeData[0]);

      progressManager.updateProgress('assortment', {
        progress: 40,
        message: 'Данные штрих-кодов извлечены из файла',
      });
    } catch (error) {
      console.error('Ошибка при чтении данных штрих-кодов:', error);
      throw new Error('Ошибка при чтении данных штрих-кодов');
    }

    let processedAssortment = 0;
    let errorAssortment = 0;
    let processedBarcodes = 0;
    let errorBarcodes = 0;

    const products: Product[] = [];

    // Сохраняем данные ассортимента
    for (let i = 0; i < assortmentData.length; i++) {
      try {
        const item = assortmentData[i];
        const product = new Product();

        const rawExternalCode = item['Внешний код'] || item[''] || null;
        const externalCodes = rawExternalCode
          ? String(rawExternalCode).split(/[\n\r;]+/).map(code => code.trim()).filter(Boolean)
          : [];
        product.external_code = externalCodes;

        product.main_color = (item['Цвет(основной)'] || item.__EMPTY)?.toString() || null;
        product.new_last = (item['Новая колодка'] || item.__EMPTY_1)?.toString() || null;
        product.mega_last = (item['Колодка MEGA'] || item.__EMPTY_2)?.toString() || null;
        product.best_last = (item['Колодка BEST'] || item.__EMPTY_3)?.toString() || null;
        product.brand = (item['Бренд'] || item['#N/A'])?.toString() || null;
        product.item_type = (item['Предмет'] || item.__EMPTY_4)?.toString() || null;

        const rawSizeDistribution = item['Распределение по размерам'] || item.__EMPTY_5 || null;
        const sizeDistribution = rawSizeDistribution
          ? String(rawSizeDistribution)
            .split(';')
            .map(pair => {
              const [size, quantity] = pair.trim().split('-').map(Number);
              return { size, quantity };
            })
            .filter(it => !Number.isNaN(it.size) && !Number.isNaN(it.quantity))
          : [];
        product.size_distribution = sizeDistribution;

        products.push(product);
        processedAssortment++;
      } catch (error) {
        errorAssortment++;
        console.error('Error processing assortment item:', error);
      }
    }

    await AppDataSource.manager.save(products, { chunk: 1000 });

    const productSizes: ProductSize[] = [];

    // Сохраняем данные штрих-кодов
    for (let i = 0; i < barcodeData.length; i++) {
      try {
        const item = barcodeData[i];

        // Проверяем наличие обязательных полей
        const externalCode = item['Внешний код']?.toString();
        const size = item['Размер']?.toString();
        const barcode = item['Штрихкод']?.toString();

        if (!externalCode || !size || !barcode) {
          console.log('Пропущена запись штрих-кода: отсутствуют обязательные поля', {
            raw_item: item, // Добавляем вывод исходных данных
            external_code: externalCode,
            size,
            barcode,
            row: i + 2,
          });
          errorBarcodes++;
          continue;
        }

        const productSize = new ProductSize();
        productSize.external_code = externalCode;
        productSize.size = size;
        productSize.barcode = barcode;
        productSize.ozon_id = item['Ozon ID']?.toString() || null;

        productSizes.push(productSize);

        processedBarcodes++;

        if (i % 100 === 0) {
          progressManager.updateProgress('assortment', {
            progress: 40 + Math.floor((i / barcodeData.length) * 60),
            message: `Обработано ${processedAssortment} записей ассортимента и ${processedBarcodes} штрих-кодов`,
          });
        }
      } catch (error) {
        errorBarcodes++;
        console.error('Error processing barcode item:', error);
      }
    }

    await AppDataSource.manager.save(productSizes, { chunk: 1000 });

    progressManager.completeProgress('assortment',
      `Загрузка завершена. Ассортимент: обработано ${processedAssortment}, ошибок ${errorAssortment}. ` +
            `Штрих-коды: обработано ${processedBarcodes}, ошибок ${errorBarcodes}`,
    );

    res.json({
      message: 'Данные успешно загружены',
      details: {
        assortment: { processed: processedAssortment, errors: errorAssortment },
        barcodes: { processed: processedBarcodes, errors: errorBarcodes },
      },
    });
  } catch (error) {
    const errorMessage = error instanceof Error ? error.message : 'Неизвестная ошибка';
    progressManager.errorProgress('assortment', errorMessage);
    res.status(500).json({ message: 'Ошибка при загрузке данных', error: errorMessage });
  }
});

// Stock Upload Route
app.post('/upload/stock', upload.single('file'), handleMulterError, async (req: Request, res: Response) => {
  try {
    if (!req.file) {
      throw new Error('Файл не загружен');
    }

    const stockService = new StockService();
    const result = await stockService.processStockFile(req.file.buffer);
    res.json(result);
  } catch (error) {
    const errorMessage = error instanceof Error ? error.message : 'Неизвестная ошибка';
    res.status(500).json({ message: 'Ошибка при обработке файла остатков', error: errorMessage });
  }
});

// Recommendations Routes
app.post('/generate-recommendations', async (req: Request, res: Response) => {
  try {
    progressManager.startProgress('recommendations');
    const recommendationService = new RecommendationService();

    await recommendationService.generateRecommendations((progress: number, message?: string) => {
      progressManager.updateProgress('recommendations', { progress, message });
    });

    progressManager.completeProgress('recommendations', 'Рекомендации успешно сгенерированы');
    res.json({ message: 'Рекомендации успешно сгенерированы' });
  } catch (error) {
    const errorMessage = error instanceof Error ? error.message : 'Неизвестная ошибка';
    progressManager.errorProgress('recommendations', errorMessage);
    res.status(500).json({ message: 'Ошибка при генерации рекомендаций' });
  }
});

app.post('/clear-recommendations', async (req: Request, res: Response) => {
  try {
    const recommendationService = new RecommendationService();
    await recommendationService.clearRecommendations();
    res.json({ message: 'Рекомендации успешно очищены' });
  } catch (error) {
    console.error('Error clearing recommendations:', error);
    res.status(500).json({ message: 'Ошибка при очистке рекомендаций' });
  }
});

app.post('/generate-rich-content', async (req: Request, res: Response) => {
  try {
    progressManager.startProgress('richContent');

    const richContentService = new RichContentService();
    await richContentService.generateRichContent((progress: number, message?: string) => {
      progressManager.updateProgress('richContent', { progress, message });
    });

    progressManager.completeProgress('richContent', 'Rich-контент успешно сгенерирован');
    res.json({ message: 'Rich-контент успешно сгенерирован' });
  } catch (error) {
    const errorMessage = error instanceof Error ? error.message : 'Неизвестная ошибка';
    progressManager.errorProgress('richContent', errorMessage);
    res.status(500).json({ message: 'Ошибка при генерации Rich-контента' });
  }
});

// Excel Generation Routes
app.get('/api/generate-rich-content-excel', async (_req: Request, res: Response) => {
  try {
    const productWorks = await AppDataSource.getRepository(ProductWork).find();

    const workbook = XLSX.utils.book_new();
    const worksheet = XLSX.utils.json_to_sheet(productWorks.map(work => ({
      article: work.article,
      new_rich: work.new_rich,
    })));

    XLSX.utils.book_append_sheet(workbook, worksheet, 'Rich Content');

    const excelBuffer = XLSX.write(workbook, { type: 'buffer', bookType: 'xlsx' });

    res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
    res.setHeader('Content-Disposition', 'attachment; filename=rich-content.xlsx');

    res.send(excelBuffer);
  } catch (error) {
    console.error('Error generating Excel:', error);
    res.status(500).json({ error: 'Failed to generate Excel file' });
  }
});

// AI Processing Routes
const excelAIController = new ExcelAIController();

app.post('/api/excel-ai/process', upload.single('file'), (req, res) => excelAIController.processExcel(req, res));

app.get('/ai_processing', (req: Request, res: Response) => {
  res.sendFile(path.join(__dirname, 'public', 'ai_processing.html'));
});

// Progress Tracking Route
app.get('/progress/:taskId', progressMiddleware);

// Template Merge Routes
const templateMergeController = new TemplateMergeController();

app.get('/template-merge', (req: Request, res: Response) => {
  res.sendFile(path.join(__dirname, 'public', 'template_merge.html'));
});

app.post('/merge-templates', upload.fields([
  { name: 'file1', maxCount: 1 },
  { name: 'file2', maxCount: 1 },
]), handleMulterError, (req: Request, res: Response) => templateMergeController.mergeTemplates(req, res));
