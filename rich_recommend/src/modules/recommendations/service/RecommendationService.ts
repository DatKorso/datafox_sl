import { AppDataSource } from '../../../data-source';
import { ProductSize } from '../../../entity/ProductSize';
import { ProductWork } from '../../../entity/ProductWork';
import { ProductRecommendation } from '../entity/ProductRecommendation';

export class RecommendationService {
    private readonly BATCH_SIZE = 1000; // Размер пакета для обработки
    private stats = {
      totalProducts: 0,
      noBarcode: 0,
      noExternalCode: 0,
      noSimilarProducts: 0,
      withRecommendations: 0,
      byType: {} as Record<string, number>,
      byGender: {} as Record<string, number>,
      byBrand: {} as Record<string, number>,
    };

    async clearRecommendations(): Promise<void> {
      console.log('Clearing existing recommendations...');
      await AppDataSource.manager.clear(ProductRecommendation);
    }

    async generateRecommendations(
      progressCallback?: (progress: number, message?: string) => void,
    ): Promise<void> {
      await this.clearRecommendations();
      this.resetStats();

      console.log('Fetching products...');
      const totalProducts = await AppDataSource.manager.count(ProductWork);
      this.stats.totalProducts = totalProducts;
      console.log(`Found ${totalProducts} products`);

      let processedCount = 0;

      // Получаем продукты порциями
      for (let offset = 0; offset < totalProducts; offset += this.BATCH_SIZE) {
        const products = await AppDataSource.manager.find(ProductWork, {
          skip: offset,
          take: this.BATCH_SIZE,
        });

        // Обрабатываем текущую порцию
        for (const sourceProduct of products) {
          const recommendations = await this.generateRecommendationsForProduct(sourceProduct);

          // Собираем статистику по типам, полу и брендам
          this.updateTypeStats(sourceProduct.type);
          this.updateGenderStats(sourceProduct.gender);
          this.updateBrandStats(sourceProduct.brand);

          if (recommendations.length > 0) {
            await AppDataSource.manager.save(ProductRecommendation, recommendations);
            this.stats.withRecommendations++;
          }

          processedCount++;
          if (progressCallback) {
            const progress = Math.floor((processedCount / totalProducts) * 100);
            progressCallback(progress, this.getProgressMessage(processedCount));
          }
        }

        console.log(this.getDetailedStats());
      }

      // Выводим финальную статистику
      console.log('\nFinal Statistics:');
      console.log(this.getDetailedStats());
    }

    private resetStats() {
      this.stats = {
        totalProducts: 0,
        noBarcode: 0,
        noExternalCode: 0,
        noSimilarProducts: 0,
        withRecommendations: 0,
        byType: {},
        byGender: {},
        byBrand: {},
      };
    }

    private updateTypeStats(type: string) {
      this.stats.byType[type] = (this.stats.byType[type] || 0) + 1;
    }

    private updateGenderStats(gender: string) {
      this.stats.byGender[gender] = (this.stats.byGender[gender] || 0) + 1;
    }

    private updateBrandStats(brand: string) {
      this.stats.byBrand[brand] = (this.stats.byBrand[brand] || 0) + 1;
    }

    private getProgressMessage(processedCount: number): string {
      return `Обработано ${processedCount} из ${this.stats.totalProducts} товаров. ` +
               `С рекомендациями: ${this.stats.withRecommendations}, ` +
               `Без штрих-кода: ${this.stats.noBarcode}, ` +
               `Без external_code: ${this.stats.noExternalCode}, ` +
               `Без похожих товаров: ${this.stats.noSimilarProducts}`;
    }

    private getDetailedStats(): string {
      const topTypes = Object.entries(this.stats.byType)
        .sort((a, b) => b[1] - a[1])
        .slice(0, 5);
      const topBrands = Object.entries(this.stats.byBrand)
        .sort((a, b) => b[1] - a[1])
        .slice(0, 5);

      return `
Статистика обработки:
--------------------
Всего товаров: ${this.stats.totalProducts}
С рекомендациями: ${this.stats.withRecommendations}
Без штрих-кода: ${this.stats.noBarcode}
Без external_code: ${this.stats.noExternalCode}
Без похожих товаров: ${this.stats.noSimilarProducts}

Топ-5 типов товаров:
${topTypes.map(([type, count]) => `- ${type}: ${count}`).join('\n')}

Распределение по полу:
${Object.entries(this.stats.byGender)
    .map(([gender, count]) => `- ${gender}: ${count}`).join('\n')}

Топ-5 брендов:
${topBrands.map(([brand, count]) => `- ${brand}: ${count}`).join('\n')}
        `;
    }

    private async generateRecommendationsForProduct(sourceProduct: ProductWork): Promise<ProductRecommendation[]> {
      const recommendations: ProductRecommendation[] = [];

      if (!sourceProduct.barcode) {
        this.stats.noBarcode++;
        console.log(`Пропущен товар ${sourceProduct.article}: отсутствует штрих-код`);
        return recommendations;
      }

      // Проверяем наличие размера у исходного товара
      if (!sourceProduct.size) {
        console.log(`Пропущен товар ${sourceProduct.article}: отсутствует размер`);
        return recommendations;
      }

      const sourceProductSize = await AppDataSource.manager.findOne(ProductSize, {
        where: { barcode: sourceProduct.barcode },
      });

      if (!sourceProductSize?.external_code) {
        this.stats.noExternalCode++;
        console.log(`Пропущен товар ${sourceProduct.article} (${sourceProduct.barcode}): не найден external_code в ProductSize`);
        return recommendations;
      }

      console.log(`Поиск рекомендаций для товара ${sourceProduct.article} (${sourceProduct.barcode}), размер: ${sourceProduct.size}, external_code: ${sourceProductSize.external_code}`);

      // Находим все похожие товары через связь с ProductSize
      const similarProducts = await AppDataSource.manager
        .createQueryBuilder(ProductWork, 'product')
        .innerJoin(ProductSize, 'product_size', 'product.barcode = product_size.barcode')
        .where('product.type = :type', { type: sourceProduct.type })
        .andWhere('product.gender = :gender', { gender: sourceProduct.gender })
        .andWhere('product.brand = :brand', { brand: sourceProduct.brand })
        .andWhere('product_size.external_code != :external_code', { external_code: sourceProductSize.external_code })
        .andWhere('product.stock > 0')
        .andWhere('product.barcode IS NOT NULL')
        .andWhere('product_size.external_code IS NOT NULL')
        .andWhere('product.size = :size', { size: sourceProduct.size }) // Добавляем фильтр по точному размеру
        .getMany();

      console.log(`Найдено ${similarProducts.length} похожих товаров для ${sourceProduct.article} размера ${sourceProduct.size}`);

      if (similarProducts.length === 0) {
        this.stats.noSimilarProducts++;
        console.log(`Причины отсутствия рекомендаций для ${sourceProduct.article}:
                - Тип: ${sourceProduct.type}
                - Пол: ${sourceProduct.gender}
                - Бренд: ${sourceProduct.brand}
                - Размер: ${sourceProduct.size}
                - External code: ${sourceProductSize.external_code}`);
        return recommendations;
      }

      for (const recommendedProduct of similarProducts) {
        if (!recommendedProduct.barcode) {
          console.log(`Пропущен похожий товар ${recommendedProduct.article}: отсутствует штрих-код`);
          continue;
        }

        const recommendation = new ProductRecommendation();
        recommendation.source_barcode = sourceProduct.barcode;
        recommendation.recommended_barcode = recommendedProduct.barcode;
        recommendation.score = this.calculateSimilarityScore(sourceProduct, recommendedProduct);
        recommendation.match_details = this.getMatchDetails(sourceProduct, recommendedProduct);

        recommendations.push(recommendation);
      }

      const sortedRecommendations = recommendations
        .sort((a, b) => b.score - a.score)
        .slice(0, 8);

      console.log(`Сгенерировано ${sortedRecommendations.length} рекомендаций для ${sourceProduct.article}`);

      return sortedRecommendations;
    }

    private calculateSimilarityScore(source: ProductWork, recommended: ProductWork): number {
      let score = 100; // Базовый счет

      // Базовые критерии с повышенным весом
      if (source.type !== recommended.type) return 0;
      if (source.gender !== recommended.gender) return 0;
      if (source.brand !== recommended.brand) score += 50; // Увеличиваем вес бренда

      // Приоритизация размера как ключевого параметра
      if (source.size && recommended.size) {
        const sourceSize = Number.parseFloat(String(source.size).replace(',', '.'));
        const recommendedSize = Number.parseFloat(String(recommended.size).replace(',', '.'));

        if (!Number.isNaN(sourceSize) && !Number.isNaN(recommendedSize)) {
          if (sourceSize === recommendedSize) {
            score += 100; // Увеличиваем вес точного совпадения размера
          } else if (Math.abs(recommendedSize - sourceSize) <= 1) {
            score += 40; // Близкий размер (±1)
          } else {
            score -= 50; // Штраф за сильное различие в размере
          }
        }
      } else {
        score -= 30; // Штраф за отсутствие информации о размере
      }

      // Сезонность как важный фактор
      if (source.season === recommended.season) {
        score += 80;
      } else {
        score -= 40; // Штраф за несовпадение сезона
      }

      // Колодка как критический параметр комфорта
      let lastMatch = false;
      if (source.mega_last && recommended.mega_last && source.mega_last === recommended.mega_last) {
        score += 90;
        lastMatch = true;
      } else if (source.best_last && recommended.best_last && source.best_last === recommended.best_last) {
        score += 70;
        lastMatch = true;
      } else if (source.new_last && recommended.new_last && source.new_last === recommended.new_last) {
        score += 50;
        lastMatch = true;
      }

      if (!lastMatch) score *= 0.7; // Увеличиваем штраф за несовпадение колодки

      // Второстепенные характеристики
      if (source.color === recommended.color) score += 40;
      if (source.material === recommended.material) score += 40;
      if (source.fastener_type === recommended.fastener_type) score += 30;

      // Наличие товара
      if (recommended.stock > 5) {
        score += 40;
      } else if (recommended.stock > 2) {
        score += 20;
      } else if (recommended.stock > 0) {
        score += 10;
      }

      return Math.min(score, 500); // Увеличиваем максимальный счет
    }

    private getMatchDetails(source: ProductWork, recommended: ProductWork): string {
      const matches = [];
      const scores = [];

      // Основные параметры
      matches.push(`Тип: ${recommended.type}${source.type === recommended.type ? ' ✓' : ''}`, `Пол: ${recommended.gender}${source.gender === recommended.gender ? ' ✓' : ''}`, `Бренд: ${recommended.brand}${source.brand === recommended.brand ? ' ✓' : ''}`);

      // Размер
      if (source.size && recommended.size) {
        const sourceSize = Number.parseFloat(String(source.size).replace(',', '.'));
        const recommendedSize = Number.parseFloat(String(recommended.size).replace(',', '.'));

        if (!Number.isNaN(sourceSize) && !Number.isNaN(recommendedSize)) {
          if (sourceSize === recommendedSize) {
            matches.push(`Размер: ${recommended.size} ✓`);
            scores.push('100 баллов за точное совпадение размера');
          } else if (Math.abs(recommendedSize - sourceSize) <= 1) {
            matches.push(`Размер: ${recommended.size} (близкий размер) ✓`);
            scores.push('40 баллов за близкий размер');
          } else {
            matches.push(`Размер: ${recommended.size} (штраф -50 баллов)`);
          }
        }
      } else {
        matches.push('Размер: нет данных (штраф -30 баллов)');
      }

      // Сезон
      if (source.season === recommended.season) {
        matches.push(`Сезон: ${recommended.season} ✓`);
        scores.push('80 баллов за сезон');
      } else {
        matches.push(`Сезон: ${recommended.season} (штраф -40 баллов)`);
      }

      // Второстепенные характеристики
      if (source.color === recommended.color) {
        matches.push(`Цвет: ${recommended.color} ✓`);
        scores.push('40 баллов за цвет');
      }

      if (source.material === recommended.material) {
        matches.push(`Материал: ${recommended.material} ✓`);
        scores.push('40 баллов за материал');
      }

      if (source.fastener_type === recommended.fastener_type) {
        matches.push(`Застежка: ${recommended.fastener_type} ✓`);
        scores.push('30 баллов за застежку');
      }

      // Колодка
      if (source.mega_last && recommended.mega_last && source.mega_last === recommended.mega_last) {
        matches.push(`Колодка MEGA: ${recommended.mega_last} ✓`);
        scores.push('90 баллов за колодку MEGA');
      } else if (source.best_last && recommended.best_last && source.best_last === recommended.best_last) {
        matches.push(`Колодка BEST: ${recommended.best_last} ✓`);
        scores.push('70 баллов за колодку BEST');
      } else if (source.new_last && recommended.new_last && source.new_last === recommended.new_last) {
        matches.push(`Колодка NEW: ${recommended.new_last} ✓`);
        scores.push('50 баллов за колодку NEW');
      } else {
        matches.push('Колодка: не совпадает (штраф 30%)');
      }

      // Наличие
      if (recommended.stock > 5) {
        matches.push(`В наличии: ${recommended.stock} шт. ✓✓`);
        scores.push('40 баллов за хороший остаток');
      } else if (recommended.stock > 2) {
        matches.push(`В наличии: ${recommended.stock} шт. ✓`);
        scores.push('20 баллов за средний остаток');
      } else if (recommended.stock > 0) {
        matches.push(`В наличии: ${recommended.stock} шт.`);
        scores.push('10 баллов за наличие');
      }

      return matches.join('\n') + '\n\nДетали счета:\n' + scores.join('\n');
    }
}
