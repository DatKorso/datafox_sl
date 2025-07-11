import { AppDataSource } from '../../../data-source';
import { ProductWork } from '../../../entity/ProductWork';
import { ProductRecommendation } from '../../recommendations/entity/ProductRecommendation';
import { RICH_CONTENT_CONFIG } from '../config';

export class RichContentService {
  async generateRichContent(onProgress?: (progress: number, message?: string) => void): Promise<void> {
    try {
      // Очищаем существующий rich-контент
      onProgress?.(5, 'Очистка существующего rich-контента');
      await this.clearExistingContent();

      // Получаем все товары с рекомендациями
      onProgress?.(10, 'Загрузка товаров');
      const products = await AppDataSource.manager.find(ProductWork);

      let processed = 0;
      let withRecommendations = 0;
      let withoutOzonId = 0;
      const errors: Array<{ barcode: string, error: string }> = [];

      // Обрабатываем каждый товар
      for (const product of products) {
        try {
          if (!product.ozon_id) {
            withoutOzonId++;
            continue;
          }

          // Получаем рекомендации
          const recommendations = await AppDataSource.manager
            .createQueryBuilder(ProductRecommendation, 'pr')
            .where('pr.source_barcode = :barcode', { barcode: product.barcode })
            .orderBy('pr.score', 'DESC')
            .limit(8)
            .getMany();

          if (recommendations.length > 0) {
            withRecommendations++;

            // Получаем информацию о рекомендованных товарах
            const recommendedProducts = await Promise.all(
              recommendations.map(async (rec) => {
                const recommendedProduct = await AppDataSource.manager.findOne(ProductWork, {
                  where: { barcode: rec.recommended_barcode },
                });
                return {
                  recommendation: rec,
                  product: recommendedProduct,
                };
              }),
            );

            // Фильтруем только те рекомендации, для которых нашли товары с ozon_id
            const validRecommendations = recommendedProducts.filter(rp => rp.product && rp.product.ozon_id);

            if (validRecommendations.length > 0) {
              // Создаем rich-контент
              const richContent = this.createRichContent(product, validRecommendations);

              // Сохраняем
              product.new_rich = richContent;
              await AppDataSource.manager.save(product);
            }
          }

          processed++;
          const progress = Math.floor((processed / products.length) * 80) + 10;
          onProgress?.(progress, `Обработано ${processed} из ${products.length}`);

        } catch (error) {
          errors.push({
            barcode: product.barcode || 'unknown',
            error: error instanceof Error ? error.message : 'Unknown error',
          });
        }
      }

      // Формируем отчет
      const report = {
        totalProducts: products.length,
        processedProducts: processed,
        productsWithRecommendations: withRecommendations,
        productsWithoutOzonId: withoutOzonId,
        averageRecommendationsPerProduct: withRecommendations > 0 ?
          processed / withRecommendations : 0,
        errors,
      };

      console.log('Rich Content Generation Report:', report);
      onProgress?.(100, `Генерация завершена. Обработано товаров: ${processed}`);

    } catch (error) {
      console.error('Error generating rich content:', error);
      throw error;
    }
  }

  private createRichContent(
    product: ProductWork,
    recommendations: Array<{ recommendation: ProductRecommendation, product: ProductWork | null }>,
  ): string {
    const richContent = {
      content: [
        {
          widgetName: 'raShowcase',
          type: 'roll',
          blocks: [
            {
              imgLink: '',
              img: {
                src: 'https://cdn1.ozone.ru/s3/multimedia-tmp-c/item-pic-f9487337ec8fc59e7fc26559542b9325.jpg',
                srcMobile: 'https://cdn1.ozone.ru/s3/multimedia-tmp-4/item-pic-88214e8b6aa2190fe6bb66b8d6a53de8.jpg',
                alt: '',
                position: 'width_full',
                positionMobile: 'width_full',
                widthMobile: 1478,
                heightMobile: 665,
              },
            },
          ],
        },
        {
          widgetName: 'raShowcase',
          type: 'tileM',
          blocks: recommendations.map(rec => ({
            img: {
              src: rec.product!.main_photo_url,
              srcMobile: rec.product!.main_photo_url,
              alt: '',
              position: RICH_CONTENT_CONFIG.TEMPLATE.positions.default,
              positionMobile: RICH_CONTENT_CONFIG.TEMPLATE.positions.default,
              widthMobile: RICH_CONTENT_CONFIG.TEMPLATE.defaultImageSize.widthMobile,
              heightMobile: RICH_CONTENT_CONFIG.TEMPLATE.defaultImageSize.heightMobile,
              isParandjaMobile: false,
            },
            imgLink: `https://www.ozon.ru/product/${rec.product!.ozon_id}`,
            title: {
              content: [RICH_CONTENT_CONFIG.TEMPLATE.text.title],
              size: RICH_CONTENT_CONFIG.TEMPLATE.text.titleSize,
              align: RICH_CONTENT_CONFIG.TEMPLATE.text.align,
              color: RICH_CONTENT_CONFIG.TEMPLATE.text.color,
            },
            text: {
              size: 'size2',
              align: 'center',
              color: 'color3',
              content: [''],
            },
          })),
        },
      ],
      version: RICH_CONTENT_CONFIG.TEMPLATE.version,
    };

    return JSON.stringify(richContent, null, 2);
  }

  private async clearExistingContent(): Promise<void> {
    await AppDataSource.manager
      .createQueryBuilder()
      .update(ProductWork)
      .set({ new_rich: null })
      .execute();
  }
}
