import { MigrationInterface, QueryRunner } from 'typeorm';

export class OptimizeDatabase1710687500000 implements MigrationInterface {
    name = 'OptimizeDatabase1710687500000';

    public async up(queryRunner: QueryRunner): Promise<void> {
      // Добавляем индексы для часто используемых полей
      await queryRunner.query('CREATE INDEX IF NOT EXISTS "idx_product_brand" ON "product" ("brand")');
      await queryRunner.query('CREATE INDEX IF NOT EXISTS "idx_product_item_type" ON "product" ("item_type")');
      await queryRunner.query('CREATE INDEX IF NOT EXISTS "idx_product_work_article" ON "product_work" ("article")');
      await queryRunner.query('CREATE INDEX IF NOT EXISTS "idx_product_work_barcode" ON "product_work" ("barcode")');
      await queryRunner.query('CREATE INDEX IF NOT EXISTS "idx_product_work_brand" ON "product_work" ("brand")');
      await queryRunner.query('CREATE INDEX IF NOT EXISTS "idx_stock_item_article" ON "stock_item" ("article")');
      await queryRunner.query('CREATE INDEX IF NOT EXISTS "idx_template_barcode" ON "template" ("barcode")');
      await queryRunner.query('CREATE INDEX IF NOT EXISTS "idx_product_size_barcode" ON "product_size" ("barcode")');

      // Добавляем ограничения для важных полей
      await queryRunner.query('ALTER TABLE "product_work" ALTER COLUMN "article" SET NOT NULL');
      await queryRunner.query('ALTER TABLE "product_work" ALTER COLUMN "brand" SET NOT NULL');
        
      // Создаем партиционированную таблицу для рекомендаций
      await queryRunner.query(`
            CREATE TABLE IF NOT EXISTS "product_recommendation_partitioned" (
                "id" SERIAL NOT NULL,
                "source_barcode" text NOT NULL,
                "recommended_barcode" text NOT NULL,
                "score" double precision NOT NULL,
                "match_details" text NOT NULL,
                "created_at" TIMESTAMP NOT NULL DEFAULT now(),
                CONSTRAINT "pk_product_recommendation_partitioned" PRIMARY KEY ("id", "created_at")
            ) PARTITION BY RANGE (created_at)
        `);

      // Создаем партиции по месяцам (пример для 3 месяцев)
      await queryRunner.query(`
            CREATE TABLE IF NOT EXISTS "product_recommendation_y2024m03" 
            PARTITION OF "product_recommendation_partitioned"
            FOR VALUES FROM ('2024-03-01') TO ('2024-04-01')
        `);
      await queryRunner.query(`
            CREATE TABLE IF NOT EXISTS "product_recommendation_y2024m04" 
            PARTITION OF "product_recommendation_partitioned"
            FOR VALUES FROM ('2024-04-01') TO ('2024-05-01')
        `);
      await queryRunner.query(`
            CREATE TABLE IF NOT EXISTS "product_recommendation_y2024m05" 
            PARTITION OF "product_recommendation_partitioned"
            FOR VALUES FROM ('2024-05-01') TO ('2024-06-01')
        `);

      // Добавляем индексы для партиционированной таблицы
      await queryRunner.query('CREATE INDEX IF NOT EXISTS "idx_recommendation_source" ON "product_recommendation_partitioned" ("source_barcode")');
      await queryRunner.query('CREATE INDEX IF NOT EXISTS "idx_recommendation_recommended" ON "product_recommendation_partitioned" ("recommended_barcode")');
    }

    public async down(queryRunner: QueryRunner): Promise<void> {
      // Удаляем индексы
      await queryRunner.query('DROP INDEX IF EXISTS "idx_product_brand"');
      await queryRunner.query('DROP INDEX IF EXISTS "idx_product_item_type"');
      await queryRunner.query('DROP INDEX IF EXISTS "idx_product_work_article"');
      await queryRunner.query('DROP INDEX IF EXISTS "idx_product_work_barcode"');
      await queryRunner.query('DROP INDEX IF EXISTS "idx_product_work_brand"');
      await queryRunner.query('DROP INDEX IF EXISTS "idx_stock_item_article"');
      await queryRunner.query('DROP INDEX IF EXISTS "idx_template_barcode"');
      await queryRunner.query('DROP INDEX IF EXISTS "idx_product_size_barcode"');

      // Удаляем ограничения
      await queryRunner.query('ALTER TABLE "product_work" ALTER COLUMN "article" DROP NOT NULL');
      await queryRunner.query('ALTER TABLE "product_work" ALTER COLUMN "brand" DROP NOT NULL');

      // Удаляем партиционированную таблицу и все её партиции
      await queryRunner.query('DROP TABLE IF EXISTS "product_recommendation_partitioned" CASCADE');
    }
} 