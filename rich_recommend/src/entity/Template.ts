import { Entity, PrimaryGeneratedColumn, Column, Index } from 'typeorm';

@Entity()
export class Template {
    @PrimaryGeneratedColumn()
    id: number;

    @Index()
    @Column()
    article: string; // Артикул*

    @Column({ type: 'text' })
    barcode: string; // Штрихкод (Серийный номер / EAN) - stored as JSON string of array

    @Column()
    main_photo_url: string; // Ссылка на главное фото*

    @Column()
    brand: string; // Бренд в одежде и обуви*

    @Column()
    color: string; // Цвет товара*

    @Column()
    type: string; // Тип*

    @Column()
    gender: string; // Пол*

    @Column({ type: 'text', nullable: true })
    season: string | null; // Сезон

    @Column({ type: 'text', nullable: true })
    rich_content: string | null; // Rich-контент JSON

    @Column({ type: 'text', nullable: true })
    material: string | null; // Материал

    @Column({ type: 'text', nullable: true, default: '[]' })
    fastener_type: string; // Вид застёжки (stored as JSON string)
} 