import { Entity, PrimaryGeneratedColumn, Column } from 'typeorm';

@Entity()
export class ProductWork {
    @PrimaryGeneratedColumn()
    id: number;

    @Column({ type: 'text' })
    article: string;

    @Column({ type: 'text', nullable: true })
    barcode: string | null;

    @Column({ type: 'text' })
    main_photo_url: string;

    @Column({ type: 'text' })
    brand: string;

    @Column({ type: 'text' })
    color: string;

    @Column({ type: 'text' })
    type: string;

    @Column({ type: 'text' })
    gender: string;

    @Column({ type: 'text', nullable: true })
    season: string | null;

    @Column({ type: 'text', nullable: true })
    rich_content: string | null;

    @Column({ type: 'text', nullable: true })
    material: string | null;

    @Column({ type: 'text', nullable: true })
    fastener_type: string | null;

    @Column({ type: 'text', nullable: true })
    external_code: string | null;

    @Column({ type: 'text', nullable: true })
    size: string | null;

    @Column({ type: 'text', nullable: true })
    ozon_id: string | null;

    @Column({ type: 'float', default: 0 })
    stock: number;

    @Column({ type: 'text', nullable: true })
    main_color: string | null;

    @Column({ type: 'text', nullable: true })
    new_last: string | null;

    @Column({ type: 'text', nullable: true })
    mega_last: string | null;

    @Column({ type: 'text', nullable: true })
    best_last: string | null;

    @Column({ type: 'text', nullable: true })
    item_type: string | null;

    @Column({ type: 'text', nullable: true })
    new_rich: string | null;
} 