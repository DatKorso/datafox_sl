import { Entity, PrimaryGeneratedColumn, Column } from 'typeorm';

@Entity()
export class Product {
    @PrimaryGeneratedColumn()
    id: number;

    @Column('simple-json')
    external_code: string[];

    @Column({ type: 'text', nullable: true })
    main_color: string | null;

    @Column({ type: 'text', nullable: true })
    new_last: string | null;

    @Column('text', { nullable: true })
    mega_last: string | null;

    @Column('text', { nullable: true })
    best_last: string | null;

    @Column({ type: 'text', nullable: true })
    brand: string | null;

    @Column({ type: 'text', nullable: true })
    item_type: string | null;

    @Column('simple-json')
    size_distribution: { size: number; quantity: number }[];
} 