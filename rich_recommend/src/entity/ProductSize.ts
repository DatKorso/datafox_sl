import { Entity, PrimaryGeneratedColumn, Column, Index } from 'typeorm';

@Entity()
export class ProductSize {
    @PrimaryGeneratedColumn()
    id: number;

    @Index()
    @Column()
    external_code: string;

    @Column()
    size: string;

    @Column()
    barcode: string;

    @Column('text', { nullable: true })
    ozon_id: string | null;
} 