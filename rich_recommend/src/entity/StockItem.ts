import { Entity, PrimaryGeneratedColumn, Column, Index } from 'typeorm';

@Entity()
export class StockItem {
    @PrimaryGeneratedColumn()
    id: number;

    @Index()
    @Column()
    sku: string;

    @Column('text', { nullable: true })
    product_flag: string | null;

    @Column()
    warehouse_name: string;

    @Column()
    cluster: string;

    @Column()
    article: string;

    @Column()
    product_name: string;

    @Column('integer', { default: 0 })
    in_transit: number;

    @Column('integer', { default: 0 })
    available_for_sale: number;

    @Column('integer', { default: 0 })
    reserved: number;

    @Column('float')
    transfer_rate_per_unit: number;

    @Column('float')
    total_transfer_rate: number;

    @Column()
    article_code: string;

    @Column({ default: '' })
    color: string;

    @Column({ default: '' })
    size: string;

    @Column({ type: 'float', default: 0 })
    stock: number;
} 