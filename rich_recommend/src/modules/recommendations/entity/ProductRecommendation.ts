import { Entity, PrimaryGeneratedColumn, Column } from 'typeorm';

@Entity()
export class ProductRecommendation {
    @PrimaryGeneratedColumn()
    id: number;

    @Column({ type: 'text' })
    source_barcode: string;

    @Column({ type: 'text' })
    recommended_barcode: string;

    @Column({ type: 'float' })
    score: number;

    @Column({ type: 'text' })
    match_details: string;
} 