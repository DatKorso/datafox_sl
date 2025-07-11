export interface StockRow {
  SKU: string;
  'Артикул': string;
  'Доступно к продаже': string | number;
}

export interface ProcessStockResult {
  message: string;
  details: {
    loaded: number;
    errors: number;
  };
} 