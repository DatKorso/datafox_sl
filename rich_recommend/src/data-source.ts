import 'reflect-metadata';
import path from 'node:path';

import { DataSource } from 'typeorm';

const host = process.env.TYPEORM_HOST;
const port = +(process.env.TYPEORM_PORT || '5432');
const username = process.env.TYPEORM_USERNAME;
const password = process.env.TYPEORM_PASSWORD;
const database = process.env.TYPEORM_DATABASE;
const logging = process.env.TYPEORM_LOGGING === 'true';
const migrationsRun = process.env.TYPEORM_MIGRATIONS_RUN === 'true';

const dirname = __dirname;

export const AppDataSource = new DataSource({
  type: 'postgres',
  host,
  port,
  username,
  password,
  database,
  synchronize: false,
  migrationsRun,
  logging,
  entities: [path.join(dirname, './**/entity/**/*.{ts,js}')],
  migrations: [path.join(dirname, './migration/*.{ts,js}')],
});
