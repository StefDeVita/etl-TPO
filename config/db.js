import sql from 'mssql';
export {sql as sql};
import dotenv from 'dotenv';
dotenv.config();
console.log(process.env.DB_SERVER)
const config = {
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
  server: process.env.DB_SERVER,
  database: process.env.DB_DATABASE,
  options: {
    encrypt: true, // Utilizado para Azure SQL
    trustServerCertificate: false // Cambiar a true si hay problemas con el certificado
  }
};

export const poolPromise = new sql.ConnectionPool(config)
  .connect()
  .then(pool => {
    console.log('Connected to Azure SQL');
    return pool;
  })
  .catch(err => {
    console.error('Database Connection Failed!', err);
    process.exit(1);
  });
