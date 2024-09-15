const sql = require('mssql');
require('dotenv').config();

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

const poolPromise = new sql.ConnectionPool(config)
  .connect()
  .then(pool => {
    console.log('Connected to Azure SQL');
    return pool;
  })
  .catch(err => {
    console.error('Database Connection Failed!', err);
    process.exit(1);
  });

module.exports = {
  sql, poolPromise
};