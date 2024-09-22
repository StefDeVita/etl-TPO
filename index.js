// index.js
import express from 'express';
import {config} from './config/config.js';

const app = express();
app.use(express.json());
const sqs = new config.AWS.SQS();
const eventBridge = new config.AWS.EventBridge();
sqs.receiveMessage({
  QueueUrl: config.AWS_SQS_QUEUE_URL // Replace with your SQS queue URL
}, (err, data) => {
  if (err) {
    console.error(err);
  } else {
    console.log(data.Messages);
    // Process the received messages here
  }
});

app.post('/api/data', async (req, res) => {
  const { tableName, data } = req.body;

  if (!tableName || !data || typeof data !== 'object' || !Object.keys(data).length) {
    return res.status(400).json({ message: 'Invalid tableName or data provided' });
  }

  try {
    // Validar que el nombre de la tabla no contenga caracteres peligrosos
    if (!/^[a-zA-Z0-9_]+$/.test(tableName)) {
      return res.status(400).json({ message: 'Invalid table name format' });
    }

    // Conexión a la base de datos
    const pool = await config.poolPromise;

    // Obtener las columnas y los valores
    const columns = Object.keys(data);
    const values = Object.values(data);

    // Construir la consulta de inserción dinámica
    const query = `
      INSERT INTO ${tableName} (${columns.join(', ')}) 
      VALUES (${columns.map((_, index) => `@value${index + 1}`).join(', ')})
    `;

    // Preparar la solicitud SQL
    const request = pool.request();
    columns.forEach((col, index) => {
      request.input(`value${index + 1}`, config.sql.VarChar, values[index]); // Puedes cambiar el tipo de dato según la necesidad
    });

    // Ejecutar la consulta
    const result = await request.query(query);

    res.status(200).json({ message: 'Data inserted successfully', result });
  } catch (error) {
    console.error('SQL error', error);
    res.status(500).json({ message: 'Internal server error', error });
  }
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`Server is running on port ${PORT}`);
});
