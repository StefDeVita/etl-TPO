// index.js
import express from 'express';
import { config } from './config/config.js';
import { sql } from './config/db.js';

const app = express();
app.use(express.json());
const sqs = new config.AWS.SQS();
const eventBridge = new config.AWS.EventBridge();

async function deleteMessages(messages){
  for (let index  = 0; index < messages.length; index++) {
    let message = messages[index];
    const deleteParams = {
      QueueUrl: config.AWS_SQS_QUEUE_URL,
      ReceiptHandle: message.ReceiptHandle
    }
    sqs.deleteMessage(deleteParams, (err, data) => {
      if (err) {
        console.error("Error falopa", err);
      }
      else {
        console.log("Message " + message + " deleted successfully.")
      }
    })

  }
}


async function processMessages(messages){
  // const { tableName, data } = messages;

  // if (!tableName || !data || typeof data !== 'object' || !Object.keys(data).length) {
  //   return res.status(400).json({ message: 'Invalid tableName or data provided' });
  // }

  for (let index = 0; index < messages.length; index++) {
    const messageString = messages[index];
    try {
      const message = JSON.parse(messageString.Body);
      const tableName = "raw_"+message.module_id;
      const data = message.data;
      // Validar que el nombre de la tabla no contenga caracteres peligrosos
      if (!/^[a-zA-Z0-9_]+$/.test(tableName)) {
        console.log('Invalid table name format' );
      }
  
      // Conexión a la base de datos
      const pool = await config.poolPromise;
      // Preparar la solicitud SQL
      const request = pool.request();
       // Parameters
      const columns = Object.keys(data);
      const values = Object.values(data);
       request.input('id_usuario', sql.Int, values[0]);
       request.input('nombre', sql.VarChar, values[1]);
       request.input('tipo_usuario', sql.VarChar, values[2]);
       request.input('fecha_registro', sql.Date, new Date(values[3]));

       // Execute the query
       const result = await request.query(`
           INSERT INTO ${tableName} (${columns.join(', ')})
           VALUES (@id_usuario, @nombre, @tipo_usuario, @fecha_registro)
       `);
      //  const result = await request.query(`
      //     SELECT * FROM ${tableName} 
      // `);
      // // Obtener las columnas y los valores
      // const columns = Object.keys(data);
      // const values = Object.values(data);
      // let newValues = [values[0],
      // ...values.slice(1).map(value => `'${value}'`)]
      // // Construir la consulta de inserción dinámica
      // const query = `
      //   INSERT INTO ${tableName} (${columns.join(', ')}) 
      //   VALUES (${newValues.join(", ")})
      // `;
      // console.log(query)
      // columns.forEach((col, index) => {
      //   request.input(`value${index + 1}`, config.sql.VarChar, values[index]); // Puedes cambiar el tipo de dato según la necesidad
      // });
  
      // // Ejecutar la consulta
      // const result = await request.query(query);
  
      console.log('Data inserted successfully', result );
    } catch (error) {
      console.error('SQL error', error);
    }
  }
}

setInterval(()=>{
  sqs.receiveMessage({
    QueueUrl: config.AWS_SQS_QUEUE_URL // Replace with your SQS queue URL
  }, (err, data) => {
    if (err) {
      console.error(err);
    } else {
      console.log(data.Messages);
      // Process the received messages here
      processMessages(data.Messages);
      deleteMessages(data.Messages);
    }
  });
  console.log("procesa")
}, 10000);



const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`Server is running on port ${PORT}`);
});
