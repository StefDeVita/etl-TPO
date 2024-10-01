// index.js
import express from 'express';
import { config } from './config/config.js';
import { sql } from './config/db.js';
import UUID from 'uuid-int';

const app = express();
app.use(express.json());
const sqs = new config.AWS.SQS();

async function parseUserModuleData(message) {
  try {
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

    console.log('Data inserted successfully', result );
  } catch (error) {
    console.error('SQL error', error);
  }
}
async function parseRealEstateModuleData(message) {
  try {
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
     request.input('id_publicacion', sql.Int, values[0]);
     request.input('fecha_publicacion', sql.Date, new Date(values[1]));
     request.input('precio_publicacion', sql.Decimal, values[2]);
     request.input('tipo_publicacion', sql.VarChar, values[3]);
     request.input('barrio', sql.VarChar, values[4]);
     request.input('latitud', sql.Decimal, values[5]);
     request.input('longitud', sql.Decimal, values[6]);
     request.input('id_usuario', sql.Int, values[7]);

     console.log(`
         INSERT INTO ${tableName} (${columns.join(', ')})
         VALUES (@id_publicacion, @fecha_publicacion, @precio_publicacion, @tipo_publicacion, @barrio, @latitud, @longitud, @id_usuario)
     `)
     // Execute the query
     const result = await request.query(`
         INSERT INTO ${tableName} (${columns.join(', ')})
         VALUES (@id_publicacion, @fecha_publicacion, @precio_publicacion, @tipo_publicacion, @barrio, @latitud, @longitud, @id_usuario)
     `);

    console.log('Data inserted successfully', result );
  } catch (error) {
    console.error('SQL error', error);
  }
}
async function parseAccountabilityModuleData(message) {

}
async function parsePaymentsModuleData(message) {

}
async function parseLegalsModuleData(message) {

}
async function parseLogisticsModuleData(message) {

}

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
        console.log("Message " + message.ReceiptHandle + " deleted successfully.")
      }
    })
  }
}


async function processMessages(messages){

  for (let index = 0; index < messages.length; index++) {
    const messageString = messages[index];
    try{
      const message = JSON.parse(messageString.Body);
      switch (message.module_id) {
        case 'usuarios':
          parseUserModuleData(message)
          break;
        case 'publicaciones':
          parseRealEstateModuleData(message)
          break;
        case 'pagos':
          parsePaymentsModuleData(message)
          break;
        case 'mudanzas':
          parseLogisticsModuleData(message)
          break;
        case 'financiamientos':
          parseAccountabilityModuleData(message)
          break;
        case 'contratos':
          parseLegalsModuleData(message)
          break;
        default:
          break;
      }
    }
    catch (err) {
      console.error("Unable to process message: ", err)
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
