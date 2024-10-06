// index.js
import express from 'express';
import { config } from './config/config.js';
import { sql } from './config/db.js';

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

    console.log('Data inserted successfully');
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
     request.input('direccion', sql.VarChar, values[3]);
     request.input('habitaciones', sql.Int, values[4]);
     request.input('barrio', sql.VarChar, values[5]);
     request.input('latitud', sql.Decimal, values[6]);
     request.input('longitud', sql.Decimal, values[7]);
     request.input('estado', sql.VarChar, values[8]);
     request.input('id_usuario', sql.Int, values[9]);
     request.input('tipo', sql.VarChar, values[10]);
     request.input('superficie_total_m2', sql.Int, values[11]);
     request.input('ganancia_generada', sql.Decimal, values[12]);
     // Execute the query
     const result = await request.query(`
         INSERT INTO ${tableName} (${columns.join(', ')})
         VALUES (@id_publicacion, @fecha_publicacion, @precio_publicacion, @direccion, @habitaciones, @barrio, @latitud, @longitud, @estado, @id_usuario, @tipo, @superficie_total_m2, @ganancia_generada)
     `);

    console.log('Data inserted successfully' );
  } catch (error) {
    console.error('SQL error', error);
  }
}
async function parseAccountabilityModuleData(message) {
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
     request.input('id_financiamiento', sql.Int, values[0]);
     request.input('fecha_solicitud', sql.Date, new Date(values[1]));
     request.input('monto_solicitado', sql.Decimal, values[2]);
     request.input('monto_aprobado', sql.Decimal, values[3]);
     request.input('estado_solicitud', sql.VarChar, values[4]);
     request.input('id_usuario', sql.Int, values[5]);
     // Execute the query
     const result = await request.query(`
         INSERT INTO ${tableName} (${columns.join(', ')})
         VALUES (@id_financiamiento, @fecha_solicitud, @monto_solicitado, @monto_aprobado, @estado_solicitud, @id_usuario)
     `);

    console.log('Data inserted successfully' );
  } catch (error) {
    console.error('SQL error', error);
  }
}
async function parsePaymentsModuleData(message) {
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
     request.input('id_pago', sql.Int, values[0]);
     request.input('fecha', sql.Date, new Date(values[1]));
     request.input('monto', sql.Decimal, values[2]);
     request.input('id_publicacion', sql.Int, values[3]);
     request.input('id_usuario', sql.Int, values[4]);
     request.input('estado', sql.VarChar, values[5]);
     // Execute the query
     const result = await request.query(`
         INSERT INTO ${tableName} (${columns.join(', ')})
         VALUES (@id_pago, @fecha, @monto, @id_publicacion, @id_usuario, @estado)
     `);

    console.log('Data inserted successfully' );
  } catch (error) {
    console.error('SQL error', error);
  }
}
async function parseLegalsModuleData(message) {
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
     request.input('id_contrato', sql.Int, values[0]);
     request.input('id_publicacion', sql.Int, values[1]);
     request.input('id_usuario_locatario', sql.Int, values[2]);
     request.input('id_usuario_locador_o_mudanza', sql.Int, values[3]);
     request.input('id_usuario_escribano', sql.Int, values[4]);
     request.input('tipo_contrato', sql.VarChar, values[5]);
     request.input('fecha_firma', sql.Date, new Date(values[6]));
     request.input('fecha_inicio', sql.Date, new Date(values[7]));
     request.input('fecha_fin', sql.Date, new Date(values[8]));
     request.input('monto', sql.Decimal, values[9]);
     request.input('estado_contrato', sql.VarChar, values[10]);
     // Execute the query
     const result = await request.query(`
         INSERT INTO ${tableName} (${columns.join(', ')})
         VALUES (@id_contrato, @id_publicacion, @id_usuario_locatario, @id_usuario_locador_o_mudanza, @id_usuario_escribano, @tipo_contrato, @fecha_firma, @fecha_inicio, @fecha_fin, @monto, @estado_contrato)
     `);

    console.log('Data inserted successfully' );
  } catch (error) {
    console.error('SQL error', error);
  }
}
async function parseLogisticsModuleData(message) {
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
     request.input('id_mudanza', sql.Int, values[0]);
     request.input('fecha_solicitud', sql.Date, new Date(values[1]));
     request.input('fecha_realizacion', sql.Date, new Date(values[2]));
     request.input('costo_mudanza', sql.Decimal, values[3]);
     request.input('barrio_origen', sql.VarChar, values[4]);
     request.input('barrio_destino', sql.VarChar, values[5]);
     request.input('latitud_origen', sql.Decimal, values[6]);
     request.input('longitud_origen', sql.Decimal, values[7]);
     request.input('latitud_destino', sql.Decimal, values[8]);
     request.input('longitud_destino', sql.Decimal, values[9]);
     request.input('id_usuario', sql.Int, values[10]);

     // Execute the query
     const result = await request.query(`
         INSERT INTO ${tableName} (${columns.join(', ')})
         VALUES (@id_mudanza, @fecha_solicitud, @fecha_realizacion, @costo_mudanza, @barrio_origen, @barrio_destino, @latitud_origen, @longitud_origen, @latitud_destino, @longitud_destino, @id_usuario)
     `);

    console.log('Data inserted successfully' );
  } catch (error) {
    console.error('SQL error', error);
  }
}
async function parseTicketsModuleData(message) {
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
     request.input('id_reclamo', sql.Int, values[0]);
     request.input('fecha__reclamo', sql.Date, new Date(values[1]));
     request.input('estado', sql.VarChar, values[2]);
     request.input('id_usuario', sql.Int, values[3]);
     request.input('categoria', sql.VarChar, values[4]);
     // Execute the query
     const result = await request.query(`
         INSERT INTO ${tableName} (${columns.join(', ')})
         VALUES (@id_reclamo, @fecha__reclamo, @estado, @id_usuario, @categoria)
     `);

    console.log('Data inserted successfully' );
  } catch (error) {
    console.error('SQL error', error);
  }
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
        console.error("Error", err);
      }
      else {
        console.log("Message " + "deleted successfully.")
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
        case 'reclamos':
          parseTicketsModuleData(message)
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
    QueueUrl: config.AWS_SQS_QUEUE_URL, // Replace with your SQS queue URL
    WaitTimeSeconds: 20,
    MaxNumberOfMessages: 10
  }, (err, data) => {
    if (err) {
      console.error(err);
    } else {
      data.Messages.forEach(message => {
        console.log(JSON.parse(message.Body));
      });
      // Process the received messages here
      processMessages(data.Messages);
      deleteMessages(data.Messages);
    }
  });
}, 20000);



const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`Server is running on port ${PORT}`);
});
