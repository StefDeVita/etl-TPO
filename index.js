// index.js
import express from 'express';
import { config } from './config/config.js';
import { sql } from './config/db.js';

const app = express();
app.use(express.json());
const sqs = new config.AWS.SQS();
async function usuarioCreado(pool, data) {
    const tableName = "raw_usuarios";
    // Validar que el nombre de la tabla no contenga caracteres peligrosos
    if (!/^[a-zA-Z0-9_]+$/.test(tableName)) {
      console.error('Invalid table name format' );
    }
    // Preparar la solicitud SQL
    const request = pool.request();
     // Parameters
    const defaultRole = "locatario";
    let rolUsuario;
    if (data.is_superuser){
      rolUsuario = "ceo";
    }
    else{
      if (data.is_staff){
        rolUsuario = "empleado";
      }
      else {
        if(data.rol_legales == "escribano"){
          rolUsuario = "abogado"
        }
        else{
          if(data.rol_logistica == "empleado_mudanza" || data.rol_logistica == "auditor"){
            rolUsuario = "mudanza"
          }
          if(data.rol_legales == "propietario"){
            rolUsuario = "locador"
          }

          else{
            rolUsuario = defaultRole;
          }
        }
      }
    }
    request.input('id_usuario', sql.VarChar, data.userId);
    request.input('nombre', sql.VarChar, data.username);
    request.input('tipo_usuario', sql.VarChar, rolUsuario);
    request.input('fecha_registro', sql.Date, new Date(data.register_date));
    request.input('fecha_nacimiento', sql.Date, new Date(data.birth_date));

    // Execute the query
    await request.query(`
        INSERT INTO ${tableName} (id_usuario, nombre, tipo_usuario, fecha_registro,fecha_nacimiento)
        VALUES (@id_usuario, @nombre, @tipo_usuario, @fecha_registro,@fecha_nacimiento)
    `);
    console.log('Data inserted successfully');
}
/* async function usuarioEliminado(pool, data) {
    const tableName = "raw_usuarios";
    // Validar que el nombre de la tabla no contenga caracteres peligrosos
    if (!/^[a-zA-Z0-9_]+$/.test(tableName)) {
      console.error('Invalid table name format' );
    }
    // Preparar la solicitud SQL
    const request = pool.request();
     // Parameters
    request.input('id_usuario', sql.VarChar, data.userId);
    const userExistsResult = await request.query(
      `SELECT COUNT(*) AS userCount FROM ${tableName} WHERE id_usuario = @id_usuario`
    );

    if (userExistsResult.recordset[0].userCount === 0) {
      throw new Error(`El usuario con id_usuario ${data.userId} no existe`);
    }
    // Execute the query
    await request.query(`

        UPDATE raw_contratos
        SET id_usuario_locatario = 'Usuario Eliminado'
        WHERE id_usuario_locatario = @id_usuario;
        UPDATE raw_contratos
        SET id_usuario_locador_o_mudanza = 'Usuario Eliminado'
        WHERE id_usuario_locador_o_mudanza = @id_usuario;
        UPDATE raw_contratos
        SET id_usuario_escribano = 'Usuario Eliminado'
        WHERE id_usuario_escribano = @id_usuario;
        DELETE FROM raw_pagos WHERE id_usuario = @id_usuario;
        DELETE FROM raw_mudanzas WHERE id_usuario = @id_usuario;
        DELETE FROM raw_reclamos WHERE id_usuario = @id_usuario;
        DELETE FROM raw_publicaciones WHERE id_usuario = @id_usuario;
        DELETE FROM ${tableName} WHERE id_usuario = @id_usuario;
    `);
    console.log('Data inserted successfully');
} */
async function usuarioModificado(pool, data) {
    const tableName = "raw_usuarios";
    // Validar que el nombre de la tabla no contenga caracteres peligrosos
    if (!/^[a-zA-Z0-9_]+$/.test(tableName)) {
      console.error('Invalid table name format' );
    }
    // Preparar la solicitud SQL
    const request = pool.request();
     // Parameters
    const defaultRole = "inquilino";
    let rolUsuario;
    if (data.rol_admin_int || data.rol_contable){
      rolUsuario = "empleado";
    }
    else{
      if (data.rol_legales){
        rolUsuario = "abogado";
      }
      else {
        if(data.rol_inmuebles){
          rolUsuario = "propietario"
        }
        else{
          if(data.rol_logistica){
            rolUsuario = ""
          }
          else{
            rolUsuario = defaultRole;
          }
        }
      }
    }
    // Check if the user exists
    request.input('id_usuario', sql.VarChar, data.userId);
    const userExistsResult = await request.query(
        `SELECT COUNT(*) AS userCount FROM ${tableName} WHERE id_usuario = @id_usuario`
    );

    if (userExistsResult.recordset[0].userCount === 0) {
        throw new Error(`El usuario con id_usuario ${data.userId} no existe`);
    }
    request.input('nombre', sql.VarChar, data.username);
    request.input('tipo_usuario', sql.VarChar, rolUsuario);
    request.input('fecha_registro', sql.Date, new Date(data.register_date));
    // Execute the query
    await request.query(`
        UPDATE ${tableName} SET nombre = @nombre, tipo_usuario = @tipo_usuario, fecha_registro = @fecha_registro WHERE id_usuario = @id_usuario
    `);
    console.log('Data modified successfully');
}
async function publicacionCreada(pool, data) {
    const tableName = "raw_publicaciones";
    // Validar que el nombre de la tabla no contenga caracteres peligrosos
    if (!/^[a-zA-Z0-9_]+$/.test(tableName)) {
      console.error('Invalid table name format');
    }
    // Preparar la solicitud SQL
    const request = pool.request();
     // Parameters
    data.price = isOverflow(data.price);
    request.input('id_publicacion', sql.VarChar, String(data.id));
    request.input('fecha_publicacion', sql.Date, new Date(data.created_at));
    request.input('precio_publicacion', sql.Decimal, data.price);
    request.input('direccion', sql.VarChar, data.address);
    request.input('habitaciones', sql.Int, data.rooms);
    request.input('barrio', sql.VarChar, data.district);
    request.input('latitud', sql.Decimal, data.latitude);
    request.input('longitud', sql.Decimal, data.longitude);
    request.input('estado', sql.VarChar, (data.active) ? "activada" : "desactivada");
    request.input('id_usuario', sql.VarChar, String(data.user_id));
    request.input('tipo', sql.VarChar, data.type);
    request.input('superficie_total_m2', sql.Int, data.surface_total);
    //TODO a chequear
    // Execute the query
    await request.query(`
        INSERT INTO ${tableName} (id_publicacion, fecha_publicacion, precio_publicacion, direccion, habitaciones, barrio, latitud, longitud, estado, id_usuario, tipo, superficie_total_m2)
        VALUES (@id_publicacion, @fecha_publicacion, @precio_publicacion, @direccion, @habitaciones, @barrio, @latitud, @longitud, @estado, @id_usuario, @tipo, @superficie_total_m2)
    `);
    console.log('Data inserted successfully' );
}
async function publicacionActualizada(pool, data) {
    const tableName = "raw_publicaciones";
    // Validar que el nombre de la tabla no contenga caracteres peligrosos
    if (!/^[a-zA-Z0-9_]+$/.test(tableName)) {
      console.error('Invalid table name format' );
    }
    // Preparar la solicitud SQL
    const request = pool.request();
    // Parameters
   request.input('id_publicacion', sql.VarChar, String(data.id));
   // Check if the publication exists
   const publicationExistsResult = await request.query(
       `SELECT COUNT(*) AS publicationCount FROM ${tableName} WHERE id_publicacion = @id_publicacion`
   );

   if (publicationExistsResult.recordset[0].publicationCount === 0) {
       throw new Error(`La publicacion con id_publicacion ${data.id} no existe`);
   }
   data.price = isOverflow(data.price);
   request.input('fecha_publicacion', sql.Date, new Date(data.created_at));
   request.input('precio_publicacion', sql.Decimal, data.price);
   request.input('direccion', sql.VarChar, data.address);
   request.input('habitaciones', sql.Int, data.rooms);
   request.input('barrio', sql.VarChar, data.district);
   request.input('latitud', sql.Decimal, data.latitude);
   request.input('longitud', sql.Decimal, data.longitude);
   request.input('estado', sql.VarChar, data.active ? 'activada': 'inactiva');
   request.input('id_usuario', sql.VarChar, data.owner_id);
   request.input('tipo', sql.VarChar, data.type);
   request.input('superficie_total_m2', sql.Int, data.surface_total);
   //TODO a chequear
    // Execute the query
    await request.query(`
        UPDATE ${tableName} SET fecha_publicacion = @fecha_publicacion, precio_publicacion = @precio_publicacion, direccion = @direccion, habitaciones = @habitaciones, barrio = @barrio, latitud = @latitud, longitud = @longitud, estado = @estado, id_usuario = @id_usuario, tipo = @tipo, superficie_total_m2 = @superficie_total_m2 WHERE id_publicacion = @id_publicacion
    `);
    console.log('Data inserted successfully' );
}
async function pagoAlquilerCreado(pool, data) {
    const tableName = "raw_pagos";
    // Validar que el nombre de la tabla no contenga caracteres peligrosos
    if (!/^[a-zA-Z0-9_]+$/.test(tableName)) {
      console.error('Invalid table name format' );
    }
    // Preparar la solicitud SQL
    const request = pool.request();
    const fechaISO = convertirFechaDDMMYYYYaISO(data.vencimiento);
    

     // Parameters
    request.input('id_pago', sql.VarChar, String(data.idFactura));
    request.input('fecha', sql.Date, new Date(fechaISO));
    request.input('monto', sql.Decimal, data.monto);
    request.input('id_usuario', sql.VarChar, data.idUsuarioPagador);
    request.input('estado', sql.VarChar, data.estado);
    request.input('financiable', sql.VarChar, data.financiable);
    request.input('descuento', sql.VarChar, '0');
    request.input('concepto', sql.VarChar, data.concepto);
    // Execute the query
    await request.query(`
        INSERT INTO ${tableName} (id_pago, fecha, monto, id_usuario, estado, financiable, descuento, concepto)
        VALUES (@id_pago, @fecha, @monto, @id_usuario, @estado, @financiable, @descuento, @concepto)
    `);
    console.log('Data inserted successfully' );
}
async function pagoRealizado(pool, data) {
    const tableName = "raw_pagos";
    // Validar que el nombre de la tabla no contenga caracteres peligrosos
    if (!/^[a-zA-Z0-9_]+$/.test(tableName)) {
      console.error('Invalid table name format' );
    }
    // Preparar la solicitud SQL
    const request = pool.request();

    if (!data.id){
      throw new Error("No hay id pago papi"); 
    }
     // Parameters
    request.input('id_pago', sql.VarChar, String(data.id));
    const paymentExistsResult = await request.query(
      `SELECT COUNT(*) AS paymentCount FROM ${tableName} WHERE id_pago = @id_pago`
    );

    if (paymentExistsResult.recordset[0].paymentCount === 0) {
      throw new Error(`El pago con id_pago ${data.id} no existe`);
    }
    request.input('descuento', sql.VarChar, String(data.descuentoAnticipado));
    request.input('estado', sql.VarChar, data.status);
    // Execute the query
    await request.query(`
      UPDATE ${tableName} SET estado = @estado, descuento = @descuento WHERE id_pago = @id_pago
    `);
    console.log('Data inserted successfully' );
}
async function nuevoContratoInmueble(pool, data) {
    const tableName = "raw_contratos";
    // Validar que el nombre de la tabla no contenga caracteres peligrosos
    if (!/^[a-zA-Z0-9_]+$/.test(tableName)) {
      console.error('Invalid table name format' );
    }
    // Preparar la solicitud SQL
    const request = pool.request();
     // Parameters
    console.log(data)
    let resultMonto = await request.query(`
      SELECT precio_publicacion FROM raw_publicaciones WHERE id_publicacion = ${data.publicationId}
    `);
    resultMonto = resultMonto.recordset[0]
    if(resultMonto && resultMonto.precio_publicacion){
      resultMonto = resultMonto.precio_publicacion
    }
    else{
      resultMonto = null;
    }
    request.input('id_contrato', sql.VarChar, String(data.contractId));
    request.input('id_publicacion', sql.VarChar, String(data.publicationId));
    request.input('id_usuario_locatario', sql.VarChar, String(data.tenantId));
    request.input('id_usuario_locador_o_mudanza', sql.VarChar, String(data.landLordId));
    request.input('id_usuario_escribano', sql.VarChar, null);
    request.input('tipo_contrato', sql.VarChar, "alquiler");
    request.input('fecha_firma', sql.Date, null);
    request.input('fecha_inicio', sql.Date, new Date(data.startDate));
    request.input('fecha_fin', sql.Date, new Date(data.endDate));
    request.input('monto', sql.Decimal, resultMonto ? resultMonto.output : null);
    request.input('estado_contrato', sql.VarChar, "pendiente");
    // Execute the query
    await request.query(`
        INSERT INTO ${tableName} (id_contrato, id_publicacion, id_usuario_locatario, id_usuario_locador_o_mudanza, id_usuario_escribano, tipo_contrato, fecha_firma, fecha_inicio, fecha_fin, monto, estado_contrato)
        VALUES (@id_contrato, @id_publicacion, @id_usuario_locatario, @id_usuario_locador_o_mudanza, @id_usuario_escribano, @tipo_contrato, @fecha_firma, @fecha_inicio, @fecha_fin, @monto, @estado_contrato)
    `);
    console.log('Data inserted successfully' );
}
async function nuevoContratoMudanza(pool, data) {
    const tableName = "raw_contratos";
    // Validar que el nombre de la tabla no contenga caracteres peligrosos
    if (!/^[a-zA-Z0-9_]+$/.test(tableName)) {
      console.error('Invalid table name format' );
    }
    // Preparar la solicitud SQL
    const request = pool.request();
     // Parameters
    request.input('id_contrato', sql.VarChar, String(data.contractId));
    request.input('id_publicacion', sql.VarChar, null);//????
    request.input('id_usuario_locatario', sql.VarChar, String(data.tenantId));
    request.input('id_usuario_locador_o_mudanza', sql.VarChar, data.busisnessId);
    request.input('id_usuario_escribano', sql.VarChar, null);
    request.input('tipo_contrato', sql.VarChar, "mudanza");
    request.input('fecha_firma', sql.Date, null);
    request.input('fecha_inicio', sql.Date, null);
    request.input('fecha_fin', sql.Date, null);
    request.input('monto', sql.Decimal, data.amount);
    request.input('estado_contrato', sql.VarChar, "pendiente");
    // Execute the query
    await request.query(`
        INSERT INTO ${tableName} (id_contrato, id_publicacion, id_usuario_locatario, id_usuario_locador_o_mudanza, id_usuario_escribano, tipo_contrato, fecha_firma, fecha_inicio, fecha_fin, monto, estado_contrato)
        VALUES (@id_contrato, @id_publicacion, @id_usuario_locatario, @id_usuario_locador_o_mudanza, @id_usuario_escribano, @tipo_contrato, @fecha_firma, @fecha_inicio, @fecha_fin, @monto, @estado_contrato)
    `);
    console.log('Data inserted successfully' );
}
async function contratoFirmado(pool, data) {
    const tableName = "raw_contratos";
    // Validar que el nombre de la tabla no contenga caracteres peligrosos
    if (!/^[a-zA-Z0-9_]+$/.test(tableName)) {
      console.error('Invalid table name format' );
    }
    // Preparar la solicitud SQL
    const request = pool.request();

    if (Array.isArray(data.signDate)){
      throw new Error("Error array fecha papi")
    }
     // Parameters
    request.input('id_contrato', sql.VarChar, String(data.contractId));
    const contractExistsResult = await request.query(
      `SELECT COUNT(*) AS contractCount FROM ${tableName} WHERE id_contrato = @id_contrato`
    );

    if (contractExistsResult.recordset[0].contractCount === 0) {
      throw new Error(`El contrato con id_contrato ${data.contractId} no existe`);
    }
    request.input('fecha_firma', sql.Date, new Date(data.signDate));
    request.input('estado_contrato', sql.VarChar, "firmado");
    const id_publicacion = await request.query(`SELECT id_publicacion FROM raw_contratos WHERE id_contrato = @id_contrato `); 
    request.input('id_publicacion', sql.VarChar, String(id_publicacion))
    // Execute the query
    await request.query(`
      UPDATE ${tableName} SET fecha_firma = @fecha_firma, estado_contrato = @estado_contrato WHERE id_contrato = @id_contrato; 
      UPDATE raw_publicaciones SET estado = 'inactiva' WHERE id_publicacion = @id_publicacion; 
    `);
    console.log('Data inserted successfully' );
}
async function contratoEliminadoDefinitivamente(pool, data) {
    const tableName = "raw_contratos";
    // Validar que el nombre de la tabla no contenga caracteres peligrosos
    if (!/^[a-zA-Z0-9_]+$/.test(tableName)) {
      console.error('Invalid table name format' );
    }
    // Preparar la solicitud SQL
    const request = pool.request();
     // Parameters
    request.input('id_contrato', sql.VarChar, String(data.contractId));
    const contractExistsResult = await request.query(
      `SELECT COUNT(*) AS contractCount FROM ${tableName} WHERE id_contrato = @id_contrato`
    );

    if (contractExistsResult.recordset[0].contractCount === 0) {
      throw new Error(`El contrato con id_contrato ${data.contractId} no existe`);
    }

    // Execute the query
    await request.query(`
      DELETE FROM ${tableName} WHERE id_contrato = @id_contrato
    `);
    console.log('Data inserted successfully' );
}
async function contratoRechazado(pool, data) {
    const tableName = "raw_contratos";
    // Validar que el nombre de la tabla no contenga caracteres peligrosos
    if (!/^[a-zA-Z0-9_]+$/.test(tableName)) {
      console.error('Invalid table name format' );
    }
    // Preparar la solicitud SQL
    const request = pool.request();
     // Parameters
    request.input('id_contrato', sql.VarChar, String(data.contractId));
    const contractExistsResult = await request.query(
      `SELECT COUNT(*) AS contractCount FROM ${tableName} WHERE id_contrato = @id_contrato`
    );

    if (contractExistsResult.recordset[0].contractCount === 0) {
      throw new Error(`El contrato con id_contrato ${data.contractId} no existe`);
    }
    request.input('estado_contrato', sql.VarChar, "rechazado");
    // Execute the query
    await request.query(`
      UPDATE ${tableName} SET estado_contrato = @estado_contrato WHERE id_contrato = @id_contrato
    `);
    console.log('Data inserted successfully' );
}
async function contratoMudanzaCompletada(pool, data) {
    const tableName = "raw_contratos";
    // Validar que el nombre de la tabla no contenga caracteres peligrosos
    if (!/^[a-zA-Z0-9_]+$/.test(tableName)) {
      console.error('Invalid table name format' );
    }
    // Preparar la solicitud SQL
    const request = pool.request();
     // Parameters

    if (isJson(data.idMudanza)){
      throw new Error("Error de mudanza papi");
    }     
    request.input('id_contrato', sql.VarChar, String(data.contractId));
    const contractExistsResult = await request.query(
      `SELECT COUNT(*) AS contractCount FROM ${tableName} WHERE id_contrato = @id_contrato`
    );

    if (contractExistsResult.recordset[0].contractCount === 0) {
      throw new Error(`El contrato con id_contrato ${data.contractId} no existe`);
    }
    request.input('estado_contrato', sql.VarChar, "finalizado");
    // Execute the query
    await request.query(`
      UPDATE ${tableName} SET estado_contrato = @estado_contrato WHERE id_contrato = @id_contrato
    `);
    console.log('Data inserted successfully' );
}
async function escribanoAsignado(pool, data) {
  const tableNameContratos = "raw_contratos";
  const tableNameUsuarios = "raw_usuarios";

  // Validar que los nombres de las tablas no contengan caracteres peligrosos
  if (!/^[a-zA-Z0-9_]+$/.test(tableNameContratos) || !/^[a-zA-Z0-9_]+$/.test(tableNameUsuarios)) {
    console.error('Invalid table name format');
  }
  // Preparar la solicitud SQL
  const request = pool.request();
  // Verificar si el contrato existe
  request.input('id_contrato', sql.VarChar, String(data.contractId));
  const contractExistsResult = await request.query(
    `SELECT COUNT(*) AS contractCount FROM ${tableNameContratos} WHERE id_contrato = @id_contrato`
  );
  if (contractExistsResult.recordset[0].contractCount === 0) {
    throw new Error(`El contrato con id_contrato ${data.contractId} no existe`);
  }
  // Verificar si el escribano existe
  request.input('id_usuario_escribano', sql.VarChar, String(data.notaryId));
  const notaryExistsResult = await request.query(
    `SELECT COUNT(*) AS userCount FROM ${tableNameUsuarios} WHERE id_usuario = @id_usuario_escribano`
  );
  if (notaryExistsResult.recordset[0].userCount === 0) {
    throw new Error(`El escribano con id_usuario ${data.notaryId} no existe`);
  }
  // Actualizar el contrato
  request.input('estado_contrato', sql.VarChar, "asignado");
  await request.query(`
    UPDATE ${tableNameContratos} 
    SET id_usuario_escribano = @id_usuario_escribano, estado_contrato = @estado_contrato 
    WHERE id_contrato = @id_contrato
  `);
  console.log('Data inserted successfully');
}
async function mudanzaSolicitada(pool, data) {
    const tableName = "raw_mudanzas";
    // Validar que el nombre de la tabla no contenga caracteres peligrosos
    if (!/^[a-zA-Z0-9_]+$/.test(tableName)) {
      console.error('Invalid table name format' );
    }
    // Preparar la solicitud SQL
    const request = pool.request();

    if (isJson(data.idMudanza)){
      throw new Error("Error de mudanza papi");
    }

     // Parameters
    request.input('id_mudanza', sql.VarChar, String(data.idMudanza));
    request.input('fecha_solicitud', sql.Date, new Date(data.startDate));
    request.input('fecha_realizacion', sql.Date, new Date(data.endDate));
    request.input('costo_mudanza', sql.Decimal, parseInt(data.amount));
    request.input('barrio_origen', sql.VarChar, null);
    request.input('barrio_destino', sql.VarChar, null);
    request.input('latitud_origen', sql.Decimal, null);
    request.input('longitud_origen', sql.Decimal, null);
    request.input('latitud_destino', sql.Decimal, null);
    request.input('longitud_destino', sql.Decimal, null);
    request.input('id_usuario', sql.VarChar, String(data.tenantId));
    // Execute the query
    await request.query(`
        INSERT INTO ${tableName} (id_mudanza, fecha_solicitud, fecha_realizacion, costo_mudanza, barrio_origen, barrio_destino, latitud_origen, longitud_origen, latitud_destino, longitud_destino, id_usuario)
        VALUES (@id_mudanza, @fecha_solicitud, @fecha_realizacion, @costo_mudanza, @barrio_origen, @barrio_destino, @latitud_origen, @longitud_origen, @latitud_destino, @longitud_destino, @id_usuario)
    `);
    console.log('Data inserted successfully' );
  
}
async function reclamoCreado(pool, data) {
  const tableName = "raw_reclamos";

  // Validar que el nombre de la tabla no contenga caracteres peligrosos
  if (!/^[a-zA-Z0-9_]+$/.test(tableName)) {
      console.error('Invalid table name format');
  }

  // Preparar la solicitud SQL
  const request = pool.request();

  // Obtener el mayor id_reclamo y calcular el nuevo id
  //const maxIdResult = await request.query(`SELECT ISNULL(MAX(id_reclamo), 0) + 1 AS newId FROM ${tableName}`);
  //const newId = maxIdResult.recordset[0].newId;

  // Parameters
  request.input('id_reclamo', sql.VarChar, String(data.id));
  request.input('fecha_reclamo', sql.Date, new Date (Date.now()));
  request.input('estado', sql.VarChar, 'abierto');
  request.input('categoria', sql.VarChar, data.categoria);
  request.input('id_usuario', sql.VarChar, data.cuitReclamante); //aca deberia ir el id no el username

  // Execute the query
  await request.query(`
      INSERT INTO ${tableName} (id_reclamo, fecha_reclamo, estado, id_usuario, categoria)
      VALUES (@id_reclamo, @fecha_reclamo, @estado, @id_usuario, @categoria)
  `);

  console.log('Data inserted successfully');
}
async function reclamoModificado(pool, data) {
    //TODO CHEQUEAR TEMA ID RECLAMO
    const tableName = "raw_reclamos";
    // Validar que el nombre de la tabla no contenga caracteres peligrosos
    if (!/^[a-zA-Z0-9_]+$/.test(tableName)) {
      console.error('Invalid table name format' );
    }
    // Preparar la solicitud SQL
    const request = pool.request();
     // Parameters
    request.input('id_reclamo', sql.VarChar, String(data.idReclamo));
    request.input('estado', sql.VarChar, data.estado);
    request.input('categoria', sql.VarChar, data.categoria);
    // Execute the query
    await request.query(`
        UPDATE ${tableName} SET categoria = @categoria , estado = @estado WHERE id_reclamo = @id_reclamo
    `);
    console.log('Data inserted successfully' );
  
}
function deleteMessage(message){
  const deleteParams = {
    QueueUrl: config.AWS_SQS_QUEUE_URL,
    ReceiptHandle: message.ReceiptHandle
  }
  console.log("BORRA MENSAJE");
  sqs.deleteMessage(deleteParams, (err, data) => {
    if (err) {
      console.error("Error: ", err);
    }
    else {
      console.log("Message " + message.ReceiptHandle + "deleted successfully.")
    }
  })
}

function isPrimaryKeyError(error) {
  return error && error.message && error.message.includes('Violation of PRIMARY KEY constraint');
}
function isJsonError(error){
  return error && error.message && error.message.includes('Error de mudanza papi');
}

function isArrayError(error){
  return error && error.message && error.message.includes('Error array fecha papi');
}

function isPagoError(error){
  return error && error.message && error.message.includes('No hay id pago papi');
}
function convertirFechaDDMMYYYYaISO(fecha) {
  const [dia, mes, año] = fecha.split('/');
  return `${año}-${mes}-${dia}`; // ISO 8601

}
function isOverflow(price){
  let newPrice = price; 
  if (price>8888888) {
    newPrice = String(price).slice(0,7); 
    newPrice = parseInt(newPrice, 10);
  }
  return newPrice;
}
function isJson(item) {
  let value = typeof item !== "string" ? JSON.stringify(item) : item;
  try {
    value = JSON.parse(value);
  } catch (e) {
    return false;
  }

  return typeof value === "object" && value !== null;
}

async function processMessages(pool, messages) {
  for (let index = 0; index < messages.length; index++) {
    const messageString = messages[index];
    try {
      const message = JSON.parse(messageString.Body);
      let messageBody = message.detail;
      if (message.detail.detailType !== undefined || message.detail['detail-type'] !== undefined) {
        messageBody = message.detail.detail;
      }
      console.log(message['detail-type']);
      console.log(messageBody);

      switch (message['detail-type']) {
        case 'ReclamoModificado':
          reclamoModificado(pool, messageBody)
            .then(() => deleteMessage(messageString))
            .catch((error) => {
              if (isPrimaryKeyError(error)) {
                console.warn('Clave primaria duplicada detectada. Eliminando mensaje...');
                deleteMessage(messageString);
              } else {
                console.error('Error procesando ReclamoModificado:', error);
              }
            });
          break;
          
        case 'ReclamoCreado':
          reclamoCreado(pool, messageBody)
            .then(() => deleteMessage(messageString))
            .catch((error) => {
              if (isPrimaryKeyError(error)) {
                console.warn('Clave primaria duplicada detectada. Eliminando mensaje...');
                deleteMessage(messageString);
              } else {
                console.error('Error procesando ReclamoCreado:', error);
              }
            });
          break;

        

        case 'ContratoMudanzaCompletada':
          contratoMudanzaCompletada(pool, messageBody)
            .then(() => deleteMessage(messageString))
            .catch((error) => {
              if (isPrimaryKeyError(error)) {
                console.warn('Clave primaria duplicada detectada. Eliminando mensaje...');
                deleteMessage(messageString);
              } 
              if (isJsonError(error)){
                console.warn('Error de los de mudanza...');
                deleteMessage(messageString);
              }
              else {
                console.error('Error procesando ContratoMudanzaCompletada:', error);
              }
            });
          break;

        case 'NuevoContratoMudanza':
          nuevoContratoMudanza(pool, messageBody)
            .then(() => deleteMessage(messageString))
            .catch((error) => {
              if (isPrimaryKeyError(error)) {
                console.warn('Clave primaria duplicada detectada. Eliminando mensaje...');
                deleteMessage(messageString);
              } else {
                console.error('Error procesando NuevoContratoMudanza:', error);
              }
            });
          break;

        case 'MudanzaSolicitada':
          mudanzaSolicitada(pool, messageBody)
            .then(() => deleteMessage(messageString))
            .catch((error) => {
              if (isPrimaryKeyError(error)) {
                console.warn('Clave primaria duplicada detectada. Eliminando mensaje...');
                deleteMessage(messageString);
              } 
              if (isJsonError(error)){
                console.warn('Error de los de mudanza...');
                deleteMessage(messageString);
              }
              else {
                console.error('Error procesando MudanzaSolicitada:', error);
              }
            });
          break;

        case 'UsuarioCreado':
          usuarioCreado(pool, messageBody)
            .then(() => deleteMessage(messageString))
            .catch((error) => {
              if (isPrimaryKeyError(error)) {
                console.warn('Clave primaria duplicada detectada. Eliminando mensaje...');
                deleteMessage(messageString);
              } else {
                console.error('Error procesando UsuarioCreado:', error);
              }
            });
          break;

        
        /*   usuarioEliminado(pool, messageBody)
            .then(() => deleteMessage(messageString))
            .catch((error) => {
              if (isPrimaryKeyError(error)) {
                console.warn('Clave primaria duplicada detectada. Eliminando mensaje...');
                deleteMessage(messageString);
              } else {
                console.error('Error procesando UsuarioEliminado:', error);
              }
            });
          break; */


        case 'UsuarioModificado':
          usuarioModificado(pool, messageBody)
            .then(() => deleteMessage(messageString))
            .catch((error) => {
              if (isPrimaryKeyError(error)) {
                console.warn('Clave primaria duplicada detectada. Eliminando mensaje...');
                deleteMessage(messageString);
              } else {
                console.error('Error procesando UsuarioModificado:', error);
              }
            });
          break;

        case 'PublicacionCreada':
          publicacionCreada(pool, messageBody)
            .then(() => deleteMessage(messageString))
            .catch((error) => {
              if (isPrimaryKeyError(error)) {
                console.warn('Clave primaria duplicada detectada. Eliminando mensaje...');
                deleteMessage(messageString);
              } else {
                console.error('Error procesando PublicacionCreada:', error);
              }
            });
          break;

        case 'PublicacionActualizada':
          publicacionActualizada(pool, messageBody)
            .then(() => deleteMessage(messageString))
            .catch((error) => {
              if (isPrimaryKeyError(error)) {
                console.warn('Clave primaria duplicada detectada. Eliminando mensaje...');
                deleteMessage(messageString);
              } else {
                console.error('Error procesando PublicacionActualizada:', error);
              }
            });
          break;

        case 'PagoCreado':
          pagoAlquilerCreado(pool, messageBody)
            .then(() => deleteMessage(messageString))
            .catch((error) => {
              if (isPrimaryKeyError(error)) {
                console.warn('Clave primaria duplicada detectada. Eliminando mensaje...');
                deleteMessage(messageString);
              } else {
                console.error('Error procesando PagoCreado:', error);
              }
            });
          break;

        case 'PagoRealizado':
          pagoRealizado(pool, messageBody)
            .then(() => deleteMessage(messageString))
            .catch((error) => {
              if (isPrimaryKeyError(error)) {
                console.warn('Clave primaria duplicada detectada. Eliminando mensaje...');
                deleteMessage(messageString);
              }
              if (isPagoError(error)){
                console.warn('Error de los de idPago...');
                deleteMessage(messageString);
              }
              else {
                console.error('Error procesando PagoRealizado:', error);
              }
            });
          break;
        
        case 'NuevoContratoInmueble':
          nuevoContratoInmueble(pool, messageBody)
          .then(() => deleteMessage(messageString))
          .catch((error) => {
            if (isPrimaryKeyError(error)) {
              console.warn('Clave primaria duplicada detectada. Eliminando mensaje...');
              deleteMessage(messageString);
            } else {
              console.error('Error procesando PagoRealizado:', error);
            }
          });
          break;
        case 'ContratoFirmado':
          contratoFirmado(pool, messageBody)
          .then(() => deleteMessage(messageString))
          .catch((error) => {
            if (isPrimaryKeyError(error)) {
              console.warn('Clave primaria duplicada detectada. Eliminando mensaje...');
              deleteMessage(messageString);
            }
            if (isArrayError(error)){
              console.warn('Error de fecha array...');
              deleteMessage(messageString);
            }
            else {
              console.error('Error procesando PagoRealizado:', error);
            }
          });
          break;
        case 'ContratoEliminadoDefinitivamente':
          contratoEliminadoDefinitivamente(pool, messageBody)
          .then(() => deleteMessage(messageString))
          .catch((error) => {
            if (isPrimaryKeyError(error)) {
              console.warn('Clave primaria duplicada detectada. Eliminando mensaje...');
              deleteMessage(messageString);
            } else {
              console.error('Error procesando ContratoEliminadoDefinitivamente:', error);
            }
          });
          break;
        case 'ContratoRechazado':
          contratoRechazado(pool, messageBody)
          .then(() => deleteMessage(messageString))
          .catch((error) => {
            if (isPrimaryKeyError(error)) {
              console.warn('Clave primaria duplicada detectada. Eliminando mensaje...');
              deleteMessage(messageString);
            } else {
              console.error('Error procesando PagoRealizado:', error);
            }
          });
          break;
        case 'EscribanoAsignado':
          escribanoAsignado(pool, messageBody)
          .then(() => deleteMessage(messageString))
          .catch((error) => {
            if (isPrimaryKeyError(error)) {
              console.warn('Clave primaria duplicada detectada. Eliminando mensaje...');
              deleteMessage(messageString);
            } else {
              console.error('Error procesando PagoRealizado:', error);
            }
          });
          break;
        case 'AdministradorCreado':
        case 'AdministradorEliminado':
        case 'AdministradorModificado':
        case 'PagoMudanzaCreado':
        case 'PagoMudanzaRealizado':
        case 'AlquilerSolicitado':
        case 'UsuarioEliminado':
        case 'EstadoVisitaModificado':
          deleteMessage(messageString);
          break;
      

        // Agrega otros casos aquí siguiendo el mismo patrón... */

        default:
          console.log("Unknown message type: ", message);
          break;
      }
    } catch (err) {
      console.error("Unable to process message: ", err);
    }
  }
}

setInterval(()=>{
  sqs.receiveMessage({
    QueueUrl: config.AWS_SQS_QUEUE_URL,
    WaitTimeSeconds: 3,
    MaxNumberOfMessages: 10
  }, async (err, data) => {
    if (err) {
      console.error(err);
    } else {
      // Conexión a la base de datos
      const pool = await config.poolPromise;
      if(pool.connected){
        // Process the received messages here
        processMessages(pool, data.Messages);
      }
      else{
        console.error("Pool connection failed, awaiting 20 seconds to retry");
      }
    }
  });
}, 3000);

app.get('/health', (req, res) => {
  const healthCheck = {
    uptime: process.uptime(),
    message: 'OK',
    timestamp: Date.now()
  };

  try {
    res.status(200).send(healthCheck);
  } catch (error) {
    healthCheck.message = error;
    res.status(503).send();
  }
});
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`Server is running on port ${PORT}`);
});