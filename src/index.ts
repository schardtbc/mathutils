

import { ParquetReader ,ParquetSchema, ParquetWriter } from 'parquets'

import { write } from 'fs';

// declare a schema for the `fruits` table
const schema = new ParquetSchema({
    name: { type: 'UTF8' },
    quantity: { type: 'INT64' },
    price: { type: 'DOUBLE' },
    date: { type: 'TIMESTAMP_MILLIS' },
    in_stock: { type: 'BOOLEAN' }
  });


async function  writesome() {
  // create new ParquetWriter that writes to 'fruits.parquet`
const writer =  await ParquetWriter.openFile(schema, 'fruits.parquet');

// append a few rows to the file
await writer.appendRow({name: 'apples', quantity: 10, price: 2.5, date: new Date(), in_stock: true});
await writer.appendRow({name: 'oranges', quantity: 10, price: 2.5, date: new Date(), in_stock: true});
await writer.close()

}

async function      readsome() {
    // create new ParquetWriter that writes to 'fruits.parquet`
  const reader =  await ParquetReader.openFile('fruits.parquet');
  
  // append a few rows to the file
  const cursor = reader.getCursor(["name","price"]);
  let record = null;
  while (record = await cursor.next()){
      console.log(record)
  }
  reader.close();
}
writesome();  
readsome();