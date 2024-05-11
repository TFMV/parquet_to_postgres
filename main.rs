// --------------------------------------------------------------------------------
// Author: Thomas F McGeehan V
//
// This file is part of a software project developed by Thomas F McGeehan V.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.
//
// For more information about the MIT License, please visit:
// https://opensource.org/licenses/MIT
//
// Acknowledgment appreciated but not required.
// --------------------------------------------------------------------------------

use futures::future::join_all;
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::record::RowAccessor;
use std::fs::File;
use std::sync::Arc;
use tokio::sync::Semaphore;
use tokio::time::Instant;
use tokio::task;
use tokio_postgres::{NoTls, types::ToSql};

type DynError = Box<dyn std::error::Error + Send + Sync>;

#[tokio::main]
async fn main() -> Result<(), DynError> {
    let start = Instant::now();

    let file = File::open("data/data.parquet")?;
    let reader = SerializedFileReader::new(file)?;
    let schema_descr = reader.metadata().file_metadata().schema_descr();

    let table_name = "pq";
    let create_table_sql = generate_create_table_sql(table_name, &schema_descr)?;

    let (client, connection) = tokio_postgres::connect("host=localhost user=postgres password=password dbname=tfmv", NoTls).await?;
    let client = Arc::new(client);

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("Connection error: {}", e);
        }
    });

    client.execute(&create_table_sql, &[]).await?;

    let stmt = client.prepare(&format!("INSERT INTO {} VALUES ({})", table_name, placeholders_from_schema(&schema_descr))).await?;

    let sem = Arc::new(Semaphore::new(10));
    let mut futures = vec![];
    let mut count = 0;

    for row in reader.get_row_iter(None)? {
        let client = Arc::clone(&client);
        let stmt = stmt.clone();
        let permit = sem.clone().acquire_owned().await;
        let row_values = row_to_sql_values(&row, &schema_descr)?;
        let future = task::spawn(async move {
            let _permit = permit;
            let result = client.execute(
                &stmt, 
                &row_values.iter().map(|v| v.as_ref() as &(dyn ToSql + Sync)).collect::<Vec<&(dyn ToSql + Sync)>>()
            ).await;
            if let Err(e) = result {
                eprintln!("Error inserting row: {}", e);
            }
            Ok::<(), DynError>(())
        });
    
        futures.push(future);
        count += 1;
        if count % 1000 == 0 {
            let _: Vec<_> = join_all(futures).await.into_iter().collect::<Result<Vec<_>, _>>()?;
            futures = Vec::new();
        }
    }   

    if !futures.is_empty() {
        let _: Vec<_> = join_all(futures).await.into_iter().collect::<Result<Vec<_>, _>>()?;
    }

    println!("Total rows: {}", count);
    println!("Time taken: {:?}", start.elapsed());
    Ok(())
}

fn placeholders_from_schema(schema: &parquet::schema::types::SchemaDescriptor) -> String {
    (1..=schema.columns().len()).map(|i| format!("${}", i)).collect::<Vec<_>>().join(", ")
}

fn row_to_sql_values(row: &parquet::record::Row, schema: &parquet::schema::types::SchemaDescriptor) -> Result<Vec<Box<dyn ToSql + Sync + Send>>, DynError> {
    let mut values = Vec::new();
    for (i, col) in schema.columns().iter().enumerate() {
        let value: Box<dyn ToSql + Sync + Send> = match col.physical_type() {
            parquet::basic::Type::BOOLEAN => Box::new(row.get_bool(i).unwrap_or_default()),
            parquet::basic::Type::INT32 => Box::new(row.get_int(i).unwrap_or_default()),
            parquet::basic::Type::INT64 => Box::new(row.get_long(i).unwrap_or_default()),
            parquet::basic::Type::FLOAT => Box::new(row.get_float(i).map(|f| f as f64).unwrap_or_default()),  // Convert f32 to f64
            parquet::basic::Type::DOUBLE => Box::new(row.get_double(i).unwrap_or_default()),
            parquet::basic::Type::BYTE_ARRAY => Box::new(row.get_string(i).map(|s| s.to_string()).unwrap_or_else(|_| "".to_string())),
            _ => Box::new("Unsupported type")
        };
        values.push(value);
    }
    Ok(values)
}

fn generate_create_table_sql(table_name: &str, schema: &parquet::schema::types::SchemaDescriptor) -> Result<String, DynError> {
    let columns_sql = schema.columns().iter().map(|col| {
        let col_name = col.name();
        let data_type = match col.physical_type() {
            parquet::basic::Type::BOOLEAN => "BOOLEAN",
            parquet::basic::Type::INT32 => "INT",
            parquet::basic::Type::INT64 => "BIGINT",
            parquet::basic::Type::FLOAT => "FLOAT",
            parquet::basic::Type::DOUBLE => "DOUBLE PRECISION",
            parquet::basic::Type::BYTE_ARRAY => "TEXT",
            _ => "TEXT"
        };
        format!("{} {}", col_name, data_type)
    }).collect::<Vec<_>>().join(", ");

    let sql = format!("CREATE TABLE IF NOT EXISTS {} ({})", table_name, columns_sql);
    Ok(sql)
}
