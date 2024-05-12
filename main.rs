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

use bb8::Pool;
use bb8_postgres::PostgresConnectionManager;
use std::fs::File;
use std::sync::Arc;
use arrow::array::{
    ArrayRef, BooleanArray, Int16Array, Int32Array, Int64Array, Float32Array, Float64Array,
    StringArray, Date32Array, Date64Array, TimestampNanosecondArray
};
use arrow::datatypes::{Schema, DataType, TimeUnit};
use arrow::record_batch::RecordBatch;
use parquet::arrow::{ArrowReader, ParquetFileArrowReader};
use parquet::file::reader::SerializedFileReader;
use tokio::sync::Semaphore;
use tokio_postgres::{NoTls, Client, Error as PgError};
use tokio::task;
use futures::future::join_all;
use tokio_postgres::types::ToSql;
use tokio_postgres::Config;

use std::error::Error;

type DynError = Box<dyn Error + Send + Sync>;

#[tokio::main]
async fn main() -> Result<(), DynError> {
    let start = std::time::Instant::now();
    let file = File::open("data/flights.parquet")?;
    let file_reader = SerializedFileReader::new(file)?;
    let mut arrow_reader = ParquetFileArrowReader::new(Arc::new(file_reader));

    let arrow_schema = arrow_reader.get_schema()?;
    let table_name = "tommy";
    let create_table_sql = generate_create_table_sql(&table_name, &arrow_schema)?;
    let insert_statement = generate_insert_statement(&table_name, &arrow_schema)?;

    // Connection Pool Setup
    let mut config = Config::new();
    config.host("localhost");
    config.user("postgres");
    config.password("password");
    config.dbname("tfmv");
    let manager = PostgresConnectionManager::new(config, NoTls);
    let pool = Pool::builder().build(manager).await?;

    let sem = Arc::new(Semaphore::new(10)); // Control concurrency
    let mut futures = vec![];

    // Assuming you have only one client that starts the connection
    let conn = pool.get().await?;
    conn.execute(&create_table_sql, &[]).await?;

    let mut record_reader = arrow_reader.get_record_reader(10000)?;

    while let Some(batch_result) = record_reader.next() {
        match batch_result {
            Ok(batch) => {
                let sem_clone = sem.clone();
                let pool_clone = pool.clone();
                let stmt_clone = insert_statement.clone();

                let future = task::spawn(async move {
                    let _permit = sem_clone.acquire().await.expect("Failed to acquire semaphore");

                    let conn = pool_clone.get().await.expect("Failed to get a connection");
                    match conn.prepare(&stmt_clone).await {
                        Ok(stmt) => {
                            if let Err(e) = insert_batch(&conn, &stmt, &batch).await {
                                eprintln!("Failed to insert batch: {:?}", e);
                            }
                        },
                        Err(e) => eprintln!("Failed to prepare statement: {:?}", e),
                    }
                });
                futures.push(future);
            },
            Err(e) => {
                eprintln!("Failed to read batch: {:?}", e);
                break;
            }
        }
    }
    
    join_all(futures).await;

    println!("Time taken: {:?}", start.elapsed());
    Ok(())
}

fn generate_create_table_sql(table_name: &str, schema: &Schema) -> Result<String, DynError> {
    let columns_sql = schema.fields().iter().map(|field| {
        let col_name = field.name();
        let data_type = match field.data_type() {
            DataType::Boolean => "BOOLEAN",
            DataType::Int32 => "INT",
            DataType::Int64 => "BIGINT",
            DataType::Float32 | DataType::Float64 => "FLOAT",
            DataType::Utf8 => "TEXT",
            _ => "TEXT"
        };
        format!("{} {}", col_name, data_type)
    }).collect::<Vec<_>>().join(", ");
    Ok(format!("CREATE TABLE IF NOT EXISTS {} ({})", table_name, columns_sql))
}

fn generate_insert_statement(table_name: &str, schema: &Schema) -> Result<String, DynError> {
    let placeholders = schema.fields().iter().enumerate().map(|(i, _)| format!("${}", i + 1)).collect::<Vec<_>>().join(", ");
    Ok(format!("INSERT INTO {} VALUES ({})", table_name, placeholders))
}

async fn insert_batch(client: &Client, stmt: &tokio_postgres::Statement, batch: &RecordBatch) -> Result<(), PgError> {
    for row in 0..batch.num_rows() {
        let row_values = batch.columns().iter().map(|col| {
            arrow_to_postgres(col, row).expect("Failed to convert Arrow data to SQL data")
        }).collect::<Vec<_>>();
        client.execute(stmt, &row_values.iter().map(|v| v.as_ref() as &(dyn ToSql + Sync)).collect::<Vec<_>>()).await?;
    }
    Ok(())
}

fn arrow_to_postgres<'a>(column: &'a ArrayRef, row: usize) -> Result<Box<dyn ToSql + Sync + Send + 'a>, String> {
    match column.data_type() {
        DataType::Boolean => {
            column.as_any().downcast_ref::<BooleanArray>()
                .map(|array| Box::new(array.value(row)) as Box<dyn ToSql + Sync + Send>)
                .ok_or_else(|| "Failed to downcast BooleanArray".to_string())
        },
        DataType::Int16 => {
            column.as_any().downcast_ref::<Int16Array>()
                .map(|array| Box::new(i32::from(array.value(row))) as Box<dyn ToSql + Sync + Send>)
                .ok_or_else(|| "Failed to downcast Int16Array".to_string())
        },
        DataType::Int32 => {
            column.as_any().downcast_ref::<Int32Array>()
                .map(|array| Box::new(array.value(row)) as Box<dyn ToSql + Sync + Send>)
                .ok_or_else(|| "Failed to downcast Int32Array".to_string())
        },
        DataType::Int64 => {
            column.as_any().downcast_ref::<Int64Array>()
                .map(|array| Box::new(array.value(row)) as Box<dyn ToSql + Sync + Send>)
                .ok_or_else(|| "Failed to downcast Int64Array".to_string())
        },
        DataType::Float32 => {
            column.as_any().downcast_ref::<Float32Array>()
                .map(|array| Box::new(f64::from(array.value(row))) as Box<dyn ToSql + Sync + Send>)
                .ok_or_else(|| "Failed to downcast Float32Array".to_string())
        },
        DataType::Float64 => {
            column.as_any().downcast_ref::<Float64Array>()
                .map(|array| Box::new(array.value(row)) as Box<dyn ToSql + Sync + Send>)
                .ok_or_else(|| "Failed to downcast Float64Array".to_string())
        },
        DataType::Utf8 => {
            column.as_any().downcast_ref::<StringArray>()
                .map(|array| Box::new(array.value(row).to_string()) as Box<dyn ToSql + Sync + Send>)
                .ok_or_else(|| "Failed to downcast StringArray".to_string())
        },
        DataType::Date32 => {
            column.as_any().downcast_ref::<Date32Array>()
                .map(|array| Box::new(array.value(row)) as Box<dyn ToSql + Sync + Send>)
                .ok_or_else(|| "Failed to downcast Date32Array".to_string())
        },
        DataType::Date64 => {
            column.as_any().downcast_ref::<Date64Array>()
                .map(|array| Box::new(array.value(row)) as Box<dyn ToSql + Sync + Send>)
                .ok_or_else(|| "Failed to downcast Date64Array".to_string())
        },
        DataType::Timestamp(TimeUnit::Nanosecond, None) => {
            column.as_any().downcast_ref::<TimestampNanosecondArray>()
                .map(|array| Box::new(array.value(row)) as Box<dyn ToSql + Sync + Send>)
                .ok_or_else(|| "Failed to downcast TimestampNanosecondArray".to_string())
        },
        _ => Err(format!("Unsupported data type: {:?}", column.data_type()))
    }
}

