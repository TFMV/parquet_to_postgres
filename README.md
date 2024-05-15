# Parquet to PostgreSQL Importer

![PQ to PG](pqpg.webp)

## About

This Rust application seamlessly transfers data from Parquet files into a PostgreSQL database. Crafted by Thomas F McGeehan V, this tool is designed for efficiency and ease of use, enabling rapid integration of complex data structures into a relational database format without losing the nuances of the original data.

## Features

- **Direct Parquet Reading**: Utilizes `parquet-rs` to read Parquet files directly, preserving the integrity and structure of the original data.
- **Dynamic Schema Mapping**: Automatically generates SQL schema based on the Parquet file's metadata, ensuring that data types are correctly mapped to PostgreSQL equivalents.
- **Asynchronous Performance**: Leverages Rust's powerful async capabilities and the Tokio runtime for non-blocking database operations, maximizing throughput and efficiency.
- **Error Handling**: Robust error handling strategies ensure that any issues during the file reading or database insertion processes are clearly logged, making troubleshooting straightforward.

## Usage

1. **Setup PostgreSQL Database**: Ensure that your PostgreSQL server is running and accessible.
2. **Configure Database Connection**: Modify the database connection string in the source code to match your database server's credentials and address.
3. **Run the Application**: Place your Parquet file in the designated directory and run the application. The data will be automatically imported into the specified PostgreSQL database.

## License

This project is open-source and available under the MIT License. For more details, see the included LICENSE file or visit [MIT License](https://opensource.org/licenses/MIT).

## Acknowledgments

While not required, acknowledgment of the use of this project in your applications is appreciated but not mandatory.

---

Developed with ❤️ by Thomas F McGeehan V
