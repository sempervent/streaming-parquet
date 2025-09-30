use crate::{
    cli::{Cli, OutputFormat},
    csv_in::{CsvConfig, CsvReader},
    discover::{discover_inputs, DiscoveryConfig, InputFile},
    error::{MawError, Result},
    parquet_in::ParquetReader,
    schema::UnifiedSchema,
    writer_csv::{CsvWriter, CsvWriterConfig},
    writer_parquet::{ParquetWriter, ParquetWriterConfig},
};
use arrow2::{array::Array, chunk::Chunk};
use std::{
    path::PathBuf,
    sync::Arc,
};
use tokio::sync::mpsc;

pub struct Pipeline {
    cli: Cli,
    unified_schema: Arc<UnifiedSchema>,
}

impl Pipeline {
    pub fn new(cli: Cli) -> Self {
        Self {
            cli,
            unified_schema: Arc::new(UnifiedSchema::new()),
        }
    }

    pub async fn execute(&self) -> Result<()> {
        // Discover input files
        let discovery_config = DiscoveryConfig {
            recursive: !self.cli.no_recursive,
            follow_symlinks: self.cli.follow_symlinks,
            max_depth: None,
        };

        let input_files = discover_inputs(&self.cli.inputs, &discovery_config)?;
        
        if input_files.is_empty() {
            return Err(MawError::InvalidInput("No input files found".to_string()));
        }

        // Build unified schema from all inputs
        let unified_schema = self.build_unified_schema(&input_files).await?;
        
        // Create output writer
        let output_path = self.cli.out.clone()
            .unwrap_or_else(|| PathBuf::from("output"));
        
        let output_format = self.determine_output_format(&output_path)?;
        
        // Set up concurrent processing
        self.process_files_concurrently(&input_files, &unified_schema, &output_path, output_format).await
    }

    async fn build_unified_schema(&self, _input_files: &[InputFile]) -> Result<UnifiedSchema> {
        // For now, create a simple unified schema
        // In a real implementation, we would sample each file and build the schema
        Ok(UnifiedSchema::new())
    }

    fn determine_output_format(&self, path: &PathBuf) -> Result<OutputFormat> {
        if let Some(format) = &self.cli.out_format {
            return Ok(format.clone());
        }

        match path.extension().and_then(|ext| ext.to_str()) {
            Some("csv") => Ok(OutputFormat::Csv),
            Some("parquet") => Ok(OutputFormat::Parquet),
            _ => Ok(OutputFormat::Csv), // Default to CSV
        }
    }

    async fn process_files_concurrently(
        &self,
        input_files: &[InputFile],
        _unified_schema: &UnifiedSchema,
        output_path: &PathBuf,
        output_format: OutputFormat,
    ) -> Result<()> {
        let (tx, rx) = mpsc::channel::<Chunk<Box<dyn Array>>>(8); // Bounded channel
        
        // Spawn readers
        let reader_handles = self.spawn_readers(input_files, tx).await?;
        
        // Spawn writer
        let writer_handle = self.spawn_writer(output_path, output_format, rx).await?;
        
        // Wait for all readers to complete
        for handle in reader_handles {
            handle.await??;
        }
        
        // Wait for writer to complete
        writer_handle.await??;
        
        Ok(())
    }

    async fn spawn_readers(
        &self,
        input_files: &[InputFile],
        tx: mpsc::Sender<Chunk<Box<dyn Array>>>,
    ) -> Result<Vec<tokio::task::JoinHandle<Result<()>>>> {
        let mut handles = Vec::new();
        
        for file in input_files {
            let tx_clone = tx.clone();
            let file_path = file.path.clone();
            let format = file.format.clone();
            let batch_size = 64_000; // Default batch size
            
            let handle = tokio::task::spawn_blocking(move || {
                match format {
                    crate::discover::FileFormat::Csv => {
                        let config = CsvConfig::default();
                        let mut reader = CsvReader::new(&file_path, &config)?;
                        
                        loop {
                            match reader.read_batch()? {
                                Some(batch) => {
                                    if tx_clone.blocking_send(batch).is_err() {
                                        break; // Channel closed
                                    }
                                }
                                None => break,
                            }
                        }
                    }
                    crate::discover::FileFormat::Parquet => {
                        let mut reader = ParquetReader::new(&file_path, batch_size)?;
                        
                        loop {
                            match reader.read_batch()? {
                                Some(batch) => {
                                    if tx_clone.blocking_send(batch).is_err() {
                                        break; // Channel closed
                                    }
                                }
                                None => break,
                            }
                        }
                    }
                }
                Ok(())
            });
            
            handles.push(handle);
        }
        
        Ok(handles)
    }

    async fn spawn_writer(
        &self,
        output_path: &PathBuf,
        output_format: OutputFormat,
        mut rx: mpsc::Receiver<Chunk<Box<dyn Array>>>,
    ) -> Result<tokio::task::JoinHandle<Result<()>>> {
        let output_path = output_path.clone();
        
        let handle = tokio::task::spawn_blocking(move || {
            match output_format {
                OutputFormat::Csv => {
                    let config = CsvWriterConfig::default();
                    let mut writer = CsvWriter::new(&output_path, &config)?;
                    
                    while let Some(batch) = rx.blocking_recv() {
                        writer.write_batch(&batch)?;
                    }
                    
                    writer.finish()?;
                }
                OutputFormat::Parquet => {
                    // For Parquet, we need the schema - this is simplified
                    let schema = arrow2::datatypes::Schema::from(vec![]);
                    let config = ParquetWriterConfig::default();
                    let mut writer = ParquetWriter::new(&output_path, Arc::new(schema), &config)?;
                    
                    while let Some(batch) = rx.blocking_recv() {
                        writer.write_batch(&batch)?;
                    }
                    
                    writer.finish()?;
                }
            }
            Ok(())
        });
        
        Ok(handle)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::discover::{FileFormat, InputFile};
    use std::path::PathBuf;
    use tempfile::tempdir;

    #[test]
    fn test_pipeline_creation() {
        let cli = Cli::parse_from(&["maw", "test.csv"]);
        let pipeline = Pipeline::new(cli);
        assert!(pipeline.cli.inputs.len() > 0);
    }

    #[test]
    fn test_output_format_detection() {
        let cli = Cli::parse_from(&["maw", "test.csv"]);
        let pipeline = Pipeline::new(cli);
        
        let csv_path = PathBuf::from("test.csv");
        let format = pipeline.determine_output_format(&csv_path).unwrap();
        assert!(matches!(format, OutputFormat::Csv));
        
        let parquet_path = PathBuf::from("test.parquet");
        let format = pipeline.determine_output_format(&parquet_path).unwrap();
        assert!(matches!(format, OutputFormat::Parquet));
    }
}
