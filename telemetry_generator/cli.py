# # #!/usr/bin/env python3
# # """
# # Telemetry Generator CLI
# # Command-line interface for generating telemetry data with configurable parameters
# # """

# # import click
# # import json
# # import os
# # import sys
# # import time
# # import random
# # import logging
# # from pathlib import Path
# # from typing import Optional, Dict, Any

# # from .generator import EnhancedTelemetryGeneratorPro, OutputFormat, RecordType
# # from .rolling_writer import RollingFileWriter
# # from .rate_control import RateLimiter
# # from .load_profiles import LOAD_PROFILES, LoadProfile

# # # Configure logging
# # logging.basicConfig(
# #     level=logging.INFO,
# #     format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
# # )
# # logger = logging.getLogger('telegen')
# # def parse_size(size_str: str) -> int:
# #     """Parse size string like '512MB' to bytes"""
# #     import re
    
# #     if isinstance(size_str, int):
# #         return size_str
    
# #     size_str = str(size_str).upper().strip()
    
# #     # Handle numeric-only values (assume bytes)
# #     if size_str.isdigit():
# #         return int(size_str)
    
# #     # Parse with suffix using regex
# #     match = re.match(r'(\d+(?:\.\d+)?)\s*([KMGT]?B?)', size_str)
# #     if not match:
# #         raise ValueError(f"Invalid size format: {size_str}")
    
# #     value, unit = match.groups()
# #     value = float(value)
    
# #     # Define multipliers - order matters! Longer units first
# #     multipliers = {
# #         'TB': 1024**4,
# #         'GB': 1024**3,
# #         'MB': 1024**2,
# #         'KB': 1024,
# #         'T': 1024**4,
# #         'G': 1024**3,
# #         'M': 1024**2,
# #         'K': 1024,
# #         'B': 1,
# #         '': 1,  # No unit
# #     }
    
# #     return int(value * multipliers.get(unit, 1))
# # # def parse_size(size_str: str) -> int:
# # #     """Parse size string like '512MB' to bytes"""
# # #     size_str = size_str.upper().strip()
    
# # #     units = {
# # #     'B': 1,
# # #     'K': 1024,
# # #     'KB': 1024,
# # #     'M': 1024 * 1024,
# # #     'MB': 1024 * 1024,
# # #     'G': 1024 * 1024 * 1024,
# # #     'GB': 1024 * 1024 * 1024,
# # #     'T': 1024 * 1024 * 1024 * 1024,
# # #     'TB': 1024 * 1024 * 1024 * 1024
# # # }
    
# # #     # Find the unit
# # #     for unit, multiplier in units.items():
# # #         if size_str.endswith(unit):
# # #             number_part = size_str[:-len(unit)].strip()
# # #             try:
# # #                 return int(float(number_part) * multiplier)
# # #             except ValueError:
# # #                 raise ValueError(f"Invalid size format: {size_str}")
    
# # #     # If no unit found, assume bytes
# # #     try:
# # #         return int(size_str)
# # #     except ValueError:
# # #         raise ValueError(f"Invalid size format: {size_str}")

# # def validate_schema(schema_path: str) -> Dict[str, Any]:
# #     """Validate and load schema file"""
# #     if not os.path.exists(schema_path):
# #         raise click.ClickException(f"Schema file not found: {schema_path}")
    
# #     try:
# #         with open(schema_path, 'r') as f:
# #             schema = json.load(f)
        
# #         if not isinstance(schema, dict):
# #             raise ValueError("Schema must be a JSON object")
        
# #         if not schema:
# #             raise ValueError("Schema cannot be empty")
            
# #         return schema
# #     except json.JSONDecodeError as e:
# #         raise click.ClickException(f"Invalid JSON in schema file: {e}")
# #     except Exception as e:
# #         raise click.ClickException(f"Error reading schema: {e}")

# # @click.group()
# # @click.version_option(version='1.0.0')
# # def cli():
# #     """Telemetry Generator - High-performance telemetry data generator"""
# #     pass

# # # @cli.command()
# # # @click.option('--schema', '-s', required=True, type=click.Path(exists=True),
# # #               help='Path to JSON schema file')
# # # @click.option('--rate', '-r', default=1000, type=int,
# # #               help='Records per second (default: 1000)')
# # # @click.option('--duration', '-d', default=60, type=int,
# # #               help='Duration in seconds (default: 60)')
# # # @click.option('--out-dir', '-o', default='data/', type=click.Path(),
# # #               help='Output directory (default: data/)')
# # # # @click.option('--rotate-size', default='',
# # # #               help='Maximum file size before rotation (default: 512MB)')
# # # @click.option('--rotate-size', default='512M',
# # #               help='Maximum file size before rotation (default: 512M)')
# # # @click.option('--format', '-f', 
# # #               type=click.Choice(['ndjson', 'json', 'binary', 'influx', 'leb128']),
# # #               default='ndjson',
# # #               help='Output format (default: ndjson)')
# # # @click.option('--seed', type=int, help='Random seed for reproducible data')
# # # @click.option('--load-profile', '-l',
# # #               type=click.Choice(['low', 'medium', 'high', 'custom']),
# # #               help='Predefined load profile')
# # # @click.option('--compress', is_flag=True, help='Enable compression (gzip)')
# # # @click.option('--batch-size', '-b', default=100, type=int,
# # #               help='Batch size for writing (default: 100)')
# # # @click.option('--prefix', '-p', default='telemetry',
# # #               help='Filename prefix (default: telemetry)')
# # # @click.option('--workers', '-w', default=4, type=int,
# # #               help='Number of worker threads (default: 4)')
# # # @click.option('--quiet', '-q', is_flag=True, help='Suppress progress output')
# # # @click.option('--verbose', '-v', is_flag=True, help='Verbose output')
# # # @click.option('--gpu', is_flag=True, help='Enable GPU acceleration if available')
# # # @click.option('--record-type-ratio', default='update:0.7,event:0.3',
# # #               help='Record type ratio (default: update:0.7,event:0.3)')
# # # def generate(schema, rate, duration, out_dir, rotate_size, format, seed, 
# # #             load_profile, compress, batch_size, prefix, workers, quiet, 
# # #             verbose, gpu, record_type_ratio):
# # #     """Generate telemetry data with specified parameters
    
# # #     Examples:
# # #         # Basic usage
# # #         telegen generate --schema schema.json --rate 1000 --duration 60
        
# # #         # High load with large files
# # #         telegen generate -s schema.json --load-profile high --rotate-size 1GB
        
# # #         # Specific format with compression
# # #         telegen generate -s schema.json -f binary --compress --out-dir /data/telemetry/
        
# # #         # Reproducible data with seed
# # #         telegen generate -s schema.json --seed 42 --rate 5000 --duration 300
# # #     """
    
# # #     # Set logging level
# # #     if verbose:
# # #         logger.setLevel(logging.DEBUG)
# # #     elif quiet:
# # #         logger.setLevel(logging.WARNING)
    
# # #     # Apply load profile if specified
# # #     if load_profile and load_profile != 'custom':
# # #         profile = LOAD_PROFILES[load_profile]
# # #         rate = profile.rate
# # #         batch_size = profile.batch_size
# # #         if profile.duration:
# # #             duration = profile.duration
# # #         if profile.workers:
# # #             workers = profile.workers
# # #         if profile.use_gpu:
# # #             gpu = True
# # #         logger.info(f"Applied load profile '{load_profile}': rate={rate}, batch={batch_size}")
    
# # #     # Parse record type ratio
# # #     ratio_dict = {}
# # #     for part in record_type_ratio.split(','):
# # #         type_name, ratio = part.split(':')
# # #         ratio_dict[RecordType[type_name.upper()]] = float(ratio)
    
# # #     # Set random seed if provided
# # #     if seed is not None:
# # #         random.seed(seed)
# # #         logger.info(f"Set random seed: {seed}")
    
# # #     # Validate schema
# # #     try:
# # #         schema_data = validate_schema(schema)
# # #         logger.info(f"Loaded schema with {len(schema_data)} fields")
# # #     except Exception as e:
# # #         raise click.ClickException(str(e))
    
# # #     # Create output directory
# # #     Path(out_dir).mkdir(parents=True, exist_ok=True)
    
# # #     # Parse rotation size
# # #     try:
# # #         max_file_size = parse_size(rotate_size)
# # #         logger.info(f"Max file size: {max_file_size:,} bytes")
# # #     except ValueError as e:
# # #         raise click.ClickException(str(e))
    
# # #     # Map format strings to OutputFormat enum
# # #     format_map = {
# # #         'ndjson': 'ndjson',
# # #         'json': OutputFormat.JSON,
# # #         'binary': OutputFormat.BINARY,
# # #         'influx': OutputFormat.INFLUX_LINE,
# # #         'leb128': 'leb128'
# # #     }
# # #     output_format = format_map[format]
    
# # #     # Initialize generator
# # #     try:
# # #         generator = EnhancedTelemetryGeneratorPro(
# # #             schema_file=schema,
# # #             output_dir=out_dir,
# # #             enable_gpu=gpu,
# # #             logger=logger
# # #         )
        
# # #         # Show schema info
# # #         if verbose:
# # #             info = generator.get_enhanced_schema_info()
# # #             logger.debug(f"Schema info: {json.dumps(info, indent=2)}")
        
# # #     except Exception as e:
# # #         raise click.ClickException(f"Failed to initialize generator: {e}")
    
# # #     # Initialize rolling file writer
# # #     writer = RollingFileWriter(
# # #         base_path=os.path.join(out_dir, prefix),
# # #         max_size_bytes=max_file_size,
# # #         format=format,
# # #         compress=compress,
# # #         logger=logger
# # #     )
    
# # #     # Initialize rate limiter
# # #     rate_limiter = RateLimiter(rate, batch_size=batch_size)
    
# # #     # Calculate total records
# # #     total_records = rate * duration
# # #     logger.info(f"Generating {total_records:,} records at {rate:,} records/sec for {duration} seconds")
    
# # #     # Progress tracking
# # #     start_time = time.time()
# # #     records_generated = 0
# # #     last_report_time = start_time
# # #     report_interval = 5  # seconds
    
# # #     # Generation loop
# # #     try:
# # #         with click.progressbar(
# # #             length=total_records,
# # #             label='Generating telemetry',
# # #             show_pos=True,
# # #             show_percent=True,
# # #             show_eta=True,
# # #             width=0,
# # #             disabled=quiet
# # #         ) as progress_bar:
            
# # #             while records_generated < total_records:
# # #                 # Generate batch
# # #                 batch_records = min(batch_size, total_records - records_generated)
                
# # #                 # Use GPU batch generation if enabled and available
# # #                 if gpu and hasattr(generator, 'generate_batch_gpu_accelerated'):
# # #                     records = generator.generate_batch_gpu_accelerated(
# # #                         batch_records,
# # #                         record_type=RecordType.UPDATE
# # #                     )
# # #                 else:
# # #                     # Regular batch generation
# # #                     records = []
# # #                     for _ in range(batch_records):
# # #                         # Determine record type based on ratio
# # #                         rand_val = random.random()
# # #                         cumulative = 0
# # #                         record_type = RecordType.UPDATE
# # #                         for rtype, ratio in ratio_dict.items():
# # #                             cumulative += ratio
# # #                             if rand_val <= cumulative:
# # #                                 record_type = rtype
# # #                                 break
                        
# # #                         record = generator.generate_enhanced_record(
# # #                             record_type=record_type
# # #                         )
# # #                         records.append(record)
                
# # #                 # Write batch to file
# # #                 for record in records:
# # #                     writer.write_record(record, generator)
                
# # #                 records_generated += batch_records
# # #                 progress_bar.update(batch_records)
                
# # #                 # Rate limiting
# # #                 rate_limiter.wait_if_needed()
                
# # #                 # Periodic status report
# # #                 current_time = time.time()
# # #                 if verbose and (current_time - last_report_time) >= report_interval:
# # #                     elapsed = current_time - start_time
# # #                     actual_rate = records_generated / elapsed if elapsed > 0 else 0
# # #                     logger.debug(
# # #                         f"Progress: {records_generated:,}/{total_records:,} records, "
# # #                         f"Rate: {actual_rate:.1f} rec/sec, "
# # #                         f"Files: {writer.file_count}, "
# # #                         f"Total size: {writer.total_bytes_written:,} bytes"
# # #                     )
# # #                     last_report_time = current_time
        
# # #         # Final cleanup
# # #         writer.close()
        
# # #     except KeyboardInterrupt:
# # #         logger.warning("Generation interrupted by user")
# # #         writer.close()
# # #         sys.exit(1)
# # #     except Exception as e:
# # #         logger.error(f"Error during generation: {e}")
# # #         writer.close()
# # #         raise click.ClickException(str(e))
    
# # #     # Summary
# # #     elapsed_time = time.time() - start_time
# # #     actual_rate = records_generated / elapsed_time if elapsed_time > 0 else 0
    
# # #     click.echo("\n" + "="*60)
# # #     click.echo("GENERATION COMPLETE")
# # #     click.echo("="*60)
# # #     click.echo(f"Records generated:  {records_generated:,}")
# # #     click.echo(f"Time elapsed:       {elapsed_time:.2f} seconds")
# # #     click.echo(f"Actual rate:        {actual_rate:.1f} records/sec")
# # #     click.echo(f"Files created:      {writer.file_count}")
# # #     click.echo(f"Total size:         {writer.total_bytes_written:,} bytes")
# # #     click.echo(f"Output directory:   {out_dir}")
# # #     click.echo("="*60)
# # @cli.command()
# # @click.option('--schema', '-s', required=True, type=click.Path(exists=True),
# #               help='Path to JSON schema file')
# # @click.option('--rate', '-r', default=1000, type=int,
# #               help='Records per second (default: 1000)')
# # @click.option('--duration', '-d', default=60, type=int,
# #               help='Duration in seconds (default: 60)')
# # @click.option('--out-dir', '-o', default='data/', type=click.Path(),
# #               help='Output directory (default: data/)')
# # @click.option('--rotate-size', default='512MB',
# #               help='Maximum file size before rotation (default: 512MB)')
# # @click.option('--format', '-f', 
# #               type=click.Choice(['ndjson', 'json', 'binary', 'influx', 'leb128']),
# #               default='ndjson',
# #               help='Output format (default: ndjson)')
# # @click.option('--seed', type=int, help='Random seed for reproducible data')
# # @click.option('--load-profile', '-l',
# #               type=click.Choice(['low', 'medium', 'high', 'custom']),
# #               help='Predefined load profile')
# # @click.option('--compress', is_flag=True, help='Enable compression (gzip)')
# # @click.option('--batch-size', '-b', default=100, type=int,
# #               help='Batch size for writing (default: 100)')
# # @click.option('--prefix', '-p', default='telemetry',
# #               help='Filename prefix (default: telemetry)')
# # @click.option('--workers', '-w', default=4, type=int,
# #               help='Number of worker threads (default: 4)')
# # @click.option('--quiet', '-q', is_flag=True, help='Suppress progress output')
# # @click.option('--verbose', '-v', is_flag=True, help='Verbose output')
# # @click.option('--gpu', is_flag=True, help='Enable GPU acceleration if available')
# # @click.option('--record-type-ratio', default='update:0.7,event:0.3',
# #               help='Record type ratio (default: update:0.7,event:0.3)')
# # def generate(schema, rate, duration, out_dir, rotate_size, format, seed, 
# #             load_profile, compress, batch_size, prefix, workers, quiet, 
# #             verbose, gpu, record_type_ratio):

# #     # Set logging level
# #     if verbose:
# #         logger.setLevel(logging.DEBUG)
# #     elif quiet:
# #         logger.setLevel(logging.WARNING)
    
# #     # Apply load profile if specified
# #     if load_profile and load_profile != 'custom':
# #         profile = LOAD_PROFILES[load_profile]
# #         rate = profile.rate
# #         batch_size = profile.batch_size
# #         if profile.duration:
# #             duration = profile.duration
# #         if profile.workers:
# #             workers = profile.workers
# #         if profile.use_gpu:
# #             gpu = True
# #         logger.info(f"Applied load profile '{load_profile}': rate={rate}, batch={batch_size}")
    
# #     # Parse record type ratio
# #     ratio_dict = {}
# #     for part in record_type_ratio.split(','):
# #         type_name, ratio = part.split(':')
# #         ratio_dict[RecordType[type_name.upper()]] = float(ratio)
    
# #     # Set random seed if provided
# #     if seed is not None:
# #         random.seed(seed)
# #         logger.info(f"Set random seed: {seed}")
    
# #     # Validate schema
# #     try:
# #         schema_data = validate_schema(schema)
# #         logger.info(f"Loaded schema with {len(schema_data)} fields")
# #     except Exception as e:
# #         raise click.ClickException(str(e))
    
# #     # Create output directory
# #     Path(out_dir).mkdir(parents=True, exist_ok=True)
    
# #     # Parse rotation size
# #     try:
# #         max_file_size = parse_size(rotate_size)
# #         logger.info(f"Max file size: {max_file_size:,} bytes")
# #     except ValueError as e:
# #         raise click.ClickException(str(e))
    
# #     # Map format strings to OutputFormat enum
# #     format_map = {
# #         'ndjson': 'ndjson',
# #         'json': OutputFormat.JSON,
# #         'binary': OutputFormat.BINARY,
# #         'influx': OutputFormat.INFLUX_LINE,
# #         'leb128': 'leb128'
# #     }
# #     output_format = format_map[format]
    
# #     # Initialize generator
# #     try:
# #         generator = EnhancedTelemetryGeneratorPro(
# #             schema_file=schema,
# #             output_dir=out_dir,
# #             enable_gpu=gpu,
# #             logger=logger
# #         )
        
# #         # Show schema info
# #         if verbose:
# #             info = generator.get_enhanced_schema_info()
# #             logger.debug(f"Schema info: {json.dumps(info, indent=2)}")
        
# #     except Exception as e:
# #         raise click.ClickException(f"Failed to initialize generator: {e}")
    
# #     # Initialize rolling file writer
# #     writer = RollingFileWriter(
# #         base_path=os.path.join(out_dir, prefix),
# #         max_size_bytes=max_file_size,
# #         format=format,
# #         compress=compress,
# #         logger=logger
# #     )
    
# #     # Initialize rate limiter
# #     rate_limiter = RateLimiter(rate, batch_size=batch_size)
    
# #     # Calculate total records
# #     total_records = rate * duration
# #     logger.info(f"Generating {total_records:,} records at {rate:,} records/sec for {duration} seconds")
    
# #     # Progress tracking
# #     start_time = time.time()
# #     records_generated = 0
# #     last_report_time = start_time
# #     report_interval = 5  # seconds
    
# #     # Choose progress bar
# #     if quiet:
# #         class DummyBar:
# #             def __enter__(self): return self
# #             def __exit__(self, exc_type, exc_val, exc_tb): return False
# #             def update(self, n=1): pass
# #         progress_bar = DummyBar()
# #     else:
# #         progress_bar = click.progressbar(
# #             length=total_records,
# #             label='Generating telemetry',
# #             show_pos=True,
# #             show_percent=True,
# #             show_eta=True,
# #             width=0
# #         )
    
# #     # Generation loop
# #     try:
# #         with progress_bar:
            
# #             while records_generated < total_records:
# #                 batch_records = min(batch_size, total_records - records_generated)
                
# #                 if gpu and hasattr(generator, 'generate_batch_gpu_accelerated'):
# #                     records = generator.generate_batch_gpu_accelerated(
# #                         batch_records,
# #                         record_type=RecordType.UPDATE
# #                     )
# #                 else:
# #                     records = []
# #                     for _ in range(batch_records):
# #                         rand_val = random.random()
# #                         cumulative = 0
# #                         record_type = RecordType.UPDATE
# #                         for rtype, ratio in ratio_dict.items():
# #                             cumulative += ratio
# #                             if rand_val <= cumulative:
# #                                 record_type = rtype
# #                                 break
# #                         record = generator.generate_enhanced_record(
# #                             record_type=record_type
# #                         )
# #                         records.append(record)
                
# #                 for record in records:
# #                     writer.write_record(record, generator)
                
# #                 records_generated += batch_records
# #                 progress_bar.update(batch_records)
                
# #                 rate_limiter.wait_if_needed()
                
# #                 current_time = time.time()
# #                 if verbose and (current_time - last_report_time) >= report_interval:
# #                     elapsed = current_time - start_time
# #                     actual_rate = records_generated / elapsed if elapsed > 0 else 0
# #                     logger.debug(
# #                         f"Progress: {records_generated:,}/{total_records:,} records, "
# #                         f"Rate: {actual_rate:.1f} rec/sec, "
# #                         f"Files: {writer.file_count}, "
# #                         f"Total size: {writer.total_bytes_written:,} bytes"
# #                     )
# #                     last_report_time = current_time
        
# #         writer.close()
        
# #     except KeyboardInterrupt:
# #         logger.warning("Generation interrupted by user")
# #         writer.close()
# #         sys.exit(1)
# #     except Exception as e:
# #         logger.error(f"Error during generation: {e}")
# #         writer.close()
# #         raise click.ClickException(str(e))
    
# #     # Summary
# #     elapsed_time = time.time() - start_time
# #     actual_rate = records_generated / elapsed_time if elapsed_time > 0 else 0
    
# #     click.echo("\n" + "="*60)
# #     click.echo("GENERATION COMPLETE")
# #     click.echo("="*60)
# #     click.echo(f"Records generated:  {records_generated:,}")
# #     click.echo(f"Time elapsed:       {elapsed_time:.2f} seconds")
# #     click.echo(f"Actual rate:        {actual_rate:.1f} records/sec")
# #     click.echo(f"Files created:      {writer.file_count}")
# #     click.echo(f"Total size:         {writer.total_bytes_written:,} bytes")
# #     click.echo(f"Output directory:   {out_dir}")
# #     click.echo("="*60)
# # @cli.command()
# # @click.option('--schema', '-s', required=True, type=click.Path(exists=True),
# #               help='Path to JSON schema file')
# # @click.option('--records', '-n', default=1000, type=int,
# #               help='Number of records to validate')
# # def validate(schema, records):
# #     """Validate schema and generate test records"""
    
# #     click.echo("Validating schema...")
    
# #     try:
# #         # Load and validate schema
# #         schema_data = validate_schema(schema)
# #         click.echo(f"✓ Schema loaded successfully with {len(schema_data)} fields")
        
# #         # Initialize generator
# #         generator = EnhancedTelemetryGeneratorPro(
# #             schema_file=schema,
# #             logger=logger
# #         )
        
# #         # Get schema info
# #         info = generator.get_enhanced_schema_info()
        
# #         click.echo("\nSchema Information:")
# #         click.echo(f"  Total fields: {info['fields_count']}")
# #         click.echo(f"  Bits per record: {info['total_bits_per_record']}")
# #         click.echo(f"  Bytes per record: {info['bytes_per_record']}")
        
# #         click.echo("\nField Details:")
# #         for field in info['fields']:
# #             click.echo(f"  - {field['name']}: {field['type']} ({field['bits']} bits)")
        
# #         # Generate test records
# #         click.echo(f"\nGenerating {records} test records...")
        
# #         with click.progressbar(length=records) as bar:
# #             for i in range(records):
# #                 record = generator.generate_enhanced_record()
# #                 bar.update(1)
        
# #         click.echo("✓ Test generation successful")
        
# #     except Exception as e:
# #         raise click.ClickException(f"Validation failed: {e}")

# # @cli.command()
# # def profiles():
# #     """List available load profiles"""
    
# #     click.echo("Available Load Profiles:")
# #     click.echo("="*50)
    
# #     for name, profile in LOAD_PROFILES.items():
# #         click.echo(f"\n{name.upper()}:")
# #         click.echo(f"  Rate: {profile.rate:,} records/sec")
# #         click.echo(f"  Batch size: {profile.batch_size}")
# #         click.echo(f"  Duration: {profile.duration or 'default'} seconds")
# #         click.echo(f"  Workers: {profile.workers or 'default'}")
# #         click.echo(f"  GPU: {'Yes' if profile.use_gpu else 'No'}")
# #         click.echo(f"  Description: {profile.description}")

# # if __name__ == '__main__':
# #     cli()
# #!/usr/bin/env python3
# """
# Telemetry Generator CLI
# Command-line interface for generating telemetry data with configurable parameters
# """

# import click
# import json
# import os
# import sys
# import time
# import random
# import logging
# from pathlib import Path
# from typing import Optional, Dict, Any

# from .generator import EnhancedTelemetryGeneratorPro, OutputFormat, RecordType
# from .rolling_writer import RollingFileWriter
# from .rate_control import RateLimiter
# from .load_profiles import LOAD_PROFILES, LoadProfile

# # Configure logging
# logging.basicConfig(
#     level=logging.INFO,
#     format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
# )
# logger = logging.getLogger('telegen')

# def parse_size(size_str: str) -> int:
#     """Parse size string like '512MB' to bytes"""
#     import re
    
#     if isinstance(size_str, int):
#         return size_str
    
#     size_str = str(size_str).upper().strip()
    
#     # Handle numeric-only values (assume bytes)
#     if size_str.isdigit():
#         return int(size_str)
    
#     # Parse with suffix using regex
#     match = re.match(r'(\d+(?:\.\d+)?)\s*([KMGT]?B?)', size_str)
#     if not match:
#         raise ValueError(f"Invalid size format: {size_str}")
    
#     value, unit = match.groups()
#     value = float(value)
    
#     # Define multipliers - order matters! Longer units first
#     multipliers = {
#         'TB': 1024**4,
#         'GB': 1024**3,
#         'MB': 1024**2,
#         'KB': 1024,
#         'T': 1024**4,
#         'G': 1024**3,
#         'M': 1024**2,
#         'K': 1024,
#         'B': 1,
#         '': 1,  # No unit
#     }
    
#     return int(value * multipliers.get(unit, 1))

# def validate_schema(schema_path: str) -> Dict[str, Any]:
#     """Validate and load schema file"""
#     if not os.path.exists(schema_path):
#         raise click.ClickException(f"Schema file not found: {schema_path}")
    
#     try:
#         with open(schema_path, 'r') as f:
#             schema = json.load(f)
        
#         if not isinstance(schema, dict):
#             raise ValueError("Schema must be a JSON object")
        
#         if not schema:
#             raise ValueError("Schema cannot be empty")
            
#         return schema
#     except json.JSONDecodeError as e:
#         raise click.ClickException(f"Invalid JSON in schema file: {e}")
#     except Exception as e:
#         raise click.ClickException(f"Error reading schema: {e}")

# @click.group()
# @click.version_option(version='1.0.0')
# def cli():
#     """Telemetry Generator - High-performance telemetry data generator"""
#     pass

# @cli.command()
# @click.option('--schema', '-s', required=True, type=click.Path(exists=True),
#               help='Path to JSON schema file')
# @click.option('--rate', '-r', default=1000, type=int,
#               help='Records per second (default: 1000)')
# @click.option('--duration', '-d', default=60, type=int,
#               help='Duration in seconds (default: 60)')
# @click.option('--out-dir', '-o', default='data/', type=click.Path(),
#               help='Output directory (default: data/)')
# @click.option('--rotate-size', default='512MB',
#               help='Maximum file size before rotation (default: 512MB)')
# @click.option('--format', '-f', 
#               type=click.Choice(['ndjson', 'json', 'binary', 'influx', 'leb128']),
#               default='ndjson',
#               help='Output format (default: ndjson)')
# @click.option('--seed', type=int, help='Random seed for reproducible data')
# @click.option('--load-profile', '-l',
#               type=click.Choice(['low', 'medium', 'high', 'custom']),
#               help='Predefined load profile')
# @click.option('--compress', is_flag=True, help='Enable compression (gzip)')
# @click.option('--batch-size', '-b', default=100, type=int,
#               help='Batch size for writing (default: 100)')
# @click.option('--prefix', '-p', default='telemetry',
#               help='Filename prefix (default: telemetry)')
# @click.option('--workers', '-w', default=4, type=int,
#               help='Number of worker threads (default: 4)')
# @click.option('--quiet', '-q', is_flag=True, help='Suppress progress output')
# @click.option('--verbose', '-v', is_flag=True, help='Verbose output')
# @click.option('--gpu', is_flag=True, help='Enable GPU acceleration if available')
# @click.option('--record-type-ratio', default='update:0.7,event:0.3',
#               help='Record type ratio (default: update:0.7,event:0.3)')
# def generate(schema, rate, duration, out_dir, rotate_size, format, seed, 
#             load_profile, compress, batch_size, prefix, workers, quiet, 
#             verbose, gpu, record_type_ratio):
#     """Generate telemetry data with specified parameters"""

#     # Set logging level
#     if verbose:
#         logger.setLevel(logging.DEBUG)
#     elif quiet:
#         logger.setLevel(logging.WARNING)
    
#     # Apply load profile if specified
#     if load_profile and load_profile != 'custom':
#         profile = LOAD_PROFILES[load_profile]
#         rate = profile.rate
#         batch_size = profile.batch_size
#         if profile.duration:
#             duration = profile.duration
#         if profile.workers:
#             workers = profile.workers
#         if profile.use_gpu:
#             gpu = True
#         logger.info(f"Applied load profile '{load_profile}': rate={rate}, batch={batch_size}")
    
#     # Parse record type ratio
#     ratio_dict = {}
#     for part in record_type_ratio.split(','):
#         type_name, ratio = part.split(':')
#         ratio_dict[RecordType[type_name.upper()]] = float(ratio)
    
#     # Set random seed if provided
#     if seed is not None:
#         random.seed(seed)
#         logger.info(f"Set random seed: {seed}")
    
#     # Validate schema
#     try:
#         schema_data = validate_schema(schema)
#         logger.info(f"Loaded schema with {len(schema_data)} fields")
#     except Exception as e:
#         raise click.ClickException(str(e))
    
#     # Create output directory
#     Path(out_dir).mkdir(parents=True, exist_ok=True)
    
#     # Parse rotation size
#     try:
#         max_file_size = parse_size(rotate_size)
#         logger.info(f"Max file size: {max_file_size:,} bytes")
#     except ValueError as e:
#         raise click.ClickException(str(e))
    
#     # Map format strings to OutputFormat enum
#     format_map = {
#         'ndjson': 'ndjson',
#         'json': OutputFormat.JSON,
#         'binary': OutputFormat.BINARY,
#         'influx': OutputFormat.INFLUX_LINE,
#         'leb128': 'leb128'
#     }
#     output_format = format_map[format]
    
#     # Initialize generator
#     try:
#         generator = EnhancedTelemetryGeneratorPro(
#             schema_file=schema,
#             output_dir=out_dir,
#             enable_gpu=gpu,
#             logger=logger
#         )
        
#         # Show schema info
#         if verbose:
#             info = generator.get_enhanced_schema_info()
#             logger.debug(f"Schema info: {json.dumps(info, indent=2)}")
        
#     except Exception as e:
#         raise click.ClickException(f"Failed to initialize generator: {e}")
    
#     # Initialize resources
#     writer = None
#     rate_limiter = None
    
#     # Calculate total records
#     total_records = rate * duration
#     logger.info(f"Generating {total_records:,} records at {rate:,} records/sec for {duration} seconds")
    
#     # Progress tracking
#     start_time = time.time()
#     records_generated = 0
#     last_report_time = start_time
#     report_interval = 5  # seconds
    
#     # Choose progress bar
#     if quiet:
#         class DummyBar:
#             def __enter__(self): return self
#             def __exit__(self, exc_type, exc_val, exc_tb): return False
#             def update(self, n=1): pass
#         progress_bar = DummyBar()
#     else:
#         progress_bar = click.progressbar(
#             length=total_records,
#             label='Generating telemetry',
#             show_pos=True,
#             show_percent=True,
#             show_eta=True,
#             width=0
#         )
    
#     # Generation loop with proper cleanup
#     try:
#         # Initialize rolling file writer
#         writer = RollingFileWriter(
#             base_path=os.path.join(out_dir, prefix),
#             max_size_bytes=max_file_size,
#             format=format,
#             compress=compress,
#             logger=logger
#         )
        
#         # Initialize rate limiter
#         rate_limiter = RateLimiter(rate, batch_size=batch_size)
#         rate_limiter.start() 

#         with progress_bar:
#             while records_generated < total_records:
#                 batch_records = min(batch_size, total_records - records_generated)
                
#                 if gpu and hasattr(generator, 'generate_batch_gpu_accelerated'):
#                     records = generator.generate_batch_gpu_accelerated(
#                         batch_records,
#                         record_type=RecordType.UPDATE
#                     )
#                 else:
#                     records = []
#                     for _ in range(batch_records):
#                         rand_val = random.random()
#                         cumulative = 0
#                         record_type = RecordType.UPDATE
#                         for rtype, ratio in ratio_dict.items():
#                             cumulative += ratio
#                             if rand_val <= cumulative:
#                                 record_type = rtype
#                                 break
#                         record = generator.generate_enhanced_record(
#                             record_type=record_type
#                         )
#                         records.append(record)
                
#                 for record in records:
#                     writer.write_record(record, generator)
                
#                 records_generated += batch_records
#                 progress_bar.update(batch_records)
                
#                 rate_limiter.wait_if_needed()
                
#                 current_time = time.time()
#                 if verbose and (current_time - last_report_time) >= report_interval:
#                     elapsed = current_time - start_time
#                     actual_rate = records_generated / elapsed if elapsed > 0 else 0
#                     logger.debug(
#                         f"Progress: {records_generated:,}/{total_records:,} records, "
#                         f"Rate: {actual_rate:.1f} rec/sec, "
#                         f"Files: {writer.file_count}, "
#                         f"Total size: {writer.total_bytes_written:,} bytes"
#                     )
#                     last_report_time = current_time
        
#     except KeyboardInterrupt:
#         logger.warning("Generation interrupted by user")
#         sys.exit(1)
#     except Exception as e:
#         logger.error(f"Error during generation: {e}")
#         raise click.ClickException(str(e))
    
#     finally:
#         # CRITICAL: Always clean up resources
#         if writer:
#             try:
#                 writer.close()
#                 logger.info("Writer closed successfully")
#             except Exception as e:
#                 logger.error(f"Error closing writer: {e}")
        
#         if rate_limiter and hasattr(rate_limiter, 'stop'):
#             try:
#                 rate_limiter.stop()
#                 logger.info("Rate limiter stopped successfully")
#             except Exception as e:
#                 logger.error(f"Error stopping rate limiter: {e}")
    
#     # Summary
#     elapsed_time = time.time() - start_time
#     actual_rate = records_generated / elapsed_time if elapsed_time > 0 else 0
    
#     click.echo("\n" + "="*60)
#     click.echo("GENERATION COMPLETE")
#     click.echo("="*60)
#     click.echo(f"Records generated:  {records_generated:,}")
#     click.echo(f"Time elapsed:       {elapsed_time:.2f} seconds")
#     click.echo(f"Actual rate:        {actual_rate:.1f} records/sec")
#     if writer:
#         click.echo(f"Files created:      {writer.file_count}")
#         click.echo(f"Total size:         {writer.total_bytes_written:,} bytes")
#     click.echo(f"Output directory:   {out_dir}")
#     click.echo("="*60)

# @cli.command()
# @click.option('--schema', '-s', required=True, type=click.Path(exists=True),
#               help='Path to JSON schema file')
# @click.option('--records', '-n', default=1000, type=int,
#               help='Number of records to validate')
# def validate(schema, records):
#     """Validate schema and generate test records"""
    
#     click.echo("Validating schema...")
    
#     try:
#         # Load and validate schema
#         schema_data = validate_schema(schema)
#         click.echo(f"✓ Schema loaded successfully with {len(schema_data)} fields")
        
#         # Initialize generator
#         generator = EnhancedTelemetryGeneratorPro(
#             schema_file=schema,
#             logger=logger
#         )
        
#         # Get schema info
#         info = generator.get_enhanced_schema_info()
        
#         click.echo("\nSchema Information:")
#         click.echo(f"  Total fields: {info['fields_count']}")
#         click.echo(f"  Bits per record: {info['total_bits_per_record']}")
#         click.echo(f"  Bytes per record: {info['bytes_per_record']}")
        
#         click.echo("\nField Details:")
#         for field in info['fields']:
#             click.echo(f"  - {field['name']}: {field['type']} ({field['bits']} bits)")
        
#         # Generate test records
#         click.echo(f"\nGenerating {records} test records...")
        
#         with click.progressbar(length=records) as bar:
#             for i in range(records):
#                 record = generator.generate_enhanced_record()
#                 bar.update(1)
        
#         click.echo("✓ Test generation successful")
        
#     except Exception as e:
#         raise click.ClickException(f"Validation failed: {e}")

# @cli.command()
# def profiles():
#     """List available load profiles"""
    
#     click.echo("Available Load Profiles:")
#     click.echo("="*50)
    
#     for name, profile in LOAD_PROFILES.items():
#         click.echo(f"\n{name.upper()}:")
#         click.echo(f"  Rate: {profile.rate:,} records/sec")
#         click.echo(f"  Batch size: {profile.batch_size}")
#         click.echo(f"  Duration: {profile.duration or 'default'} seconds")
#         click.echo(f"  Workers: {profile.workers or 'default'}")
#         click.echo(f"  GPU: {'Yes' if profile.use_gpu else 'No'}")
#         click.echo(f"  Description: {profile.description}")

# if __name__ == '__main__':
#     cli()
import click
import json
import os
import sys
import time
import random
import logging
from pathlib import Path
from typing import Optional, Dict, Any

from .generator import EnhancedTelemetryGeneratorPro, OutputFormat, RecordType
from .rolling_writer import RollingFileWriter
from .rate_control import RateLimiter
from .load_profiles import LOAD_PROFILES, LoadProfile

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('telegen')

def parse_size(size_str: str) -> int:
    """Parse size string like '512MB' to bytes"""
    import re
    
    if isinstance(size_str, int):
        return size_str
    
    size_str = str(size_str).upper().strip()
    
    # Handle numeric-only values (assume bytes)
    if size_str.isdigit():
        return int(size_str)
    
    # Parse with suffix using regex
    match = re.match(r'(\d+(?:\.\d+)?)\s*([KMGT]?B?)', size_str)
    if not match:
        raise ValueError(f"Invalid size format: {size_str}")
    
    value, unit = match.groups()
    value = float(value)
    
    # Define multipliers - order matters! Longer units first
    multipliers = {
        'TB': 1024**4,
        'GB': 1024**3,
        'MB': 1024**2,
        'KB': 1024,
        'T': 1024**4,
        'G': 1024**3,
        'M': 1024**2,
        'K': 1024,
        'B': 1,
        '': 1,  # No unit
    }
    
    return int(value * multipliers.get(unit, 1))

def validate_schema(schema_path: str) -> Dict[str, Any]:
    """Validate and load schema file"""
    if not os.path.exists(schema_path):
        raise click.ClickException(f"Schema file not found: {schema_path}")
    
    try:
        with open(schema_path, 'r') as f:
            schema = json.load(f)
        
        if not isinstance(schema, dict):
            raise ValueError("Schema must be a JSON object")
        
        if not schema:
            raise ValueError("Schema cannot be empty")
            
        return schema
    except json.JSONDecodeError as e:
        raise click.ClickException(f"Invalid JSON in schema file: {e}")
    except Exception as e:
        raise click.ClickException(f"Error reading schema: {e}")

@click.group()
@click.version_option(version='1.0.0')
def cli():
    """Telemetry Generator - High-performance telemetry data generator"""
    pass

@cli.command()
@click.option('--schema', '-s', required=True, type=click.Path(exists=True),
              help='Path to JSON schema file')
@click.option('--rate', '-r', default=1000, type=int,
              help='Records per second (default: 1000)')
@click.option('--duration', '-d', default=60, type=int,
              help='Duration in seconds (default: 60)')
@click.option('--out-dir', '-o', default='data/', type=click.Path(),
              help='Output directory (default: data/)')
@click.option('--rotate-size', default='512MB',
              help='Maximum file size before rotation (default: 512MB)')
@click.option('--format', '-f', 
              type=click.Choice(['ndjson', 'json', 'binary', 'influx', 'leb128']),
              default='ndjson',
              help='Output format (default: ndjson)')
@click.option('--seed', type=int, help='Random seed for reproducible data')
@click.option('--load-profile', '-l',
              type=click.Choice(['low', 'medium', 'high', 'custom']),
              help='Predefined load profile')
@click.option('--compress', is_flag=True, help='Enable compression (gzip)')
@click.option('--batch-size', '-b', default=100, type=int,
              help='Batch size for writing (default: 100)')
@click.option('--prefix', '-p', default='telemetry',
              help='Filename prefix (default: telemetry)')
@click.option('--workers', '-w', default=4, type=int,
              help='Number of worker threads (default: 4)')
@click.option('--quiet', '-q', is_flag=True, help='Suppress progress output')
@click.option('--verbose', '-v', is_flag=True, help='Verbose output')
@click.option('--gpu', is_flag=True, help='Enable GPU acceleration if available')
@click.option('--record-type-ratio', default='update:0.7,event:0.3',
              help='Record type ratio (default: update:0.7,event:0.3)')
def generate(schema, rate, duration, out_dir, rotate_size, format, seed, 
            load_profile, compress, batch_size, prefix, workers, quiet, 
            verbose, gpu, record_type_ratio):
    """Generate telemetry data with specified parameters"""

    # Set logging level
    if verbose:
        logger.setLevel(logging.DEBUG)
    elif quiet:
        logger.setLevel(logging.WARNING)
    
    # Apply load profile if specified
    if load_profile and load_profile != 'custom':
        profile = LOAD_PROFILES[load_profile]
        rate = profile.rate
        batch_size = profile.batch_size
        if profile.duration:
            duration = profile.duration
        if profile.workers:
            workers = profile.workers
        if profile.use_gpu:
            gpu = True
        logger.info(f"Applied load profile '{load_profile}': rate={rate}, batch={batch_size}")
    
    # Parse record type ratio
    ratio_dict = {}
    for part in record_type_ratio.split(','):
        type_name, ratio = part.split(':')
        ratio_dict[RecordType[type_name.upper()]] = float(ratio)
    
    # Set random seed if provided
    if seed is not None:
        random.seed(seed)
        logger.info(f"Set random seed: {seed}")
    
    # Validate schema
    try:
        schema_data = validate_schema(schema)
        logger.info(f"Loaded schema with {len(schema_data)} fields")
    except Exception as e:
        raise click.ClickException(str(e))
    
    # Create output directory
    Path(out_dir).mkdir(parents=True, exist_ok=True)
    
    # Parse rotation size
    try:
        max_file_size = parse_size(rotate_size)
        logger.info(f"Max file size: {max_file_size:,} bytes")
    except ValueError as e:
        raise click.ClickException(str(e))
    
    # Map format strings to OutputFormat enum
    format_map = {
        'ndjson': 'ndjson',
        'json': OutputFormat.JSON,
        'binary': OutputFormat.BINARY,
        'influx': OutputFormat.INFLUX_LINE,
        'leb128': 'leb128'
    }
    output_format = format_map[format]
    
    # Initialize generator
    try:
        generator = EnhancedTelemetryGeneratorPro(
            schema_file=schema,
            output_dir=out_dir,
            enable_gpu=gpu,
            logger=logger
        )
        
        # Show schema info
        if verbose:
            info = generator.get_enhanced_schema_info()
            logger.debug(f"Schema info: {json.dumps(info, indent=2)}")
        
    except Exception as e:
        raise click.ClickException(f"Failed to initialize generator: {e}")
    
    # Initialize resources
    writer = None
    rate_limiter = None
    
    # Calculate total records
    total_records = rate * duration
    logger.info(f"Generating {total_records:,} records at {rate:,} records/sec for {duration} seconds")
    
    # Progress tracking
    start_time = time.time()
    records_generated = 0
    last_report_time = start_time
    report_interval = 5  # seconds
    
    # Choose progress bar
    if quiet:
        class DummyBar:
            def __enter__(self): return self
            def __exit__(self, exc_type, exc_val, exc_tb): return False
            def update(self, n=1): pass
        progress_bar = DummyBar()
    else:
        progress_bar = click.progressbar(
            length=total_records,
            label='Generating telemetry',
            show_pos=True,
            show_percent=True,
            show_eta=True,
            width=0
        )
    
    # Generation loop with proper cleanup
    try:
        # Initialize rolling file writer
        writer = RollingFileWriter(
            base_path=os.path.join(out_dir, prefix),
            max_size_bytes=max_file_size,
            format=format,
            compress=compress,
            logger=logger
        )
        
        # Initialize rate limiter
        rate_limiter = RateLimiter(rate, batch_size=batch_size)
        
        # **FIX: Start the rate limiter!**
        rate_limiter.start()
        
        with progress_bar:
            while records_generated < total_records:
                batch_records = min(batch_size, total_records - records_generated)
                
                if gpu and hasattr(generator, 'generate_batch_gpu_accelerated'):
                    records = generator.generate_batch_gpu_accelerated(
                        batch_records,
                        record_type=RecordType.UPDATE
                    )
                else:
                    records = []
                    for _ in range(batch_records):
                        rand_val = random.random()
                        cumulative = 0
                        record_type = RecordType.UPDATE
                        for rtype, ratio in ratio_dict.items():
                            cumulative += ratio
                            if rand_val <= cumulative:
                                record_type = rtype
                                break
                        record = generator.generate_enhanced_record(
                            record_type=record_type
                        )
                        records.append(record)
                
                for record in records:
                    writer.write_record(record, generator)
                
                records_generated += batch_records
                progress_bar.update(batch_records)
                
                rate_limiter.wait_if_needed()
                
                current_time = time.time()
                if verbose and (current_time - last_report_time) >= report_interval:
                    elapsed = current_time - start_time
                    actual_rate = records_generated / elapsed if elapsed > 0 else 0
                    logger.debug(
                        f"Progress: {records_generated:,}/{total_records:,} records, "
                        f"Rate: {actual_rate:.1f} rec/sec, "
                        f"Files: {writer.file_count}, "
                        f"Total size: {writer.total_bytes_written:,} bytes"
                    )
                    last_report_time = current_time
        
        # **FIX: Flush writer before cleanup**
        if writer:
            writer.flush()
            
    except KeyboardInterrupt:
        logger.warning("Generation interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Error during generation: {e}")
        raise click.ClickException(str(e))
    finally:
        # CRITICAL: Always clean up resources
        if writer:
            try:
                writer.close()
                logger.info("Writer closed successfully")
            except Exception as e:
                logger.error(f"Error closing writer: {e}")
        
        if rate_limiter and hasattr(rate_limiter, 'stop'):
            try:
                rate_limiter.stop()
                logger.info("Rate limiter stopped successfully")
            except Exception as e:
                logger.error(f"Error stopping rate limiter: {e}")
    
    # Summary
    elapsed_time = time.time() - start_time
    actual_rate = records_generated / elapsed_time if elapsed_time > 0 else 0
    
    click.echo("\n" + "="*60)
    click.echo("GENERATION COMPLETE")
    click.echo("="*60)
    click.echo(f"Records generated:  {records_generated:,}")
    click.echo(f"Time elapsed:       {elapsed_time:.2f} seconds")
    click.echo(f"Actual rate:        {actual_rate:.1f} records/sec")
    if writer:
        click.echo(f"Files created:      {writer.file_count}")
        click.echo(f"Total size:         {writer.total_bytes_written:,} bytes")
    click.echo(f"Output directory:   {out_dir}")
    click.echo("="*60)

@cli.command()
@click.option('--schema', '-s', required=True, type=click.Path(exists=True),
              help='Path to JSON schema file')
@click.option('--records', '-n', default=1000, type=int,
              help='Number of records to validate')
def validate(schema, records):
    """Validate schema and generate test records"""
    
    click.echo("Validating schema...")
    
    try:
        # Load and validate schema
        schema_data = validate_schema(schema)
        click.echo(f"✓ Schema loaded successfully with {len(schema_data)} fields")
        
        # Initialize generator
        generator = EnhancedTelemetryGeneratorPro(
            schema_file=schema,
            logger=logger
        )
        
        # Get schema info
        info = generator.get_enhanced_schema_info()
        
        click.echo("\nSchema Information:")
        click.echo(f"  Total fields: {info['fields_count']}")
        click.echo(f"  Bits per record: {info['total_bits_per_record']}")
        click.echo(f"  Bytes per record: {info['bytes_per_record']}")
        
        click.echo("\nField Details:")
        for field in info['fields']:
            click.echo(f"  - {field['name']}: {field['type']} ({field['bits']} bits)")
        
        # Generate test records
        click.echo(f"\nGenerating {records} test records...")
        
        with click.progressbar(length=records) as bar:
            for i in range(records):
                record = generator.generate_enhanced_record()
                bar.update(1)
        
        click.echo("✓ Test generation successful")
        
    except Exception as e:
        raise click.ClickException(f"Validation failed: {e}")

@cli.command()
def profiles():
    """List available load profiles"""
    
    click.echo("Available Load Profiles:")
    click.echo("="*50)
    
    for name, profile in LOAD_PROFILES.items():
        click.echo(f"\n{name.upper()}:")
        click.echo(f"  Rate: {profile.rate:,} records/sec")
        click.echo(f"  Batch size: {profile.batch_size}")
        click.echo(f"  Duration: {profile.duration or 'default'} seconds")
        click.echo(f"  Workers: {profile.workers or 'default'}")
        click.echo(f"  GPU: {'Yes' if profile.use_gpu else 'No'}")
        click.echo(f"  Description: {profile.description}")

if __name__ == '__main__':
    cli()