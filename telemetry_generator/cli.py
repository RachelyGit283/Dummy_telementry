import click
import json
import os
import sys
import time
import random
import logging
import math
from pathlib import Path
from typing import Optional, Dict, Any

from .telemetry_generator import EnhancedTelemetryGeneratorPro, OutputFormat, RecordType, BinarySchemaProcessor
from .rolling_writer import RollingFileWriter
from .rate_control import RateLimiter
from .load_profiles import LOAD_PROFILES, LoadProfile
from .fault_injector import FaultType

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

def validate_binary_schema(schema_path: str, types_path: str = None) -> Dict[str, Any]:
    """Validate and load new binary schema format with optional types mapping"""
    if not os.path.exists(schema_path):
        raise click.ClickException(f"Schema file not found: {schema_path}")
    
    try:
        with open(schema_path, 'r') as f:
            schema = json.load(f)
        
        if not isinstance(schema, dict):
            raise ValueError("Schema must be a JSON object")
        
        # Check for new binary schema format markers
        required_fields = ["schema_name", "endianness", "total_bits"]
        missing_fields = [field for field in required_fields if field not in schema]
        
        if missing_fields:
            raise ValueError(f"Missing required binary schema fields: {missing_fields}")
        
        # Validate endianness
        endianness = schema.get("endianness", "").lower()
        if endianness not in ["little", "big"]:
            raise ValueError(f"Invalid endianness '{endianness}'. Must be 'little' or 'big'")
        
        # Validate total_bits
        total_bits = schema.get("total_bits", 0)
        if not isinstance(total_bits, int) or total_bits <= 0:
            raise ValueError(f"total_bits must be a positive integer, got: {total_bits}")
        
        if total_bits % 8 != 0:
            click.echo(f"Warning: total_bits ({total_bits}) is not byte-aligned")
        
        # Test schema processing
        try:
            processor = BinarySchemaProcessor(schema, types_path)
            click.echo(f"Schema validation: '{processor.schema_name}' loaded successfully")
        except Exception as e:
            raise ValueError(f"Schema processing failed: {e}")
        
        return schema
        
    except json.JSONDecodeError as e:
        raise click.ClickException(f"Invalid JSON in schema file: {e}")
    except Exception as e:
        raise click.ClickException(f"Error reading/validating binary schema: {e}")

def detect_schema_format(schema_path: str) -> str:
    """Detect if schema is old format or new binary format"""
    try:
        with open(schema_path, 'r') as f:
            schema = json.load(f)
        
        # Check for binary schema markers
        if all(key in schema for key in ["schema_name", "endianness", "total_bits"]):
            return "binary"
        
        # Check for old schema format
        if any("type" in field_info and isinstance(field_info, dict) 
               for field_info in schema.values() if isinstance(field_info, dict)):
            return "legacy"
        
        return "unknown"
    except:
        return "unknown"

def validate_schema_legacy(schema_path: str) -> Dict[str, Any]:
    """Validate legacy schema format (for backward compatibility)"""
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
    """Telemetry Generator - High-performance telemetry data generator with binary schema support and fault injection"""
    pass

@cli.command()
@click.option('--schema', '-s', required=True, type=click.Path(exists=True),
              help='Path to binary JSON schema file')
@click.option('--types', '-t', type=click.Path(exists=True),
              help='Path to types mapping JSON file')
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
              default='binary',
              help='Output format (default: binary)')
@click.option('--seed', type=int, help='Random seed for reproducible data')
@click.option('--load-profile', '-l',
              type=click.Choice(['low', 'medium', 'high', 'stress', 'burst', 'realistic', 'endurance', 'spike', 'ramp', 'chaos', 'custom']),
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
@click.option('--measurement-name', default='telemetry',
              help='InfluxDB measurement name (default: telemetry)')
# NEW: Fault Injection options
@click.option('--enable-faults', is_flag=True, 
              help='Enable fault injection for realistic error simulation')
@click.option('--fault-rate', default=0.05, type=float,
              help='Global fault injection rate (0.0-1.0, default: 0.05)')
@click.option('--fault-types', 
              help='Comma-separated list of fault types (e.g., out_of_range,wrong_type,missing_field)')
@click.option('--fault-config', type=click.Path(exists=True),
              help='Path to fault injection configuration JSON file')
@click.option('--fault-profile', 
              type=click.Choice(['development', 'testing', 'stress']),
              help='Predefined fault injection profile')
@click.option('--save-fault-report', type=click.Path(),
              help='Path to save fault injection report')
def generate(schema, types, rate, duration, out_dir, rotate_size, format, seed, 
            load_profile, compress, batch_size, prefix, workers, quiet, 
            verbose, gpu, record_type_ratio, measurement_name,
            # NEW: Fault injection parameters
            enable_faults, fault_rate, fault_types, fault_config, fault_profile, save_fault_report):
    """Generate telemetry data with binary schema format and optional fault injection"""

    # Set logging level
    if verbose:
        logger.setLevel(logging.DEBUG)
    elif quiet:
        logger.setLevel(logging.WARNING)
    
    # Detect and validate schema format
    schema_format = detect_schema_format(schema)
    if schema_format == "binary":
        # Use binary schema validation
        try:
            schema_data = validate_binary_schema(schema, types)
            processor = BinarySchemaProcessor(schema_data, types)
            logger.info(f"Loaded binary schema '{processor.schema_name}' with {len(processor.fields)} fields")
            logger.info(f"Record size: {math.ceil(processor.total_bits/8)} bytes, Endianness: {processor.endianness}")
        except Exception as e:
            raise click.ClickException(str(e))
    elif schema_format == "legacy":
        # Support legacy schemas with warning
        click.echo("Warning: Using legacy schema format. Consider upgrading to binary schema format.")
        try:
            schema_data = validate_schema_legacy(schema)
            logger.info(f"Loaded legacy schema with {len(schema_data)} fields")
        except Exception as e:
            raise click.ClickException(str(e))
    else:
        raise click.ClickException(
            "Unknown or invalid schema format. Expected binary schema with schema_name, endianness, total_bits."
        )
    
    # Process fault injection settings
    fault_types_list = None
    if fault_types:
        fault_types_list = [ft.strip() for ft in fault_types.split(',')]
        # Validate fault types
        valid_fault_types = [ft.value for ft in FaultType]
        invalid_types = [ft for ft in fault_types_list if ft not in valid_fault_types]
        if invalid_types:
            raise click.ClickException(f"Invalid fault types: {invalid_types}. Valid types: {valid_fault_types}")
    
    # Apply predefined fault profile
    if fault_profile:
        if fault_profile == 'development':
            enable_faults = True
            fault_rate = 0.02  # 2%
            fault_types_list = ['out_of_range', 'null_values']
        elif fault_profile == 'testing':
            enable_faults = True
            fault_rate = 0.05  # 5%
            fault_types_list = ['out_of_range', 'wrong_type', 'enum_invalid', 'null_values']
        elif fault_profile == 'stress':
            enable_faults = True
            fault_rate = 0.15  # 15%
            fault_types_list = [ft.value for ft in FaultType]  # All fault types
        
        logger.info(f"Applied fault profile '{fault_profile}': rate={fault_rate:.1%}, types={len(fault_types_list or [])}")
    
    # Validate fault rate
    if fault_rate < 0.0 or fault_rate > 1.0:
        raise click.ClickException("Fault rate must be between 0.0 and 1.0")
    
    # Apply load profile if specified
    if load_profile and load_profile != 'custom':
        if load_profile not in LOAD_PROFILES:
            raise click.ClickException(f"Unknown load profile: {load_profile}")
        
        profile = LOAD_PROFILES[load_profile]
        rate = profile.rate
        batch_size = profile.batch_size
        if profile.duration:
            duration = profile.duration
        if profile.workers:
            workers = profile.workers
        if profile.use_gpu:
            gpu = True
        logger.info(f"Applied load profile '{load_profile}': rate={rate:,}, batch={batch_size}, duration={duration}s")
    
    # Parse record type ratio
    ratio_dict = {}
    try:
        for part in record_type_ratio.split(','):
            type_name, ratio = part.split(':')
            ratio_dict[RecordType[type_name.upper()]] = float(ratio)
    except (ValueError, KeyError) as e:
        raise click.ClickException(f"Invalid record type ratio format: {record_type_ratio}")
    
    # Validate ratios sum to ~1.0
    total_ratio = sum(ratio_dict.values())
    if abs(total_ratio - 1.0) > 0.01:
        logger.warning(f"Record type ratios sum to {total_ratio:.3f}, not 1.0. Normalizing...")
        ratio_dict = {k: v/total_ratio for k, v in ratio_dict.items()}
    
    # Set random seed if provided
    if seed is not None:
        random.seed(seed)
        logger.info(f"Set random seed: {seed}")
    
    # Create output directory
    Path(out_dir).mkdir(parents=True, exist_ok=True)
    
    # Parse rotation size
    try:
        max_file_size = parse_size(rotate_size)
        logger.info(f"Max file size: {max_file_size:,} bytes ({max_file_size/(1024*1024):.1f} MB)")
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
    
    # Initialize generator with fault injection
    try:
        if schema_format == "binary":
            generator = EnhancedTelemetryGeneratorPro(
                schema_file=schema,
                types_file=types,
                output_dir=out_dir,
                enable_gpu=gpu,
                enable_fault_injection=enable_faults,
                fault_rate=fault_rate,
                fault_config_file=fault_config,
                fault_types=fault_types_list,
                logger=logger
            )
            
            # Show binary schema info
            if verbose:
                info = generator.get_enhanced_schema_info()
                logger.debug(f"Binary schema details: {json.dumps(info, indent=2)}")
                
                # Show fault injection info
                if enable_faults:
                    fault_stats = generator.get_fault_statistics()
                    logger.debug(f"Fault injection initialized: {fault_stats}")
        else:
            # Legacy schema support - fall back to old generator
            raise click.ClickException("Legacy schema format not fully supported in this version. Please upgrade to binary schema format.")
        
    except Exception as e:
        raise click.ClickException(f"Failed to initialize generator: {e}")
    
    # Initialize resources
    writer = None
    rate_limiter = None
    
    # Calculate total records and estimate storage
    total_records = rate * duration
    
    if schema_format == "binary":
        storage_info = generator.estimate_storage_requirements(
            total_records, output_format, 
            compression_ratio=0.3 if compress else 1.0
        )
        
        logger.info(f"Generation plan:")
        logger.info(f"  Records: {total_records:,} at {rate:,} rec/s for {duration}s")
        logger.info(f"  Estimated storage: {storage_info['compressed_mb']:.1f} MB")
        logger.info(f"  Format: {format} ({'compressed' if compress else 'uncompressed'})")
        
        # Show fault injection status
        if enable_faults:
            logger.info(f"  Fault injection: ENABLED ({fault_rate:.1%} rate)")
            if fault_types_list:
                logger.info(f"  Fault types: {', '.join(fault_types_list)}")
        else:
            logger.info(f"  Fault injection: DISABLED")
    
    # Progress tracking
    start_time = time.time()
    records_generated = 0
    faulty_records = 0
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
        if schema_format == "binary":
            progress_label = f'Generating {processor.schema_name}'
            if enable_faults:
                progress_label += f' (faults: {fault_rate:.1%})'
        else:
            progress_label = 'Generating telemetry'
        
        progress_bar = click.progressbar(
            length=total_records,
            label=progress_label,
            show_pos=True,
            show_percent=True,
            show_eta=True,
            width=0
        )
    
    # Storage for fault details
    all_fault_details = []
    
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
        rate_limiter = RateLimiter(rate, batch_size=batch_size, logger=logger)
        rate_limiter.start()
        
        with progress_bar:
            while records_generated < total_records:
                batch_records = min(batch_size, total_records - records_generated)
                
                # Generate batch of records with fault injection
                if gpu and generator.gpu_generator:
                    # Use GPU acceleration if available (currently doesn't support fault injection)
                    rand_val = random.random()
                    cumulative = 0
                    record_type = RecordType.UPDATE
                    for rtype, ratio in ratio_dict.items():
                        cumulative += ratio
                        if rand_val <= cumulative:
                            record_type = rtype
                            break
                    
                    records = generator.generate_batch_gpu_accelerated(
                        batch_records, record_type
                    )
                    fault_details = []  # GPU mode doesn't support faults yet
                else:
                    # Regular generation with fault support
                    records = []
                    batch_fault_details = []
                    
                    for _ in range(batch_records):
                        rand_val = random.random()
                        cumulative = 0
                        record_type = RecordType.UPDATE
                        for rtype, ratio in ratio_dict.items():
                            cumulative += ratio
                            if rand_val <= cumulative:
                                record_type = rtype
                                break
                        
                        record, fault_details = generator.generate_enhanced_record(
                            record_type=record_type
                        )
                        records.append(record)
                        
                        if fault_details:
                            batch_fault_details.extend(fault_details)
                            faulty_records += 1
                    
                    all_fault_details.extend(batch_fault_details)
                
                # Write records
                for record in records:
                    writer.write_record(record, generator)
                
                records_generated += batch_records
                progress_bar.update(batch_records)
                
                # Rate limiting
                rate_limiter.wait_if_needed(batch_records)
                
                # Progress reporting
                current_time = time.time()
                if verbose and (current_time - last_report_time) >= report_interval:
                    elapsed = current_time - start_time
                    actual_rate = records_generated / elapsed if elapsed > 0 else 0
                    
                    logger.debug(
                        f"Progress: {records_generated:,}/{total_records:,} records "
                        f"({100*records_generated/total_records:.1f}%)"
                    )
                    logger.debug(
                        f"Rate: target={rate:,}, actual={actual_rate:.1f} rec/s"
                    )
                    logger.debug(
                        f"Files: {writer.file_count}, Size: {writer.total_bytes_written:,} bytes "
                        f"({writer.total_bytes_written/(1024*1024):.1f} MB)"
                    )
                    
                    if enable_faults:
                        fault_rate_actual = (faulty_records / records_generated * 100) if records_generated > 0 else 0
                        logger.debug(f"Faults: {faulty_records:,} records ({fault_rate_actual:.1f}%)")
                    
                    last_report_time = current_time
        
        # Flush writer before cleanup
        if writer:
            writer.flush()
            
    except KeyboardInterrupt:
        logger.warning("Generation interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Error during generation: {e}")
        if verbose:
            import traceback
            traceback.print_exc()
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
                logger.info("Rate limiter stopped")
            except Exception as e:
                logger.error(f"Error stopping rate limiter: {e}")
    
    # Final summary
    elapsed_time = time.time() - start_time
    actual_rate = records_generated / elapsed_time if elapsed_time > 0 else 0
    final_rate_stats = rate_limiter.get_stats() if rate_limiter else None
    
    # Get fault statistics
    fault_statistics = generator.get_fault_statistics() if enable_faults else None
    
    click.echo("\n" + "="*70)
    if schema_format == "binary":
        click.echo("BINARY TELEMETRY GENERATION COMPLETE")
        click.echo("="*70)
        click.echo(f"Schema:             {processor.schema_name}")
        click.echo(f"Endianness:         {processor.endianness}")
        click.echo(f"Record size:        {math.ceil(processor.total_bits/8)} bytes")
    else:
        click.echo("TELEMETRY GENERATION COMPLETE")
        click.echo("="*70)
    
    click.echo(f"Records generated:  {records_generated:,}")
    click.echo(f"Time elapsed:       {elapsed_time:.2f} seconds")
    click.echo(f"Target rate:        {rate:,} records/sec")
    click.echo(f"Actual rate:        {actual_rate:.1f} records/sec")
    if final_rate_stats:
        click.echo(f"Rate accuracy:      {100*final_rate_stats.actual_rate/final_rate_stats.target_rate:.1f}%")
    if writer:
        click.echo(f"Files created:      {writer.file_count}")
        click.echo(f"Total size:         {writer.total_bytes_written:,} bytes ({writer.total_bytes_written/(1024*1024):.1f} MB)")
        click.echo(f"Avg file size:      {writer.total_bytes_written/writer.file_count/1024/1024:.1f} MB")
    click.echo(f"Output directory:   {out_dir}")
    if gpu and hasattr(generator, 'gpu_generator') and generator.gpu_generator and generator.gpu_generator.use_gpu:
        click.echo(f"GPU acceleration:   ENABLED")
    
    # Fault injection summary
    if enable_faults and fault_statistics:
        click.echo("\nFAULT INJECTION SUMMARY")
        click.echo("-" * 30)
        click.echo(f"Faulty records:     {fault_statistics['faulty_records']:,} ({fault_statistics['fault_rate_percent']:.1f}%)")
        click.echo(f"Fault types used:   {len(fault_statistics['fault_types'])}")
        
        # Show top fault types
        if fault_statistics['fault_types']:
            sorted_faults = sorted(fault_statistics['fault_types'].items(), key=lambda x: x[1], reverse=True)
            top_faults = sorted_faults[:3]
            click.echo(f"Top fault types:    {', '.join([f'{ft}: {count}' for ft, count in top_faults])}")
        
        # Show affected fields
        if fault_statistics['affected_fields']:
            affected_count = len(fault_statistics['affected_fields'])
            click.echo(f"Fields affected:    {affected_count}")
    
    click.echo("="*70)
    
    # Save fault report if requested
    if save_fault_report and enable_faults and all_fault_details:
        try:
            fault_report = {
                "generation_summary": {
                    "total_records": records_generated,
                    "faulty_records": faulty_records,
                    "fault_rate_actual": (faulty_records / records_generated * 100) if records_generated > 0 else 0,
                    "fault_rate_configured": fault_rate * 100,
                    "generation_time_seconds": elapsed_time
                },
                "fault_statistics": fault_statistics,
                "fault_details": all_fault_details[:1000]  # Limit to first 1000 faults to avoid huge files
            }
            
            with open(save_fault_report, 'w', encoding='utf-8') as f:
                json.dump({str(k): str(v) for k, v in fault_report.items()}, f, indent=2, ensure_ascii=False)
            
            click.echo(f"Fault report saved: {save_fault_report}")
            logger.info(f"Detailed fault report written to {save_fault_report}")
            
        except Exception as e:
            logger.error(f"Failed to save fault report: {e}")

@cli.command()
@click.option('--schema', '-s', required=True, type=click.Path(exists=True),
              help='Path to binary JSON schema file')
@click.option('--types', '-t', type=click.Path(exists=True),
              help='Path to types mapping JSON file')
@click.option('--records', '-n', default=1000, type=int,
              help='Number of test records to generate (default: 1000)')
@click.option('--show-sample', is_flag=True,
              help='Show sample record data')
@click.option('--check-size', is_flag=True,
              help='Verify all records have consistent size')
@click.option('--test-faults', is_flag=True,
              help='Test fault injection functionality')
@click.option('--fault-rate', default=0.1, type=float,
              help='Fault rate for testing (default: 0.1)')
def validate(schema, types, records, show_sample, check_size, test_faults, fault_rate):
    """Validate binary schema and generate test records with optional fault injection testing"""
    
    click.echo("Validating schema...")
    
    # Detect schema format first
    schema_format = detect_schema_format(schema)
    if schema_format == "binary":
        # Binary schema validation
        try:
            schema_data = validate_binary_schema(schema, types)
            processor = BinarySchemaProcessor(schema_data, types)
            
            click.echo(f"Binary schema '{processor.schema_name}' loaded successfully")
            
            # Initialize generator
            generator = EnhancedTelemetryGeneratorPro(
                schema_file=schema,
                types_file=types,
                enable_fault_injection=test_faults,
                fault_rate=fault_rate if test_faults else 0,
                logger=logger
            )
            
            # Get schema info
            info = generator.get_enhanced_schema_info()
            
            click.echo("\n" + "="*50)
            click.echo("BINARY SCHEMA INFORMATION")
            click.echo("="*50)
            click.echo(f"Schema name:        {info['schema_name']}")
            click.echo(f"Endianness:         {processor.endianness}")
            click.echo(f"Total fields:       {info['fields_count']}")
            click.echo(f"Total bits:         {info['total_bits']}")
            click.echo(f"Record size:        {info['total_bytes']} bytes")
            
            if processor.validation:
                click.echo(f"Validation:         {list(processor.validation.keys())}")
            
            # Fault injection status
            if test_faults:
                click.echo(f"Fault injection:    ENABLED ({fault_rate:.1%} rate)")
            else:
                click.echo(f"Fault injection:    DISABLED")
            
            click.echo("\n" + "-"*50)
            click.echo("FIELD LAYOUT")
            click.echo("-"*50)
            for field in info['fields']:
                enum_info = ""
                if processor.fields_by_name[field['name']].get('enum'):
                    enum_values = list(processor.fields_by_name[field['name']]['enum'].values())
                    enum_info = f" (enum: {', '.join(str(v) for v in enum_values[:3])}{'...' if len(enum_values) > 3 else ''})"
                
                click.echo(f"  {field['name']:20} {field['type']:12} "
                          f"{field['bits']:2}b pos {field['position']:8}{enum_info}")
            
            # Generate test records
            if test_faults:
                click.echo(f"\nGenerating {records} test records with fault injection...")
            else:
                click.echo(f"\nGenerating {records} test binary records...")
            
            test_data = []
            sizes = []
            fault_summary = {"clean": 0, "faulty": 0, "fault_types": {}}
            
            with click.progressbar(length=records, label='Testing') as bar:
                for i in range(records):
                    try:
                        if test_faults:
                            record, fault_details = generator.generate_enhanced_record()
                            if fault_details:
                                fault_summary["faulty"] += 1
                                for fault in fault_details:
                                    fault_type = fault["fault_type"]
                                    fault_summary["fault_types"][fault_type] = fault_summary["fault_types"].get(fault_type, 0) + 1
                            else:
                                fault_summary["clean"] += 1
                        else:
                            record = generator.generate_clean_record()
                            fault_summary["clean"] += 1
                        
                        binary_data = generator.pack_record_enhanced(record)
                        test_data.append((record, binary_data))
                        sizes.append(len(binary_data))
                        bar.update(1)
                    except Exception as e:
                        raise click.ClickException(f"Record generation failed at record {i+1}: {e}")
            
            # Validate binary packing
            expected_size = math.ceil(processor.total_bits / 8)
            size_set = set(sizes)
            
            click.echo(f"\n" + "="*50)
            click.echo("VALIDATION RESULTS")
            click.echo("="*50)
            
            if len(size_set) == 1 and sizes[0] == expected_size:
                click.echo(f"Size validation:    PASSED - All records exactly {expected_size} bytes")
            else:
                click.echo(f"Size validation:    FAILED")
                click.echo(f"  Expected size:    {expected_size} bytes")
                click.echo(f"  Actual sizes:     {size_set}")
                return
            
            if check_size and test_data:
                click.echo(f"Size consistency:   Checked {len(test_data)} records - All consistent")
            
            # Fault injection results
            if test_faults:
                click.echo(f"\nFAULT INJECTION TEST RESULTS")
                click.echo("-" * 30)
                actual_fault_rate = (fault_summary["faulty"] / records * 100) if records > 0 else 0
                click.echo(f"Clean records:      {fault_summary['clean']:,}")
                click.echo(f"Faulty records:     {fault_summary['faulty']:,} ({actual_fault_rate:.1f}%)")
                click.echo(f"Target fault rate:  {fault_rate:.1%}")
                click.echo(f"Actual fault rate:  {actual_fault_rate:.1f}%")
                
                if fault_summary["fault_types"]:
                    click.echo(f"Fault types found:  {len(fault_summary['fault_types'])}")
                    for fault_type, count in sorted(fault_summary["fault_types"].items(), key=lambda x: x[1], reverse=True):
                        click.echo(f"  {fault_type:20} {count:,} times")
            
            # Show sample record
            if show_sample and test_data:
                sample_record, sample_binary = test_data[0]
                
                click.echo(f"\n" + "-"*50)
                click.echo("SAMPLE RECORD")
                click.echo("-"*50)
                click.echo(f"Sequence ID:        {sample_record.sequence_id}")
                click.echo(f"Timestamp:          {sample_record.timestamp}")
                click.echo(f"Type:               {sample_record.record_type.value}")
                click.echo(f"Binary size:        {len(sample_binary)} bytes")
                click.echo(f"Binary (hex):       {sample_binary.hex()}")
                
                click.echo("\nData fields:")
                for key, value in sample_record.data.items():
                    value_str = str(value)
                    if len(value_str) > 30:
                        value_str = value_str[:27] + "..."
                    click.echo(f"  {key:20} {value_str}")
            
            # Test format conversions
            if test_data:
                sample_record = test_data[0][0]
                try:
                    json_str = generator.format_json(sample_record)
                    json.loads(json_str)  # Validate JSON
                    click.echo(f"JSON format:        PASSED")
                except Exception as e:
                    click.echo(f"JSON format:        FAILED - {e}")
                
                try:
                    ndjson_str = generator.format_ndjson(sample_record)
                    json.loads(ndjson_str.strip())  # Validate NDJSON
                    click.echo(f"NDJSON format:      PASSED")
                except Exception as e:
                    click.echo(f"NDJSON format:      FAILED - {e}")
            
            click.echo("\n" + "="*50)
            if test_faults:
                click.echo("BINARY SCHEMA AND FAULT INJECTION VALIDATION SUCCESSFUL")
            else:
                click.echo("BINARY SCHEMA VALIDATION SUCCESSFUL")
            click.echo("="*50)
            click.echo(f"Schema is ready for production use with {records} test records generated.")
            
        except Exception as e:
            raise click.ClickException(f"Binary schema validation failed: {e}")
    
    elif schema_format == "legacy":
        # Legacy schema validation
        try:
            schema_data = validate_schema_legacy(schema)
            click.echo(f"Legacy schema loaded successfully with {len(schema_data)} fields")
            click.echo("Warning: Legacy schema format. Consider upgrading to binary schema format.")
            click.echo("Note: Fault injection not supported for legacy schemas.")
            
            # Show legacy schema info
            click.echo(f"\nLegacy Schema Information:")
            for field_name, field_info in schema_data.items():
                if isinstance(field_info, dict) and "type" in field_info:
                    field_type = field_info.get("type", "unknown")
                    bits = field_info.get("bits", "?")
                    click.echo(f"  {field_name:20} {field_type:12} {bits} bits")
            
            click.echo("\nNote: Full validation requires binary schema format")
            
        except Exception as e:
            raise click.ClickException(f"Legacy schema validation failed: {e}")
    
    else:
        raise click.ClickException("Unknown schema format. Expected binary schema.")

@cli.command()
@click.argument('output_file', type=click.Path())
@click.option('--fault-rate', default=0.05, type=float, help='Global fault injection rate')
@click.option('--include-types', help='Comma-separated fault types to include')
@click.option('--exclude-types', help='Comma-separated fault types to exclude')
@click.option('--profile', type=click.Choice(['development', 'testing', 'stress', 'custom']),
              default='testing', help='Fault profile template')
def create_fault_config(output_file, fault_rate, include_types, exclude_types, profile):
    """Create a fault injection configuration file"""
    
    # Parse include/exclude types
    include_list = [t.strip() for t in include_types.split(',')] if include_types else None
    exclude_list = [t.strip() for t in exclude_types.split(',')] if exclude_types else []
    
    # Validate fault types
    valid_fault_types = [ft.value for ft in FaultType]
    if include_list:
        invalid = [t for t in include_list if t not in valid_fault_types]
        if invalid:
            raise click.ClickException(f"Invalid fault types: {invalid}")
    
    if exclude_list:
        invalid = [t for t in exclude_list if t not in valid_fault_types]
        if invalid:
            raise click.ClickException(f"Invalid fault types in exclude: {invalid}")
    
    # Create config based on profile
    config = {
        "global_fault_rate": fault_rate,
        "description": f"Fault injection configuration - {profile} profile",
        "fault_configs": []
    }
    
    # Define fault configs based on profile
    if profile == 'development':
        fault_configs = [
            {
                "fault_type": "out_of_range",
                "probability": 0.01,
                "severity": "low",
                "field_patterns": ["*_id", "metric_id"],
                "parameters": {"multiplier_range": [2, 5]}
            },
            {
                "fault_type": "null_values", 
                "probability": 0.005,
                "severity": "low",
                "exclude_fields": ["schema_version", "timestamp_ns"],
                "parameters": {"null_chance": 0.6}
            }
        ]
    elif profile == 'testing':
        fault_configs = [
            {
                "fault_type": "out_of_range",
                "probability": 0.02,
                "severity": "medium",
                "field_patterns": ["*_id", "metric_id", "gpu_index"],
                "parameters": {"multiplier_range": [2, 10], "negative_chance": 0.3}
            },
            {
                "fault_type": "wrong_type",
                "probability": 0.015,
                "severity": "high", 
                "field_patterns": ["timestamp_*", "*_id"],
                "exclude_fields": ["device_id_ascii"],
                "parameters": {"string_instead_of_int": True}
            },
            {
                "fault_type": "enum_invalid",
                "probability": 0.015,
                "severity": "medium",
                "field_patterns": ["scope", "value_type"],
                "parameters": {"invalid_values": [99, 255, -1]}
            },
            {
                "fault_type": "null_values",
                "probability": 0.01,
                "severity": "medium", 
                "exclude_fields": ["schema_version"],
                "parameters": {"null_chance": 0.5}
            }
        ]
    elif profile == 'stress':
        fault_configs = []
        for fault_type in FaultType:
            fault_configs.append({
                "fault_type": fault_type.value,
                "probability": 0.02,
                "severity": "high",
                "field_patterns": ["*"],
                "parameters": {}
            })
    else:  # custom
        fault_configs = [
            {
                "fault_type": "out_of_range",
                "probability": fault_rate / 4,
                "severity": "medium",
                "field_patterns": ["*"],
                "parameters": {"multiplier_range": [2, 10]}
            }
        ]
    
    # Filter by include/exclude
    if include_list:
        fault_configs = [fc for fc in fault_configs if fc["fault_type"] in include_list]
    
    if exclude_list:
        fault_configs = [fc for fc in fault_configs if fc["fault_type"] not in exclude_list]
    
    config["fault_configs"] = fault_configs
    
    # Write config file
    try:
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(config, f, indent=2, ensure_ascii=False)
        
        click.echo(f"Fault configuration created: {output_file}")
        click.echo(f"Profile: {profile}")
        click.echo(f"Global fault rate: {fault_rate:.1%}")
        click.echo(f"Fault types included: {len(fault_configs)}")
        
        if fault_configs:
            click.echo("\nIncluded fault types:")
            for fc in fault_configs:
                click.echo(f"  - {fc['fault_type']} (probability: {fc['probability']:.3f})")
        
    except Exception as e:
        raise click.ClickException(f"Failed to create config file: {e}")

@cli.command()
def list_fault_types():
    """List all available fault injection types"""
    
    click.echo("Available Fault Injection Types:")
    click.echo("=" * 50)
    
    descriptions = {
        FaultType.OUT_OF_RANGE: "Values outside expected ranges",
        FaultType.WRONG_TYPE: "Incorrect data types (string instead of int, etc.)",
        FaultType.MISSING_FIELD: "Required fields missing from records",
        FaultType.INVALID_STRUCTURE: "Malformed record structure",
        FaultType.NULL_VALUES: "Null, empty, or zero values",
        FaultType.ENCODING_ERROR: "Character encoding corruption",
        FaultType.ENUM_INVALID: "Invalid enumeration values",
        FaultType.STRING_CORRUPTION: "Corrupted or truncated strings",
        FaultType.TIMESTAMP_DRIFT: "Incorrect or drifting timestamps",
        FaultType.SEQUENCE_BREAK: "Broken sequence numbering"
    }
    
    for fault_type in FaultType:
        desc = descriptions.get(fault_type, "No description available")
        click.echo(f"{fault_type.value:20} - {desc}")
    
    click.echo("\nUsage examples:")
    click.echo("  --fault-types out_of_range,wrong_type")
    click.echo("  --fault-config my_faults.json")
    click.echo("  --fault-profile testing")

@cli.command()
def profiles():
    """List available load profiles with resource estimates"""
    
    click.echo("Available Load Profiles:")
    click.echo("="*80)
    
    for name, profile in LOAD_PROFILES.items():
        click.echo(f"\n{name.upper()}:")
        click.echo(f"  Rate:           {profile.rate:,} records/sec")
        click.echo(f"  Batch size:     {profile.batch_size}")
        click.echo(f"  Duration:       {profile.duration or 'default'} seconds")
        click.echo(f"  Workers:        {profile.workers or 'default'}")
        click.echo(f"  GPU:            {'Yes' if profile.use_gpu else 'No'}")
        click.echo(f"  Description:    {profile.description}")
        
        # Show resource estimates
        try:
            from .load_profiles import ProfileManager
            resources = ProfileManager.estimate_resources(profile)
            click.echo(f"  Est. Memory:    {resources['estimated_memory_mb']} MB")
            click.echo(f"  Est. Disk:      {resources['estimated_disk_mb']} MB")
            click.echo(f"  Est. CPU:       {resources['estimated_cpu_percent']}%")
        except Exception:
            pass  # Skip if resource estimation fails

@cli.command()
@click.argument('schema_file', type=click.Path(exists=True))
def info(schema_file):
    """Show detailed information about a schema file"""
    
    schema_format = detect_schema_format(schema_file)
    
    click.echo(f"Schema file: {schema_file}")
    click.echo(f"Format: {schema_format}")
    
    if schema_format == "binary":
        try:
            schema_data = validate_binary_schema(schema_file)
            processor = BinarySchemaProcessor(schema_data)
            
            click.echo(f"\nBinary Schema: {processor.schema_name}")
            click.echo(f"Endianness: {processor.endianness}")
            click.echo(f"Total bits: {processor.total_bits}")
            click.echo(f"Record size: {math.ceil(processor.total_bits/8)} bytes")
            click.echo(f"Fields: {len(processor.fields)}")
            
            if processor.validation:
                click.echo(f"Validation: {processor.validation}")
            
            # Show bit utilization
            used_bits = max(field["end_bit"] for field in processor.fields) + 1 if processor.fields else 0
            utilization = (used_bits / processor.total_bits) * 100 if processor.total_bits > 0 else 0
            
            click.echo(f"\nBit utilization: {used_bits}/{processor.total_bits} ({utilization:.1f}%)")
            
            if utilization < 100:
                unused_bits = processor.total_bits - used_bits
                click.echo(f"Unused bits: {unused_bits} ({unused_bits/8:.1f} bytes)")
            
            # Show field details
            click.echo(f"\nField Details:")
            for field in processor.fields:
                enum_info = ""
                if field.get('enum'):
                    enum_values = list(field['enum'].values())
                    enum_info = f" (enum: {', '.join(str(v) for v in enum_values[:3])}{'...' if len(enum_values) > 3 else ''})"
                
                click.echo(f"  {field['name']:20} {field['type']:12} "
                          f"{field['bits']:2}b pos {field['start_bit']}-{field['end_bit']}{enum_info}")
                
        except Exception as e:
            click.echo(f"Error processing binary schema: {e}")
    
    elif schema_format == "legacy":
        click.echo("This is a legacy schema format.")
        click.echo("Consider converting to binary schema format for better performance.")
        
        try:
            schema_data = validate_schema_legacy(schema_file)
            click.echo(f"\nLegacy fields: {len(schema_data)}")
            
            for field_name, field_info in schema_data.items():
                if isinstance(field_info, dict) and "type" in field_info:
                    field_type = field_info.get("type", "unknown")
                    bits = field_info.get("bits", "?")
                    click.echo(f"  {field_name:20} {field_type:12} {bits} bits")
        except Exception as e:
            click.echo(f"Error reading legacy schema: {e}")
    
    else:
        click.echo("Unknown or invalid schema format.")

if __name__ == '__main__':
    cli()