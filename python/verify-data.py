#!/usr/bin/env python

"""
Script to verify data integrity of downloaded files using checksums.
Usage: python verify-data.py [data_directory] [--workers N]
"""

import os
import sys
import hashlib
import glob
import argparse
from pathlib import Path
from multiprocessing import Pool, cpu_count
from functools import partial
import time


def calculate_sha256(file_path):
    """Calculate SHA256 hash of a file"""
    sha256_hash = hashlib.sha256()
    try:
        with open(file_path, "rb") as f:
            # Read file in chunks to handle large files
            for chunk in iter(lambda: f.read(4096), b""):
                sha256_hash.update(chunk)
        return sha256_hash.hexdigest()
    except Exception as e:
        print(f"Error reading {file_path}: {e}")
        return None


def read_checksum_file(checksum_path):
    """Read expected checksum from .CHECKSUM file"""
    try:
        with open(checksum_path, 'r') as f:
            # Binance checksum format: "hash  filename"
            line = f.readline().strip()
            return line.split()[0] if line else None
    except Exception as e:
        print(f"Error reading {checksum_path}: {e}")
        return None


def verify_single_file_worker(checksum_file_path):
    """Worker function to verify a single file (for multiprocessing)"""
    checksum_file = Path(checksum_file_path)
    data_file = checksum_file.with_suffix('')

    result = {
        'file': data_file.name,
        'path': str(data_file),
        'status': 'unknown',
        'expected': '',
        'actual': ''
    }

    # Check if data file exists
    if not data_file.exists():
        result['status'] = 'missing'
        return result

    # Read expected checksum
    expected_hash = read_checksum_file(checksum_file)
    if not expected_hash:
        result['status'] = 'invalid_checksum'
        return result

    # Calculate actual checksum
    actual_hash = calculate_sha256(data_file)
    if not actual_hash:
        result['status'] = 'read_error'
        return result

    # Compare checksums
    result['expected'] = expected_hash
    result['actual'] = actual_hash

    if expected_hash.lower() == actual_hash.lower():
        result['status'] = 'verified'
    else:
        result['status'] = 'corrupted'

    return result


def verify_directory_parallel(data_dir, workers=None):
    """Verify all files in parallel using multiprocessing"""
    data_path = Path(data_dir)

    if not data_path.exists():
        print(f"Directory {data_dir} does not exist!")
        return False

    # Find all .CHECKSUM files
    checksum_files = list(data_path.glob("**/*.CHECKSUM"))

    if not checksum_files:
        print("No .CHECKSUM files found!")
        return False

    if workers is None:
        workers = min(cpu_count(), len(checksum_files))

    print(f"Found {len(checksum_files)} checksum files to verify...")
    print(f"Using {workers} worker processes...")

    start_time = time.time()

    # Process files in parallel
    with Pool(processes=workers) as pool:
        results = pool.map(verify_single_file_worker, [str(cf) for cf in checksum_files])

    end_time = time.time()

    # Process results
    verified = 0
    failed = 0
    missing = 0

    for result in results:
        status = result['status']

        if status == 'verified':
            print(f"✅ VERIFIED: {result['file']}")
            verified += 1
        elif status == 'corrupted':
            print(f"❌ CORRUPTED: {result['file']}")
            print(f"   Expected: {result['expected']}")
            print(f"   Actual:   {result['actual']}")
            failed += 1
        elif status == 'missing':
            print(f"📁 MISSING: {result['file']}")
            missing += 1

    # Summary
    total = verified + failed + missing
    elapsed = end_time - start_time

    print(f"\n📊 VERIFICATION SUMMARY:")
    print(f"   Total files: {total}")
    print(f"   ✅ Verified: {verified}")
    print(f"   ❌ Failed:   {failed}")
    print(f"   📁 Missing:  {missing}")
    print(f"   Success rate: {verified / total * 100:.1f}%" if total > 0 else "   No files processed")
    print(f"   Time elapsed: {elapsed:.2f} seconds")
    print(f"   Speed: {total / elapsed:.2f} files/second" if elapsed > 0 else "")

    return failed == 0 and missing == 0


def verify_directory_sequential(data_dir):
    """Original sequential verification (for comparison)"""
    data_path = Path(data_dir)

    if not data_path.exists():
        print(f"Directory {data_dir} does not exist!")
        return False

    # Find all .CHECKSUM files
    checksum_files = list(data_path.glob("**/*.CHECKSUM"))

    if not checksum_files:
        print("No .CHECKSUM files found!")
        return False

    print(f"Found {len(checksum_files)} checksum files to verify (sequential mode)...")

    start_time = time.time()

    verified = 0
    failed = 0
    missing = 0

    for checksum_file in checksum_files:
        # Get corresponding data file (remove .CHECKSUM extension)
        data_file = checksum_file.with_suffix('')

        if not data_file.exists():
            print(f"❌ MISSING: {data_file}")
            missing += 1
            continue

        # Read expected checksum
        expected_hash = read_checksum_file(checksum_file)
        if not expected_hash:
            print(f"❌ INVALID CHECKSUM FILE: {checksum_file}")
            failed += 1
            continue

        # Calculate actual checksum
        actual_hash = calculate_sha256(data_file)
        if not actual_hash:
            failed += 1
            continue

        # Compare
        if expected_hash.lower() == actual_hash.lower():
            print(f"✅ VERIFIED: {data_file.name}")
            verified += 1
        else:
            print(f"❌ CORRUPTED: {data_file}")
            print(f"   Expected: {expected_hash}")
            print(f"   Actual:   {actual_hash}")
            failed += 1

    # Summary
    total = verified + failed + missing
    elapsed = time.time() - start_time

    print(f"\n📊 VERIFICATION SUMMARY:")
    print(f"   Total files: {total}")
    print(f"   ✅ Verified: {verified}")
    print(f"   ❌ Failed:   {failed}")
    print(f"   📁 Missing:  {missing}")
    print(f"   Success rate: {verified / total * 100:.1f}%" if total > 0 else "   No files processed")
    print(f"   Time elapsed: {elapsed:.2f} seconds")
    print(f"   Speed: {total / elapsed:.2f} files/second" if elapsed > 0 else "")

    return failed == 0 and missing == 0


def verify_single_file(file_path):
    """Verify a single file against its checksum"""
    data_file = Path(file_path)
    checksum_file = data_file.with_suffix(data_file.suffix + '.CHECKSUM')

    if not data_file.exists():
        print(f"Data file not found: {data_file}")
        return False

    if not checksum_file.exists():
        print(f"Checksum file not found: {checksum_file}")
        return False

    expected_hash = read_checksum_file(checksum_file)
    actual_hash = calculate_sha256(data_file)

    if expected_hash and actual_hash and expected_hash.lower() == actual_hash.lower():
        print(f"✅ VERIFIED: {data_file}")
        return True
    else:
        print(f"❌ CORRUPTED: {data_file}")
        return False


def main():
    parser = argparse.ArgumentParser(description='Verify data integrity using checksums')
    parser.add_argument('target', help='Directory or file to verify')
    parser.add_argument('--workers', '-w', type=int, default=None,
                        help='Number of worker processes (default: CPU count)')
    parser.add_argument('--sequential', '-s', action='store_true',
                        help='Use sequential processing instead of parallel')

    args = parser.parse_args()

    target_path = Path(args.target)

    if target_path.is_file():
        success = verify_single_file(args.target)
    elif target_path.is_dir():
        if args.sequential:
            success = verify_directory_sequential(args.target)
        else:
            success = verify_directory_parallel(args.target, args.workers)
    else:
        print(f"Target not found: {args.target}")
        sys.exit(1)

    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
