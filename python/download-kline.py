#!/usr/bin/env python

"""
  script to download klines.
  set the absolute path destination folder for STORE_DIRECTORY, and run

  e.g. STORE_DIRECTORY=/data/ ./download-kline.py

"""
import sys
from datetime import *
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
import threading
import os
from enums import *
from utility import download_file, get_all_symbols, get_parser, get_start_end_date_objects, convert_to_date_object, \
  get_path


# Thread-safe progress tracking
class DownloadTracker:
    def __init__(self, total_files):
        self.lock = threading.Lock()
        self.completed = 0
        self.failed = 0
        self.skipped = 0
        self.total = total_files
        self.failed_files = []

    def mark_completed(self, file_name):
        with self.lock:
            self.completed += 1
            self._print_progress()

    def mark_failed(self, file_name):
        with self.lock:
            self.failed += 1
            self._print_progress()

    def mark_skipped(self, file_name):
        with self.lock:
            self.skipped += 1
            self._print_progress()

    def _print_progress(self):
        processed = self.completed + self.failed + self.skipped
        if processed % 10 == 0 or processed == self.total:  # Update every 10 files or at end
            progress = processed / self.total * 100
            print(
                f"\r📊 Progress: {progress:.1f}% ({processed}/{self.total}) ✅{self.completed} ❌{self.failed} ⏭️{self.skipped}",
                end="", flush=True)
            if processed == self.total:
                print()  # New line when complete


def is_file_complete(file_path):
    """Check if file exists and has reasonable size (not empty/corrupted)"""
    if not os.path.exists(file_path):
        return False

    # Check if file is not empty (basic corruption check)
    file_size = os.path.getsize(file_path)
    return file_size > 0


def download_single_kline_file(download_info, tracker):
    """Download a single kline file (worker function for threading)"""
    trading_type, symbol, interval, date, start_date, end_date, folder, checksum, date_range = download_info

    # Check date range
    current_date = convert_to_date_object(date)
    if not (current_date >= start_date and current_date <= end_date):
        return True  # Skip silently

    try:
        # Build file paths
        path = get_path(trading_type, "klines", "daily", symbol, interval)
        file_name = "{}-{}-{}.zip".format(symbol.upper(), interval, date)

        # Check if main file already exists
        full_path = os.path.join(folder or os.getcwd(), path, file_name)
        if is_file_complete(full_path):
            tracker.mark_skipped(file_name)
            return True

        # Download main file
        download_file(path, file_name, date_range, folder)

        # Download checksum if requested
        if checksum == 1:
            checksum_path = get_path(trading_type, "klines", "daily", symbol, interval)
            checksum_file_name = "{}-{}-{}.zip.CHECKSUM".format(symbol.upper(), interval, date)

            # Check if checksum file already exists
            checksum_full_path = os.path.join(folder or os.getcwd(), checksum_path, checksum_file_name)
            if not is_file_complete(checksum_full_path):
                download_file(checksum_path, checksum_file_name, date_range, folder)

        tracker.mark_completed(file_name)
        return True

    except Exception as e:
        print(f"\n❌ Error downloading {symbol}-{interval}-{date}: {e}")
        tracker.failed_files.append(f"{symbol}-{interval}-{date}")
        tracker.mark_failed(f"{symbol}-{interval}-{date}")
        return False


def build_download_queue(trading_type, symbols, intervals, dates, start_date, end_date, folder, checksum, date_range):
    """Build complete download queue and remove duplicates"""
    download_queue = []
    seen_files = set()

    for symbol in symbols:
        for interval in intervals:
            for date in dates:
                # Create unique identifier
                file_id = f"{symbol}-{interval}-{date}"

                if file_id not in seen_files:
                    seen_files.add(file_id)
                    download_info = (trading_type, symbol, interval, date, start_date, end_date, folder, checksum,
                                     date_range)
                    download_queue.append(download_info)

    return download_queue


def download_daily_klines(trading_type, symbols, num_symbols, intervals, dates, start_date, end_date, folder, checksum,
                          max_workers):
    current = 0
    date_range = None

    if start_date and end_date:
        date_range = start_date + " " + end_date

    if not start_date:
        start_date = START_DATE
    else:
        start_date = convert_to_date_object(start_date)

    if not end_date:
        end_date = END_DATE
    else:
        end_date = convert_to_date_object(end_date)

    # Get valid intervals for daily
    intervals = list(set(intervals) & set(DAILY_INTERVALS))
    print("Found {} symbols".format(num_symbols))

    # Build complete download queue
    print("Building download queue...")
    download_queue = build_download_queue(trading_type, symbols, intervals, dates, start_date, end_date, folder,
                                          checksum, date_range)

    total_files = len(download_queue)
    if total_files == 0:
        print("No files to download.")
        return

    print(f"Queued {total_files} files for download using {max_workers} workers...")

    # Initialize progress tracker
    tracker = DownloadTracker(total_files)

    # Download files in parallel
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all download tasks
        futures = [
            executor.submit(download_single_kline_file, download_info, tracker)
            for download_info in download_queue
        ]

        # Wait for all downloads to complete
        for future in futures:
            try:
                future.result()  # This will raise any exception that occurred
            except Exception as e:
                print(f"\n⚠️ Task failed: {e}")

    # Final summary
    print(f"\n🎉 Download Summary:")
    print(f"   ✅ Completed: {tracker.completed}")
    print(f"   ❌ Failed: {tracker.failed}")
    print(f"   ⏭️ Skipped: {tracker.skipped}")
    print(f"   📊 Total: {tracker.total}")

    success_rate = (tracker.completed / tracker.total * 100) if tracker.total > 0 else 0
    print(f"   🎯 Success rate: {success_rate:.1f}%")


def download_monthly_klines(trading_type, symbols, num_symbols, intervals, years, months, start_date, end_date, folder, checksum):
  current = 0
  date_range = None

  if start_date and end_date:
    date_range = start_date + " " + end_date

  if not start_date:
    start_date = START_DATE
  else:
    start_date = convert_to_date_object(start_date)

  if not end_date:
    end_date = END_DATE
  else:
    end_date = convert_to_date_object(end_date)

  #Get valid intervals for daily
  intervals = list(set(intervals) & set(DAILY_INTERVALS))
  print("Found {} symbols".format(num_symbols))

  for symbol in symbols:
    print("[{}/{}] - start download monthly {} klines ".format(current+1, num_symbols, symbol))
    for interval in intervals:
      for year in years:
        for month in months:
          current_date = convert_to_date_object('{}-{}-01'.format(year, month))
          if current_date >= start_date and current_date <= end_date:
            path = get_path(trading_type, "klines", "monthly", symbol, interval)
            file_name = "{}-{}-{}-{}.zip".format(symbol.upper(), interval, year, '{:02d}'.format(month))
            download_file(path, file_name, date_range, folder)

            if checksum == 1:
              checksum_path = get_path(trading_type, "klines", "monthly", symbol, interval)
              checksum_file_name = "{}-{}-{}-{}.zip.CHECKSUM".format(symbol.upper(), interval, year, '{:02d}'.format(month))
              download_file(checksum_path, checksum_file_name, date_range, folder)

    current += 1

if __name__ == "__main__":
    parser = get_parser('klines')
    parser.add_argument('--workers', '-w', type=int, default=512,
                        help='Number of worker threads for parallel downloads (default: 128, recommended: 128-256)')
    args = parser.parse_args(sys.argv[1:])

    if not args.symbols:
      print("fetching all symbols from exchange")
      symbols = get_all_symbols(args.type)
      num_symbols = len(symbols)
    else:
      symbols = args.symbols
      num_symbols = len(symbols)

    if args.dates:
      dates = args.dates
    else:
      period = convert_to_date_object(datetime.today().strftime('%Y-%m-%d')) - convert_to_date_object(
        PERIOD_START_DATE)
      dates = pd.date_range(end=datetime.today(), periods=period.days + 1).tolist()
      dates = [date.strftime("%Y-%m-%d") for date in dates]
      if args.skip_monthly == 0:
        download_monthly_klines(args.type, symbols, num_symbols, args.intervals, args.years, args.months, args.startDate, args.endDate, args.folder, args.checksum)
    if args.skip_daily == 0:
        download_daily_klines(args.type, symbols, num_symbols, args.intervals, dates, args.startDate, args.endDate,
                              args.folder, args.checksum, args.workers)
