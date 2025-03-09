import pandas as pd
from binascii import hexlify
import datetime
import os
import glob
import dask.dataframe as dd
from dask.distributed import Client
from decoda import *

# Set up Dask client for parallel processing
def setup_dask_client():
    client = Client()
    print(f"Dask dashboard available at: {client.dashboard_link}")
    return client

# Access Dask Client GUI http://127.0.0.1:8787

# Load required labels
df_req = pd.read_csv("combined_req.csv", delimiter=",", header=0, dtype=str)
df_req.columns = df_req.columns.str.strip().str.replace(' ', '_')

desired_spn_name = df_req["Signal_Name"].dropna().tolist()
desired_pgns = df_req["PGN"].dropna().astype(int).tolist()
desired_spns = df_req["SPN"].dropna().astype(int).tolist()

# Convert decimal to hex values
def hexlify_with_nan(x):
    if pd.isna(x):
        return ''
    try:
        return hexlify(bytes([int(x)])).decode('utf-8')
    except (ValueError, TypeError):
        return ''

def decode_spns(hex_data, pgn_id):
    try:
        try:
            spec = spec_provider.provide()
        except Exception as e:
            with open("new_error.txt","w") as f:
                f.write(e)

        if not hex_data or pd.isna(hex_data) or hex_data == "nan":
            return {}

        hex_data = ''.join(c for c in hex_data if c.lower() in '0123456789abcdef')
        if not hex_data:
            return {}

        if len(hex_data) % 2 != 0:
            hex_data = '0' + hex_data

        data_bytes = bytes.fromhex(hex_data.zfill(16))
        try:
            pgn = spec.PGNs.get_by_id(pgn_id)
        except Exception as e:
            with open("new_error.txt","w") as f:
                f.write(e)
        if not pgn:
            return {}

        decoded_spns = pgn.decode(data_bytes)
        return {str(spn.name): spn.display_value for spn in decoded_spns if spn.id in desired_spns}
    except Exception as e:
        with open("error_log.txt", "a") as f:
            f.write(f"{datetime.datetime.now()} - Error decoding SPNs for PGN {pgn_id}: {e}\n")
        return {}

def process_csv(in_file_path, out_folder_path):
    try:
        os.makedirs(out_folder_path, exist_ok=True)
        df = pd.read_csv(in_file_path, delimiter=";", header=1, dtype=str, low_memory=False)

        max_cols = df.shape[1]
        column_names = ["timestamp", "can_id", "dlc"] + [f"data_byte_{i}" for i in range(max_cols - 3)]
        df.columns = column_names[:len(df.columns)]

        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
        df["pgn_id"] = df["can_id"].apply(lambda x: parts_from_can_id(int(str(x), 16) if pd.notna(x) and x != 'nan' else 0)[1])
        df["pgn_id"] = pd.to_numeric(df["pgn_id"], errors='coerce').astype("Int64")

        # Debugging: Print PGNs before filtering
        print(f"Unique PGNs in {os.path.basename(in_file_path)}:", df["pgn_id"].dropna().unique())
        print("Desired PGNs:", desired_pgns)

        # Filter only desired PGNs
        desired_pgns_set = set(desired_pgns)
        df = df[df["pgn_id"].isin(desired_pgns_set)]

        if df.empty:
            print(f"No matching PGNs found in {in_file_path}, skipping.")
            return False

        # Convert data bytes to hex
        data_byte_cols = [col for col in df.columns if "data_byte" in col]
        cols_to_convert = ["dlc"] + data_byte_cols if "dlc" in df.columns else data_byte_cols
        df[cols_to_convert] = df[cols_to_convert].apply(pd.to_numeric, errors='coerce')
        df[data_byte_cols] = df[data_byte_cols].map(hexlify_with_nan)
        df["data_hex"] = df[data_byte_cols].astype(str).agg(''.join, axis=1)

        # Process with Dask
        ddf = dd.from_pandas(df, npartitions=os.cpu_count()-1)
        ddf["decoded_data"] = ddf.apply(lambda row: decode_spns(row['data_hex'], row['pgn_id']), axis=1, meta=object)

        # Expand dictionary into separate columns
        decoded_df = ddf["decoded_data"].apply(pd.Series).fillna("")
        ddf = ddf[["timestamp"]].merge(decoded_df, left_index=True, right_index=True)
        
        # Convert back to Pandas DataFrame
        df = ddf.compute()

        # Debugging: Print first few timestamps before rounding
        print("Before rounding:", df["timestamp"].head(5))

        # Apply rounding and forward fill
        df["timestamp"] = df["timestamp"].dt.round("10ms")
        df.sort_values("timestamp", inplace=True)

        # Apply forward fill
        df = df.ffill()

        # Debugging: Print first few timestamps after rounding
        print("After rounding and ffill:", df["timestamp"].head(5))

        # Drop duplicate timestamps (keep last value within the 10 ms)
        df.drop_duplicates(subset=["timestamp"], keep="last", inplace=True)

        # Save processed data
        output_file_path = os.path.join(out_folder_path, os.path.splitext(os.path.basename(in_file_path))[0] + "_processed.csv")
        df.to_csv(output_file_path, index=False)
        return True

    except Exception as e:
        with open("error_log.txt", "a") as f:
            f.write(f"{datetime.datetime.now()} - Error processing file {in_file_path}: {e}\n")
        return False

def process_all_csv_files_with_dask(root_dir, dest_dir):
    client = setup_dask_client()
    file_paths = glob.glob(os.path.join(root_dir, '**/*.csv'), recursive=True)
    print(f"Found {len(file_paths)} CSV files to process")
    
    from dask import delayed, compute
    tasks = [delayed(process_csv)(file_path, dest_dir) for file_path in file_paths]
    results = compute(*tasks)
    
    print(f"Completed processing {sum(results)} out of {len(file_paths)} files")
    client.close()

if __name__ == "__main__":
    root_directory = "/Users/amith_abhay/Documents/CANdataset" #replace 
    dest_directory = "/Users/amith_abhay/Documents/Processed_CANdataset" #replace
    process_all_csv_files_with_dask(root_directory, dest_directory)
