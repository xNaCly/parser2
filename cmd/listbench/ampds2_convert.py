import sys
import json
import gzip
import hdf5plugin
import h5py

# Reads the data from the AMPds2 dataset h5 file and writes it to a json file.
# Only reads the first meter of the first building, as only that is used
# for the comparison/benchmark.
# Automatically determines .json.gz filepath from the .h5 filepath.

if len(sys.argv) != 2:
    print("Usage: python ampds2_convert.py <filename>")
    sys.exit(1)

filename = sys.argv[1]

def read_data():
    data = []
    with h5py.File(filename, "r") as f:
        building = f.get("building1")
        elec = building.get("elec")
        meter1 = elec.get("meter1")
        table = meter1.get("table")

        for i in range(len(table)):
            if i % 1000 == 0:
                # Log some progress when some data is read
                print(f"{i} / {len(table)} ({i/len(table)*100:.2f} %)", table[i])

            # Convert data point to map
            entry = list(table[i][1])
            for j in range(len(entry)):
                entry[j] = entry[j].item() # convert numpy float to python float for json serialization
            data.append({
                "timestamp": table[i][0].item(),
                "voltage": entry[0],
                "current": entry[1],
                "frequency": entry[2],
                "power": entry[0] * entry[1],
            })

    return data


data = read_data()

# Write data to json file
json_filename = filename.replace(".h5", ".json.gz")
with gzip.open(json_filename, "w") as f:
    f.write(json.dumps(data).encode("utf-8"))


