hyperfine \
    "./main -jit=1 benchmark/string/string100k.neemann" -n="100000"  \
    "./main -jit=1 benchmark/string/string500k.neemann" -n="500000"  \
    "./main -jit=1 benchmark/string/string1mio.neemann" -n="1000000" \
    "./main -jit=1 benchmark/string/string5mio.neemann" -n="5000000" \
    --export-csv=stringJIT.csv

hyperfine \
    "./main -jit=0 benchmark/string/string100k.neemann" -n="100000"  \
    "./main -jit=0 benchmark/string/string500k.neemann" -n="500000"  \
    "./main -jit=0 benchmark/string/string1mio.neemann" -n="1000000" \
    "./main -jit=0 benchmark/string/string5mio.neemann" -n="5000000" \
    --export-csv=string.csv
