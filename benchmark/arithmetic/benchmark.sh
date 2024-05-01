hyperfine \
    "./main -jit=1 benchmark/arithmetic/arithmetic100k.neemann" -n="100000"  \
    "./main -jit=1 benchmark/arithmetic/arithmetic500k.neemann" -n="500000"  \
    "./main -jit=1 benchmark/arithmetic/arithmetic1mio.neemann" -n="1000000" \
    "./main -jit=1 benchmark/arithmetic/arithmetic5mio.neemann" -n="5000000" \
    --export-csv=arithmeticJIT.csv

hyperfine \
    "./main -jit=0 benchmark/arithmetic/arithmetic100k.neemann" -n="100000"  \
    "./main -jit=0 benchmark/arithmetic/arithmetic500k.neemann" -n="500000"  \
    "./main -jit=0 benchmark/arithmetic/arithmetic1mio.neemann" -n="1000000" \
    "./main -jit=0 benchmark/arithmetic/arithmetic5mio.neemann" -n="5000000" \
    --export-csv=arithmetic.csv
