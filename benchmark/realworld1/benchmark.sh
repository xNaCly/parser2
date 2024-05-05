hyperfine \
    "./main -jit=1 benchmark/realworld1/realworld100k.neemann" -n="100000"  \
    "./main -jit=1 benchmark/realworld1/realworld500k.neemann" -n="500000"  \
    "./main -jit=1 benchmark/realworld1/realworld1mio.neemann" -n="1000000" \
    "./main -jit=1 benchmark/realworld1/realworld5mio.neemann" -n="5000000" \
    --export-csv=realworldJIT.csv

hyperfine \
    "./main -jit=0 benchmark/realworld1/realworld100k.neemann" -n="100000"  \
    "./main -jit=0 benchmark/realworld1/realworld500k.neemann" -n="500000"  \
    "./main -jit=0 benchmark/realworld1/realworld1mio.neemann" -n="1000000" \
    "./main -jit=0 benchmark/realworld1/realworld5mio.neemann" -n="5000000" \
    --export-csv=realworld.csv
