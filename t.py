import gzip

with gzip.open('semmed_data/entity.csv.gz', 'rt') as f:  # 'rt' mode for text reading
    for i, line in enumerate(f):
        if i >= 5:  # Print first 5 lines
            break
        print(line.strip())