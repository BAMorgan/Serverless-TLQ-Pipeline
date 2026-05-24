# Dataset Handling

The original benchmark datasets are local inputs, not repository source files. CSVs under this directory are ignored by Git because the larger files are 60 MB to 187 MB and should be stored externally.

Use the same schema as `tests/fixtures/sales_input.csv`:

```text
Region,Country,Item Type,Sales Channel,Order Priority,Order Date,Order ID,Ship Date,Units Sold,Unit Price,Unit Cost,Total Revenue,Total Cost,Total Profit
```

Recommended local filenames:

- `100000 Sales Records.csv`
- `500000 Sales Records.csv`
- `1000000 Sales Records.csv`
- `1500000 Sales Records.csv`

For portfolio reproduction, keep small curated benchmark inputs in `benchmark_inputs/` and store full datasets in S3, Git LFS, or a documented external download location. The benchmark runner accepts an explicit input path with:

```bash
python run_callservices.py --rows 1500000 --input-file "data/1500000 Sales Records.csv"
```
