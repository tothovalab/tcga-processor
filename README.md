# tcga-processor
Compilation of scripts to easily download and process NCI GDC portal data

download TCGA data using sample sheets from the GDC portal, process RNA-Seq transcriptome data, merging expression data from multiple samples and process variant call data (MAF files), combining mutation data from multiple samples.

### Requires:
Python: 3.6+
Python Packages: pandas, requests

## Installation

```
git clone [https://github.com/tothovalab/tcga-processor.git](https://github.com/tothovalab/tcga-processor.git)
cd tcga-processor
```
```
pip install pandas requests
```

### download TCGA data
script: download_tcga_data.py: downloads TCGA data files based on a sample sheet obtained from the GDC portal. Validates file IDs, downloads data in batches, and extracts files to a specified output directory.
params: --sample-sheet: (Required) Path to the sample sheet TSV file downloaded from the GDC portal with all required file IDs; --output-directory: (Optional) Directory where data will be saved. Default is ./outputs.

```
python download_tcga_data.py --sample-sheet example_sheet.tsv [--output-directory /example_output_dir]
```

### process TCGA data
scripts: process_tcga_data_transcriptome.py: processes and combines extracted TCGA RNA-Seq data files (tsv files). Merges data on the gene_name column and includes all expression columns by default. Column names are renamed to include sample identifiers.
params: --sample-sheet: (Required) Path to the sample sheet TSV file; --outputs-dir: (Optional) Directory containing extracted files. Default is ./outputs; --output-directory: (Optional) Directory where combined data will be saved. Default is the current working directory; --output-file: (Optional) Name of the output combined TSV file. Default is combined_data.tsv; --expression-columns: (Optional) List of expression columns to extract. Default is all columns.

```
python process_tcga_data_transcriptome.py --sample-sheet example_sheet.tsv [--outputs-dir /example_output_dir] [--output-directory /example_output] [--output-file combined_data.tsv] [--expression-columns column1 column2 ...]
```
scripts: process_tcga_data_variantcalls.py: processes and combines extracted TCGA MAF files. Concatenates MAF files, retains specified columns, calculates Variant Allele Frequency (VAF), and saves combined data to a TSV file.
params: --sample-sheet: (Required) Path to the sample sheet TSV file; --outputs-dir: (Optional) Directory containing extracted MAF files. Default is ./outputs; --output-directory: (Optional) Directory where combined MAF file will be saved. Default is the current working directory; --output-file: (Optional) Name of the output combined MAF TSV file. Default is combined_maf.tsv; --retain-columns: (Optional) List of columns to retain. Default is a predefined set of common columns; --calculate-vaf: (Optional) Include this flag to calculate the Variant Allele Frequency (VAF).

```
python process_tcga_data_transcriptome.py --sample-sheet example_sheet.tsv [--outputs-dir /example_output_dir] [--output-directory /example_output] [--output-file combined_maf.tsv] [--expression-columns column1 column2 ...]
```


