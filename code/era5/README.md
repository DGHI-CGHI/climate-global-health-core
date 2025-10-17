# ERA5 Public S3 → Parallel Download → Regional NetCDF Subsetter

This tool inventories the **NCAR public ERA5** archive on AWS S3, downloads monthly NetCDF files in **parallel**, and writes per-region **subset NetCDFs**. Accumulated forecast variables (e.g., SSRD, RO) are converted to **hourly rates**.

* Source bucket: `s3://nsf-ncar-era5` (anonymous; no requester-pays)
* Streams used:

  * `e5.oper.an.sfc` (instantaneous hourly analysis)
  * `e5.oper.fc.sfc.accumu` (forecast-accumulated surface fluxes)
  * `e5.oper.fc.sfc.meanflux` (mean flux for `mtpr`)

---

## Quick start

```bash
# 1) Install R packages (see below).
# 2) Configure REGIONS and WANT inside the script (optional; sane defaults exist).
# 3) Run for a Year–Month, provide a single root directory:
Rscript era5_download.R 2019 1 --out=/path/to/run_root
```

The script creates and uses:

```
/path/to/run_root/
  ├─ era5_nc/        # monthly downloads (mirrors S3 path)
  ├─ era5_regions/   # per-region subsets: <Region>/<Year>/<Month>/*.nc
  └─ wbgt/           # optional WBGT outputs if enabled
```

Example outputs:

```
era5_regions/SEUS/2004/05/ssrd_200405_2004050106-2004051606.nc
era5_regions/Africa/2001/01/2t_200101_2001010100-2001013123.nc
```

---

## Features

* **Parallel** downloads (HTTP/2, resumable) and **parallel** subsetting across files
* Robust S3 ListBucket v2 with pagination and region redirects
* Automatic **accumulated→hourly** conversion

  * Energy (ssrd, sshf, slhf, e): J m⁻² hr⁻¹ → W m⁻² (÷ 3600)
  * Depth (tp, ro): m hr⁻¹ → mm hr⁻¹ (× 1000)
  * First step becomes `NA` after differencing
* Longitude wrap ([0,360] vs. [-180,180]) and ascending-lat fixes
* Variable resolver handles common naming variants inside NetCDF

---

## Requirements

* **R ≥ 4.1** recommended
* Packages: `xml2`, `curl`, `data.table`, `ncdf4`, `future`, `future.apply`, `digest`, `parallelly`
* System: libcurl with HTTPS/HTTP2 support

Install in R:

```r
install.packages(c("xml2","curl","data.table","ncdf4","future","future.apply","digest","parallelly"))
```

---

## Configuration

Open `era5_download.R` and adjust:

* `REGIONS`: named bounding boxes (lon/lat degrees)
* `WANT`: list of variables by canonical names (e.g., `"2m_temperature"`, `"surface_solar_radiation_downwards"`)
* Worker counts: `DOWNLOAD_WORKERS`, `SUBSET_WORKERS` (or leave as dynamic)
* File retention: set `delete_global = FALSE` in `subset_file_to_regions()` to keep monthly downloads

---

## CLI

```bash
Rscript era5_download.R <year> <month> --out=/path/to/run_root
# Windows example:
Rscript .\era5_download.R 2020 12 --out="D:\data\era5"
```

**Notes**

* `<month>` is 1–12. Prefer **absolute** paths for `--out`.
* WBGT stage is wired but **disabled** by default; enable and point to your WBGT script if needed.

---

## Troubleshooting

* **403 listing S3**: check stream names/prefix and connectivity; bucket is public.
* **Too many workers**: cap `DOWNLOAD_WORKERS` / `SUBSET_WORKERS` to avoid oversubscription.
* **Variable not found**: share the filename; the resolver can be extended with an extra alias.

