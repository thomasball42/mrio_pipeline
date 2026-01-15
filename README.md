# MRIO Agricultural Trade Analysis Pipeline - Python Version

This is a Python conversion of the R-based Multi-Regional Input-Output (MRIO) pipeline for analyzing agricultural trade and its impact on cropland use and species habitats.


>[!IMPORTANT]Code Origins
>This code is built upon code from the following publications:
>
>**Schwarzmueller, F. & Kastner, T (2022), Agricultural trade and its impact on cropland use and the global loss of species' habitats. Sustainability Science, doi: [10.1007/s11625-022-01138-7](https://link.springer.com/article/10.1007/s11625-022-01138-7)**
>
>**Ball, T.S., Dales, M., Eyres, A. et al. Food impacts on species extinction risks can vary by three orders of magnitude. Nat Food 6, 848–856 (2025). doi: [10.1038/s43016-025-01224-w](https://www.nature.com/articles/s43016-025-01224-w)**
>
>Please cite appropriately when using this code.

## Files Overview

### Main Execution
- [`main.py`](main.py) - Main execution script (equivalent to `_Execute.R`)

### Core Modules
- [`processing/unzip_data.py`](processing/unzip_data.py) - Utility for unzipping FAOSTAT data (equivalent to `Unzip data.R`)
- [`processing/calculate_trade_matrix.py`](processing/calculate_trade_matrix.py) - Calculates apparent consumption and trade links (equivalent to `Calculating_Trade_Matrix.R`)
- [`processing/animal_products_to_feed.py`](processing/animal_products_to_feed.py) - Converts animal products into embedded feed items (equivalent to `animal_products_to_feed.R`)
- [`provenance/_provenance.py`,](provenance/_provenance.py) [`provenance/_get_impacts_bd.py`](provenance/_get_impacts_bd.py) and [`provenance/_process_dat.py`](provenance/_process_dat.py) - Modified version of [LIFE impact code](https://github.com/thomasball42/food_LIFE)

### Configuration
- [`requirements.txt`](requirements.txt) - Python package dependencies
- [`README.md`](README.md) - This documentation file
- [`PipelineChangesLDN.md`](PipelineChangesLDN.md)- List of significant changes made from original code

---
## Installation

### Prerequisites
- Python 3.8 or higher
- pip (Python package manager)

### Setup
1. Clone or download this repository
2. Navigate to the python directory
3. Install required packages:
   ```bash
   pip install -r requirements.txt
   ```

---
## Usage

### Quick Start
1. If downloading from Github LFS takes too long, download the FAOSTAT data files and place in the input_data directory
    - [Production_Crops_Livestock_E_All_Data_(Normalized).zip](https://bulks-faostat.fao.org/production/Production_Crops_Livestock_E_All_Data_(Normalized).zip)
    - [FoodBalanceSheetsHistoric_E_All_Data_(Normalized).zip](https://bulks-faostat.fao.org/production/FoodBalanceSheetsHistoric_E_All_Data_(Normalized).zip)
    - [FoodBalanceSheets_E_All_Data_(Normalized).zip](https://bulks-faostat.fao.org/production/FoodBalanceSheets_E_All_Data_(Normalized).zip)
    - [Trade_DetailedTradeMatrix_E_All_Data_(Normalized).zip](https://bulks-faostat.fao.org/production/Trade_DetailedTradeMatrix_E_All_Data_(Normalized).zip)
    - [CommodityBalances_(non-food)_(-2013_old_methodology)_E_All_Data_(Normalized).zip](https://bulks-faostat.fao.org/production/CommodityBalances_(non-food)_(-2013_old_methodology)_E_All_Data_(Normalized).zip)
    - [SUA_Crops_Livestock_E_All_Data_(Normalized).zip](https://bulks-faostat.fao.org/production/SUA_Crops_Livestock_E_All_Data_(Normalized).zip)
    - [Inputs_LandUse_E_All_Data.zip](https://bulks-faostat.fao.org/production/Inputs_LandUse_E_All_Data.zip)

2. Configure settings in main.py

3. Run the main script:
   ```bash
   python main.py
   ```

### Configuration Options

You can modify the following options in [`main.py`](main.py):

#### Years to Process
```python
YEARS = list(range(1986, 2022))  # Limits: 1986-2022, default: 2013-2022
```

#### Conversion Method
Choose from the following nutritional conversion methods:

```python
conversion_opt = "dry_matter"
```
Available options:
- `"dry_matter"` (default)
- `"Energy"`
- `"Protein"`
- `"Fiber_TD"`
- `"Zinc"`
- `"Iron"`
- `"Calcium"`
- `"Folate_Tot"`
- `"Riboflavin"`
- `"Choline_Tot"`
- `"Potassium"`
- `"Vit_E"`
- `"Vit_B12"`
- `"Vit_K"`
- `"Vit_A"`


#### Import/Export Preference
Choose whether to prefer import or export data:
```python
prefer_import = "import"  # or "export"
```

### Pipeline Components
Control which parts of the pipeline to run:
```python
PIPELINE_COMPONENTS:list = [0]
```

Component options:
- `0` = Full pipeline (all components)
- `1` = Unzipping data only
- `2` = Trade matrix calculation
- `3` = Animal products to feed calculation
- `4` = Country-level impact calculations (as in [LIFE](https://github.com/thomasball42/food_LIFE))

### Countries
Which countries to analyse in detail:
```python
COUNTRIES = ["GBR"]
```

---
## Required Data Files

The pipeline expects the following data files in the input_data directory:

### FAOSTAT Data (provided, or download above)
- `CommodityBalances_(non-food)_(-2013_old_methodology)_E_All_Data_(Normalized).zip`
- `FoodBalanceSheets_E_All_Data_(Normalized).zip`
- `FoodBalanceSheetsHistoric_E_All_Data_(Normalized).zip`
- `Production_Crops_Livestock_E_All_Data_(Normalized).zip`
- `SUA_Crops_Livestock_E_All_Data_(Normalized).zip`
- `Trade_DetailedTradeMatrix_E_All_Data_(Normalized).zip`
- `Inputs_LandUse_E_All_Data.zip`
### Mapping and Conversion Files (provided in `input_data`)
- `CB_code_FAO_code_for_conversion_factors.csv`
- `CB_items_split.csv`
- `CB_to_primary_items_map.csv`
- `commodity_crosswalk.csv`
- `composition_old_vs_new.csv`
- `content_factors_per_100g.xlsx`
- `country_opp_cost_v6.csv`
- `nocsDataExport_20251021-164754.xlsx`
- `Planet-Based Diets - Data and Viewer.xlsx`
- `primary_item_map_feed.csv`
- `Reporting_Dates.xls`
- `schwarzmueller_wwf.csv`
- `SUA_Crops_Livestock_E_ItemCodes.csv` (also produced during unzip)
- `tb_pasture_factors_2.csv`
- `weighing_factors.csv`

---
## Output Files

For each processed year, the pipeline generates:

- `results/{year}/.mrio/TradeMatrix_{conversion}_{year}.csv` - Main trade links for apparent consumption
- `results/{year}/.mrio/TradeMatrixFeed_{conversion}_{year}.csv` - as above, broken down for feed
- `results/{year}/.mrio/Pasture_calc.csv` - Pasture efficiencies calculated for the relevant year
- and all additional files as in [LIFE](https://github.com/thomasball42/food_LIFE)
   - `kdf.csv` has been renamed `impacts_aggregate.csv`
   - `xdf.csv` have been renamed `impacts_full.csv`
   - `df_uk.csv` is now `df_{country_iso}.csv` rather than `uk` for all countries
   - `food_commodity_impacts.csv` has been added with calculated per kg impacts

---
>[!NOTE]Performance
>Processing time: ~20 minutes for all years (1986-2013) on a machine with 32GB RAM
>Recommended minimum 32GB RAM

---
>[!NOTE]Reliability
> In theory this code should run cleanly for 1986 - 2022, however commodity crosswalks are missing for the 2000 MAPSPAM commodities and FAO data mapping is unreliable prior to 2010. We therefore cannot guarantee the accuracy of results prior to 2010. 
