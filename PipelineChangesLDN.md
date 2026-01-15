# Changes made to MRIO model or LIFE impact calculation pipeline
> [!CAUTION]
> Multiple different code conventions are used in the code due to multiple authors writing/structuring this code across 3+ years. Proceed at your own risk.

---

## MRIO model:

> [!NOTE]
> The base code used in this model was manually translated from the R source code from [Schwarzmuller & Kastner, (2022)](https://link.springer.com/article/10.1007/s11625-022-01138-7) - no AI was used. The outputs from Python were verified to match those from the R code, prior to the modifications implemented below.


### calculate_trade_matrix.py
- Changed input from the legacy separate FAO crop and livestock production files to the combined `Production_Crops_Livestock_E_All_Data_(Normalised).csv` file.  
- Added support for non-historic `FoodBalanceSheets` inputs.
- Fixed missing data for Indian cattle which is correctly reported on the [FAO Supply Utilisation Accounts](https://www.fao.org/faostat/en/#data/SCL) but is missing in the [production dataset](https://www.fao.org/faostat/en/#data/QCL) (for seemingly political reasons?)
- Added `numba jit` compilation of the matrix pseudo-inversion for runtime optimisation.
- Added error calculation to the MRIO model by assuming the error is equal to the difference between the MRIO approach and a naive approach (where a naive approach assumes consumption due to a country is simply the Imports + Production - Exports directly attributable to that country). 

### animal_products_to_feed.py
- Changed input from the legacy separate FAO crop and livestock production files to the combined `Production_Crops_Livestock_E_All_Data_(Normalised).csv` file.  
- Added support for non-historic `CommodityBalances` inputs.
- Added per-year pasture efficiency calculations using:

Country's total pasture area $= A$
  
For commodity $i$ let the production $= P_i$ and the weighing factor between feed and commodity $= \alpha_i$ 

Therefore the Area of pasture attributable to commodity $i$ is $A_i \times \frac{P_i \alpha_i}{\sum\limits_{r}{P_r \alpha_r}}$ where $r$ are all ruminant species.

Hence the Efficiency of pasture for commodity $i$ in $\text{m}^2 / \text{kg}$ is $\text{E}_i = \frac{\text{A}_i}{P_i} = \frac{A \times \alpha_i}{\sum\limits_{r}{P_r \alpha_r}}$  

---
## Impacts Pipeline

### _provenance_dat.py
- Utilised existing outputs from `animal_products_to_feed.py` to avoid double counting. The theoretical methodology is the same as in [Ball et al (2025)](https://www.nature.com/articles/s43016-025-01224-w) but the implementation avoids any iterative processes. The following equations highlight the structure of this implementation: 

The consumption of animal $i$ for a consumer country, $c$ produced in country $p$ is $C_{c, p, i}$.
The feed weighting for the commodity is $\alpha_i$
The animal feed for a producer, $p$ that originates in country $o$ is $\beta_{p, o}$. Therefore the share of a feed in $p$ of origin $o$ consumed by the animal in $p$ driven by consumption in $c$ is $\frac{C_{c,p,i}\alpha_i}{\sum\limits_{c, i}{C_{c,p,i} \alpha_i}} \times \beta_{p,o}$



---

## Other:
- Added crosswalks for a number of new commodities
- Added ISO codes for Palestine (by updating `nocsDataExport.xlsx`)
- Fixed offals of some species being included in traded mass
- Option added to select which sections of the code to run in main.py
- Parallel processing added to country impact calculations
- Added unzipping of files too large to provide uncompressed


