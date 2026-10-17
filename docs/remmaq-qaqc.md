# REMMAQ QA/QC framework

Summary of *Marco QA/QC para el Procesamiento y Validación de Datos de la REMMAQ*,
the validation protocol of the Red Metropolitana de Monitoreo Atmosférico de
Quito (Secretaría de Ambiente, Investigación, Análisis y Monitoreo), dated
**2025-11-18**. Five pages, in Spanish.

- Original: [references/remmaq-qaqc-framework-2025-11.pdf](references/remmaq-qaqc-framework-2025-11.pdf)
- Published at `https://datosambiente.quito.gob.ec/datos/Marco_QAQC_REMMAQ.pdf`,
  next to the history exports.

It describes how the `LIMPIO` (validated) sheets of the history exports were
produced. The live feed has not been through it. What the notebook found when it
re-ran these rules is in [ANALYSIS.md](ANALYSIS.md) §3 and
`notebooks/01_exploring.ipynb` §9.

The protocol cites WHO, US-EPA, WMO-GAW and EMEP/EEA guidance throughout. Those
references are listed at the end rather than repeated per rule.

---

## 1. Air quality (pollutants)

Seven steps, applied in order.

| # | Step | Rule |
|---|---|---|
| 1 | Normalise raw data | Convert fields to numeric, strip invalid characters, check timestamps are unique and every record is complete |
| 2 | Physical and plausibility limits | Range filters from analyser operating limits (40 CFR Part 53), atmospheric plausibility (WMO-GAW 227) and instrument-artefact criteria (EMEP/EEA). Out-of-range values are marked invalid |
| 3 | Robust outliers | `abs(x − median) > 7 × MAD`, computed **globally** |
| 4 | Distribution tails | Values below **p0.5** or above **p99.5**, to catch electrical noise, full-scale saturation or truncated readings |
| 5 | Spike check | `abs(x − lag) > 4 × MAD` **and** `abs(x − lead) > 4 × MAD`, i.e. a single reading far from both the previous and the next hour |
| 6 | Decision | A value is invalid **only when two or more criteria agree** (physical, statistical, temporal). Stated aim: few false positives, real variability kept |
| 7 | Output | The series is split into valid values, outliers and audit flags |

The document gives **no numeric limits per pollutant** for step 2. The analyser
ranges are still an open item ([ANALYSIS.md](ANALYSIS.md) §5, item 3).

According to the document, the validated output is suitable for long-term trends,
atmospheric modelling, epidemiology, local regulatory reporting ("NECAA" in the
original, presumably the Norma Ecuatoriana de Calidad del Aire) and international
comparison.

## 2. Meteorology

The statistical rules from section 1 are **not** used for weather variables,
because they would flag real behaviour such as sunrise warming, fog, or wind gusts.
Only two checks apply: physical limits tuned to Quito (2,800 m above sea level,
latitude 0°) and impossible jumps.

| Variable | Code | Valid range | Rejected jump | Why no MAD / percentiles |
|---|---|---|---|---|
| Relative humidity | HUM | 0–100 % | > 30 % in 1 h | Variability tied to fog, inversions, advection |
| Air temperature | TMP | −5 to 40 °C | > 10 °C in 1 h | Would flag real dawn/dusk changes |
| Wind speed | VEL | 0–40 m/s | > 20 m/s in 1 h | Non-Gaussian, naturally long tails |
| Wind direction | DIR | 0–360° | — (also rejects prolonged fixed direction) | Circular variable |
| Solar radiation | RS | 0–1400 W/m² | > 300 W/m² in one reading | Deterministic daily curve |
| Precipitation | LLU | 0–500 mm/h | > 100 mm/h | Almost all zeros, strongly skewed |
| UV index | IUV | 0–20 | > 5 units in 1 h | Deterministic, like RS |

## 3. Local conditions the protocol accounts for

- Typical pressure **715–740 hPa**.
- Exceptionally high radiation and UV at latitude 0°.
- Abrupt, natural wind-direction changes from urban canyons and topography.
- Strong microclimates with localised rain.
- Frequent morning thermal inversions.

---

## 4. What this means for the pipeline

From the notebook's re-run of steps 2–6 over `LIMPIO` (§9):

- The rules reproduce the Secretaría's decisions to within **0.05 % (O₃), 0.16 % (NO₂)
  and 0.44 % (PM2.5)**, so the implementation is faithful.
- The global `7 × MAD` threshold works out to about **60 µg/m³ for PM2.5** and
  **115 µg/m³ for O₃**. Quito reaches both on a bad day. Together with the tail rule,
  that is two criteria, so real multi-hour episodes get invalidated.

Silver therefore takes the protocol's rules but not its single invalid/valid
decision:

| Rule from the protocol | Silver treatment |
|---|---|
| Physical limits (step 2) | **Quarantine** |
| Spike check (step 5) | **Quarantine**: a real episode has neighbours, so this cannot hide one |
| Global MAD (step 3) and tails (step 4) | **Flag for review**, keep the value; MAD to be computed per station, parameter and month of year |
| Meteorology ranges and jumps (section 2) | Apply as written to the weather layers of the REMMAQ feed |

Every decision is recorded with the rule name, threshold and value in
`silver.measurements_quarantine`.

## 5. References cited by the document

US-EPA QA Handbook Vol. II (2013) and Vol. IV (2016); EPA QA/G-9 (1999); AQS Data
Coding Manual (2020); EPA Technical Note on Statistical Treatment of Air Quality
Data (2018); 40 CFR Parts 50, 53 and 58; WHO Global Air Quality Guidelines (2021);
WHO Air Quality Monitoring practical guide (2023); WMO-GAW Reports 143, 197, 227
and 229; EEA e-Reporting and Data Validation Guide (2019); EMEP/EEA Emission
Inventory Guidebook (2023); EEA PM monitoring guidance (2013); AirNow/AQS QA/QC
overview (2020); NIST/NOAA traceability protocols (2018); EC-JRC FAIRMODE
validation guidance (2018); NOAA UV Index standard; INAMHI climatology 1981–2010.
